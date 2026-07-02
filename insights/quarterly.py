"""Quarterly cycle: synthesize the prior quarter's approved monthlies.

Runs on the 1st of Jan/Apr/Jul/Oct (GHA quarterly-insights workflow).
Same shape as annual but operates on the calendar quarter that just
ended, so it works from the very first quarter with approved monthlies
instead of waiting for a full prior year.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional

from sqlalchemy import asc

from database import Publication, get_session
from insights import compose, cycle_common, notify

logger = logging.getLogger(__name__)


def _gather_approved_monthlies(session, period_start: date,
                               period_end: date) -> list[Publication]:
    """Approved/published monthlies inside the quarter, oldest-first."""
    return (
        session.query(Publication)
        .filter(Publication.cadence == "monthly")
        .filter(Publication.status.in_(("approved", "published")))
        .filter(Publication.period_start >= period_start)
        .filter(Publication.period_end <= period_end)
        .order_by(asc(Publication.period_start))
        .all()
    )


def run_quarterly_cycle(period_start: Optional[date] = None,
                        *, force: bool = False) -> Publication:
    """Compose the quarter-in-review (default: the quarter that just ended)."""
    if period_start is None:
        period_start, period_end = compose.quarterly_period_for(date.today())
    else:
        # Caller may pass any day in the target quarter; align to its 1st.
        q_start_month = ((period_start.month - 1) // 3) * 3 + 1
        period_start = date(period_start.year, q_start_month, 1)
        if q_start_month == 10:
            next_q_start = date(period_start.year + 1, 1, 1)
        else:
            next_q_start = date(period_start.year, q_start_month + 3, 1)
        period_end = next_q_start - timedelta(days=1)

    label = compose.quarter_label(period_start)
    session = get_session()
    publication: Optional[Publication] = None

    try:
        monthlies = _gather_approved_monthlies(session, period_start, period_end)
        if not monthlies:
            raise RuntimeError(
                f"No approved monthlies for {label} — quarterly composition "
                "has nothing to synthesize."
            )

        publication = cycle_common.find_or_create_publication(
            session,
            cadence="quarterly",
            period_start=period_start,
            period_end=period_end,
            source_publication_ids=[m.id for m in monthlies],
        )

        # Auto-reclaim stale rows. ``expired`` always; ``awaiting_approval``
        # only with --force (see weekly.py for the same pattern).
        if publication.status == "expired" or (
            force and publication.status == "awaiting_approval"
        ):
            if publication.status == "awaiting_approval":
                cycle_common.transition_status(publication, "expired")
                session.flush()
                publication = cycle_common.find_or_create_publication(
                    session,
                    cadence="quarterly",
                    period_start=period_start,
                    period_end=period_end,
                    source_publication_ids=[m.id for m in monthlies],
                )
            cycle_common.reset_to_generating(session, publication)

        if publication.status in ("awaiting_approval", "approved", "published"):
            logger.info(
                "Quarterly publication %s already at '%s' — skipping compose.",
                publication.id, publication.status,
            )
            return cycle_common.detach_for_caller(session, publication)

        publication.source_publication_ids = [m.id for m in monthlies]

        draft = compose.compose_quarterly(
            [(m.period_start, m.draft_markdown or "") for m in monthlies],
            period_start, period_end,
        )

        cycle_common.finalize_for_approval(
            session, publication, draft,
            title_for_pdf=f"Insights: {label} in Review",
        )
        return cycle_common.detach_for_caller(session, publication)

    except Exception as exc:
        session.rollback()
        try:
            if publication is not None and publication.status == "generating":
                cycle_common.transition_status(publication, "failed")
                publication.error_message = f"{type(exc).__name__}: {exc}"
                session.commit()
        except Exception:  # noqa: BLE001
            session.rollback()

        notify.alert_failure(
            "quarterly", label, exc,
            context={"publication_id": publication.id if publication else None},
        )
        raise
    finally:
        session.close()
