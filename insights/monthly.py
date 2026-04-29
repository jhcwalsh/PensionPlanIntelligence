"""Monthly cycle: synthesize the 4 most recent approved weeklies.

Runs on the 1st of each month at 02:00 ET. Picks the four most recent
approved (or published) weekly publications whose ``period_start`` is
within the prior month, hands their Markdown to ``compose_monthly``,
and runs the same approval flow as the weekly cycle.

Rejected / expired / awaiting_approval weeklies are excluded — only
content the founder has signed off on feeds into the monthly.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Optional

from sqlalchemy import desc

from database import Publication, get_session
from insights import compose, cycle_common, notify

logger = logging.getLogger(__name__)


def _gather_approved_weeklies(session, period_start: date,
                              period_end: date,
                              limit: int = 4) -> list[Publication]:
    """Pull up to ``limit`` approved weeklies whose period falls in [start, end].

    Sorted oldest-first so the synthesized monthly reads chronologically.
    """
    weeklies = (
        session.query(Publication)
        .filter(Publication.cadence == "weekly")
        .filter(Publication.status.in_(("approved", "published")))
        .filter(Publication.period_start >= period_start)
        .filter(Publication.period_end <= period_end)
        .order_by(desc(Publication.period_start))
        .limit(limit)
        .all()
    )
    return list(reversed(weeklies))


def run_monthly_cycle(period_start: Optional[date] = None,
                       *, force: bool = False) -> Publication:
    """Compose, render, and email the monthly CIO Insights for the prior month."""
    if period_start is None:
        period_start, period_end = compose.monthly_period_for(date.today())
    else:
        # Caller may pass any day in the target month; align to the 1st.
        period_start = period_start.replace(day=1)
        next_month = (period_start.replace(day=28) + timedelta(days=4)).replace(day=1)
        period_end = next_month - timedelta(days=1)

    period_label = period_start.strftime("%Y-%m")
    session = get_session()
    publication: Optional[Publication] = None

    try:
        weeklies = _gather_approved_weeklies(session, period_start, period_end)
        if not weeklies:
            raise RuntimeError(
                f"No approved weeklies found for {period_label} — "
                "monthly composition has nothing to synthesize."
            )

        publication = cycle_common.find_or_create_publication(
            session,
            cadence="monthly",
            period_start=period_start,
            period_end=period_end,
            source_publication_ids=[w.id for w in weeklies],
        )

        if force and publication.status == "awaiting_approval":
            cycle_common.transition_status(publication, "expired")
            session.flush()
            publication = cycle_common.find_or_create_publication(
                session,
                cadence="monthly",
                period_start=period_start,
                period_end=period_end,
                source_publication_ids=[w.id for w in weeklies],
            )
            publication.status = "generating"
            publication.draft_markdown = None
            publication.composed_at = None
            publication.expires_at = None
            session.flush()

        if publication.status in ("awaiting_approval", "approved", "published"):
            logger.info(
                "Monthly publication %s already at '%s' — skipping compose.",
                publication.id, publication.status,
            )
            return cycle_common.detach_for_caller(session, publication)

        # Lock in the source ids when (re-)composing so the audit trail
        # reflects exactly which weeklies fed this monthly.
        publication.source_publication_ids = [w.id for w in weeklies]

        draft = compose.compose_monthly(
            [w.draft_markdown or "" for w in weeklies],
            period_start, period_end,
        )

        cycle_common.finalize_for_approval(
            session, publication, draft,
            title_for_pdf=f"Monthly CIO Insights: {period_start.strftime('%B %Y')}",
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
            "monthly", period_label, exc,
            context={"publication_id": publication.id if publication else None},
        )
        raise
    finally:
        session.close()
