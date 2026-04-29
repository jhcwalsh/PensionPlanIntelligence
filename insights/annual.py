"""Annual cycle: synthesize the 12 approved monthlies of the prior year.

Runs every Jan 5 02:00 ET. Same shape as monthly but operates on the
calendar year that just ended.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from sqlalchemy import asc

from database import Publication, get_session
from insights import compose, cycle_common, notify

logger = logging.getLogger(__name__)


def _gather_approved_monthlies(session, year: int) -> list[Publication]:
    """All approved/published monthlies for ``year``, sorted oldest-first."""
    start = date(year, 1, 1)
    end = date(year, 12, 31)
    return (
        session.query(Publication)
        .filter(Publication.cadence == "monthly")
        .filter(Publication.status.in_(("approved", "published")))
        .filter(Publication.period_start >= start)
        .filter(Publication.period_end <= end)
        .order_by(asc(Publication.period_start))
        .all()
    )


def run_annual_cycle(year: Optional[int] = None,
                     *, force: bool = False) -> Publication:
    """Compose the year-in-review for ``year`` (default: prior calendar year)."""
    if year is None:
        period_start, period_end = compose.annual_period_for(date.today())
        year = period_start.year
    else:
        period_start = date(year, 1, 1)
        period_end = date(year, 12, 31)

    period_label = str(year)
    session = get_session()
    publication: Optional[Publication] = None

    try:
        monthlies = _gather_approved_monthlies(session, year)
        if not monthlies:
            raise RuntimeError(
                f"No approved monthlies for {year} — annual composition has "
                "nothing to synthesize."
            )

        publication = cycle_common.find_or_create_publication(
            session,
            cadence="annual",
            period_start=period_start,
            period_end=period_end,
            source_publication_ids=[m.id for m in monthlies],
        )

        if force and publication.status == "awaiting_approval":
            cycle_common.transition_status(publication, "expired")
            session.flush()
            publication = cycle_common.find_or_create_publication(
                session,
                cadence="annual",
                period_start=period_start,
                period_end=period_end,
                source_publication_ids=[m.id for m in monthlies],
            )
            publication.status = "generating"
            publication.draft_markdown = None
            publication.composed_at = None
            publication.expires_at = None
            session.flush()

        if publication.status in ("awaiting_approval", "approved", "published"):
            logger.info(
                "Annual publication %s already at '%s' — skipping compose.",
                publication.id, publication.status,
            )
            return cycle_common.detach_for_caller(session, publication)

        publication.source_publication_ids = [m.id for m in monthlies]

        draft = compose.compose_annual(
            [m.draft_markdown or "" for m in monthlies],
            period_start, period_end,
        )

        cycle_common.finalize_for_approval(
            session, publication, draft,
            title_for_pdf=f"CIO Insights: {year} Year in Review",
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
            "annual", period_label, exc,
            context={"publication_id": publication.id if publication else None},
        )
        raise
    finally:
        session.close()
