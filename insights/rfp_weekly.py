"""Weekly Consultant RFP cycle: compose → email → wait for approval.

Pure compose-from-existing-DB cycle — the heavy fetch/extract work is
already done by ``insights.weekly`` (board materials) and
``scripts.run_rfp_extraction`` (structured RFP records) earlier in the
weekly bat run. This module just queries ``RFPRecord`` (via
``compose.compose_rfp_weekly``), composes a consultant-only brief
grouped by lifecycle stage, and pushes it through the same Publication
approval flow as CIO Insights weekly.

Compared to ``insights.weekly`` this cadence has *no* scrape/extract
phase and therefore no ``WeeklyRun`` bookkeeping — it is the compose
tail only. The Publication state machine, force-reclaim semantics, and
failure handling are otherwise identical, so most of the body delegates
to ``insights.cycle_common``.

Run:
    python -m insights.scheduler rfp_weekly
    python -m insights.scheduler rfp_weekly --period 2026-05-03
    python -m insights.scheduler rfp_weekly --period 2026-05-03 --force
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional

from database import Publication, get_session
from insights import compose, cycle_common, notify

logger = logging.getLogger(__name__)

# Statuses that mean the publication is past the compose stage; re-running
# the cycle for one of these is a no-op (the magic-link email already went
# out, or the brief was already approved/published).
_TERMINAL_STATUSES = ("awaiting_approval", "approved", "published")

_CADENCE = "rfp_weekly"


def _resolve_period(period_start: Optional[date]) -> tuple[date, date]:
    """Return the (start, end) week bounds for the cycle.

    With no explicit start, defaults to the most recent completed
    Sun→Sat week; otherwise the end is six days after the given start.
    """
    if period_start is None:
        return compose.weekly_period_for(date.today())
    return period_start, period_start + timedelta(days=6)


def _find_or_create(session, period_start: date, period_end: date) -> Publication:
    """``cycle_common.find_or_create_publication`` bound to this cadence."""
    return cycle_common.find_or_create_publication(
        session,
        cadence=_CADENCE,
        period_start=period_start,
        period_end=period_end,
    )


def run_rfp_weekly_cycle(period_start: Optional[date] = None,
                          *, force: bool = False) -> Publication:
    """Run the weekly consultant RFP cycle for ``period_start``.

    Steps:
        1. Resolve period to the most recent completed Sun→Sat if not given.
        2. find_or_create Publication with cadence="rfp_weekly".
        3. If ``force``, reclaim a stale awaiting_approval row for re-compose.
        4. Compose brief via ``compose.compose_rfp_weekly``.
        5. Render PDF, send approval email, transition to awaiting_approval.

    ``force=True`` expires any existing awaiting_approval row for the
    same period and re-composes from scratch.

    Returns the (detached) ``Publication`` so callers can read its status
    after the session closes. Failures alert via Slack
    (``insights.notify``); the underlying exception is re-raised so the
    scheduler exits nonzero.
    """
    period_start, period_end = _resolve_period(period_start)
    period_label = period_start.isoformat()
    session = get_session()
    publication: Optional[Publication] = None

    try:
        publication = _find_or_create(session, period_start, period_end)

        # --force reclaims a still-pending row: expire it, re-fetch the
        # (uniquely-constrained) row, and re-arm it for composition. Mirrors
        # the awaiting_approval branch of ``insights.weekly.run_weekly_cycle``.
        if force and publication.status == "awaiting_approval":
            cycle_common.transition_status(publication, "expired")
            session.flush()
            publication = _find_or_create(session, period_start, period_end)
            cycle_common.reset_to_generating(session, publication)

        if publication.status in _TERMINAL_STATUSES:
            logger.info(
                "RFP weekly publication %s already at status '%s' — skipping compose.",
                publication.id, publication.status,
            )
            return cycle_common.detach_for_caller(session, publication)

        draft = compose.compose_rfp_weekly(session, period_start, period_end)
        cycle_common.finalize_for_approval(
            session, publication, draft,
            title_for_pdf=(
                f"Weekly Consultant RFP Brief: "
                f"{period_start.isoformat()} – {period_end.isoformat()}"
            ),
        )

        return cycle_common.detach_for_caller(session, publication)

    except Exception as exc:
        session.rollback()
        # Best-effort: mark a half-composed row failed (with the error text)
        # so it isn't mistaken for a clean retry candidate. Swallow any
        # secondary failure here — the alert + re-raise below is what matters.
        try:
            if publication is not None and publication.status == "generating":
                cycle_common.transition_status(publication, "failed")
                publication.error_message = f"{type(exc).__name__}: {exc}"
                session.commit()
        except Exception:  # noqa: BLE001
            session.rollback()

        notify.alert_failure(
            _CADENCE, period_label, exc,
            context={"publication_id": publication.id if publication else None},
        )
        raise
    finally:
        session.close()
