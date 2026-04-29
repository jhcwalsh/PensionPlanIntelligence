"""Weekly cycle: scrape → extract → compose → email → wait for approval.

Runs every Sunday 02:00 ET (or on-demand via
``python -m insights.scheduler weekly --period <date>``).

The scrape/extract phase delegates to the existing ``fetcher`` and
``extractor`` modules, but tracks per-plan progress in
``WeeklyRunPlan`` so a partial run can resume cleanly.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Optional

from database import (
    Document, Plan, Publication, WeeklyRun, WeeklyRunPlan, get_session,
)
from insights import compose, config, cycle_common, notify

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Run lifecycle (scrape + extract phase)
# ---------------------------------------------------------------------------

def _find_or_create_run(session, period_start: date, period_end: date) -> WeeklyRun:
    """Idempotent ``WeeklyRun`` row creation, like Publication."""
    existing = (
        session.query(WeeklyRun)
        .filter_by(period_start=period_start)
        .one_or_none()
    )
    if existing is not None:
        return existing
    run = WeeklyRun(
        period_start=period_start,
        period_end=period_end,
        status="running",
    )
    session.add(run)
    session.flush()
    return run


def _seed_plan_rows(session, run: WeeklyRun, plan_ids: list[str]) -> None:
    """Insert one ``WeeklyRunPlan`` row per (run, plan) if not present.

    Reseeds are no-ops thanks to the ``(run_id, plan_id)`` unique
    constraint — supports adding plans mid-run safely.
    """
    existing = {
        rp.plan_id for rp in
        session.query(WeeklyRunPlan).filter_by(run_id=run.id).all()
    }
    for plan_id in plan_ids:
        if plan_id in existing:
            continue
        session.add(WeeklyRunPlan(run_id=run.id, plan_id=plan_id, status="pending"))
    run.plans_total = len(plan_ids)
    session.flush()


def _run_scrape_and_extract(session, run: WeeklyRun) -> None:
    """Drive the per-plan fetch/extract loop, recording progress per plan.

    Mock mode skips the actual scrape/extract — tests assume the DB
    has whatever documents they seeded. The plan rows still get
    walked so the resumability path is exercised.
    """
    pending = (
        session.query(WeeklyRunPlan)
        .filter(WeeklyRunPlan.run_id == run.id)
        .filter(WeeklyRunPlan.status.in_(["pending", "fetching", "extracting"]))
        .all()
    )

    if config.is_mock():
        # Mark every plan succeeded immediately so the cycle can move on.
        for rp in pending:
            rp.status = "succeeded"
            rp.completed_at = datetime.utcnow()
        run.plans_completed = run.plans_total or len(pending)
        session.commit()
        return

    # Live: import lazily so mock mode doesn't need the heavy dependencies.
    from fetcher import run_fetcher
    from extractor import run_extractor

    for rp in pending:
        rp.status = "fetching"
        rp.started_at = datetime.utcnow()
        session.commit()
        try:
            run_fetcher(plan_ids=[rp.plan_id], max_docs_per_plan=50)
            rp.status = "extracting"
            session.commit()
            run_extractor(retry_failed=False)
            rp.status = "succeeded"
        except Exception as exc:  # noqa: BLE001
            logger.exception("Plan %s failed in run %s", rp.plan_id, run.id)
            rp.status = "failed"
            rp.error_message = f"{type(exc).__name__}: {exc}"
        finally:
            rp.completed_at = datetime.utcnow()
            session.commit()

    run.plans_completed = (
        session.query(WeeklyRunPlan)
        .filter_by(run_id=run.id, status="succeeded")
        .count()
    )
    run.documents_fetched = (
        session.query(Document)
        .filter(Document.downloaded_at >= datetime.combine(run.period_start, datetime.min.time()))
        .count()
    )
    session.commit()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_weekly_cycle(period_start: Optional[date] = None,
                     *, force: bool = False,
                     skip_scrape: bool = False) -> Publication:
    """Run the full weekly cycle for ``period_start`` (or the most recent week).

    Steps:
        1. find_or_create WeeklyRun + per-plan rows
        2. drive fetcher/extractor per plan, recording progress
        3. find_or_create Publication
        4. compose weekly markdown via insights.compose.compose_weekly
        5. render PDF, send approval email, transition to awaiting_approval

    ``force=True`` expires any existing awaiting_approval row for the
    same period and re-composes from scratch.

    Failures are logged and reported to Slack via ``insights.notify``;
    the underlying exception is re-raised so the scheduler exits nonzero.
    """
    if period_start is None:
        period_start, period_end = compose.weekly_period_for(date.today())
    else:
        period_end = period_start + timedelta(days=6)

    period_label = period_start.isoformat()
    session = get_session()
    publication: Optional[Publication] = None

    try:
        run = _find_or_create_run(session, period_start, period_end)

        if not skip_scrape:
            plan_ids = [p.id for p in session.query(Plan).all()]
            _seed_plan_rows(session, run, plan_ids)
            _run_scrape_and_extract(session, run)

        publication = cycle_common.find_or_create_publication(
            session,
            cadence="weekly",
            period_start=period_start,
            period_end=period_end,
        )

        if force and publication.status == "awaiting_approval":
            cycle_common.transition_status(publication, "expired")
            session.flush()
            publication = cycle_common.find_or_create_publication(
                session,
                cadence="weekly",
                period_start=period_start,
                period_end=period_end,
            )
            # The unique constraint will return the just-expired row;
            # bump it back to generating so the cycle can re-fill it.
            publication.status = "generating"
            publication.draft_markdown = None
            publication.composed_at = None
            publication.expires_at = None
            session.flush()

        if publication.status in ("awaiting_approval", "approved", "published"):
            logger.info(
                "Publication %s already at status '%s' — skipping compose.",
                publication.id, publication.status
            )
            run.status = "succeeded"
            run.completed_at = datetime.utcnow()
            session.commit()
            return cycle_common.detach_for_caller(session, publication)

        # Compose draft and finalize.
        draft = compose.compose_weekly(session, period_start, period_end)
        cycle_common.finalize_for_approval(
            session, publication, draft,
            title_for_pdf=f"7-Day Highlights: {period_start.isoformat()} – {period_end.isoformat()}",
        )

        run.status = "succeeded"
        run.completed_at = datetime.utcnow()
        session.commit()
        return cycle_common.detach_for_caller(session, publication)

    except Exception as exc:
        session.rollback()
        # Mark publication failed if we got that far; mark run failed regardless.
        try:
            run = (
                session.query(WeeklyRun)
                .filter_by(period_start=period_start)
                .one_or_none()
            )
            if run is not None:
                run.status = "failed"
                run.error_message = f"{type(exc).__name__}: {exc}"
                run.completed_at = datetime.utcnow()
            if publication is not None and publication.status in ("generating",):
                cycle_common.transition_status(publication, "failed")
                publication.error_message = f"{type(exc).__name__}: {exc}"
            session.commit()
        except Exception:  # noqa: BLE001
            session.rollback()

        notify.alert_failure(
            "weekly", period_label, exc,
            context={"publication_id": publication.id if publication else None},
        )
        raise
    finally:
        session.close()
