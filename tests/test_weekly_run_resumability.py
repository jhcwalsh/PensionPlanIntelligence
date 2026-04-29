"""Weekly run resumability — partial-run + restart picks up where it left off."""

from __future__ import annotations

from datetime import date

from database import Plan, WeeklyRun, WeeklyRunPlan, get_session
from insights.weekly import (
    _find_or_create_run, _run_scrape_and_extract, _seed_plan_rows,
)


def _seed_plans(plan_ids: list[str]) -> None:
    s = get_session()
    try:
        for pid in plan_ids:
            s.add(Plan(id=pid, name=pid.upper()))
        s.commit()
    finally:
        s.close()


def test_seed_plans_is_idempotent():
    _seed_plans(["a", "b", "c"])
    s = get_session()
    try:
        run = _find_or_create_run(s, date(2026, 4, 19), date(2026, 4, 25))
        s.commit()
        _seed_plan_rows(s, run, ["a", "b", "c"])
        s.commit()
        # Second seed call should be a no-op, not a duplicate insert.
        _seed_plan_rows(s, run, ["a", "b", "c"])
        s.commit()
        rows = s.query(WeeklyRunPlan).filter_by(run_id=run.id).all()
        assert len(rows) == 3
    finally:
        s.close()


def test_partial_run_resumes_only_pending_plans():
    """If 2 of 3 plans succeeded last time, the next pass should only touch the third."""
    _seed_plans(["a", "b", "c"])

    s = get_session()
    try:
        run = _find_or_create_run(s, date(2026, 4, 19), date(2026, 4, 25))
        s.commit()
        _seed_plan_rows(s, run, ["a", "b", "c"])
        s.commit()

        # Mark a and b as succeeded already (simulating prior partial run).
        from datetime import datetime
        rows = s.query(WeeklyRunPlan).filter_by(run_id=run.id).all()
        for r in rows:
            if r.plan_id in {"a", "b"}:
                r.status = "succeeded"
                r.completed_at = datetime.utcnow()
        s.commit()

        # Now run the (mock-mode) scrape phase. It should mark the
        # remaining 'c' row succeeded but leave a/b alone.
        _run_scrape_and_extract(s, run)
        s.commit()

        rows = s.query(WeeklyRunPlan).filter_by(run_id=run.id).all()
        statuses = {r.plan_id: r.status for r in rows}
        assert statuses == {"a": "succeeded", "b": "succeeded", "c": "succeeded"}

        # plans_completed should have grown.
        run = s.get(WeeklyRun, run.id)
        assert run.plans_completed >= 1
    finally:
        s.close()


def test_find_or_create_run_idempotent():
    s = get_session()
    try:
        a = _find_or_create_run(s, date(2026, 4, 19), date(2026, 4, 25))
        s.commit()
        b = _find_or_create_run(s, date(2026, 4, 19), date(2026, 4, 25))
        assert a.id == b.id
        assert s.query(WeeklyRun).count() == 1
    finally:
        s.close()
