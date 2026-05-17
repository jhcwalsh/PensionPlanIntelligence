"""--force resends even when a publication for today already exists."""
from __future__ import annotations

from datetime import date, datetime

import pytest

from database import DailyRun, Plan, Publication, get_session
from insights import approval, daily


@pytest.fixture()
def plan_row():
    s = get_session()
    try:
        s.add(Plan(id="p1", name="Plan One", abbreviation="P1"))
        s.commit()
    finally:
        s.close()


def test_force_resends_when_publication_already_published(plan_row):
    # First run on a quiet day — auto-send.
    daily.run_daily_cycle(now=datetime(2026, 5, 16, 13, 0))
    first = approval.list_mock_emails()
    assert len(first) == 1

    # Second run same day, no --force → skipped, no new email.
    daily.run_daily_cycle(now=datetime(2026, 5, 16, 13, 5))
    after_skip = approval.list_mock_emails()
    assert len(after_skip) == 1

    # Third run with --force → publication recycled + new email.
    daily.run_daily_cycle(now=datetime(2026, 5, 16, 13, 10), force=True)
    after_force = approval.list_mock_emails()
    assert len(after_force) == 2

    s = get_session()
    try:
        # The unique constraint on (cadence, period_start) means the same
        # row is reused for the forced re-send — the original publication
        # is bumped through expired → generating → published.
        all_daily = s.query(Publication).filter_by(cadence="daily").all()
        assert len(all_daily) == 1
        assert all_daily[0].status == "published"
        # The DailyRun audit log records BOTH sends.
        assert s.query(DailyRun).count() == 2
    finally:
        s.close()
