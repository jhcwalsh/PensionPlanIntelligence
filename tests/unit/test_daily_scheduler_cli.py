"""CLI surface: python -m insights.scheduler daily."""
from __future__ import annotations

from datetime import date

import pytest

from database import Plan, Publication, get_session
from insights import approval, scheduler


@pytest.fixture()
def plan_row():
    s = get_session()
    try:
        s.add(Plan(id="p1", name="Plan One", abbreviation="P1"))
        s.commit()
    finally:
        s.close()


def test_scheduler_daily_returns_zero_and_sends_email(plan_row, capsys):
    rc = scheduler.main(["daily"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "daily cycle complete" in out

    emails = approval.list_mock_emails()
    assert len(emails) == 1

    s = get_session()
    try:
        pubs = s.query(Publication).filter_by(cadence="daily").all()
        assert len(pubs) == 1
        assert pubs[0].status == "published"
    finally:
        s.close()


def test_scheduler_daily_accepts_force_flag(plan_row):
    rc1 = scheduler.main(["daily"])
    rc2 = scheduler.main(["daily", "--force"])
    assert rc1 == 0 and rc2 == 0
    assert len(approval.list_mock_emails()) == 2
