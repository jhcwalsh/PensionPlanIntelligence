"""End-to-end mock-mode weekly cycle.

Since 2026-08-16 the weekly cadence composes *silently*: no approval email,
no informational email, and no ``notes/`` file. It still runs, because the
weekly Publication row is what ``monthly._gather_approved_weeklies`` reads —
monthly raises outright if no weeklies exist for its period.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from database import ApprovalToken, Plan, Publication, get_session
from insights import approval, weekly


@pytest.fixture()
def seeded_plans():
    s = get_session()
    try:
        for pid in ["calpers", "calstrs"]:
            s.add(Plan(id=pid, name=pid.upper(), abbreviation=pid.upper()))
        s.commit()
    finally:
        s.close()


def test_weekly_cycle_publishes_without_approval(seeded_plans):
    pub = weekly.run_weekly_cycle(
        period_start=date(2026, 4, 19), skip_scrape=False
    )
    assert pub.cadence == "weekly"
    assert pub.period_start == date(2026, 4, 19)
    assert pub.period_end == date(2026, 4, 25)
    assert pub.status == "published"
    assert pub.draft_markdown
    assert pub.pdf_path
    assert Path(pub.pdf_path).exists()

    # No approval tokens are minted any more.
    s = get_session()
    try:
        assert s.query(ApprovalToken).filter_by(publication_id=pub.id).count() == 0
    finally:
        s.close()


def test_weekly_cycle_is_idempotent_for_same_period(seeded_plans):
    a = weekly.run_weekly_cycle(period_start=date(2026, 4, 19))
    b = weekly.run_weekly_cycle(period_start=date(2026, 4, 19))
    assert a.id == b.id

    s = get_session()
    try:
        assert s.query(Publication).count() == 1
    finally:
        s.close()


def test_weekly_cycle_sends_no_email(seeded_plans):
    """The whole point of the weekly cadence now is to feed monthly quietly."""
    weekly.run_weekly_cycle(period_start=date(2026, 4, 19))
    assert approval.list_mock_emails() == []


def test_weekly_cycle_writes_no_note(seeded_plans, monkeypatch):
    """archive=False — the Weekly Insights tab is a frozen back catalogue."""
    import insights.publish as _publish

    wrote: list = []
    monkeypatch.setattr(_publish, "write_note",
                        lambda p: wrote.append(p) or Path("unused.md"))
    weekly.run_weekly_cycle(period_start=date(2026, 4, 19))
    assert wrote == []


def test_weekly_still_feeds_monthly(seeded_plans):
    """Regression guard for the reason weekly survives at all: monthly reads
    published weeklies and raises when it finds none."""
    from insights.monthly import _gather_approved_weeklies

    weekly.run_weekly_cycle(period_start=date(2026, 4, 19))
    s = get_session()
    try:
        found = _gather_approved_weeklies(s, date(2026, 4, 1), date(2026, 4, 30))
        assert [p.period_start for p in found] == [date(2026, 4, 19)]
    finally:
        s.close()
