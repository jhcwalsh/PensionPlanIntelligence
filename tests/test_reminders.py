"""Reminder + expiry sweep — 72h trigger, 7d expiry."""

from __future__ import annotations

from datetime import date, datetime, timedelta

from database import Publication, get_session
from insights import reminders


def _seed_pub(*, composed_hours_ago: float) -> int:
    s = get_session()
    try:
        composed = datetime.utcnow() - timedelta(hours=composed_hours_ago)
        pub = Publication(
            cadence="weekly",
            period_start=date(2026, 4, 19),
            period_end=date(2026, 4, 25),
            status="awaiting_approval",
            draft_markdown="# draft",
            composed_at=composed,
            expires_at=composed + timedelta(days=7),
        )
        s.add(pub)
        s.commit()
        return pub.id
    finally:
        s.close()


def test_reminder_at_72h_triggers():
    pub_id = _seed_pub(composed_hours_ago=73)
    stats = reminders.run_reminders()
    assert stats["reminders_sent"] == 1

    s = get_session()
    try:
        pub = s.get(Publication, pub_id)
        assert pub.reminder_sent_at is not None
        assert pub.status == "awaiting_approval"  # not expired yet
    finally:
        s.close()


def test_reminder_at_71h_does_not_trigger():
    _seed_pub(composed_hours_ago=71)
    stats = reminders.run_reminders()
    assert stats["reminders_sent"] == 0


def test_reminder_only_sent_once():
    pub_id = _seed_pub(composed_hours_ago=80)
    reminders.run_reminders()
    second = reminders.run_reminders()
    # Already-reminded pubs shouldn't be re-nagged on the next pass.
    assert second["reminders_sent"] == 0


def test_stale_draft_expires_after_7_days():
    pub_id = _seed_pub(composed_hours_ago=24 * 8)  # 8 days old
    stats = reminders.run_reminders()
    assert stats["expired"] == 1

    s = get_session()
    try:
        pub = s.get(Publication, pub_id)
        assert pub.status == "expired"
    finally:
        s.close()
