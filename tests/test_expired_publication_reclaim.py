"""Stale-publication auto-reclaim.

Today's bug: the Sunday cron picked period 2026-05-03 to 2026-05-09, found an
existing Publication for that period in status ``expired`` (composed manually
mid-week, never approved), and crashed in ``finalize_for_approval`` because
the cycle only knew how to reset ``awaiting_approval`` rows.

The fix: ``expired`` publications are auto-reclaimed by the cycle (no
``--force`` needed); ``awaiting_approval`` still requires ``--force`` so the
cron doesn't blast a pending magic-link email.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from database import Plan, Publication, get_session
from insights import weekly


@pytest.fixture()
def seeded_plans():
    s = get_session()
    try:
        for pid in ["calpers", "calstrs"]:
            s.add(Plan(id=pid, name=pid.upper(), abbreviation=pid.upper()))
        s.commit()
    finally:
        s.close()


def _seed_expired_publication(period_start: date, period_end: date) -> int:
    s = get_session()
    try:
        old = datetime.utcnow() - timedelta(days=8)
        pub = Publication(
            cadence="weekly",
            period_start=period_start,
            period_end=period_end,
            status="expired",
            draft_markdown="# stale draft\n",
            composed_at=old,
            expires_at=old + timedelta(days=7),
        )
        s.add(pub)
        s.commit()
        return pub.id
    finally:
        s.close()


def test_expired_publication_auto_reclaimed_without_force(seeded_plans):
    """The cron path (force=False) re-composes over an expired row."""
    period_start = date(2026, 4, 19)
    period_end = date(2026, 4, 25)
    pub_id = _seed_expired_publication(period_start, period_end)

    pub = weekly.run_weekly_cycle(
        period_start=period_start, skip_scrape=False, force=False,
    )

    # Same row, freshly recomposed.
    assert pub.id == pub_id
    assert pub.status == "awaiting_approval"
    assert pub.draft_markdown
    assert pub.draft_markdown != "# stale draft\n"
    assert pub.composed_at is not None
    assert pub.composed_at > datetime.utcnow() - timedelta(minutes=1)


def test_awaiting_approval_publication_NOT_reclaimed_without_force(seeded_plans):
    """Pending magic-link state is preserved unless --force is passed."""
    period_start = date(2026, 4, 19)
    period_end = date(2026, 4, 25)
    s = get_session()
    try:
        pub = Publication(
            cadence="weekly",
            period_start=period_start,
            period_end=period_end,
            status="awaiting_approval",
            draft_markdown="# pending draft\n",
            composed_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(days=6),
        )
        s.add(pub)
        s.commit()
        original_draft = pub.draft_markdown
        original_id = pub.id
    finally:
        s.close()

    pub = weekly.run_weekly_cycle(
        period_start=period_start, skip_scrape=False, force=False,
    )

    # Same row, untouched.
    assert pub.id == original_id
    assert pub.status == "awaiting_approval"
    assert pub.draft_markdown == original_draft
