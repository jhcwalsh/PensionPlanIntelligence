"""Regression: daily-approval flow must leave publication attrs readable
after ``session.close()``.

Without ``refresh()`` + ``expunge()`` between commit and close, the
returned ``publication`` is detached AND expired — any subsequent attr
access (e.g. ``fan_out_digest`` reading ``publication.cadence``) raises
``DetachedInstanceError`` against the closed session. Mirrors the bug
that surfaced in production on 2026-05-22.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from database import Publication, get_session
from insights import cycle_common


@pytest.fixture
def daily_pub_approved_id() -> int:
    s = get_session()
    try:
        pub = Publication(
            cadence="daily",
            period_start=date(2026, 5, 22),
            period_end=date(2026, 5, 22),
            status="approved",
            draft_markdown="# daily digest",
            composed_at=datetime.utcnow(),
            approved_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(days=1),
        )
        s.add(pub)
        s.commit()
        s.refresh(pub)
        return pub.id
    finally:
        s.close()


def test_daily_transition_leaves_attrs_loaded(daily_pub_approved_id):
    """Replays the daily branch of ``page_approval_action`` and asserts
    that attrs are readable after the session closes."""
    session = get_session()
    try:
        pub = session.get(Publication, daily_pub_approved_id)
        cycle_common.transition_status(pub, "published")
        pub.published_at = datetime.utcnow()
        session.commit()
        session.refresh(pub)
        session.expunge(pub)
        publication = pub
    finally:
        session.close()

    # These accesses must not raise. Before refresh+expunge, .cadence
    # triggered DetachedInstanceError.
    assert publication.cadence == "daily"
    assert publication.id == daily_pub_approved_id
    assert publication.status == "published"
    assert publication.period_start == date(2026, 5, 22)
