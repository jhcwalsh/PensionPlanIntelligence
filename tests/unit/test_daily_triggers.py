"""Trigger rules that route a day from auto-send to approval-gated."""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from database import Document, Plan, get_session
from insights import daily


@pytest.fixture()
def seed_plans():
    s = get_session()
    try:
        s.add_all([
            Plan(id="p1", name="Plan One", abbreviation="P1"),
            Plan(id="p2", name="Plan Two", abbreviation="P2"),
        ])
        s.commit()
    finally:
        s.close()


def _doc(plan_id: str, title: str, downloaded_at: datetime, *, doc_type: str = "agenda") -> Document:
    return Document(
        plan_id=plan_id,
        url=f"u-{plan_id}-{title[:10]}-{downloaded_at.isoformat()}",
        filename=title,
        doc_type=doc_type,
        downloaded_at=downloaded_at,
    )


def test_no_docs_yields_no_triggers(seed_plans):
    reasons = daily.apply_triggers([], now_utc=datetime(2026, 5, 16), session=get_session())
    assert reasons == []


def test_volume_under_threshold_no_trigger(seed_plans):
    docs = [_doc("p1", f"Doc {i}.pdf", datetime(2026, 5, 16)) for i in range(9)]
    reasons = daily.apply_triggers(docs, now_utc=datetime(2026, 5, 16), session=get_session())
    assert all(not r.startswith("volume:") for r in reasons)


def test_volume_over_threshold_fires(seed_plans):
    docs = [_doc("p1", f"Doc {i}.pdf", datetime(2026, 5, 16)) for i in range(11)]
    reasons = daily.apply_triggers(docs, now_utc=datetime(2026, 5, 16), session=get_session())
    assert "volume:11" in reasons


def test_keyword_match_case_insensitive(seed_plans):
    docs = [_doc("p1", "Search for Investment Consultant.pdf", datetime(2026, 5, 16))]
    reasons = daily.apply_triggers(docs, now_utc=datetime(2026, 5, 16), session=get_session())
    # Default keywords include "search" — case-insensitive match expected.
    assert any(r.startswith("keyword:") for r in reasons)


def test_keyword_match_skipped_when_no_keyword_in_title(seed_plans):
    docs = [_doc("p1", "Routine Board Minutes.pdf", datetime(2026, 5, 16))]
    reasons = daily.apply_triggers(docs, now_utc=datetime(2026, 5, 16), session=get_session())
    assert all(not r.startswith("keyword:") for r in reasons)


def test_reappearing_plan_triggers(seed_plans):
    s = get_session()
    try:
        # p1 has a doc downloaded 40 days ago — old.
        old = datetime(2026, 4, 6, 10, 0)
        s.add(_doc("p1", "Old Agenda.pdf", old))
        s.commit()
    finally:
        s.close()

    today_doc = _doc("p1", "Fresh Agenda.pdf", datetime(2026, 5, 16, 10, 0))
    reasons = daily.apply_triggers(
        [today_doc],
        now_utc=datetime(2026, 5, 16, 13, 0),
        session=get_session(),
    )
    assert any(r.startswith("reappear:p1") for r in reasons)


def test_reappearing_plan_no_trigger_when_recent(seed_plans):
    s = get_session()
    try:
        # p1 last had a doc 5 days ago — under the 30-day threshold.
        recent = datetime(2026, 5, 11, 10, 0)
        s.add(_doc("p1", "Recent Agenda.pdf", recent))
        s.commit()
    finally:
        s.close()

    today_doc = _doc("p1", "Fresh Agenda.pdf", datetime(2026, 5, 16, 10, 0))
    reasons = daily.apply_triggers(
        [today_doc],
        now_utc=datetime(2026, 5, 16, 13, 0),
        session=get_session(),
    )
    assert all(not r.startswith("reappear:") for r in reasons)


def test_brand_new_plan_does_not_trigger_reappear(seed_plans):
    """First-ever appearance of a plan should NOT fire reappear — otherwise the
    trigger would fire on every plan's first appearance, including the
    first-ever cycle run."""
    today_doc = _doc("p1", "First Ever.pdf", datetime(2026, 5, 16, 10, 0))
    reasons = daily.apply_triggers(
        [today_doc],
        now_utc=datetime(2026, 5, 16, 13, 0),
        session=get_session(),
    )
    assert all(not r.startswith("reappear:") for r in reasons)


def test_all_three_rules_can_co_fire(seed_plans):
    s = get_session()
    try:
        # p1 last seen 40 days ago — reappear trigger.
        s.add(_doc("p1", "Old.pdf", datetime(2026, 4, 6)))
        s.commit()
    finally:
        s.close()

    docs = [_doc("p1", f"Doc {i} RFP.pdf", datetime(2026, 5, 16)) for i in range(11)]
    reasons = daily.apply_triggers(
        docs, now_utc=datetime(2026, 5, 16, 13, 0), session=get_session()
    )
    assert any(r.startswith("volume:") for r in reasons)
    assert any(r.startswith("keyword:") for r in reasons)
    assert any(r.startswith("reappear:") for r in reasons)
