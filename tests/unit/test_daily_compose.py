"""Daily digest markdown composition (mock LLM mode)."""
from __future__ import annotations

from datetime import datetime

import pytest

from database import Document, Plan, get_session
from insights import daily


@pytest.fixture()
def two_plans_three_docs():
    s = get_session()
    try:
        s.add_all([
            Plan(id="calpers", name="CalPERS", abbreviation="CalPERS"),
            Plan(id="ttrs", name="Texas TRS", abbreviation="TTRS"),
        ])
        s.flush()
        s.add_all([
            Document(plan_id="calpers", url="u-cp-1", filename="IC Minutes.pdf",
                     doc_type="minutes",
                     downloaded_at=datetime(2026, 5, 16, 10, 0),
                     meeting_date=datetime(2026, 5, 12)),
            Document(plan_id="calpers", url="u-cp-2", filename="Board Agenda.pdf",
                     doc_type="agenda",
                     downloaded_at=datetime(2026, 5, 16, 10, 30),
                     meeting_date=datetime(2026, 5, 15)),
            Document(plan_id="ttrs", url="u-tt-1", filename="Board Pack.pdf",
                     doc_type="board_pack",
                     downloaded_at=datetime(2026, 5, 16, 11, 0),
                     meeting_date=datetime(2026, 5, 14)),
        ])
        s.commit()
        yield
    finally:
        s.close()


def test_compose_daily_quiet_day_returns_one_liner():
    md = daily.compose_daily(
        [], triggers=[], digest_date=datetime(2026, 5, 16),
    )
    assert "No new documents fetched" in md
    # Quiet day should NOT mention any plan section.
    assert "##" not in md


def test_compose_daily_normal_day_groups_by_plan(two_plans_three_docs):
    s = get_session()
    try:
        docs = (
            s.query(Document)
            .filter(Document.downloaded_at.isnot(None))
            .order_by(Document.plan_id, Document.meeting_date.desc().nullslast())
            .all()
        )
    finally:
        s.close()

    md = daily.compose_daily(docs, triggers=[], digest_date=datetime(2026, 5, 16))
    # Two plan headers.
    assert md.count("\n## ") == 2
    assert "## CalPERS" in md
    assert "## Texas TRS" in md
    # Each section has bullets with link syntax.
    assert "- [" in md and "](" in md


def test_compose_daily_triggers_header_when_present(two_plans_three_docs):
    s = get_session()
    try:
        docs = s.query(Document).filter(Document.downloaded_at.isnot(None)).all()
    finally:
        s.close()

    md_with = daily.compose_daily(
        docs, triggers=["volume:14", "keyword:RFP"],
        digest_date=datetime(2026, 5, 16),
    )
    assert "Triggers: volume:14, keyword:RFP" in md_with

    md_without = daily.compose_daily(
        docs, triggers=[], digest_date=datetime(2026, 5, 16),
    )
    assert "Triggers:" not in md_without


def test_compose_daily_doc_links_use_approval_base_url(two_plans_three_docs, monkeypatch):
    monkeypatch.setattr("insights.config.APPROVAL_BASE_URL", "https://test.local")
    s = get_session()
    try:
        docs = s.query(Document).filter(Document.downloaded_at.isnot(None)).all()
    finally:
        s.close()

    md = daily.compose_daily(docs, triggers=[], digest_date=datetime(2026, 5, 16))
    assert "https://test.local/?document=" in md


def test_compose_daily_does_not_call_llm_on_quiet_day(monkeypatch):
    sentinel = {"called": False}

    def boom(*a, **k):
        sentinel["called"] = True
        raise RuntimeError("LLM should not be invoked on a quiet day")

    monkeypatch.setattr("insights.daily._synthesize_plan_paragraph", boom)
    md = daily.compose_daily([], triggers=[], digest_date=datetime(2026, 5, 16))
    assert sentinel["called"] is False
    assert "No new documents" in md
