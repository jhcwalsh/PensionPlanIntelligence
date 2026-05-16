"""End-to-end mock-mode daily digest: quiet day, normal day, triggered day,
idempotency, and approval-gated branches."""
from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

import pytest

from database import (
    ApprovalToken, DailyRun, Document, Plan, Publication, get_session,
)
from insights import approval, daily


@pytest.fixture()
def seeded_plans():
    s = get_session()
    try:
        for pid, name in [("calpers", "CalPERS"), ("calstrs", "CalSTRS")]:
            s.add(Plan(id=pid, name=name, abbreviation=name))
        s.commit()
    finally:
        s.close()


def _add_doc(plan_id: str, filename: str, downloaded_at: datetime,
             meeting_date: datetime | None = None, doc_type: str = "agenda"):
    s = get_session()
    try:
        s.add(Document(
            plan_id=plan_id,
            url=f"u-{plan_id}-{filename}-{downloaded_at.isoformat()}",
            filename=filename,
            doc_type=doc_type,
            downloaded_at=downloaded_at,
            meeting_date=meeting_date,
        ))
        s.commit()
    finally:
        s.close()


def test_e2e_quiet_day_auto_sends_nothing_today(seeded_plans):
    pub = daily.run_cycle(now=datetime(2026, 5, 16, 13, 0))
    assert pub.cadence == "daily"
    assert pub.status == "published"
    assert "No new documents fetched" in pub.draft_markdown

    s = get_session()
    try:
        # No approval token on a quiet auto-send day.
        assert s.query(ApprovalToken).count() == 0
        runs = s.query(DailyRun).all()
        assert len(runs) == 1
        assert runs[0].docs_count == 0
        assert runs[0].triggers == []
        assert runs[0].approval_gated is False
    finally:
        s.close()

    emails = approval.list_mock_emails()
    assert len(emails) == 1


def test_e2e_normal_day_auto_sends_with_per_plan_sections(seeded_plans):
    _add_doc("calpers", "IC Minutes.pdf", datetime(2026, 5, 16, 10, 0),
             meeting_date=datetime(2026, 5, 12), doc_type="minutes")
    _add_doc("calstrs", "Board Pack.pdf", datetime(2026, 5, 16, 11, 0),
             meeting_date=datetime(2026, 5, 14), doc_type="board_pack")

    pub = daily.run_cycle(now=datetime(2026, 5, 16, 13, 0))
    assert pub.status == "published"
    assert "## CalPERS" in pub.draft_markdown
    assert "## CalSTRS" in pub.draft_markdown

    s = get_session()
    try:
        assert s.query(ApprovalToken).count() == 0
        run = s.query(DailyRun).one()
        assert run.docs_count == 2
        assert run.approval_gated is False
    finally:
        s.close()


def test_e2e_triggered_day_goes_to_approval(seeded_plans):
    # 11 docs > threshold 10 → volume trigger.
    for i in range(11):
        _add_doc("calpers", f"Doc {i}.pdf",
                 datetime(2026, 5, 16, 10, i),
                 meeting_date=datetime(2026, 5, 12))

    pub = daily.run_cycle(now=datetime(2026, 5, 16, 13, 0))
    assert pub.status == "awaiting_approval"

    s = get_session()
    try:
        tokens = s.query(ApprovalToken).filter_by(publication_id=pub.id).all()
        assert {t.action for t in tokens} == {"approve", "reject"}

        run = s.query(DailyRun).one()
        assert run.docs_count == 11
        assert any(t.startswith("volume:") for t in run.triggers)
        assert run.approval_gated is True
    finally:
        s.close()

    emails = approval.list_mock_emails()
    metadata = json.loads(emails[0].with_suffix(".json").read_text(encoding="utf-8"))
    # Approval-gated days use the "[Action required]" subject prefix.
    assert "Action required" in metadata["subject"]


def test_e2e_idempotent_same_day_no_double_send(seeded_plans):
    daily.run_cycle(now=datetime(2026, 5, 16, 13, 0))
    daily.run_cycle(now=datetime(2026, 5, 16, 13, 30))

    s = get_session()
    try:
        assert s.query(Publication).filter_by(cadence="daily").count() == 1
        # One DailyRun row from the first run; the second was a no-op.
        assert s.query(DailyRun).count() == 1
    finally:
        s.close()

    assert len(approval.list_mock_emails()) == 1


def test_e2e_lookback_advances_after_send(seeded_plans):
    # Day 1: one doc, gets digested.
    _add_doc("calpers", "Day1.pdf", datetime(2026, 5, 15, 10, 0))
    daily.run_cycle(now=datetime(2026, 5, 15, 13, 0))

    # Day 2: a new doc lands; the old doc must NOT reappear.
    _add_doc("calpers", "Day2.pdf", datetime(2026, 5, 16, 10, 0))
    pub2 = daily.run_cycle(now=datetime(2026, 5, 16, 13, 0))

    assert "Day2.pdf" in pub2.draft_markdown
    assert "Day1.pdf" not in pub2.draft_markdown


def test_e2e_force_resends_same_day(seeded_plans):
    daily.run_cycle(now=datetime(2026, 5, 16, 13, 0))
    daily.run_cycle(now=datetime(2026, 5, 16, 13, 5), force=True)
    assert len(approval.list_mock_emails()) == 2
