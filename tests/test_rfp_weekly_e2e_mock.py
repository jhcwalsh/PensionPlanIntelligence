"""End-to-end mock-mode rfp_weekly cycle: compose → email → awaiting_approval.

Mock mode short-circuits the Claude call in ``compose.compose_rfp_weekly``,
so this test exercises Publication / approval-token / PDF / mock-email
plumbing without needing live API keys. Bucket-mapping and period-filter
logic is covered by ``tests/unit/test_compose_rfp_weekly.py``.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

import pytest

from database import (
    ApprovalToken, Document, Plan, Publication, RFPRecord, get_session,
)
from insights import approval, rfp_weekly


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _seed_rfp(session, *, rfp_id, doc_id, plan_id, rfp_type, status,
              extracted_at, awarded=None, incumbent=None,
              title="Investment Consulting Services"):
    payload = {
        "rfp_id": rfp_id,
        "plan_id": plan_id,
        "rfp_type": rfp_type,
        "title": title,
        "status": status,
        "awarded_manager": awarded,
        "incumbent_manager": incumbent,
        "shortlisted_managers": [],
        "source_document": {"url": "x", "page_number": 1, "document_id": doc_id},
        "source_quote": "verbatim source quote here",
        "extraction_confidence": 0.9,
    }
    rec = RFPRecord(
        rfp_id=rfp_id,
        document_id=doc_id,
        plan_id=plan_id,
        record=json.dumps(payload),
        extraction_confidence=0.9,
        needs_review=False,
    )
    rec.extracted_at = extracted_at
    session.add(rec)
    session.commit()


@pytest.fixture()
def seeded_rfps():
    """Two consultant RFPs in-window (one Awarded, one Planned), one out-of-window."""
    s = get_session()
    try:
        s.add(Plan(id="testplan", name="Test Plan", abbreviation="TEST"))
        s.add(Document(id=1, plan_id="testplan",
                       url="https://example.com/d1.pdf", filename="d1.pdf"))
        s.add(Document(id=2, plan_id="testplan",
                       url="https://example.com/d2.pdf", filename="d2.pdf"))
        s.add(Document(id=3, plan_id="testplan",
                       url="https://example.com/d3.pdf", filename="d3.pdf"))
        s.commit()

        _seed_rfp(s, rfp_id="a" * 16, doc_id=1, plan_id="testplan",
                  rfp_type="Consultant", status="Awarded",
                  awarded="Meketa", incumbent="Wilshire",
                  extracted_at=datetime(2026, 4, 22, 12, 0))
        _seed_rfp(s, rfp_id="b" * 16, doc_id=2, plan_id="testplan",
                  rfp_type="Consultant", status="Planned",
                  extracted_at=datetime(2026, 4, 23, 12, 0))
        # Out of window — previous week
        _seed_rfp(s, rfp_id="c" * 16, doc_id=3, plan_id="testplan",
                  rfp_type="Consultant", status="Issued",
                  extracted_at=datetime(2026, 4, 12, 12, 0))
    finally:
        s.close()


# ---------------------------------------------------------------------------
# Cycle tests
# ---------------------------------------------------------------------------

def test_rfp_weekly_cycle_produces_awaiting_approval_publication(seeded_rfps):
    pub = rfp_weekly.run_rfp_weekly_cycle(period_start=date(2026, 4, 19))
    assert pub.cadence == "rfp_weekly"
    assert pub.period_start == date(2026, 4, 19)
    assert pub.period_end == date(2026, 4, 25)
    assert pub.status == "awaiting_approval"
    assert pub.draft_markdown
    assert pub.pdf_path
    assert Path(pub.pdf_path).exists()

    s = get_session()
    try:
        tokens = s.query(ApprovalToken).filter_by(publication_id=pub.id).all()
        assert {t.action for t in tokens} == {"approve", "reject"}
    finally:
        s.close()


def test_rfp_weekly_cycle_writes_mock_email(seeded_rfps):
    rfp_weekly.run_rfp_weekly_cycle(period_start=date(2026, 4, 19))

    emails = approval.list_mock_emails()
    assert len(emails) == 1
    metadata = json.loads(emails[0].read_text(encoding="utf-8"))
    assert "Action required" in metadata["subject"]
    assert "Consultant RFP Brief" in metadata["subject"]
    assert metadata["has_attachment"] is True
    assert metadata["pdf_filename"].startswith("weekly_consultant_rfps_")


def test_rfp_weekly_cycle_is_idempotent_for_same_period(seeded_rfps):
    a = rfp_weekly.run_rfp_weekly_cycle(period_start=date(2026, 4, 19))
    b = rfp_weekly.run_rfp_weekly_cycle(period_start=date(2026, 4, 19))
    assert a.id == b.id

    s = get_session()
    try:
        assert s.query(Publication).filter_by(cadence="rfp_weekly").count() == 1
    finally:
        s.close()


def test_rfp_weekly_cycle_with_empty_period_still_publishes():
    """No consultant RFPs in window — publication created with mock markdown.

    In mock mode the compose function returns canned markdown unconditionally,
    so the publication still reaches awaiting_approval. The empty-state
    real-mode branch is exercised by the unit-test query helper.
    """
    s = get_session()
    try:
        s.add(Plan(id="testplan", name="Test Plan", abbreviation="TEST"))
        s.commit()
    finally:
        s.close()

    pub = rfp_weekly.run_rfp_weekly_cycle(period_start=date(2026, 4, 19))
    assert pub.status == "awaiting_approval"
    assert pub.draft_markdown
    assert pub.pdf_path
