"""
End-to-end RFP orchestrator test in mock LLM mode.

Inserts three Document rows pointing to fixture text content (with
extracted_text already populated, so the existing extractor stage is
skipped), runs the orchestrator, and asserts the resulting DB state.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

import database as db
from database import (
    Document, DocumentHealth, PipelineRun, Plan, RFPRecord,
    RFP_PROMPT_VERSION,
)
from lib.schema_validator import validate_record
from rfp.orchestrator import run_rfp_extraction


FIXTURE_DOCS = Path(__file__).resolve().parents[2] / "fixtures" / "documents"
FIXTURE_RESPONSES = Path(__file__).resolve().parents[2] / "fixtures" / "llm_responses"


FIXTURES = [
    # (id, plan_id, filename, url, expected_record_count)
    (1, "calpers", "calpers_2024_board.txt",
     "https://www.calpers.ca.gov/board/2024-03/packet.pdf", 2),
    (2, "calstrs", "calstrs_2024_investment.txt",
     "https://www.calstrs.com/board/2024-04/packet.pdf", 1),
    (3, "calpers", "calpers_2024_governance.txt",
     "https://www.calpers.ca.gov/governance/2024-02/packet.pdf", 0),
]


@pytest.fixture
def seeded_with_documents(seeded_session, monkeypatch):
    monkeypatch.setenv("LLM_FIXTURE_DIR", str(FIXTURE_RESPONSES))
    for doc_id, plan_id, fname, url, _expected in FIXTURES:
        text = (FIXTURE_DOCS / fname).read_text()
        seeded_session.add(Document(
            id=doc_id,
            plan_id=plan_id,
            url=url,
            filename=fname,
            doc_type="board_pack",
            local_path="/nonexistent/path/" + fname,   # forces cached-text diagnosis
            extracted_text=text,
            extraction_status="done",
            page_count=text.count("[Page "),
            meeting_date=datetime(2024, 3, 15),
        ))
    seeded_session.commit()
    return seeded_session


def test_orchestrator_extracts_expected_records(seeded_with_documents):
    run_id = run_rfp_extraction()

    session = db.get_session()
    try:
        # Total RFP records
        all_records = session.query(RFPRecord).all()
        expected_total = sum(c for *_, c in FIXTURES)
        assert len(all_records) == expected_total

        # Per-document record counts
        for doc_id, plan_id, fname, url, expected_count in FIXTURES:
            recs = session.query(RFPRecord).filter_by(document_id=doc_id).all()
            assert len(recs) == expected_count, f"{fname} produced {len(recs)} records, expected {expected_count}"

        # Every record validates against the schema
        for r in all_records:
            payload = json.loads(r.record)
            errors = validate_record(payload)
            assert not errors, f"{r.rfp_id} schema errors: {errors}"

        # Document health rows for all three documents, all healthy
        healths = session.query(DocumentHealth).all()
        assert len(healths) == 3
        assert all(h.stage1_verdict == "STAGE_1_HEALTHY" for h in healths), \
            [(h.document_id, h.stage1_verdict, h.rationale) for h in healths]

        # Pipeline run row
        run = session.get(PipelineRun, run_id)
        assert run is not None
        assert run.status == "succeeded"
        assert run.documents_discovered == 3
        assert run.documents_processed == 3
        assert run.records_extracted == 3
    finally:
        session.close()


def test_orchestrator_is_idempotent(seeded_with_documents):
    """Running twice produces no duplicate records."""
    first_run = run_rfp_extraction()

    session = db.get_session()
    try:
        first_count = session.query(RFPRecord).count()
        first_ids = {r.rfp_id for r in session.query(RFPRecord).all()}
    finally:
        session.close()

    second_run = run_rfp_extraction()
    assert first_run != second_run

    session = db.get_session()
    try:
        assert session.query(RFPRecord).count() == first_count
        second_ids = {r.rfp_id for r in session.query(RFPRecord).all()}
        assert first_ids == second_ids

        # Second run sees the same docs but they're filtered out by
        # get_documents_pending_rfp_extraction, so 0 are processed.
        run = session.get(PipelineRun, second_run)
        assert run.documents_discovered == 0
        assert run.records_extracted == 0
    finally:
        session.close()


def test_governance_doc_produces_zero_records(seeded_with_documents):
    run_rfp_extraction()
    session = db.get_session()
    try:
        recs = session.query(RFPRecord).filter_by(document_id=3).all()
        assert recs == []
        # The doc mentions "RFP-related content" once (a disclaimer), so the
        # diagnostic correctly flags it as STAGE_1_HEALTHY — but the LLM
        # extraction returns no records because there are no actual RFPs.
        h = session.get(DocumentHealth, (3, RFP_PROMPT_VERSION))
        assert h is not None
        assert h.stage1_verdict == "STAGE_1_HEALTHY"
    finally:
        session.close()


def test_records_have_provenance(seeded_with_documents):
    run_rfp_extraction()
    session = db.get_session()
    try:
        for r in session.query(RFPRecord).all():
            payload = json.loads(r.record)
            assert payload["source_document"]["url"]
            assert payload["source_document"]["page_number"] >= 1
            assert payload["source_document"]["document_id"] == r.document_id
            assert len(payload["source_quote"]) >= 10
    finally:
        session.close()


def test_records_use_canonical_url_even_if_model_lies(seeded_with_documents,
                                                       tmp_path, monkeypatch):
    """
    Sanity check: orchestrator forces source_document.url to the document's
    actual URL, not whatever the LLM emitted. This protects against prompt
    injection or hallucinated URLs.
    """
    run_rfp_extraction()
    session = db.get_session()
    try:
        for r in session.query(RFPRecord).all():
            doc = session.get(Document, r.document_id)
            payload = json.loads(r.record)
            assert payload["source_document"]["url"] == doc.url
    finally:
        session.close()
