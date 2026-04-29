"""New RFP-pipeline tables create cleanly and the helpers behave."""

from __future__ import annotations

import json

import database
from database import (
    DocumentHealth, PipelineRun, RFPRecord,
    upsert_document_health, upsert_rfp_record,
    get_documents_pending_rfp_extraction,
    Document, Plan,
)


def _seed_doc(session, doc_id: int = 1) -> Document:
    plan = session.get(Plan, "calpers")
    doc = Document(
        id=doc_id,
        plan_id="calpers",
        url=f"https://example.com/doc-{doc_id}.pdf",
        filename=f"doc-{doc_id}.pdf",
        doc_type="board_pack",
        local_path=f"/tmp/doc-{doc_id}.pdf",
        extraction_status="done",
        extracted_text="[Page 1]\nsome text",
        page_count=1,
    )
    session.add(doc)
    session.commit()
    return doc


def test_init_db_creates_new_tables(tmp_db):
    from sqlalchemy import inspect
    insp = inspect(database.engine)
    tables = set(insp.get_table_names())
    assert {"document_health", "rfp_records", "pipeline_runs"}.issubset(tables)


def test_upsert_rfp_record_inserts_then_updates(seeded_session):
    doc = _seed_doc(seeded_session)
    seeded_session.commit()

    upsert_rfp_record(
        seeded_session,
        rfp_id="abc1234567890def",
        document_id=doc.id,
        plan_id="calpers",
        record_json=json.dumps({"foo": "bar"}),
        extraction_confidence=0.85,
    )
    seeded_session.commit()

    rec = seeded_session.get(RFPRecord, "abc1234567890def")
    assert rec.needs_review is False
    assert rec.prompt_version == database.RFP_PROMPT_VERSION

    upsert_rfp_record(
        seeded_session,
        rfp_id="abc1234567890def",
        document_id=doc.id,
        plan_id="calpers",
        record_json=json.dumps({"foo": "baz"}),
        extraction_confidence=0.50,
    )
    seeded_session.commit()

    rec = seeded_session.get(RFPRecord, "abc1234567890def")
    assert rec.needs_review is True
    assert json.loads(rec.record)["foo"] == "baz"


def test_upsert_document_health_inserts_then_updates(seeded_session):
    doc = _seed_doc(seeded_session)
    upsert_document_health(
        seeded_session, document_id=doc.id, verdict="STAGE_1_HEALTHY",
        blank_pages=0, scanned_pages=0, garbled_pages=0,
        task_relevant_pages=3, structure_score=0.9,
        rationale_json="[]",
    )
    seeded_session.commit()
    h = seeded_session.get(DocumentHealth, (doc.id, database.RFP_PROMPT_VERSION))
    assert h.stage1_verdict == "STAGE_1_HEALTHY"

    upsert_document_health(
        seeded_session, document_id=doc.id, verdict="STAGE_1_SUSPECTED",
        blank_pages=2, scanned_pages=1, garbled_pages=0,
        task_relevant_pages=1, structure_score=0.4,
        rationale_json="[\"low text density\"]",
    )
    seeded_session.commit()
    h = seeded_session.get(DocumentHealth, (doc.id, database.RFP_PROMPT_VERSION))
    assert h.stage1_verdict == "STAGE_1_SUSPECTED"
    assert h.blank_pages == 2


def test_upsert_document_health_separate_prompt_versions_coexist(seeded_session):
    doc = _seed_doc(seeded_session)
    for version, verdict in [("rfp_v1", "STAGE_1_HEALTHY"),
                             ("rfp_v2", "STAGE_1_SUSPECTED")]:
        upsert_document_health(
            seeded_session, document_id=doc.id, verdict=verdict,
            blank_pages=0, scanned_pages=0, garbled_pages=0,
            task_relevant_pages=1, structure_score=1.0,
            rationale_json="[]", prompt_version=version,
        )
    seeded_session.commit()
    rows = seeded_session.query(DocumentHealth).filter_by(document_id=doc.id).all()
    assert len(rows) == 2
    assert {r.prompt_version for r in rows} == {"rfp_v1", "rfp_v2"}


def test_get_documents_pending_rfp_extraction_excludes_processed(seeded_session):
    doc1 = _seed_doc(seeded_session, doc_id=1)
    doc2 = _seed_doc(seeded_session, doc_id=2)

    pending = get_documents_pending_rfp_extraction(seeded_session)
    assert {d.id for d in pending} == {1, 2}

    # A doc is "processed" once it has a DocumentHealth row at the current
    # prompt version — even if it produced zero RFP records (e.g. a
    # governance doc with no RFPs).
    upsert_document_health(
        seeded_session, document_id=doc1.id, verdict="STAGE_1_HEALTHY",
        blank_pages=0, scanned_pages=0, garbled_pages=0,
        task_relevant_pages=2, structure_score=1.0, rationale_json="[]",
    )
    seeded_session.commit()

    pending = get_documents_pending_rfp_extraction(seeded_session)
    assert {d.id for d in pending} == {2}


def test_pipeline_run_default_run_id(seeded_session):
    run = PipelineRun(status="running")
    seeded_session.add(run)
    seeded_session.commit()
    assert run.run_id and len(run.run_id) == 32
