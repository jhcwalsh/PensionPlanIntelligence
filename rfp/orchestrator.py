"""
End-to-end RFP extraction orchestrator.

For each document that has been text-extracted but not yet processed under
the current prompt version, run:
    1. Stage-1 diagnostic (skipped extraction if NO_TASK_CONTENT)
    2. Page-level relevance filter + chunker
    3. LLM extraction (real or mock) per chunk
    4. Schema validation + deterministic rfp_id + upsert

A single PipelineRun row tracks the run. structlog JSON output gives one
line per document plus a final summary line, both correlated by run_id.
"""

from __future__ import annotations

import contextlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import database as db
from database import (
    Document, DocumentHealth, PipelineRun, Plan, RFPRecord,
    RFP_PROMPT_VERSION,
    get_documents_pending_rfp_extraction,
    upsert_document_health, upsert_rfp_record,
)
from lib.pipeline_diagnostic import (
    NO_TASK_CONTENT, STAGE_1_HEALTHY, STAGE_1_SUSPECTED,
    TASK_PROFILES, TaskProfile, diagnose_document,
)
from lib.schema_validator import validate_record
from rfp.alerting import maybe_alert_on_run
from rfp.ids import compute_rfp_id
from rfp.llm import extract_rfps
from rfp.logging_setup import get_logger
from rfp.relevance import Chunk, chunk_relevant_pages, split_pages


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _anchor_date(rec: dict) -> str | None:
    for k in ("release_date", "response_due_date", "award_date"):
        v = rec.get(k)
        if v:
            return v
    return None


def _diagnose_with_cached_text(doc: Document, profile: TaskProfile) -> tuple:
    """
    Diagnose a document by feeding the diagnostic the page texts we already
    have cached on Document.extracted_text (split via [Page N] markers).

    has_image is False for every page in cached-text mode — we don't have a
    way to detect scanned/image-only pages without re-opening the PDF. That
    means a fully-scanned PDF that pdfplumber returned no text for will be
    classified as 'blank' (rather than 'scanned') here. The bad-ratio
    threshold still flips it to STAGE_1_SUSPECTED, so the orchestrator
    behaviour is the same — only the rationale wording changes.

    If the local PDF is available, we open it for proper image detection.
    """
    if doc.local_path and Path(doc.local_path).exists():
        return diagnose_document(doc.local_path, profile)
    pages = split_pages(doc.extracted_text or "")
    texts = [p.text for p in pages]
    has_images = [False] * len(pages)
    return diagnose_document(
        pdf_path=doc.local_path or "",
        profile=profile,
        loader=lambda: (texts, has_images),
    )


def _process_document(
    session, doc: Document, plan: Plan, profile: TaskProfile, log
) -> tuple[int, int]:
    """
    Returns (records_extracted, dropped_invalid).
    """
    log = log.bind(document_id=doc.id, plan_id=doc.plan_id, url=doc.url)

    diag = _diagnose_with_cached_text(doc, profile)
    upsert_document_health(
        session,
        document_id=doc.id,
        verdict=diag.verdict,
        blank_pages=diag.blank_pages,
        scanned_pages=diag.scanned_pages,
        garbled_pages=diag.garbled_pages,
        task_relevant_pages=diag.task_relevant_pages,
        structure_score=diag.structure_score or 0.0,
        rationale_json=json.dumps(diag.rationale),
    )

    if diag.verdict == NO_TASK_CONTENT:
        log.info("doc_skipped_no_task_content",
                 task_relevant_pages=diag.task_relevant_pages)
        session.commit()
        return 0, 0

    if diag.verdict == STAGE_1_SUSPECTED:
        log.warning("doc_stage1_suspected",
                    blank=diag.blank_pages, scanned=diag.scanned_pages,
                    garbled=diag.garbled_pages, rationale=diag.rationale)

    pages = split_pages(doc.extracted_text or "")
    chunks: list[Chunk] = chunk_relevant_pages(pages, profile)
    if not chunks:
        log.info("doc_no_relevant_chunks",
                 verdict=diag.verdict, page_count=len(pages))
        session.commit()
        return 0, 0

    extracted = 0
    dropped = 0
    for chunk in chunks:
        result = extract_rfps(
            chunk=chunk,
            plan_id=doc.plan_id,
            document_id=doc.id,
            document_url=doc.url,
        )
        for rec in result.records:
            # Force the source_document.url and document_id to authoritative values,
            # even if the model emitted something different.
            rec["source_document"]["url"] = doc.url
            rec["source_document"]["document_id"] = doc.id
            # Compute deterministic rfp_id from canonical fields and overwrite
            # whatever the model produced.
            rec["rfp_id"] = compute_rfp_id(
                plan_id=rec["plan_id"],
                rfp_type=rec["rfp_type"],
                anchor_date=_anchor_date(rec),
                title=rec["title"],
            )

            errors = validate_record(rec)
            if errors:
                dropped += 1
                log.warning("record_dropped_schema_invalid",
                            errors=errors[:5], rfp_id=rec.get("rfp_id"))
                continue

            upsert_rfp_record(
                session,
                rfp_id=rec["rfp_id"],
                document_id=doc.id,
                plan_id=rec["plan_id"],
                record_json=json.dumps(rec, sort_keys=True),
                extraction_confidence=float(rec["extraction_confidence"]),
            )
            extracted += 1

    session.commit()
    log.info("doc_processed", extracted=extracted, dropped=dropped,
             chunks=len(chunks), verdict=diag.verdict)
    return extracted, dropped


def run_rfp_extraction(
    plan_ids: list[str] | None = None,
    *,
    profile_name: str = "rfp",
    run_id: str | None = None,
) -> str:
    """
    Process all eligible documents and return the run_id of the new
    PipelineRun row.
    """
    db.init_db()
    session = db.get_session()
    profile = TASK_PROFILES[profile_name]
    log = get_logger(component="rfp_orchestrator")

    run = PipelineRun(status="running")
    if run_id:
        run.run_id = run_id
    session.add(run)
    session.commit()
    log = log.bind(run_id=run.run_id)
    log.info("run_started", prompt_version=RFP_PROMPT_VERSION,
             plan_filter=plan_ids)

    docs = get_documents_pending_rfp_extraction(session, plan_ids=plan_ids)
    run.documents_discovered = len(docs)
    session.commit()

    processed = 0
    extracted_total = 0
    docs_with_zero = 0
    errors: list[str] = []

    for doc in docs:
        try:
            extracted, _ = _process_document(session, doc, doc.plan, profile, log)
            processed += 1
            extracted_total += extracted
            if extracted == 0:
                docs_with_zero += 1
        except Exception as e:
            session.rollback()
            errors.append(f"document_id={doc.id}: {e!r}")
            log.error("doc_failed", document_id=doc.id, error=repr(e))

    run.documents_processed = processed
    run.records_extracted = extracted_total
    run.errors = json.dumps(errors)
    run.completed_at = _utcnow()
    run.status = "failed" if errors and processed == 0 else "succeeded"
    session.commit()

    log.info("run_completed",
             status=run.status,
             documents_discovered=run.documents_discovered,
             documents_processed=processed,
             records_extracted=extracted_total,
             docs_with_zero_records=docs_with_zero,
             errors_count=len(errors))

    maybe_alert_on_run(
        run_id=run.run_id,
        status=run.status,
        documents_processed=processed,
        docs_with_zero_records=docs_with_zero,
        errors=errors,
    )

    final_run_id = run.run_id
    session.close()
    return final_run_id
