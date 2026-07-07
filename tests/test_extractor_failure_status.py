"""Failed extractions must not write text — no gzipped-""-blob rows.

Regression tests for the bug where run_extractor stored an empty string on
failure. GzippedText compresses "" into a ~20-byte blob, so failed docs
looked like they had extracted text at the raw-SQL level, and a failed
*re*-extraction clobbered previously good text.
"""

import sqlite3

from database import Document, Plan, get_session
from extractor import run_extractor


def _seed_doc(session, **overrides):
    plan = session.get(Plan, "testplan")
    if plan is None:
        plan = Plan(id="testplan", name="Test Plan", abbreviation="TEST",
                    state="CA", aum_billions=1.0)
        session.add(plan)
    fields = dict(
        plan_id="testplan",
        url="https://example.com/doc.pdf",
        filename="doc.pdf",
        doc_type="minutes",
        local_path=None,          # missing file → extraction fails
        extraction_status="pending",
    )
    fields.update(overrides)
    doc = Document(**fields)
    session.add(doc)
    session.commit()
    return doc.id


def test_failed_extraction_leaves_extracted_text_null(tmp_db):
    session = get_session()
    doc_id = _seed_doc(session)
    session.close()

    run_extractor(doc_ids=[doc_id])

    session = get_session()
    doc = session.get(Document, doc_id)
    assert doc.extraction_status == "failed"
    assert doc.extracted_text is None
    session.close()

    # The raw column must be NULL — not a gzipped empty string.
    raw = sqlite3.connect(tmp_db).execute(
        "SELECT extracted_text FROM documents WHERE id=?", (doc_id,)
    ).fetchone()[0]
    assert raw is None


def test_failed_reextraction_preserves_existing_text(tmp_db):
    session = get_session()
    doc_id = _seed_doc(session, extracted_text="minutes of the board meeting",
                       extraction_status="done", page_count=3)
    session.close()

    run_extractor(doc_ids=[doc_id])

    session = get_session()
    doc = session.get(Document, doc_id)
    assert doc.extraction_status == "failed"
    assert doc.extracted_text == "minutes of the board meeting"
    assert doc.page_count == 3
    session.close()
