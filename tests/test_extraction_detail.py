"""Every not-fully-extracted document gets an indexed, queryable reason.

``extraction_details`` holds one row per document that failed extraction or
was only partially scanned (OCR gates / page caps), so those docs can be
found and re-processed later. A clean successful extraction clears the row.
"""

import pytest

import extractor
from database import Document, ExtractionDetail, Plan, get_session


def _seed_doc(session, **overrides):
    if session.get(Plan, "testplan") is None:
        session.add(Plan(id="testplan", name="Test Plan", abbreviation="TEST",
                         state="CA", aum_billions=1.0))
    fields = dict(plan_id="testplan", url="https://x/d.pdf", filename="d.pdf",
                  doc_type="minutes", local_path=None,
                  extraction_status="pending")
    fields.update(overrides)
    doc = Document(**fields)
    session.add(doc)
    session.commit()
    return doc.id


def _detail(doc_id):
    session = get_session()
    row = session.get(ExtractionDetail, doc_id)
    session.close()
    return row


def test_missing_file_indexed_as_file_missing(tmp_db):
    session = get_session()
    doc_id = _seed_doc(session)
    session.close()

    extractor.run_extractor(doc_ids=[doc_id])

    row = _detail(doc_id)
    assert row is not None
    assert row.reason == "file_missing"


def test_ocr_gate_skip_indexed_with_page_count(tmp_db, tmp_path, monkeypatch):
    pdf = tmp_path / "pack.pdf"
    pdf.write_bytes(b"%PDF-1.4 dummy")
    monkeypatch.setattr(extractor, "extract_pdf_pdfplumber", lambda p: ("", 12))
    monkeypatch.setattr(extractor, "extract_pdf_pymupdf", lambda p: ("", 12))

    session = get_session()
    doc_id = _seed_doc(session, doc_type="board_pack", local_path=str(pdf))
    session.close()

    extractor.run_extractor(doc_ids=[doc_id])

    row = _detail(doc_id)
    assert row.reason == "ocr_gate_doc_type"
    assert row.pages_total == 12


def test_partial_ocr_indexed_but_doc_still_done(tmp_db, tmp_path, monkeypatch):
    pdf = tmp_path / "scan.pdf"
    pdf.write_bytes(b"%PDF-1.4 dummy")
    monkeypatch.setattr(extractor, "extract_pdf_pdfplumber", lambda p: ("", 120))
    monkeypatch.setattr(extractor, "extract_pdf_pymupdf", lambda p: ("", 120))
    monkeypatch.setattr(
        extractor, "extract_pdf_ocr",
        lambda p: ("ocr text " * 50, 120, extractor.OcrInfo(pages_ocred=100)))

    session = get_session()
    doc_id = _seed_doc(session, local_path=str(pdf))
    session.close()

    extractor.run_extractor(doc_ids=[doc_id])

    session = get_session()
    doc = session.get(Document, doc_id)
    assert doc.extraction_status == "done"
    session.close()

    row = _detail(doc_id)
    assert row.reason == "ocr_partial"
    assert row.pages_total == 120
    assert row.pages_ocred == 100


def test_successful_extraction_clears_stale_detail(tmp_db, tmp_path, monkeypatch):
    pdf = tmp_path / "ok.pdf"
    pdf.write_bytes(b"%PDF-1.4 dummy")
    monkeypatch.setattr(extractor, "extract_pdf_pdfplumber",
                        lambda p: ("plenty of extracted text " * 10, 4))

    session = get_session()
    doc_id = _seed_doc(session, local_path=str(pdf), extraction_status="failed")
    session.add(ExtractionDetail(document_id=doc_id, reason="ocr_empty"))
    session.commit()
    session.close()

    extractor.run_extractor(doc_ids=[doc_id])

    assert _detail(doc_id) is None
