"""Vision OCR is expensive — gate it by doc_type and document size.

Only cafr / agenda / minutes are OCR-worthy (board packs are the 200-page
scans that burn money for little value), and a document whose page count
exceeds MAX_VISION_OCR_DOC_PAGES is skipped outright rather than OCR'd
up to the per-page cap.
"""

import pytest

import extractor
from database import Document


@pytest.fixture
def spy_ocr(monkeypatch):
    calls = []

    def fake_ocr(path):
        calls.append(path)
        return "ocr text " * 20, 3, extractor.OcrInfo(pages_ocred=3)

    monkeypatch.setattr(extractor, "extract_pdf_ocr", fake_ocr)
    # Force the text-layer extractors to come up empty so extract_pdf
    # reaches the OCR fallback decision.
    monkeypatch.setattr(extractor, "extract_pdf_pdfplumber", lambda p: ("", 3))
    monkeypatch.setattr(extractor, "extract_pdf_pymupdf", lambda p: ("", 3))
    return calls


def _doc(tmp_path, doc_type):
    pdf = tmp_path / "scan.pdf"
    pdf.write_bytes(b"%PDF-1.4 dummy")
    return Document(plan_id="testplan", url="https://x/scan.pdf",
                    filename="scan.pdf", doc_type=doc_type,
                    local_path=str(pdf), extraction_status="pending")


def test_ocr_not_attempted_for_board_pack(tmp_path, spy_ocr):
    outcome = extractor.extract_document(_doc(tmp_path, "board_pack"))
    assert spy_ocr == []
    assert outcome.status == "failed"


def test_ocr_attempted_for_minutes(tmp_path, spy_ocr):
    outcome = extractor.extract_document(_doc(tmp_path, "minutes"))
    assert len(spy_ocr) == 1
    assert outcome.status == "done"


def test_ocr_skips_documents_over_page_cap(tmp_path, monkeypatch):
    fitz = pytest.importorskip("fitz")
    pdf = tmp_path / "big.pdf"
    doc = fitz.open()
    for _ in range(3):
        doc.new_page()
    doc.save(str(pdf))

    monkeypatch.setattr(extractor, "MAX_VISION_OCR_DOC_PAGES", 2)
    # Any attempt to build an API client means the gate failed.
    import summarizer
    monkeypatch.setattr(summarizer, "_get_client",
                        lambda: pytest.fail("OCR ran despite page cap"))

    text, pages, info = extractor.extract_pdf_ocr(str(pdf))
    assert text == ""
    assert info.reason == "page_cap"
