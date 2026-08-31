"""An API failure is not a fact about the document.

On 2026-08-31 an exhausted Anthropic credit balance turned 30 `ocr_deferred`
documents into `ocr_empty`. Nothing was spent and nothing was learned, but
`ocr_empty` asserts the document yielded no text — so the work dropped out of
the priced backlog `scripts/pending_spend.py` reports and became invisible
rather than pending. Retryable-but-unattempted must stay distinguishable from
attempted-and-barren.
"""
import pytest

import extractor
from scripts.pending_spend import OCR_OWED_REASONS


@pytest.fixture
def no_text_layer(monkeypatch):
    monkeypatch.setattr(extractor, "extract_pdf_pdfplumber", lambda p: ("", 3))
    monkeypatch.setattr(extractor, "extract_pdf_pymupdf", lambda p: ("", 3))


def test_every_page_failing_at_the_api_reports_unavailable(monkeypatch, no_text_layer):
    monkeypatch.setattr(extractor, "extract_pdf_ocr",
                        lambda p: ("", 3, extractor.OcrInfo(reason="api_error")))
    _text, _pages, reason, _ocred = extractor.extract_pdf("x.pdf", allow_ocr=True)
    assert reason == "ocr_unavailable"


def test_a_genuinely_blank_document_still_reports_empty(monkeypatch, no_text_layer):
    """The distinction has to cut both ways, or it is just a rename."""
    monkeypatch.setattr(extractor, "extract_pdf_ocr",
                        lambda p: ("", 3, extractor.OcrInfo(pages_ocred=3)))
    _text, _pages, reason, _ocred = extractor.extract_pdf("x.pdf", allow_ocr=True)
    assert reason == "ocr_empty"


def test_the_page_cap_is_unaffected(monkeypatch, no_text_layer):
    monkeypatch.setattr(extractor, "extract_pdf_ocr",
                        lambda p: ("", 3, extractor.OcrInfo(reason="page_cap")))
    _text, _pages, reason, _ocred = extractor.extract_pdf("x.pdf", allow_ocr=True)
    assert reason == "ocr_gate_page_cap"


def test_unavailable_work_is_still_priced():
    """The point of the split: it must reach the backlog report.

    ocr_deferred alone would leave an API outage's documents unpriced, which
    is exactly how 30 of them went missing.
    """
    assert "ocr_unavailable" in OCR_OWED_REASONS
    assert "ocr_deferred" in OCR_OWED_REASONS


def test_a_partial_api_failure_keeps_the_text_it_got(monkeypatch, no_text_layer):
    """Only a total failure means nothing was learned. If some pages came
    back, the document has been read in part and that is ocr_partial."""
    monkeypatch.setattr(extractor, "extract_pdf_ocr",
                        lambda p: ("page one text", 3,
                                   extractor.OcrInfo(pages_ocred=1)))
    text, _pages, reason, ocred = extractor.extract_pdf("x.pdf", allow_ocr=True)
    assert text == "page one text"
    assert reason == "ocr_partial"
    assert ocred == 1
