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
def no_text_layer(monkeypatch, tmp_path):
    """A file that really is a PDF, with no text layer in it.

    The bytes matter: extract_pdf now checks the magic number first, so a
    made-up path short-circuits to 'not_a_pdf' before any of this is reached.
    """
    monkeypatch.setattr(extractor, "extract_pdf_pdfplumber", lambda p: ("", 3))
    monkeypatch.setattr(extractor, "extract_pdf_pymupdf", lambda p: ("", 3))
    path = tmp_path / "scan.pdf"
    path.write_bytes(b"%PDF-1.4\n% a scan with no text layer\n")
    return str(path)


def test_every_page_failing_at_the_api_reports_unavailable(monkeypatch, no_text_layer):
    monkeypatch.setattr(extractor, "extract_pdf_ocr",
                        lambda p: ("", 3, extractor.OcrInfo(reason="api_error")))
    _text, _pages, reason, _ocred = extractor.extract_pdf(no_text_layer,
                                                          allow_ocr=True)
    assert reason == "ocr_unavailable"


def test_a_genuinely_blank_document_still_reports_empty(monkeypatch, no_text_layer):
    """The distinction has to cut both ways, or it is just a rename."""
    monkeypatch.setattr(extractor, "extract_pdf_ocr",
                        lambda p: ("", 3, extractor.OcrInfo(pages_ocred=3)))
    _text, _pages, reason, _ocred = extractor.extract_pdf(no_text_layer,
                                                          allow_ocr=True)
    assert reason == "ocr_empty"


def test_the_page_cap_is_unaffected(monkeypatch, no_text_layer):
    monkeypatch.setattr(extractor, "extract_pdf_ocr",
                        lambda p: ("", 3, extractor.OcrInfo(reason="page_cap")))
    _text, _pages, reason, _ocred = extractor.extract_pdf(no_text_layer,
                                                          allow_ocr=True)
    assert reason == "ocr_gate_page_cap"


def test_unavailable_work_is_still_priced():
    """The point of the split: it must reach the backlog report.

    ocr_deferred alone would leave an API outage's documents unpriced, which
    is exactly how 30 of them went missing.
    """
    assert "ocr_unavailable" in OCR_OWED_REASONS
    assert "ocr_deferred" in OCR_OWED_REASONS


def test_html_saved_as_pdf_is_named_for_what_it_is(tmp_path):
    """Thirteen documents in the OCR backlog were HTML error pages stored
    under a .pdf name. Recorded as ocr_empty, they asserted that OCR had read
    a scan and found nothing — hiding a fetcher problem inside an extraction
    one, and implying spend would fix it when no amount of OCR could."""
    p = tmp_path / "notice.pdf"
    p.write_bytes(b"<!DOCTYPE html><html><body>Access denied</body></html>")
    text, pages, reason, ocred = extractor.extract_pdf(str(p), allow_ocr=True)
    assert (text, pages, reason, ocred) == ("", 0, "not_a_pdf", None)


def test_a_real_pdf_is_not_rejected_by_the_magic_check(tmp_path, monkeypatch):
    p = tmp_path / "real.pdf"
    p.write_bytes(b"%PDF-1.4\n% a real one\n")
    monkeypatch.setattr(extractor, "extract_pdf_pdfplumber",
                        lambda x: ("plenty of genuine text " * 20, 2))
    _text, _pages, reason, _ocred = extractor.extract_pdf(str(p), allow_ocr=False)
    assert reason is None


def test_an_unreadable_path_is_not_a_pdf(tmp_path):
    """OSError must not escape into the extraction loop."""
    _text, _pages, reason, _ocred = extractor.extract_pdf(
        str(tmp_path / "nope.pdf"), allow_ocr=True)
    assert reason == "not_a_pdf"


def test_a_partial_api_failure_keeps_the_text_it_got(monkeypatch, no_text_layer):
    """Only a total failure means nothing was learned. If some pages came
    back, the document has been read in part and that is ocr_partial."""
    monkeypatch.setattr(extractor, "extract_pdf_ocr",
                        lambda p: ("page one text", 3,
                                   extractor.OcrInfo(pages_ocred=1)))
    text, _pages, reason, ocred = extractor.extract_pdf(no_text_layer,
                                                        allow_ocr=True)
    assert text == "page one text"
    assert reason == "ocr_partial"
    assert ocred == 1
