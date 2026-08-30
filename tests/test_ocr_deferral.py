"""Deferring OCR must record the work, not silently drop it.

Vision OCR is the only part of text extraction that costs money. `--no-ocr`
lets a bulk run take everything a text layer gives up for free and leave the
scanned documents for a funded pass later -- but only if those documents stay
findable. An untracked skip is indistinguishable from a document that has
nothing to extract, and the backlog becomes unpriceable.
"""
import pytest

import extractor


@pytest.fixture
def pdf_with_no_text_layer(tmp_path, monkeypatch):
    """A PDF whose text layer yields nothing, so extraction reaches the gate."""
    monkeypatch.setattr(extractor, "extract_pdf_pdfplumber", lambda p: ("", 12))
    monkeypatch.setattr(extractor, "extract_pdf_pymupdf", lambda p: ("", 12))
    path = tmp_path / "scanned.pdf"
    path.write_bytes(b"%PDF-1.4 fake")
    return str(path)


def test_ocr_runs_by_default(pdf_with_no_text_layer, monkeypatch):
    called = []
    monkeypatch.setattr(extractor, "extract_pdf_ocr",
                        lambda p: called.append(p) or ("scanned text", 12,
                                                       extractor.OcrInfo(pages_ocred=12)))
    text, pages, reason, _ = extractor.extract_pdf(pdf_with_no_text_layer)
    assert called, "OCR should run when allowed"
    assert text == "scanned text"
    assert reason is None


def test_deferred_ocr_does_not_call_the_model(pdf_with_no_text_layer, monkeypatch):
    def explode(path):
        raise AssertionError("OCR must not run when it has been deferred")

    monkeypatch.setattr(extractor, "extract_pdf_ocr", explode)
    text, pages, reason, ocred = extractor.extract_pdf(
        pdf_with_no_text_layer, allow_ocr=False, gate_reason="ocr_deferred")

    assert reason == "ocr_deferred"
    assert not text
    assert ocred is None


def test_deferral_is_distinguishable_from_an_ineligible_doc_type(
        pdf_with_no_text_layer, monkeypatch):
    """The distinction the pricing script depends on.

    Both skip OCR, but only one is a funding decision someone would revisit.
    Collapsing them into one reason makes the backlog unmeasurable.
    """
    monkeypatch.setattr(extractor, "extract_pdf_ocr",
                        lambda p: pytest.fail("should not run"))

    _, _, deferred, _ = extractor.extract_pdf(
        pdf_with_no_text_layer, allow_ocr=False, gate_reason="ocr_deferred")
    _, _, ineligible, _ = extractor.extract_pdf(
        pdf_with_no_text_layer, allow_ocr=False)

    assert deferred == "ocr_deferred"
    assert ineligible == "ocr_gate_doc_type"
    assert deferred != ineligible


def test_extract_document_defers_only_ocr_worthy_types(tmp_path, monkeypatch):
    """A doc_type that was never OCR-worthy is not deferred work."""
    monkeypatch.setattr(extractor, "extract_pdf_pdfplumber", lambda p: ("", 5))
    monkeypatch.setattr(extractor, "extract_pdf_pymupdf", lambda p: ("", 5))
    monkeypatch.setattr(extractor, "OCR_ENABLED", False)
    path = tmp_path / "x.pdf"
    path.write_bytes(b"%PDF-1.4 fake")

    class FakeDoc:
        local_path = str(path)
        filename = "x.pdf"
        meeting_date = None
        downloaded_at = None

    agenda = FakeDoc()
    agenda.doc_type = "agenda"          # in OCR_DOC_TYPES
    assert extractor.extract_document(agenda).reason == "ocr_deferred"

    other = FakeDoc()
    other.doc_type = "board_pack"       # not in OCR_DOC_TYPES
    assert extractor.extract_document(other).reason == "ocr_gate_doc_type"
