"""Vision OCR is verbatim transcription; it runs on Haiku, not Sonnet.

At ~1,650 image tokens in and ~550 out per page, Sonnet costs about
$0.013 a page and Haiku about $0.004. The task is copying text off a
rendered page, which is the shape of work the cheaper tier is for.
"""

import types

import pytest

import extractor
import summarizer


def test_ocr_pages_are_sent_to_haiku(tmp_path, monkeypatch):
    fitz = pytest.importorskip("fitz")
    pdf = tmp_path / "scan.pdf"
    doc = fitz.open()
    doc.new_page()
    doc.save(str(pdf))

    seen = []

    def create(**kwargs):
        seen.append(kwargs["model"])
        return types.SimpleNamespace(content=[types.SimpleNamespace(text="page text")])

    monkeypatch.setattr(summarizer, "_get_client",
                        lambda: types.SimpleNamespace(
                            messages=types.SimpleNamespace(create=create)))

    text, pages, info = extractor.extract_pdf_ocr(str(pdf))

    assert seen == [summarizer.MODEL_HAIKU]
    assert "page text" in text
