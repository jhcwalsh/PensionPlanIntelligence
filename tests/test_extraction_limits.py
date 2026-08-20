"""What gets stored is not what gets sent to Claude.

The 150k extraction cap was capping full-text search at roughly the first 35
pages of the largest board packets — 444 documents, 10.5% of the corpus, sit
exactly at it. It was never the cost control: summarizer.smart_truncate caps
the prompt at 50k chars independently.
"""
from __future__ import annotations

import pathlib

import extractor
import summarizer


def test_storage_cap_is_far_above_the_prompt_cap():
    assert extractor.MAX_STORED_CHARS > summarizer.SMART_TRUNCATE_TARGET * 10


def test_prompt_cap_is_unchanged():
    """Changing this changes what every summarisation call costs."""
    assert summarizer.SMART_TRUNCATE_TARGET == 50_000


def test_the_old_single_cap_is_gone():
    src = pathlib.Path(extractor.__file__).read_text(encoding="utf-8")
    assert "MAX_TEXT_CHARS" not in src, (
        "MAX_TEXT_CHARS conflated storage with prompt cost; use "
        "MAX_STORED_CHARS for storage and leave the prompt to smart_truncate")


def test_every_extraction_path_uses_the_storage_cap():
    src = pathlib.Path(extractor.__file__).read_text(encoding="utf-8")
    assert src.count("[:MAX_STORED_CHARS]") == 4, (
        "all four extraction paths (pdfplumber, PyMuPDF, OCR, DOCX) must "
        "truncate to the storage cap")


def test_smart_truncate_still_bounds_a_large_document():
    """The prompt stays bounded even though storage no longer is."""
    huge = "investment " * 200_000          # ~2.2M chars
    assert len(summarizer.smart_truncate(huge)) <= summarizer.SMART_TRUNCATE_TARGET
