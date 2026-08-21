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


def test_smart_truncate_bounds_a_large_document_with_real_keyword_hits():
    """The prompt stays bounded even though storage no longer is.

    The input must contain words INVESTMENT_SIGNAL actually matches, or
    hit_positions is empty, middle_chunks is empty, and smart_truncate
    degenerates to head+tail — passing without ever exercising the
    multi-chunk assembly this is meant to bound.

    The bound is not exact: chunks are joined with a 9-char "\\n\\n[...]\\n\\n"
    separator and two more wrap the middle, none counted against
    middle_budget (summarizer.py:143-144). Allow for that overhead rather
    than asserting a limit the function was never written to hold.
    """
    # "portfolio", "manager", "allocation" and "benchmark" are all in
    # INVESTMENT_SIGNAL, so this genuinely populates middle_chunks.
    unit = "the portfolio manager reviewed allocation against benchmark. "
    huge = unit * 40_000                       # ~2.3M chars, well past the new cap
    assert len(huge) > extractor.MAX_STORED_CHARS / 2

    out = summarizer.smart_truncate(huge)

    # Prove the middle path actually ran, rather than head+tail only.
    assert "[...]" in out
    assert len(out) > summarizer.HEAD_CHARS + summarizer.TAIL_CHARS, \
        "degenerated to head+tail — the keyword path did not run"

    # Bounded, allowing the uncounted separator overhead.
    # The real overage is 18 chars (observed output: 50,018). 100 leaves room
    # for a separator tweak without letting a few-hundred-char regression in
    # the truncation budget through unnoticed.
    slack = 100
    assert len(out) <= summarizer.SMART_TRUNCATE_TARGET + slack, len(out)
