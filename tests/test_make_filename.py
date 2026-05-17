"""Tests for fetcher.make_filename — covers the messy URL shapes that
were producing useless filenames in the daily-digest email
(DocumentDownload-ashx.ashx, open.pdf, etc.).
"""
from __future__ import annotations

import pytest

from fetcher import make_filename


# ---------------------------------------------------------------------------
# Happy path: real document filename
# ---------------------------------------------------------------------------

def test_real_pdf_url_returns_basename():
    assert make_filename(
        "https://example.com/board/2026-01_minutes.pdf", ""
    ) == "2026-01_minutes.pdf"


def test_real_docx_url_returns_basename():
    assert make_filename(
        "https://example.com/agendas/april.docx", ""
    ) == "april.docx"


def test_pdf_with_querystring_keeps_basename():
    assert make_filename(
        "https://example.com/files/report.pdf?download=1", ""
    ) == "report.pdf"


# ---------------------------------------------------------------------------
# ASP.NET / PHP handler URLs (TCRS, etc.)
# ---------------------------------------------------------------------------

def test_ashx_handler_falls_back_to_link_text():
    """TCRS pattern: /DocumentDownload.ashx?id=4567 with descriptive
    link text should produce a meaningful filename, not 'DocumentDownload-ashx.ashx'."""
    name = make_filename(
        "https://treasury.tn.gov/DocumentDownload.ashx?id=4567",
        "March 2026 Board Materials",
    )
    assert "ashx" not in name.lower()
    assert "documentdownload" not in name.lower()
    assert name.endswith(".pdf")
    assert "March-2026-Board-Materials" in name or "march" in name.lower()


def test_ashx_handler_no_link_text_uses_parent_segment():
    """No link text + handler basename → use parent path segment."""
    name = make_filename(
        "https://example.com/reports/2026-q1/DocumentDownload.ashx",
        "",
    )
    assert "ashx" not in name.lower()
    assert "2026-q1" in name
    assert name.endswith(".pdf")


def test_aspx_handler_treated_same_as_ashx():
    name = make_filename(
        "https://example.com/files/Default.aspx?id=7",
        "January Investment Committee",
    )
    assert "aspx" not in name.lower()
    assert "default" not in name.lower()
    assert name.endswith(".pdf")


# ---------------------------------------------------------------------------
# Generic last-segment slugs (NCRS /open, /download)
# ---------------------------------------------------------------------------

def test_open_slug_uses_parent_segment():
    """NCRS pattern: /.../15-2025annualcomprehensivefinancialreport/open
    should use the parent segment, not the literal 'open'."""
    name = make_filename(
        "https://www.myncretirement.gov/documents/files/governance/"
        "boarddocs/15-2025annualcomprehensivefinancialreport/open",
        "2025 ACFR",
    )
    assert name != "open.pdf"
    assert "15-2025annualcomprehensivefinancialreport" in name
    assert name.endswith(".pdf")


def test_download_slug_uses_parent_segment():
    name = make_filename(
        "https://example.com/reports/q1-board-pack/download",
        "",
    )
    assert name == "q1-board-pack.pdf"


def test_view_slug_uses_parent_segment():
    name = make_filename(
        "https://example.com/files/2026-budget/view",
        "",
    )
    assert name == "2026-budget.pdf"


# ---------------------------------------------------------------------------
# Falls all the way through to URL hash
# ---------------------------------------------------------------------------

def test_handler_with_no_link_text_no_parent_uses_hash():
    """Pathological case — no useful information anywhere."""
    name = make_filename("https://example.com/get.ashx", "")
    assert "ashx" not in name.lower()
    assert "get" not in name.lower()
    assert name.endswith(".pdf")
    # Hash-based slug is 12 hex chars
    assert len(name) == len("xxxxxxxxxxxx.pdf")


# ---------------------------------------------------------------------------
# Extension handling
# ---------------------------------------------------------------------------

def test_url_with_no_extension_assumes_pdf():
    name = make_filename("https://example.com/files/april-board", "")
    assert name == "april-board.pdf"


def test_link_text_extension_overrides_default():
    """When link text contains a literal ``.docx`` (not just the bare word),
    use it. This matches existing behaviour pre-refactor — the regex looks
    for a real extension token, not free-form mentions."""
    name = make_filename(
        "https://example.com/files/april-board",
        "April Board Pack april-board.docx",
    )
    assert name.endswith(".docx")


def test_handler_extension_never_used_as_final_extension():
    """An .ashx URL with no link-text extension hint must default to .pdf,
    never .ashx."""
    name = make_filename(
        "https://example.com/reports/q1/DocumentDownload.ashx",
        "",
    )
    assert name.endswith(".pdf")
    assert not name.endswith(".ashx")
