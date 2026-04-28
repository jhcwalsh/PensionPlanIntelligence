"""
Read the cover/intro pages of a CAFR/ACFR PDF and extract the fiscal year.

Used by:
  - refresh_cafrs.py: confirm a downloaded PDF is the year we expected.
  - one-off backfills for documents where the URL doesn't carry a year.
"""

import re
from pathlib import Path

import fitz  # PyMuPDF


# Patterns that strongly indicate a fiscal year on a CAFR/ACFR cover.
# Order matters: more-specific patterns first so we don't match a stray year
# (e.g., a copyright date) before the FYE phrase.
_YEAR_PATTERNS = [
    # "For the fiscal year ended June 30, 2024" / "For Fiscal Year Ended December 31, 2024"
    re.compile(
        r"fiscal\s+year\s+end(?:ed|ing)\s+"
        r"(?:january|february|march|april|may|june|july|august|september|"
        r"october|november|december)\s+\d{1,2},?\s+(20\d{2})",
        re.IGNORECASE,
    ),
    # "Fiscal Year 2024" / "FY 2024" / "FY2024"
    re.compile(r"\bfiscal\s+year\s+(20\d{2})\b", re.IGNORECASE),
    re.compile(r"\bfy\s*(20\d{2})\b", re.IGNORECASE),
    # "Annual Comprehensive Financial Report 2024" / "ACFR 2024" / "2024 ACFR"
    re.compile(
        r"(?:annual\s+comprehensive\s+financial\s+report|"
        r"comprehensive\s+annual\s+financial\s+report|"
        r"\bacfr\b|\bcafr\b)[\s\S]{0,40}?(20\d{2})",
        re.IGNORECASE,
    ),
    re.compile(
        r"(20\d{2})[\s\S]{0,40}?"
        r"(?:annual\s+comprehensive\s+financial\s+report|"
        r"comprehensive\s+annual\s+financial\s+report|"
        r"\bacfr\b|\bcafr\b)",
        re.IGNORECASE,
    ),
    # Last-resort: any 4-digit year on the first page (used only if nothing else)
    # Handled separately below so it doesn't outrank the specific patterns.
]

_FALLBACK_YEAR = re.compile(r"\b(20\d{2})\b")


def fiscal_year_from_pdf(path: str | Path,
                         pages_to_scan: int = 3,
                         min_year: int = 2000,
                         max_year: int | None = None) -> int | None:
    """
    Open a PDF and try to identify its fiscal year from the first few pages.

    Returns an int year (e.g. 2024) on success, None if no plausible year found
    or the file can't be opened.
    """
    path = Path(path)
    if not path.exists():
        return None

    if max_year is None:
        from datetime import datetime
        max_year = datetime.utcnow().year + 1

    try:
        doc = fitz.open(path)
    except Exception:
        return None

    try:
        text_parts = []
        for i in range(min(pages_to_scan, doc.page_count)):
            try:
                text_parts.append(doc.load_page(i).get_text())
            except Exception:
                continue
        text = "\n".join(text_parts)
    finally:
        doc.close()

    if not text:
        return None

    for pat in _YEAR_PATTERNS:
        for m in pat.finditer(text):
            try:
                yr = int(m.group(1))
            except (IndexError, ValueError):
                continue
            if min_year <= yr <= max_year:
                return yr

    # Fallback: pick the largest plausible year that appears on the cover.
    candidates = [int(y) for y in _FALLBACK_YEAR.findall(text[:5000])]
    candidates = [y for y in candidates if min_year <= y <= max_year]
    if candidates:
        return max(candidates)

    return None


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("usage: python cafr_year_check.py <pdf_path> [<pdf_path> ...]")
        raise SystemExit(2)
    for p in sys.argv[1:]:
        yr = fiscal_year_from_pdf(p)
        print(f"{p}: {yr}")
