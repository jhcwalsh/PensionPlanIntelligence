"""
Text and metadata extraction from PDF and DOCX files.

Three-tier PDF strategy:
  1. pdfplumber — structured extraction (tables, layout) of the embedded
     text layer.
  2. pymupdf (fitz) — secondary text-layer extraction; tolerates a
     wider range of broken PDFs.
  3. Claude Sonnet vision — page-by-page transcription for image-only
     PDFs (scanned minutes, image-export board packs). Better at tables
     and multi-column layouts than Tesseract; costs ~$0.02–0.05 per
     multi-page document. Triggered only when both text-layer paths
     return < 100 chars of real content.
"""

import base64
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from rich.console import Console

from database import (
    Document, ExtractionDetail, get_session, get_unextracted_documents,
)

console = Console(legacy_windows=False)

# Max characters to store (Claude's context window is large but we want to be economical)
MAX_TEXT_CHARS = 150_000

# Cap pages sent to vision OCR to bound cost on accidental 500-page agendas.
# A typical board pack is 5–60 pages; 100 covers the long tail.
MAX_VISION_OCR_PAGES = 100

# OCR-worthiness gate: vision OCR costs real money per page, so only doc
# types whose text density justifies it get the fallback — and a scan whose
# page count exceeds the doc cap is skipped outright (a 200-page image-only
# board pack isn't worth transcribing even partially).
OCR_DOC_TYPES = {"cafr", "agenda", "minutes"}
MAX_VISION_OCR_DOC_PAGES = 50

# 2x render is roughly 1200x1600 px for letter-size — plenty of resolution for
# Sonnet vision and only modestly more image tokens than 1x.
VISION_OCR_RENDER_SCALE = 2

VISION_OCR_SYSTEM_PROMPT = (
    "You are a precision text transcriber. Output the exact text visible "
    "on the page provided, verbatim. Rules:\n"
    "- Preserve all numbers, currency symbols, percent signs, and decimal "
    "places exactly as shown.\n"
    "- Render tables using markdown pipe format, one row per line: "
    "| col1 | col2 | col3 |.\n"
    "- Preserve line breaks between paragraphs and headings.\n"
    "- Do not summarize, paraphrase, interpret, or add commentary.\n"
    "- Do not output preambles like 'Here is the text'.\n"
    "- If the page is blank or contains no readable text, output nothing."
)


# ---------------------------------------------------------------------------
# PDF extraction
# ---------------------------------------------------------------------------

def extract_pdf_pdfplumber(path: str) -> tuple[str, int]:
    """Extract text from PDF using pdfplumber. Returns (text, page_count)."""
    try:
        import pdfplumber
        pages_text = []
        with pdfplumber.open(path) as pdf:
            page_count = len(pdf.pages)
            for i, page in enumerate(pdf.pages):
                text = page.extract_text() or ""
                # Also extract tables as plain text
                tables = page.extract_tables()
                for table in tables:
                    for row in table:
                        row_text = " | ".join(str(cell or "").strip() for cell in row)
                        if row_text.strip():
                            text += "\n" + row_text
                if text.strip():
                    pages_text.append(f"[Page {i + 1}]\n{text}")
        full_text = "\n\n".join(pages_text)
        return full_text[:MAX_TEXT_CHARS], page_count
    except Exception as e:
        console.print(f"  [yellow]pdfplumber failed: {e}, trying pymupdf...[/yellow]")
        return "", 0


def extract_pdf_pymupdf(path: str) -> tuple[str, int]:
    """Fallback PDF extraction using pymupdf (fitz)."""
    try:
        import fitz  # pymupdf
        doc = fitz.open(path)
        page_count = len(doc)
        pages_text = []
        for i, page in enumerate(doc):
            text = page.get_text("text")
            if text.strip():
                pages_text.append(f"[Page {i + 1}]\n{text}")
        full_text = "\n\n".join(pages_text)
        return full_text[:MAX_TEXT_CHARS], page_count
    except Exception as e:
        console.print(f"  [red]pymupdf also failed: {e}[/red]")
        return "", 0


@dataclass
class OcrInfo:
    """How far vision OCR actually got (for the extraction_details index)."""
    pages_ocred: int = 0
    reason: str | None = None  # 'page_cap' when the whole-doc gate fired


def extract_pdf_ocr(path: str) -> tuple[str, int, OcrInfo]:
    """OCR fallback using Claude Sonnet vision.

    Renders each page with pymupdf and asks Sonnet for verbatim
    transcription. Replaces the prior Tesseract path: better at tables,
    multi-column layouts, and scanned forms, at a cost of ~$0.02–0.05
    per multi-page document. Failure on a single page (network blip,
    transient API error) is logged and skipped — other pages still
    contribute. The function returns whatever was successfully
    transcribed; an empty result causes ``extract_document`` to mark
    the row ``failed`` as before.
    """
    try:
        import fitz  # pymupdf
    except ImportError as e:
        console.print(f"  [yellow]OCR skipped: pymupdf not installed ({e})[/yellow]")
        return "", 0, OcrInfo()

    try:
        doc = fitz.open(path)
    except Exception as e:
        console.print(f"  [red]Vision OCR failed: {e}[/red]")
        return "", 0, OcrInfo()

    if len(doc) > MAX_VISION_OCR_DOC_PAGES:
        console.print(
            f"  [yellow]OCR skipped: {len(doc)} pages exceeds the "
            f"{MAX_VISION_OCR_DOC_PAGES}-page document cap[/yellow]"
        )
        return "", len(doc), OcrInfo(reason="page_cap")

    try:
        from summarizer import MODEL_SONNET, _get_client
    except ImportError as e:
        console.print(f"  [yellow]OCR skipped: anthropic SDK not available ({e})[/yellow]")
        return "", 0, OcrInfo()

    try:
        client = _get_client()
    except Exception as e:
        console.print(f"  [yellow]OCR skipped: no Anthropic credentials ({e})[/yellow]")
        return "", 0, OcrInfo()

    try:
        page_count = len(doc)
        pages_text = []
        pages_attempted = 0
        mat = fitz.Matrix(VISION_OCR_RENDER_SCALE, VISION_OCR_RENDER_SCALE)
        for i, page in enumerate(doc):
            if i >= MAX_VISION_OCR_PAGES:
                console.print(
                    f"  [yellow]Vision OCR cap reached at page {MAX_VISION_OCR_PAGES}; "
                    f"skipping remaining {page_count - i} pages[/yellow]"
                )
                break
            pages_attempted = i + 1
            pix = page.get_pixmap(matrix=mat)
            png_b64 = base64.b64encode(pix.tobytes("png")).decode("ascii")
            try:
                msg = client.messages.create(
                    model=MODEL_SONNET,
                    max_tokens=4096,
                    system=VISION_OCR_SYSTEM_PROMPT,
                    messages=[{
                        "role": "user",
                        "content": [
                            {
                                "type": "image",
                                "source": {
                                    "type": "base64",
                                    "media_type": "image/png",
                                    "data": png_b64,
                                },
                            },
                            {"type": "text", "text": f"Transcribe page {i + 1}."},
                        ],
                    }],
                )
                page_text = msg.content[0].text if msg.content else ""
            except Exception as e:
                console.print(f"  [red]Vision OCR failed on page {i + 1}: {e}[/red]")
                continue
            if page_text.strip():
                pages_text.append(f"[Page {i + 1}]\n{page_text}")
        full_text = "\n\n".join(pages_text)
        return full_text[:MAX_TEXT_CHARS], page_count, OcrInfo(pages_ocred=pages_attempted)
    except Exception as e:
        console.print(f"  [red]Vision OCR failed: {e}[/red]")
        return "", 0, OcrInfo()


def extract_pdf(path: str, allow_ocr: bool = True) -> tuple[str, int, str | None, int | None]:
    """Extract PDF text. Returns (text, pages, reason, pages_ocred) where
    reason is an extraction_details reason for empty/partial results."""
    text, pages = extract_pdf_pdfplumber(path)
    if len(text.strip()) < 100:
        text, pages = extract_pdf_pymupdf(path)
    if len(text.strip()) >= 100:
        return text, pages, None, None
    if not allow_ocr:
        return text, pages, "ocr_gate_doc_type", None
    console.print("  [dim]Trying OCR...[/dim]")
    text, pages, info = extract_pdf_ocr(path)
    if not text.strip():
        reason = "ocr_gate_page_cap" if info.reason == "page_cap" else "ocr_empty"
        return text, pages, reason, info.pages_ocred
    if info.pages_ocred < pages:
        return text, pages, "ocr_partial", info.pages_ocred
    return text, pages, None, None


# ---------------------------------------------------------------------------
# DOCX extraction
# ---------------------------------------------------------------------------

def extract_docx(path: str) -> tuple[str, int]:
    """Extract text from a Word document."""
    try:
        from docx import Document as DocxDocument
        doc = DocxDocument(path)
        paragraphs = [p.text for p in doc.paragraphs if p.text.strip()]

        # Also extract table content
        for table in doc.tables:
            for row in table.rows:
                row_text = " | ".join(cell.text.strip() for cell in row.cells)
                if row_text.strip():
                    paragraphs.append(row_text)

        full_text = "\n".join(paragraphs)
        return full_text[:MAX_TEXT_CHARS], 1  # DOCX doesn't have "pages" in the same way
    except Exception as e:
        console.print(f"  [red]DOCX extraction failed: {e}[/red]")
        return "", 0


# ---------------------------------------------------------------------------
# Metadata helpers
# ---------------------------------------------------------------------------

MEETING_TYPE_PATTERNS = {
    "investment": [r"investment\s+committee", r"investment\s+board", r"portfolio"],
    "audit": [r"audit\s+committee", r"risk\s+committee"],
    "board": [r"board\s+of\s+(trustees|directors|retirement)", r"full\s+board"],
    "actuarial": [r"actuarial", r"funded\s+status"],
}


def infer_meeting_type(text: str, filename: str) -> str:
    combined = (text[:2000] + " " + filename).lower()
    for mtype, patterns in MEETING_TYPE_PATTERNS.items():
        for p in patterns:
            if re.search(p, combined, re.IGNORECASE):
                return mtype
    return "board"


DATE_RE = re.compile(
    r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+\d{1,2},?\s+\d{4}\b",
    re.IGNORECASE
)


_FILENAME_DATE_PATTERNS = [
    # 04292026 / 04-29-2026 / 04_29_2026 / 04.29.2026 — concatenated MMDDYYYY
    # with optional separators. Matches "agenda.board.04292026.pdf",
    # "Board_Pack_04-29-2026.pdf", etc.
    (r"(?:^|[^0-9])(\d{2})[\-_.]?(\d{2})[\-_.]?(\d{4})(?:[^0-9]|$)", "MDY"),
    # M.D.YY or M-D-YY (two-digit year). Matches "IC_Agenda_4.24.26.pdf".
    # Constrained to . / - separators so we don't snag random number runs.
    (r"(?:^|[^0-9])(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{2})(?:[^0-9]|$)", "MDY2"),
    # YYYYMMDD / YYYY-MM-DD / YYYY_MM_DD
    (r"(?:^|[^0-9])(\d{4})[\-_.]?(\d{2})[\-_.]?(\d{2})(?:[^0-9]|$)", "YMD"),
    # Month DD YYYY (with various separators)
    (r"(January|February|March|April|May|June|July|August|September|October|"
     r"November|December)[\s\-_]+(\d{1,2})[\s\-_,]+(\d{4})", "WORD_MDY"),
    # Month YYYY only (no day) — fall back to first of month.
    # Matches "December-2025-Board-Highlights.pdf".
    (r"(January|February|March|April|May|June|July|August|September|October|"
     r"November|December)[\s\-_]+(\d{4})", "WORD_MY"),
]


def parse_date_from_filename(filename: str | None) -> datetime | None:
    """Extract a plausible meeting date from a filename.

    Tries several common shapes (MMDDYYYY without separators, M.D.YY,
    YYYYMMDD, "Month DD YYYY", "Month YYYY") and returns the first
    valid result. Returns None if no plausible date is found.

    The fetcher's ``parse_date_from_text`` only handles separator-bearing
    formats, which misses board-agenda filenames like
    ``agenda.board.04292026.pdf``. This helper is the second-pass fallback.
    """
    if not filename:
        return None
    base = re.sub(r"\.[a-zA-Z0-9]+$", "", filename)
    for pattern, kind in _FILENAME_DATE_PATTERNS:
        m = re.search(pattern, base, re.IGNORECASE)
        if not m:
            continue
        try:
            if kind == "MDY":
                month, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
            elif kind == "MDY2":
                month, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
                year = year + 2000 if year < 100 else year
            elif kind == "YMD":
                year, month, day = int(m.group(1)), int(m.group(2)), int(m.group(3))
            elif kind == "WORD_MDY":
                return datetime.strptime(
                    f"{m.group(1)} {m.group(2)} {m.group(3)}", "%B %d %Y"
                )
            elif kind == "WORD_MY":
                return datetime.strptime(f"{m.group(1)} 1 {m.group(2)}", "%B %d %Y")
            else:
                continue
            if (1 <= month <= 12 and 1 <= day <= 31
                    and 2000 <= year <= 2035):
                return datetime(year, month, day)
        except ValueError:
            continue
    return None


def _date_is_plausible(d: datetime, downloaded_at: datetime | None) -> bool:
    """Reject dates that can't possibly be a real meeting given fetch time.

    A meeting date should be within ~60 days after download (forward-scheduled
    agendas) and within ~5 years before (the longest historical material we
    routinely fetch). Anything outside that window is almost certainly a
    parser misread.
    """
    if downloaded_at is None:
        return True
    if d > downloaded_at + timedelta(days=60):
        return False
    if d < downloaded_at - timedelta(days=5 * 365):
        return False
    return True


def infer_meeting_date(
    text: str,
    existing_date: datetime | None,
    filename: str | None = None,
    downloaded_at: datetime | None = None,
) -> datetime | None:
    """Best-effort meeting date for a Document.

    Priority order:
      1. ``existing_date`` if already set and plausible (fetcher wins)
      2. ``parse_date_from_filename`` — strong signal, low false-positive rate
      3. First Month-DD-YYYY in the first 2000 chars of ``text`` — last resort

    Sanity-checks every candidate against ``downloaded_at`` (when provided):
    a date >60 days after fetch or >5 years before is treated as a parser
    error and discarded. Returns None when no plausible date is found —
    "no date" is preferred over a wrong date.
    """
    if existing_date and _date_is_plausible(existing_date, downloaded_at):
        return existing_date

    fname_date = parse_date_from_filename(filename)
    if fname_date and _date_is_plausible(fname_date, downloaded_at):
        return fname_date

    m = DATE_RE.search(text[:2000])
    if m:
        raw = m.group(0).replace(",", "").strip()
        for fmt in ("%B %d %Y", "%B %d, %Y"):
            try:
                d = datetime.strptime(raw, fmt)
                if _date_is_plausible(d, downloaded_at):
                    return d
            except ValueError:
                continue
    return None


# ---------------------------------------------------------------------------
# Main extraction runner
# ---------------------------------------------------------------------------

@dataclass
class ExtractOutcome:
    """Result of one document extraction, including why it fell short.

    ``reason`` is non-None whenever the document is not fully extracted —
    see database.ExtractionDetail for the vocabulary. A ``done`` outcome
    can still carry reason='ocr_partial'.
    """
    text: str = ""
    pages: int = 0
    status: str = "failed"
    reason: str | None = None
    pages_ocred: int | None = None


def extract_document(doc: Document) -> ExtractOutcome:
    """Extract text from a document's local file."""
    if not doc.local_path or not Path(doc.local_path).exists():
        return ExtractOutcome(reason="file_missing")

    path = doc.local_path
    ext = Path(path).suffix.lower()

    console.print(f"  Extracting [cyan]{Path(path).name}[/cyan]")

    if ext == ".pdf":
        text, pages, reason, pages_ocred = extract_pdf(
            path, allow_ocr=doc.doc_type in OCR_DOC_TYPES)
    elif ext in (".docx", ".doc"):
        text, pages = extract_docx(path)
        reason, pages_ocred = ("extract_empty" if not text.strip() else None), None
    else:
        return ExtractOutcome(reason="unsupported_format")

    if not text.strip():
        return ExtractOutcome(pages=pages, reason=reason or "extract_empty",
                              pages_ocred=pages_ocred)

    return ExtractOutcome(text=text, pages=pages, status="done",
                          reason=reason, pages_ocred=pages_ocred)


def run_extractor(doc_ids: list[int] = None, retry_failed: bool = False):
    """
    Extract text for all pending documents (or specific doc_ids).
    Updates extraction_status, extracted_text, page_count in DB.
    Pass retry_failed=True to re-attempt previously failed documents.
    """
    session = get_session()
    try:
        if doc_ids:
            docs = session.query(Document).filter(Document.id.in_(doc_ids)).all()
        elif retry_failed:
            docs = session.query(Document).filter(
                Document.extraction_status == "failed"
            ).all()
            # Reset to pending so the run loop can update them
            for doc in docs:
                doc.extraction_status = "pending"
            session.commit()
        else:
            docs = get_unextracted_documents(session)

        if not docs:
            console.print("[yellow]No documents pending extraction.[/yellow]")
            return

        console.print(f"[bold]Extracting text from {len(docs)} documents...[/bold]")

        for doc in docs:
            outcome = extract_document(doc)

            # On failure, leave extracted_text alone: never store "" (the
            # GzippedText wrapper would persist it as a non-NULL gzip blob),
            # and never clobber text kept from an earlier successful pass.
            if outcome.status == "done":
                doc.extracted_text = outcome.text
                doc.page_count = outcome.pages
            doc.extraction_status = outcome.status

            # Keep the extraction_details index in sync: any shortfall
            # (failure or partial scan) is recorded so the doc can be
            # found and re-processed later; a clean pass clears it.
            if outcome.reason:
                session.merge(ExtractionDetail(
                    document_id=doc.id, reason=outcome.reason,
                    pages_total=outcome.pages or None,
                    pages_ocred=outcome.pages_ocred,
                    detected_at=datetime.utcnow()))
            else:
                session.query(ExtractionDetail).filter(
                    ExtractionDetail.document_id == doc.id).delete()

            # Try to infer meeting date from content if not already set
            if outcome.text:
                doc.meeting_date = infer_meeting_date(
                    outcome.text, doc.meeting_date,
                    filename=doc.filename,
                    downloaded_at=doc.downloaded_at,
                )

            session.commit()
            status_color = "green" if outcome.status == "done" else "red"
            note = f" [{outcome.reason}]" if outcome.reason else ""
            console.print(f"    [{status_color}]{outcome.status}[/{status_color}] "
                          f"— {outcome.pages} pages, {len(outcome.text):,} chars{note}")

        done = sum(1 for d in docs if d.extraction_status == "done")
        console.print(f"\n[bold green]{done}/{len(docs)} documents extracted successfully.[/bold green]")

    finally:
        session.close()


if __name__ == "__main__":
    run_extractor()
