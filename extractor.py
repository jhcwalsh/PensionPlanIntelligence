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
from datetime import datetime
from pathlib import Path

from rich.console import Console

from database import Document, get_session, get_unextracted_documents

console = Console(legacy_windows=False)

# Max characters to store (Claude's context window is large but we want to be economical)
MAX_TEXT_CHARS = 150_000

# Cap pages sent to vision OCR to bound cost on accidental 500-page agendas.
# A typical board pack is 5–60 pages; 100 covers the long tail.
MAX_VISION_OCR_PAGES = 100

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


def extract_pdf_ocr(path: str) -> tuple[str, int]:
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
        return "", 0

    try:
        from summarizer import MODEL_SONNET, _get_client
    except ImportError as e:
        console.print(f"  [yellow]OCR skipped: anthropic SDK not available ({e})[/yellow]")
        return "", 0

    try:
        client = _get_client()
    except Exception as e:
        console.print(f"  [yellow]OCR skipped: no Anthropic credentials ({e})[/yellow]")
        return "", 0

    try:
        doc = fitz.open(path)
        page_count = len(doc)
        pages_text = []
        mat = fitz.Matrix(VISION_OCR_RENDER_SCALE, VISION_OCR_RENDER_SCALE)
        for i, page in enumerate(doc):
            if i >= MAX_VISION_OCR_PAGES:
                console.print(
                    f"  [yellow]Vision OCR cap reached at page {MAX_VISION_OCR_PAGES}; "
                    f"skipping remaining {page_count - i} pages[/yellow]"
                )
                break
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
        return full_text[:MAX_TEXT_CHARS], page_count
    except Exception as e:
        console.print(f"  [red]Vision OCR failed: {e}[/red]")
        return "", 0


def extract_pdf(path: str) -> tuple[str, int]:
    text, pages = extract_pdf_pdfplumber(path)
    if len(text.strip()) < 100:
        text, pages = extract_pdf_pymupdf(path)
    if len(text.strip()) < 100:
        console.print("  [dim]Trying OCR...[/dim]")
        text, pages = extract_pdf_ocr(path)
    return text, pages


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


def infer_meeting_date(text: str, existing_date: datetime | None) -> datetime | None:
    """Try to find a meeting date in the first 2000 chars of extracted text."""
    if existing_date:
        return existing_date
    m = DATE_RE.search(text[:2000])
    if m:
        raw = m.group(0).replace(",", "").strip()
        for fmt in ("%B %d %Y", "%B %d, %Y"):
            try:
                return datetime.strptime(raw, fmt)
            except ValueError:
                continue
    return None


# ---------------------------------------------------------------------------
# Main extraction runner
# ---------------------------------------------------------------------------

def extract_document(doc: Document) -> tuple[str, int, str]:
    """
    Extract text from a document's local file.
    Returns (text, page_count, status).
    """
    if not doc.local_path or not Path(doc.local_path).exists():
        return "", 0, "failed"

    path = doc.local_path
    ext = Path(path).suffix.lower()

    console.print(f"  Extracting [cyan]{Path(path).name}[/cyan]")

    if ext == ".pdf":
        text, pages = extract_pdf(path)
    elif ext in (".docx", ".doc"):
        text, pages = extract_docx(path)
    else:
        return "", 0, "failed"

    if not text.strip():
        return "", 0, "failed"

    return text, pages, "done"


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
            text, pages, status = extract_document(doc)

            doc.extracted_text = text
            doc.page_count = pages
            doc.extraction_status = status

            # Try to infer meeting date from content if not already set
            if text:
                doc.meeting_date = infer_meeting_date(text, doc.meeting_date)

            session.commit()
            status_color = "green" if status == "done" else "red"
            console.print(f"    [{status_color}]{status}[/{status_color}] "
                          f"— {pages} pages, {len(text):,} chars")

        done = sum(1 for d in docs if d.extraction_status == "done")
        console.print(f"\n[bold green]{done}/{len(docs)} documents extracted successfully.[/bold green]")

    finally:
        session.close()


if __name__ == "__main__":
    run_extractor()
