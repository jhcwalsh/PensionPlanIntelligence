"""
Text and metadata extraction from PDF and DOCX files.

Uses pdfplumber for structured extraction (tables, layout) and
pymupdf (fitz) as a fallback for scanned/complex PDFs.
"""

import os
import re
from datetime import datetime
from pathlib import Path

from rich.console import Console

from database import Document, get_session, get_unextracted_documents

console = Console(legacy_windows=False)

# Max characters to store (Claude's context window is large but we want to be economical)
MAX_TEXT_CHARS = 150_000


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
            pages_text.append(f"[Page {i + 1}]\n{text}")
        full_text = "\n\n".join(pages_text)
        return full_text[:MAX_TEXT_CHARS], page_count
    except Exception as e:
        console.print(f"  [red]pymupdf also failed: {e}[/red]")
        return "", 0


def extract_pdf_ocr(path: str) -> tuple[str, int]:
    """OCR fallback using pymupdf to render pages + pytesseract."""
    try:
        import pytesseract
        from PIL import Image
        import fitz
    except ImportError as e:
        console.print(f"  [yellow]OCR skipped: {e}[/yellow]")
        return "", 0
    # Set explicit path for Windows installs not on system PATH
    import sys
    if sys.platform == "win32":
        pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
    try:
        pytesseract.get_tesseract_version()
    except pytesseract.TesseractNotFoundError:
        console.print("  [yellow]OCR skipped: Tesseract not installed (install from https://github.com/UB-Mannheim/tesseract/wiki)[/yellow]")
        return "", 0
    try:
        doc = fitz.open(path)
        page_count = len(doc)
        pages_text = []
        mat = fitz.Matrix(2, 2)  # 2x zoom improves OCR accuracy
        for i, page in enumerate(doc):
            pix = page.get_pixmap(matrix=mat)
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            text = pytesseract.image_to_string(img)
            if text.strip():
                pages_text.append(f"[Page {i + 1}]\n{text}")
        full_text = "\n\n".join(pages_text)
        return full_text[:MAX_TEXT_CHARS], page_count
    except Exception as e:
        console.print(f"  [red]OCR failed: {e}[/red]")
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
