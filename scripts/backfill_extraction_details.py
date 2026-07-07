"""One-off backfill: populate extraction_details for existing shortfalls.

run_extractor now maintains the table going forward; this stamps reasons
onto rows that predate it, using the same gate logic offline (no API):

- failed, no file on disk                          -> file_missing
- failed, file, doc_type not OCR-worthy            -> ocr_gate_doc_type
- failed, file, OCR-worthy, over the doc page cap  -> ocr_gate_page_cap
- failed, file, OCR-worthy, within cap             -> ocr_empty
- done, OCR text, page_count > per-page OCR cap    -> ocr_partial

Idempotent: session.merge upserts by document_id, so re-running refreshes
rather than duplicates. Safe to run repeatedly.
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

from database import Document, ExtractionDetail, get_session, init_db
from extractor import MAX_VISION_OCR_DOC_PAGES, MAX_VISION_OCR_PAGES, OCR_DOC_TYPES


def _pdf_pages(path: str) -> int | None:
    try:
        import fitz
        with fitz.open(path) as d:
            return len(d)
    except Exception:
        return None


def main() -> int:
    init_db()
    session = get_session()
    now = datetime.utcnow()
    counts: dict[str, int] = {}
    try:
        for doc in session.query(Document).filter(
                Document.extraction_status == "failed"):
            pages = None
            if not doc.local_path or not Path(doc.local_path).exists():
                reason = "file_missing"
            elif doc.doc_type not in OCR_DOC_TYPES:
                reason = "ocr_gate_doc_type"
                pages = _pdf_pages(doc.local_path)
            else:
                pages = _pdf_pages(doc.local_path)
                if pages is not None and pages > MAX_VISION_OCR_DOC_PAGES:
                    reason = "ocr_gate_page_cap"
                else:
                    reason = "ocr_empty"
            session.merge(ExtractionDetail(
                document_id=doc.id, reason=reason, pages_total=pages,
                pages_ocred=None, detected_at=now))
            counts[reason] = counts.get(reason, 0) + 1

        # Historical partial scans: OCR'd text (leading page marker) on a
        # document longer than the per-page cap means later pages were
        # never transcribed.
        for doc in session.query(Document).filter(
                Document.extraction_status == "done",
                Document.page_count > MAX_VISION_OCR_PAGES):
            if doc.extracted_text and doc.extracted_text.startswith("[Page 1]"):
                session.merge(ExtractionDetail(
                    document_id=doc.id, reason="ocr_partial",
                    pages_total=doc.page_count,
                    pages_ocred=MAX_VISION_OCR_PAGES, detected_at=now))
                counts["ocr_partial"] = counts.get("ocr_partial", 0) + 1

        session.commit()
    finally:
        session.close()

    for reason, n in sorted(counts.items()):
        print(f"{reason:20s} {n}")
    print(f"total: {sum(counts.values())}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
