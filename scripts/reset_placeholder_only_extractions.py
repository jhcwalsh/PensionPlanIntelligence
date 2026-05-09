"""One-shot: reset documents with placeholder-only extracted_text to 'pending'.

Pre-OCR-fallback runs of the extractor wrote ``[Page N]\\n`` page headers
even when the underlying PDF had no text layer. Those rows ended up
``extraction_status='done'`` with a 30–250 char body that's nothing but
the header sequence ``[Page 1]\\n\\n[Page 2]\\n\\n...``. They never get
re-extracted because ``retry_failed`` only revisits ``failed`` rows.

This script flips every such row back to ``pending`` so the next
``run_extractor()`` call re-runs them through the (now OCR-aware)
pipeline. Idempotent: re-running it after a successful re-extract is a
no-op because the ``extracted_text`` no longer matches the placeholder
pattern.
"""

from __future__ import annotations

import re
import sys

from database import Document, get_session

PLACEHOLDER_RE = re.compile(r"^(\s*\[Page \d+\]\s*)+\Z")


def main() -> int:
    session = get_session()
    try:
        done_rows = session.query(Document).filter(
            Document.extraction_status == "done"
        ).all()
        targets = [
            d for d in done_rows
            if PLACEHOLDER_RE.match(d.extracted_text or "")
        ]
        print(f"done rows scanned: {len(done_rows):,}")
        print(f"placeholder-only rows to reset: {len(targets):,}")
        if not targets:
            return 0

        for d in targets:
            d.extraction_status = "pending"
            d.extracted_text = None
            d.page_count = None
        session.commit()
        print(f"reset {len(targets):,} docs to extraction_status='pending'")
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
