"""One-shot cleanup: NULL out documents.extracted_text rows that hold no text.

Before run_extractor stopped writing text on failure, failed extractions
stored "" — which the GzippedText wrapper persisted as a ~20-byte gzip blob,
making the rows look non-NULL at the raw-SQL level. This script finds rows
whose stored value decodes to empty/whitespace-only text and sets the column
to NULL. Idempotent — NULL rows are never candidates.
"""

import gzip
import os
import sqlite3
import sys

DB_PATH = os.environ.get(
    "DB_PATH",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                 "db", "pension.db"),
)

GZIP_MAGIC = b"\x1f\x8b"


def _decode(val) -> str:
    if isinstance(val, (bytes, bytearray)):
        raw = bytes(val)
        if raw.startswith(GZIP_MAGIC):
            return gzip.decompress(raw).decode("utf-8")
        return raw.decode("utf-8", "replace")
    return val or ""


def main() -> int:
    print(f"DB: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    try:
        empty_ids = [
            doc_id
            for doc_id, val in conn.execute(
                "SELECT id, extracted_text FROM documents "
                "WHERE extracted_text IS NOT NULL"
            )
            if not _decode(val).strip()
        ]
        print(f"empty-text rows: {len(empty_ids):,}")

        conn.executemany(
            "UPDATE documents SET extracted_text = NULL WHERE id = ?",
            [(i,) for i in empty_ids],
        )
        conn.commit()
        print("nulled out.")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
