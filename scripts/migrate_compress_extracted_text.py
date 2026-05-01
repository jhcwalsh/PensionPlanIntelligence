"""One-shot migration: gzip-compress documents.extracted_text in place.

Idempotent — skips rows already starting with the gzip magic header.
Streams row-by-row to keep memory bounded on the ~95 MB column.
Runs VACUUM at the end so the file actually shrinks on disk.
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


def main() -> int:
    print(f"DB: {DB_PATH}")
    before = os.path.getsize(DB_PATH)
    print(f"size before: {before/1024/1024:7.2f} MB")

    conn = sqlite3.connect(DB_PATH)
    try:
        ids = [r[0] for r in conn.execute(
            "SELECT id FROM documents WHERE extracted_text IS NOT NULL"
        )]
        print(f"candidates: {len(ids):,}")

        compressed = skipped = 0
        for doc_id in ids:
            row = conn.execute(
                "SELECT extracted_text FROM documents WHERE id = ?", (doc_id,)
            ).fetchone()
            val = row[0]

            if isinstance(val, (bytes, bytearray)) and bytes(val).startswith(GZIP_MAGIC):
                skipped += 1
                continue

            if isinstance(val, str):
                blob = gzip.compress(val.encode("utf-8"))
            elif isinstance(val, (bytes, bytearray)):
                blob = gzip.compress(bytes(val))
            else:
                skipped += 1
                continue

            conn.execute(
                "UPDATE documents SET extracted_text = ? WHERE id = ?",
                (blob, doc_id),
            )
            compressed += 1
            if compressed % 250 == 0:
                conn.commit()
                print(f"  {compressed:,}/{len(ids):,} compressed")

        conn.commit()
        print(f"compressed: {compressed:,}  skipped: {skipped:,}")

        print("VACUUM...")
        conn.isolation_level = None
        conn.execute("VACUUM")
    finally:
        conn.close()

    after = os.path.getsize(DB_PATH)
    print(f"size after:  {after/1024/1024:7.2f} MB "
          f"(saved {(before-after)/1024/1024:7.2f} MB, "
          f"{100*(before-after)/before:5.1f}%)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
