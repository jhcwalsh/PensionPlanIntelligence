"""One-off cleanup: drop pre-2026 documents stuck in extraction_status='failed'.

Companion to prune_pre_2026_docs.py (owner decision July 2026): failed docs
have no extracted text by definition, and the pre-2026 ones aren't worth the
vision-OCR spend to recover, so the rows go entirely. Because these rows
never got a meeting_date (extraction is what infers it), the cutoff test
falls back to dating the filename. Undatable docs are kept — they might be
2026 — as is anything dated 2026+.

Pruned URLs are recorded in ``pruned_documents`` so the fetcher won't
re-download them (see database.PrunedDocument). Idempotent: pruned rows are
gone on re-run.

Run:
    python -m scripts.prune_pre_2026_failed_docs           # dry-run preview
    python -m scripts.prune_pre_2026_failed_docs --apply   # execute + VACUUM
"""

from __future__ import annotations

import argparse
import os
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

DB_PATH = (
    os.environ.get("DB_PATH")
    or str(Path(__file__).parent.parent / "db" / "pension.db")
)
CUTOFF_YEAR = 2026
PRUNE_REASON = "pre-2026-failed-unextractable-prune"

_YEAR4 = re.compile(r"(?<!\d)(20[0-2]\d)(?!\d)")
_MDY2 = re.compile(r"(?<!\d)\d{1,2}[.\-_ ]\d{1,2}[.\-_ ](\d{2})(?!\d)")
_RUN6 = re.compile(r"(?<!\d)(\d{6})(?!\d)")


def infer_year(filename: str | None, meeting_date: str | None) -> int | None:
    """Best-effort document year; None when not confidently datable."""
    if meeting_date:
        return int(meeting_date[:4])
    if not filename:
        return None
    m = _YEAR4.search(filename)
    if m:
        return int(m.group(1))
    m = _MDY2.search(filename)
    if m and int(m.group(1)) <= 29:
        return 2000 + int(m.group(1))
    m = _RUN6.search(filename)
    if m:
        run = m.group(1)
        # yymmdd (e.g. 241218), else mmddyy (e.g. 071813)
        if 20 <= int(run[:2]) <= 29 and 1 <= int(run[2:4]) <= 12 and 1 <= int(run[4:]) <= 31:
            return 2000 + int(run[:2])
        if 1 <= int(run[:2]) <= 12 and 1 <= int(run[2:4]) <= 31 and int(run[4:]) <= 29:
            return 2000 + int(run[4:])
    return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="prune_pre_2026_failed_docs")
    parser.add_argument("--apply", action="store_true",
                        help="Execute the prune (default: dry-run preview).")
    args = parser.parse_args(argv)

    size_before = os.path.getsize(DB_PATH) / 1e6
    conn = sqlite3.connect(DB_PATH)
    try:
        rows = conn.execute(
            "SELECT id, filename, meeting_date, doc_type FROM documents "
            "WHERE extraction_status = 'failed'"
        ).fetchall()
        ids = [i for i, fn, md, _ in rows
               if (y := infer_year(fn, md)) is not None and y < CUTOFF_YEAR]
        kept = len(rows) - len(ids)

        print(f"failed documents:    {len(rows)}")
        print(f"pre-{CUTOFF_YEAR} (to prune): {len(ids)}")
        print(f"kept ({CUTOFF_YEAR}+ or undatable): {kept}")
        if not ids:
            print("Nothing to do.")
            return 0

        ph = ",".join("?" * len(ids))
        n_summ = conn.execute(
            f"SELECT COUNT(*) FROM summaries WHERE document_id IN ({ph})", ids
        ).fetchone()[0]
        n_health = conn.execute(
            f"SELECT COUNT(*) FROM document_health WHERE document_id IN ({ph})", ids
        ).fetchone()[0]
        print(f"attached summaries:       {n_summ}")
        print(f"attached document_health: {n_health}")

        if not args.apply:
            preview = [r for r in rows if r[0] in set(ids)][:15]
            for i, fn, md, dt in preview:
                print(f"  would prune: [{i}] {dt} {fn} ({infer_year(fn, md)})")
            print("\nDry-run only. Re-run with --apply to execute.")
            return 0

        now_iso = datetime.utcnow().isoformat()
        n_pruned = conn.execute(
            f"INSERT OR IGNORE INTO pruned_documents "
            f"(url, plan_id, doc_type, meeting_date, pruned_at, reason) "
            f"SELECT url, plan_id, doc_type, meeting_date, ?, ? "
            f"FROM documents WHERE id IN ({ph})",
            (now_iso, PRUNE_REASON, *ids),
        ).rowcount
        print(f"recorded pruned URLs:     {n_pruned}")

        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute(f"DELETE FROM document_health WHERE document_id IN ({ph})", ids)
        conn.execute(f"DELETE FROM summaries WHERE document_id IN ({ph})", ids)
        conn.execute(f"DELETE FROM documents WHERE id IN ({ph})", ids)
        conn.commit()
        conn.execute("VACUUM")
    finally:
        conn.close()

    size_after = os.path.getsize(DB_PATH) / 1e6
    print(f"deleted {len(ids)} docs + {n_summ} summaries + {n_health} health rows.")
    print(f"DB size: {size_before:.1f} MB → {size_after:.1f} MB "
          f"(saved {size_before - size_after:.1f} MB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
