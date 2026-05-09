"""One-off cleanup: drop pre-2026 agenda + performance documents.

Conservative pre-2026 prune (owner decision May 2026): keeps minutes
and board_packs (decision artifacts / richest data), drops agendas
(forward-looking, low residual value once the meeting passed) and the
two stand-alone performance reports. CAFRs are unaffected here, and
IPS lives in its own ips_documents table.

Idempotent: re-running after the prune is a no-op since the candidate
rows are already gone.

Run:
    python -m scripts.prune_pre_2026_docs           # dry-run preview
    python -m scripts.prune_pre_2026_docs --apply   # execute + VACUUM
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path

DB_PATH = (
    os.environ.get("DB_PATH")
    or str(Path(__file__).parent.parent / "db" / "pension.db")
)
DOC_TYPES = ("agenda", "performance")
CUTOFF_ISO = "2026-01-01"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="prune_pre_2026_docs")
    parser.add_argument("--apply", action="store_true",
                        help="Execute the prune (default: dry-run preview).")
    args = parser.parse_args(argv)

    size_before = os.path.getsize(DB_PATH) / 1e6
    conn = sqlite3.connect(DB_PATH)
    try:
        ids = [r[0] for r in conn.execute(
            f"SELECT id FROM documents "
            f"WHERE doc_type IN ({','.join('?' * len(DOC_TYPES))}) "
            f"AND meeting_date IS NOT NULL AND meeting_date < ?",
            (*DOC_TYPES, CUTOFF_ISO),
        )]

        print(f"candidate documents: {len(ids)}")
        print(f"  doc_types: {DOC_TYPES}")
        print(f"  cutoff:    meeting_date < {CUTOFF_ISO}")
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
            print("\nDry-run only. Re-run with --apply to execute.")
            return 0

        # FKs are NO ACTION (not CASCADE), so delete children first.
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute(f"DELETE FROM document_health WHERE document_id IN ({ph})", ids)
        conn.execute(f"DELETE FROM summaries WHERE document_id IN ({ph})", ids)
        conn.execute(f"DELETE FROM documents WHERE id IN ({ph})", ids)
        conn.commit()
        # VACUUM auto-commits and must run outside an open transaction.
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
