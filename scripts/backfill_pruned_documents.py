"""One-off backfill: seed pruned_documents from the May-2026 pre-2026 prune
and remove any rows that re-appeared in subsequent fetches.

Background. The 2026-05-08 13:47 UTC daily pipeline run re-downloaded 522 of
the 617 documents the 2026-05-08 04:43 UTC prune (commit 47a1fda) had
deleted, because fetcher.py only de-dups on row-presence in ``documents``.
This script:

  1. Extracts ``db/pension.db`` at 47a1fda^ (the parent of the prune commit)
     into a temp file via ``git show``.
  2. Reads the URLs the prune targeted (dated pre-2026 agendas + the 2
     performance reports — same SELECT as scripts/prune_pre_2026_docs.py).
  3. Inserts those URLs into ``pruned_documents`` in the current DB so the
     fetcher's new gate (``database.document_pruned``) blocks future
     re-fetches.
  4. Finds rows currently in ``documents`` whose URL is now in
     ``pruned_documents`` (i.e. the ones today's run re-fetched) and deletes
     them along with attached summaries / document_health rows.
  5. VACUUM.

Idempotent. Re-running after a successful pass is a no-op (URLs already in
pruned_documents, current DB has no matching documents to delete).

Run:
    python -m scripts.backfill_pruned_documents             # dry-run
    python -m scripts.backfill_pruned_documents --apply     # execute + VACUUM
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
DB_PATH = os.environ.get("DB_PATH") or str(REPO_ROOT / "db" / "pension.db")
PRE_PRUNE_REV = "47a1fda^"           # parent of the May-2026 prune commit
DOC_TYPES = ("agenda", "performance")
CUTOFF_ISO = "2026-01-01"
REASON = "pre-2026-agenda-prune"


def extract_pre_prune_db() -> Path:
    """git show <rev>:db/pension.db > <tmp>; return the tmp path."""
    tmp_dir = Path(tempfile.mkdtemp(prefix="prepruneprune_db_"))
    out = tmp_dir / "pension.db"
    with out.open("wb") as fh:
        subprocess.check_call(
            ["git", "show", f"{PRE_PRUNE_REV}:db/pension.db"],
            cwd=str(REPO_ROOT),
            stdout=fh,
        )
    return out


def read_pruned_urls(pre_prune_db: Path) -> list[tuple[str, str, str, str]]:
    """Return (url, plan_id, doc_type, meeting_date_iso) for the prune set."""
    conn = sqlite3.connect(str(pre_prune_db))
    try:
        rows = conn.execute(
            f"SELECT url, plan_id, doc_type, meeting_date FROM documents "
            f"WHERE doc_type IN ({','.join('?' * len(DOC_TYPES))}) "
            f"AND meeting_date IS NOT NULL AND meeting_date < ?",
            (*DOC_TYPES, CUTOFF_ISO),
        ).fetchall()
        return rows
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="backfill_pruned_documents")
    parser.add_argument("--apply", action="store_true",
                        help="Execute (default: dry-run).")
    args = parser.parse_args(argv)

    print(f"reading pre-prune DB at {PRE_PRUNE_REV} ...")
    pre_prune_db = extract_pre_prune_db()
    pruned = read_pruned_urls(pre_prune_db)
    print(f"  pruned-set size (matches commit message: 615+2=617): {len(pruned)}")

    size_before = os.path.getsize(DB_PATH) / 1e6
    conn = sqlite3.connect(DB_PATH)
    try:
        # Sanity: ensure pruned_documents exists.
        existing_tbl = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='pruned_documents'"
        ).fetchone()
        if not existing_tbl:
            print("ERROR: pruned_documents table missing — run database.init_db() first.")
            return 1

        already = {r[0] for r in conn.execute(
            "SELECT url FROM pruned_documents"
        ).fetchall()}
        new_urls = [r for r in pruned if r[0] not in already]
        print(f"  already in pruned_documents: {len(already)}")
        print(f"  new to insert:               {len(new_urls)}")

        # Detect today's re-fetches: docs in current DB whose URL is in the prune set.
        prune_url_set = {r[0] for r in pruned}
        if prune_url_set:
            ph = ",".join("?" * len(prune_url_set))
            rebound_ids = [r[0] for r in conn.execute(
                f"SELECT id FROM documents WHERE url IN ({ph})",
                tuple(prune_url_set),
            ).fetchall()]
        else:
            rebound_ids = []

        print(f"  rebound documents currently in DB: {len(rebound_ids)}")

        if rebound_ids:
            ph_ids = ",".join("?" * len(rebound_ids))
            n_summ = conn.execute(
                f"SELECT COUNT(*) FROM summaries WHERE document_id IN ({ph_ids})",
                rebound_ids,
            ).fetchone()[0]
            n_health = conn.execute(
                f"SELECT COUNT(*) FROM document_health WHERE document_id IN ({ph_ids})",
                rebound_ids,
            ).fetchone()[0]
            print(f"    attached summaries:       {n_summ}")
            print(f"    attached document_health: {n_health}")
        else:
            n_summ = n_health = 0

        if not args.apply:
            print("\nDry-run only. Re-run with --apply to execute.")
            return 0

        # 1. Insert pruned URLs. (Raw sqlite3 doesn't honor the SQLAlchemy
        # default for pruned_at, so set it explicitly.)
        now = datetime.utcnow().isoformat()
        conn.executemany(
            "INSERT INTO pruned_documents "
            "(url, plan_id, doc_type, meeting_date, pruned_at, reason) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [(url, plan_id, dt, mdate, now, REASON)
             for (url, plan_id, dt, mdate) in new_urls],
        )

        # 2. Delete rebound rows + their dependents.
        if rebound_ids:
            conn.execute("PRAGMA foreign_keys=OFF")
            conn.execute(
                f"DELETE FROM document_health WHERE document_id IN ({ph_ids})",
                rebound_ids,
            )
            conn.execute(
                f"DELETE FROM summaries WHERE document_id IN ({ph_ids})",
                rebound_ids,
            )
            conn.execute(
                f"DELETE FROM documents WHERE id IN ({ph_ids})",
                rebound_ids,
            )

        conn.commit()
        # VACUUM must run outside a transaction.
        conn.execute("VACUUM")
    finally:
        conn.close()
        try:
            os.remove(pre_prune_db)
            os.rmdir(pre_prune_db.parent)
        except OSError:
            pass

    size_after = os.path.getsize(DB_PATH) / 1e6
    print(f"\ninserted {len(new_urls)} pruned_documents rows.")
    print(f"deleted {len(rebound_ids)} rebound docs + {n_summ} summaries "
          f"+ {n_health} health rows.")
    print(f"DB size: {size_before:.1f} MB → {size_after:.1f} MB "
          f"(saved {size_before - size_after:.1f} MB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
