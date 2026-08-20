"""Stamp +00:00 onto every naive datetime value in a SQLite pension DB.

Why a wholesale stamp is correct
--------------------------------
The datetime audit (2026-08-19) established two things about the existing data:

1. All 45 populated DateTime columns hold naive values — including the 17 whose
   column default is the timezone-aware database.utcnow, because SQLAlchemy's
   SQLite DATETIME format has no timezone field and strips the offset on write.
2. No writer in the codebase has ever used local time. Every datetime.now(...)
   call passes timezone.utc explicitly, and there are no naive datetime.now()
   calls at all.

Together those mean every stored value is already UTC, so the offset can simply
be appended rather than computed. No per-column analysis, no DST correction.

EXPORT ONLY — do not run this against the live SQLite database.

SQLAlchemy's SQLite DATETIME parses a trailing +00:00 when the column declares
timezone=True, but SQLite writes still strip it. Stamping the live file would
leave existing rows aware and every subsequent row naive — manufacturing the
very naive/aware mixture this work exists to remove. Run it only on a copy
being exported to Postgres.

Run this against the SQLite file as part of the Postgres export, so values
arrive at Neon already carrying their offset.

Idempotent on the offset suffix — re-running is safe, the same contract as
scripts/migrate_compress_extracted_text.py.

    python scripts/backfill_utc_datetimes.py [path/to/pension.db]

See docs/superpowers/plans/2026-08-19-datetime-audit.md, Task 6.
"""

from __future__ import annotations

import pathlib
import re
import sqlite3
import sys

import sqlalchemy as sa

# Importable both as `scripts.backfill_utc_datetimes` (tests, where pytest puts
# the repo root on the path) and as a direct script, where it does not.
REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import database  # noqa: E402  (needs REPO_ROOT on sys.path first)

# Trailing timezone designator: +00:00, +0000, or Z.
HAS_OFFSET = re.compile(r"([+-]\d{2}:?\d{2}|Z)$")


def stamp_utc(db_path: str) -> dict[str, int]:
    """Append +00:00 to naive datetime values. Returns table.column -> rows changed.

    Only columns the SQLAlchemy models declare as DateTime are touched, and only
    tables actually present in the file — the model set is wider than any given
    DB, and older files legitimately lack newer tables.
    """
    con = sqlite3.connect(db_path)
    try:
        present = {row[0] for row in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        changed: dict[str, int] = {}

        for table_name, table in sorted(database.Base.metadata.tables.items()):
            if table_name not in present:
                continue
            for col in table.columns:
                if not isinstance(col.type, sa.DateTime):
                    continue
                rows = con.execute(
                    f'SELECT rowid, "{col.name}" FROM "{table_name}" '
                    f'WHERE "{col.name}" IS NOT NULL'
                ).fetchall()
                updates = [
                    (f"{value}+00:00", rowid)
                    for rowid, value in rows
                    if not HAS_OFFSET.search(str(value))
                ]
                if updates:
                    con.executemany(
                        f'UPDATE "{table_name}" SET "{col.name}" = ? WHERE rowid = ?',
                        updates,
                    )
                changed[f"{table_name}.{col.name}"] = len(updates)

        con.commit()
        return changed
    finally:
        con.close()


def main(argv: list[str]) -> int:
    path = argv[1] if len(argv) > 1 else "db/pension.db"
    changed = stamp_utc(path)
    total = sum(changed.values())
    for key, count in sorted(changed.items(), key=lambda kv: (-kv[1], kv[0])):
        if count:
            print(f"{count:>7}  {key}")
    print(f"\n{total} value(s) stamped across "
          f"{sum(1 for c in changed.values() if c)} column(s) in {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
