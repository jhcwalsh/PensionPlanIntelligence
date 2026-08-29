"""Add the PDF-retention columns to an existing Postgres `documents` table.

This repo has no migration framework: `init_db()` calls `create_all`, which
creates *missing tables* but never adds a column to a table that already
exists. On a fresh database the model in `database.py` is enough. On the live
Neon database, which already has `documents`, the three retention columns
have to be added once by hand -- that is all this script does.

It is not a migration system and must not grow into one. Idempotent
(`ADD COLUMN IF NOT EXISTS`), safe to re-run, and a no-op on anything that
is not Postgres.

Run once, by a human, against the live database:

    python -m scripts.add_retention_columns

Columns (see the `Document` model for the semantics of retention_status):
    content_sha256    VARCHAR(64)   the R2 object key
    r2_uploaded_at    TIMESTAMPTZ   when it landed
    retention_status  VARCHAR(32)   null / "unrecoverable" / "transient"
"""
from __future__ import annotations

import sys

from sqlalchemy import text

import database

COLUMNS = (
    ("content_sha256", "VARCHAR(64)"),
    ("r2_uploaded_at", "TIMESTAMP WITH TIME ZONE"),
    ("retention_status", "VARCHAR(32)"),
)


def main() -> int:
    dialect = database.engine.dialect.name
    if dialect != "postgresql":
        # Non-zero on purpose. An unset DATABASE_URL silently resolves to
        # SQLite (see CLAUDE.md), and this script's whole job is to touch the
        # live Postgres database -- exiting 0 there would report success for
        # a run that did nothing to the database the operator meant.
        print(f"Backend is {dialect!r}, not postgresql -- nothing to do. "
              "create_all() already covers SQLite and fresh databases. "
              "If you meant to reach Neon, DATABASE_URL is not set.")
        return 1

    with database.engine.begin() as conn:
        for name, ddl_type in COLUMNS:
            conn.execute(text(
                f"ALTER TABLE documents ADD COLUMN IF NOT EXISTS {name} "
                f"{ddl_type}"
            ))
            print(f"  documents.{name} present")
    print("Retention columns are in place.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
