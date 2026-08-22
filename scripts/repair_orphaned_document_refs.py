"""One-shot repair: reconcile foreign keys that point at deleted documents.

SQLite never enforced foreign keys here (they are off by default and nothing
turned them on), so the prune scripts were free to delete rows out of
`documents` while other tables still referenced them. Postgres does enforce
them, and the migration stops dead on the first violation:

    ForeignKeyViolation: Key (document_id)=(4315) is not present in
    table "documents"

`scripts/prune_pre_2026_failed_docs.py` deletes attached `summaries` and
`document_health` rows before deleting a document, but knows nothing about the
CAFR tables. Document 4315 -- the 2025 OPERS Annual Report -- was pruned as
"unextractable" because the *generic* text extraction failed, even though
`extract_cafr_investments` had already read the PDF directly and stored a
high-quality extraction from pages 99-146.

Two policies, chosen per table by what the reference means:

  null    The reference is provenance. The row is worth more than the pointer,
          so the row stays and the pointer is cleared. CAFR extractions
          deliberately outlive their PDFs -- the PDFs are never committed --
          and `cafr_extract` 151 alone carries 158 child rows of allocation
          and performance data.

  delete  The reference is the row's identity. `document_health.document_id`
          is half of a composite primary key, so it cannot be cleared, and the
          rows belong to the RFP subsystem removed on 2026-08-16.

Anything not named in POLICY is refused rather than guessed at.

Every row this touches is written to a JSON dump first. Idempotent: a second
run finds nothing to do. After the Postgres cutover this class of drift stops
happening, because the database rejects it at write time.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys

from sqlalchemy.dialects import sqlite as sqlite_dialect
from sqlalchemy.schema import CreateIndex, CreateTable

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import database  # noqa: E402

DB_PATH = os.environ.get("DB_PATH", os.path.join(ROOT, "db", "pension.db"))

NULL = "null"
DELETE = "delete"

# (child table, child column) -> policy. See the module docstring.
POLICY = {
    ("cafr_extract", "document_id"): NULL,
    ("cafr_refresh_log", "document_id"): NULL,
    ("cafr_actuarial", "document_id"): NULL,
    ("document_health", "document_id"): DELETE,
    ("summaries", "document_id"): DELETE,
}


class UnknownOrphan(Exception):
    """A foreign key broke in a table with no policy -- refuse to guess."""


def find_orphans(conn: sqlite3.Connection) -> list[dict]:
    """Every broken foreign key in the file, as (table, column, rowid).

    PRAGMA foreign_key_check is exhaustive across all tables, which a
    hand-written audit is not. It reports the offending key by its ordinal
    within the child table, so foreign_key_list resolves that to a column.
    """
    out = []
    for table, rowid, parent, fk_ordinal in conn.execute("PRAGMA foreign_key_check"):
        fks = list(conn.execute("PRAGMA foreign_key_list(%s)" % table))
        column = next(fk[3] for fk in fks if fk[0] == fk_ordinal)
        out.append({"table": table, "column": column,
                    "rowid": rowid, "parent": parent})
    return out


def _row_dump(conn: sqlite3.Connection, table: str, rowid: int) -> dict:
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT * FROM %s WHERE rowid = ?" % table, (rowid,)).fetchone()
    finally:
        conn.row_factory = None
    if row is None:
        return {}
    # extracted_text and friends are gzip blobs; keep the dump readable.
    return {k: ("<%d bytes>" % len(v) if isinstance(v, bytes) else v)
            for k, v in dict(row).items()}


def _is_not_null(conn: sqlite3.Connection, table: str, column: str) -> bool:
    return any(r[1] == column and r[3] == 1
               for r in conn.execute("PRAGMA table_info(%s)" % table))


def _relax_not_null(conn: sqlite3.Connection, table: str, column: str) -> None:
    """Rebuild `table` from its SQLAlchemy model so `column` accepts NULL.

    SQLite cannot drop a NOT NULL constraint in place, and this repo has no
    migration framework by design -- the model is the schema. So the new table
    is emitted from the model rather than from hand-written DDL, which also
    means it cannot drift from what init_db() creates on a fresh file.

    legacy_alter_table is essential here: without it, renaming a table rewrites
    every *other* table's foreign-key clause to follow the rename, silently
    repointing cafr_allocation and cafr_performance at the temporary name.
    """
    model = database.Base.metadata.tables[table]
    dialect = sqlite_dialect.dialect()
    columns = ", ".join('"%s"' % c.name for c in model.columns)
    old = "_repair_old_%s" % table

    conn.execute("PRAGMA legacy_alter_table = ON")
    try:
        conn.execute("ALTER TABLE %s RENAME TO %s" % (table, old))
        conn.execute(str(CreateTable(model).compile(dialect=dialect)))
        conn.execute("INSERT INTO %s (%s) SELECT %s FROM %s"
                     % (table, columns, columns, old))
        conn.execute("DROP TABLE %s" % old)
        for index in model.indexes:
            conn.execute(str(CreateIndex(index).compile(dialect=dialect)))
    finally:
        conn.execute("PRAGMA legacy_alter_table = OFF")


def repair(db_path: str = DB_PATH, dump_path: str | None = None,
           dry_run: bool = False) -> dict:
    """Apply POLICY to every orphan in `db_path`. Returns a per-key summary."""
    conn = sqlite3.connect(db_path)
    try:
        orphans = find_orphans(conn)

        unknown = {(o["table"], o["column"]) for o in orphans} - set(POLICY)
        if unknown:
            raise UnknownOrphan(
                "no policy for "
                + ", ".join("%s.%s" % (t, c) for t, c in sorted(unknown))
                + " -- decide whether the row or the reference is worth "
                  "keeping, then add it to POLICY")

        dump = [dict(o, row=_row_dump(conn, o["table"], o["rowid"]))
                for o in orphans]
        if dump_path and dump:
            with open(dump_path, "w", encoding="utf-8") as fh:
                json.dump(dump, fh, indent=2, default=str)

        summary: dict[str, int] = {}
        for (table, column), policy in POLICY.items():
            rowids = [o["rowid"] for o in orphans
                      if o["table"] == table and o["column"] == column]
            if not rowids:
                continue
            summary["%s.%s" % (table, column)] = len(rowids)
            if dry_run:
                continue
            marks = ",".join("?" * len(rowids))
            if policy == NULL:
                if _is_not_null(conn, table, column):
                    _relax_not_null(conn, table, column)
                conn.execute("UPDATE %s SET %s = NULL WHERE rowid IN (%s)"
                             % (table, column, marks), rowids)
            else:
                conn.execute("DELETE FROM %s WHERE rowid IN (%s)"
                             % (table, marks), rowids)

        if dry_run:
            return summary

        conn.commit()
        remaining = find_orphans(conn)
        if remaining:
            raise AssertionError(
                "%d orphans survived: %s" % (len(remaining), remaining))
        return summary
    finally:
        conn.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=DB_PATH)
    ap.add_argument("--dump",
                    help="write the affected rows here before touching them")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    print("DB: %s" % args.db)
    summary = repair(args.db, dump_path=args.dump, dry_run=args.dry_run)
    if not summary:
        print("no orphaned document references -- nothing to do")
        return 0
    verb = "would repair" if args.dry_run else "repaired"
    for key, count in sorted(summary.items()):
        table, column = key.rsplit(".", 1)
        print("  %s %d x %s (%s)" % (verb, count, key, POLICY[(table, column)]))
    if args.dump:
        print("dump: %s" % args.dump)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
