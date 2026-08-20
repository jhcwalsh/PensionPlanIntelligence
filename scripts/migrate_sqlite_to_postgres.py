"""Copy a SQLite pension database into Postgres, preserving ids.

Only tables declared in Base.metadata are copied — which correctly excludes
the five SQLite FTS5 shadow tables, since FTS5 has no Postgres equivalent and
the replacement index is built from the copied rows instead.

Datetimes are normalised with database.as_utc on the way in. That is correct
whether or not scripts/backfill_utc_datetimes.py has been run: every stored
value is UTC either way (2026-08-19 audit).

    python scripts/migrate_sqlite_to_postgres.py db/pension.db "$POSTGRES_URL"
"""
from __future__ import annotations

import datetime as _dt
import pathlib
import sys

import sqlalchemy as sa

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import database  # noqa: E402


def _normalise(row: dict) -> dict:
    return {k: (database.as_utc(v) if isinstance(v, _dt.datetime) else v)
            for k, v in row.items()}


def migrate(sqlite_path: str, pg_url: str, batch_size: int = 500) -> dict[str, int]:
    src = sa.create_engine(f"sqlite:///{sqlite_path}")
    dst = sa.create_engine(pg_url, future=True)
    database.Base.metadata.create_all(dst)

    present = set(sa.inspect(src).get_table_names())
    copied: dict[str, int] = {}
    try:
        # sorted_tables is dependency-ordered, so foreign keys resolve.
        for table in database.Base.metadata.sorted_tables:
            if table.name not in present:
                continue
            total = 0
            with src.connect() as sconn:
                result = sconn.execution_options(stream_results=True).execute(
                    sa.select(table))
                while True:
                    chunk = result.fetchmany(batch_size)
                    if not chunk:
                        break
                    rows = [_normalise(dict(r._mapping)) for r in chunk]
                    with dst.begin() as dconn:
                        dconn.execute(sa.insert(table), rows)
                    total += len(rows)
            copied[table.name] = total
        return copied
    finally:
        src.dispose()
        dst.dispose()


def main(argv: list[str]) -> int:
    if len(argv) < 3:
        print(__doc__)
        return 2
    counts = migrate(argv[1], argv[2])
    for name, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        if n:
            print(f"{n:>8}  {name}")
    print(f"\n{sum(counts.values())} rows across "
          f"{sum(1 for n in counts.values() if n)} tables")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
