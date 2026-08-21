"""Copy a SQLite pension database into Postgres, preserving ids.

Only tables declared in Base.metadata are copied — which correctly excludes
the five SQLite FTS5 shadow tables, since FTS5 has no Postgres equivalent and
the replacement index is built from the copied rows instead.

Datetimes are normalised with database.as_utc on the way in. That is correct
whether or not scripts/backfill_utc_datetimes.py has been run: every stored
value is UTC either way (2026-08-19 audit).

The destination must be empty. migrate() commits per batch, so a dropped
connection leaves durable partial state; re-running over it would hit primary
key violations partway through, and reset_sequences would never have run, so
anything written to the half-loaded database collides. Rather than let that
happen quietly, a non-empty destination is refused — pass --replace to drop
and recreate the schema and start clean.

    python scripts/migrate_sqlite_to_postgres.py db/pension.db "$POSTGRES_URL"
    python scripts/migrate_sqlite_to_postgres.py db/pension.db "$POSTGRES_URL" --replace
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


def _non_empty_tables(engine) -> list[str]:
    present = set(sa.inspect(engine).get_table_names())
    out: list[str] = []
    with engine.connect() as conn:
        for table in database.Base.metadata.sorted_tables:
            if table.name not in present:
                continue
            n = conn.execute(
                sa.select(sa.func.count()).select_from(table)).scalar() or 0
            if n:
                out.append(f"{table.name} ({n})")
    return out


# 100, not 500: rows are decompressed to str on read and re-gzipped on write,
# so a batch is held in memory as plain text. MAX_STORED_CHARS is now
# 2,000,000, at which a 500-row documents batch could reach ~1 GB. 100 keeps
# the worst case an order of magnitude smaller for a negligible time cost.
DEFAULT_BATCH_SIZE = 100


def migrate(sqlite_path: str, pg_url: str,
            batch_size: int = DEFAULT_BATCH_SIZE,
            replace: bool = False) -> dict[str, int]:
    src = sa.create_engine(f"sqlite:///{sqlite_path}")
    dst = sa.create_engine(database.normalise_pg_url(pg_url), future=True)
    if replace:
        database.Base.metadata.drop_all(dst)
    database.Base.metadata.create_all(dst)

    occupied = _non_empty_tables(dst)
    if occupied:
        src.dispose()
        dst.dispose()
        raise RuntimeError(
            "refusing to migrate into a non-empty database; these tables "
            "already hold rows: " + ", ".join(occupied) + ". This copy "
            "commits per batch, so a partially loaded destination is durable "
            "and re-running would collide on primary keys. Pass replace=True "
            "(--replace on the CLI) to drop and recreate the schema first.")

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
        reset_sequences(pg_url)
        return copied
    finally:
        src.dispose()
        dst.dispose()


def reset_sequences(pg_url: str) -> dict[str, int]:
    """Advance each identity sequence past the largest id just inserted.

    Inserting explicit ids does not move the sequence, so the next natural
    insert would collide on id 1. setval with a floor of 1 keeps empty tables
    legal — a sequence may not be set below its minimum.
    """
    engine = sa.create_engine(database.normalise_pg_url(pg_url), future=True)
    out: dict[str, int] = {}
    try:
        with engine.begin() as conn:
            for table in database.Base.metadata.sorted_tables:
                pks = list(table.primary_key.columns)
                if len(pks) != 1:
                    continue
                col = pks[0]
                if not isinstance(col.type, sa.Integer):
                    continue
                seq = conn.execute(sa.text(
                    "SELECT pg_get_serial_sequence(:t, :c)"),
                    {"t": table.name, "c": col.name}).scalar()
                if seq is None:          # not an identity/serial column
                    continue
                high = conn.execute(sa.text(
                    f'SELECT COALESCE(MAX("{col.name}"), 0) FROM "{table.name}"'
                )).scalar() or 0
                target = max(high, 1)
                conn.execute(sa.text("SELECT setval(:s, :v, :called)"),
                             {"s": seq, "v": target, "called": high > 0})
                out[table.name] = target
        return out
    finally:
        engine.dispose()


def main(argv: list[str]) -> int:
    args = [a for a in argv[1:] if not a.startswith("--")]
    flags = {a for a in argv[1:] if a.startswith("--")}
    unknown = flags - {"--replace"}
    if len(args) < 2 or unknown:
        print(__doc__)
        return 2
    counts = migrate(args[0], args[1], replace="--replace" in flags)
    for name, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        if n:
            print(f"{n:>8}  {name}")
    print(f"\n{sum(counts.values())} rows across "
          f"{sum(1 for n in counts.values() if n)} tables")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
