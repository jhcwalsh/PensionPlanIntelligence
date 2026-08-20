"""Compare a migrated Postgres database against its SQLite source.

Spec §9 step 3 requires row counts per table plus every plan's twin
_canonical_hash matching before and after. Both are computed here so the check
is reproducible rather than a manual ritual.

    python scripts/verify_migration.py db/pension.db "$POSTGRES_URL"
"""
from __future__ import annotations

import pathlib
import sys

import sqlalchemy as sa

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import database  # noqa: E402


def _counts(engine) -> dict[str, int]:
    present = set(sa.inspect(engine).get_table_names())
    out = {}
    with engine.connect() as conn:
        for table in database.Base.metadata.sorted_tables:
            if table.name not in present:
                continue
            out[table.name] = conn.execute(
                sa.select(sa.func.count()).select_from(table)).scalar()
    return out


def _twin_hashes(engine) -> dict[str, str]:
    """Latest snapshot hash per plan, keyed by plan id."""
    out: dict[str, str] = {}
    present = set(sa.inspect(engine).get_table_names())
    if "twin_snapshots" not in present:
        return out
    t = database.TwinSnapshot.__table__
    with engine.connect() as conn:
        rows = conn.execute(
            sa.select(t.c.plan_id, t.c.facets_hash, t.c.built_at)
            .order_by(t.c.plan_id, t.c.built_at.desc())).fetchall()
    for plan_id, digest, _built in rows:
        out.setdefault(plan_id, digest)
    return out


def compare(sqlite_path: str, pg_url: str) -> dict:
    src = sa.create_engine(f"sqlite:///{sqlite_path}")
    dst = sa.create_engine(pg_url, future=True)
    try:
        s_counts, p_counts = _counts(src), _counts(dst)
        row_counts = {
            name: {"sqlite": s_counts.get(name, 0), "postgres": p_counts.get(name, 0)}
            for name in sorted(set(s_counts) | set(p_counts))
        }
        mismatches = [n for n, v in row_counts.items() if v["sqlite"] != v["postgres"]]
        s_hash, p_hash = _twin_hashes(src), _twin_hashes(dst)
        hash_mismatches = sorted(
            pid for pid in set(s_hash) | set(p_hash)
            if s_hash.get(pid) != p_hash.get(pid))
        return {"row_counts": row_counts,
                "count_mismatches": mismatches,
                "twin_hash_mismatches": hash_mismatches}
    finally:
        src.dispose()
        dst.dispose()


def main(argv: list[str]) -> int:
    if len(argv) < 3:
        print(__doc__)
        return 2
    report = compare(argv[1], argv[2])
    bad = report["count_mismatches"]
    twins = report["twin_hash_mismatches"]
    for name, v in report["row_counts"].items():
        flag = "  <-- MISMATCH" if name in bad else ""
        print(f"{v['sqlite']:>8} -> {v['postgres']:>8}  {name}{flag}")
    print(f"\n{len(bad)} table(s) with differing counts; "
          f"{len(twins)} plan(s) with a differing twin hash")
    return 1 if (bad or twins) else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
