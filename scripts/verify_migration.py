"""Compare a migrated Postgres database against its SQLite source.

Spec §9 step 3 requires row counts per table plus every plan's twin
_canonical_hash matching before and after. Both are computed here so the check
is reproducible rather than a manual ritual.

Row counts alone are not enough to trust before deleting the SQLite file:
documents.extracted_text is 35 MB compressed / 123 MB of text and is the
column most likely to be mangled crossing into BYTEA, and a count would not
notice. So every document's text is also md5-compared, read through the ORM
on both sides so GzippedText decompresses it — the comparison is of the text
callers see, not of the stored bytes. It streams id by id rather than loading
all ~4,200 documents at once.

    python scripts/verify_migration.py db/pension.db "$POSTGRES_URL"
"""
from __future__ import annotations

import hashlib
import pathlib
import sys

import sqlalchemy as sa
from sqlalchemy.orm import Session

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
            sa.select(t.c.plan_id, t.c.facets_hash, t.c.built_at, t.c.id)
            .order_by(t.c.plan_id, t.c.built_at.desc(), t.c.id.desc())).fetchall()
    for plan_id, digest, _built, _id in rows:
        out.setdefault(plan_id, digest)
    return out


CONTENT_CHUNK = 200


def _text_digest(value) -> str:
    """md5 of one document's text; a sentinel for NULL, distinct from ''."""
    if value is None:
        return "<null>"
    return hashlib.md5(value.encode("utf-8")).hexdigest()


def _document_ids(engine) -> list[int]:
    t = database.Document.__table__
    with engine.connect() as conn:
        return [r[0] for r in conn.execute(sa.select(t.c.id).order_by(t.c.id))]


def _digests(session, ids: list[int]) -> dict[int, str]:
    """Text digests for one chunk of ids, read through the ORM.

    Selecting the columns rather than whole entities keeps nothing in the
    identity map, so memory stays flat across the whole corpus, while the
    GzippedText TypeDecorator still runs and hands back str on both engines.
    """
    rows = (session.query(database.Document.id, database.Document.extracted_text)
            .filter(database.Document.id.in_(ids)).all())
    return {doc_id: _text_digest(txt) for doc_id, txt in rows}


def _content_mismatches(src, dst, chunk_size: int = CONTENT_CHUNK) -> list[int]:
    """Document ids whose extracted_text differs between the two databases.

    Streamed in chunks: at 123 MB of text, materialising every document at
    once is not an option on the machine that runs the cutover.
    """
    if ("documents" not in set(sa.inspect(src).get_table_names())
            or "documents" not in set(sa.inspect(dst).get_table_names())):
        return []
    ids = sorted(set(_document_ids(src)) | set(_document_ids(dst)))
    bad: list[int] = []
    with Session(src) as ssession, Session(dst) as dsession:
        for start in range(0, len(ids), chunk_size):
            chunk = ids[start:start + chunk_size]
            s_map = _digests(ssession, chunk)
            d_map = _digests(dsession, chunk)
            bad.extend(i for i in chunk if s_map.get(i) != d_map.get(i))
    return bad


def compare(sqlite_path: str, pg_url: str) -> dict:
    src = sa.create_engine(f"sqlite:///{sqlite_path}")
    dst = sa.create_engine(database.normalise_pg_url(pg_url), future=True)
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
        content = _content_mismatches(src, dst)
        return {"row_counts": row_counts,
                "count_mismatches": mismatches,
                "twin_hash_mismatches": hash_mismatches,
                "content_mismatches": content}
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
    content = report["content_mismatches"]
    for name, v in report["row_counts"].items():
        flag = "  <-- MISMATCH" if name in bad else ""
        print(f"{v['sqlite']:>8} -> {v['postgres']:>8}  {name}{flag}")
    print(f"\n{len(bad)} table(s) with differing counts; "
          f"{len(twins)} plan(s) with a differing twin hash; "
          f"{len(content)} document(s) with differing extracted_text")
    if content:
        print("differing document ids: "
              + ", ".join(str(i) for i in content[:50])
              + (" ..." if len(content) > 50 else ""))
    return 1 if (bad or twins or content) else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
