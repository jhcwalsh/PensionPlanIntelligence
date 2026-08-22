"""Call every read in queries.py against two databases and diff the results.

Step 4 of the migration is "dual-run staging on Postgres beside prod on
SQLite; compare pages". Comparing the 25 functions the pages are built from is
the same check made precise: it covers every branch a page can render, it runs
in seconds, and it names the function that differs rather than leaving someone
to spot a changed number in a table. The read layer only exists to be called
this way -- step 2 moved these queries out of app.py, where Streamlit's
caching machinery made them unreachable.

Read-only on both sides.

    python scripts/compare_backends.py db/pension.db "$DATABASE_URL"

See docs/superpowers/plans/2026-08-21-postgres-dual-run.md.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import pathlib
import sys
from decimal import Decimal

from sqlalchemy.orm import sessionmaker

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import database  # noqa: E402
import queries  # noqa: E402

FLOAT_PLACES = 6

# Columns held as gzipped text: compared by digest so two corpora of it are
# never resident at once. MAX_STORED_CHARS is 2,000,000.
DIGEST_COLUMNS = {"extracted_text"}


def normalise(value):
    """Reduce a value to something two backends can be compared on.

    Postgres returns timezone-aware datetimes and Decimal where SQLite returns
    naive datetimes and float. Both hold the same data -- every stored datetime
    is UTC either way, per the 2026-08-19 audit -- so the shapes are converged
    rather than the differences reported. Converging too eagerly is the real
    risk here: the tolerance below is tight enough that a genuinely different
    number still shows.
    """
    if isinstance(value, dt.datetime):
        return database.as_utc(value).isoformat()
    if isinstance(value, bool):
        return value                                  # before the int branch
    if isinstance(value, Decimal):
        return round(float(value), FLOAT_PLACES)
    if isinstance(value, float):
        return round(value, FLOAT_PLACES)
    if isinstance(value, (list, tuple)):
        return [normalise(v) for v in value]
    if isinstance(value, set):
        return sorted(normalise(v) for v in value)
    if isinstance(value, dict):
        return {k: normalise(v) for k, v in sorted(value.items())}
    if hasattr(value, "__table__"):                   # an ORM instance
        out = {}
        for column in value.__table__.columns:
            item = getattr(value, column.name)
            if column.name in DIGEST_COLUMNS:
                item = (hashlib.md5(item.encode("utf-8")).hexdigest()
                        if item is not None else None)
            out[column.name] = normalise(item)
        return out
    return value


CUTOFF = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)

# One entry per public function in queries.py. queries._aggregator_plan_ids is
# deliberately absent: it is private and takes no session, so calling it as
# fn(session) would raise TypeError straight into `errored`.
#
# documents_by_ids is passed 4315 on purpose -- that document was pruned, so
# the case checks both backends handle a missing id alike rather than only
# ever seeing ids that resolve.
CASES = {
    "plans": {"args": ()},
    "recent_summaries": {"args": (None, 50)},
    "corpus_stats": {"args": ()},
    "plan_coverage_rows": {"args": ()},
    "plans_index_rows": {"args": ()},
    "cafr_coverage_rows": {"args": ()},
    "cafr_plan_detail": {"args": ("opers",)},
    "cafr_extract_fy_range": {"args": ()},
    "allocation_rows": {"args": (("%equity%",), ())},
    "investment_action_docs": {"args": ("opers", CUTOFF)},
    "documents_by_ids": {"args": ([1, 2, 3, 4315],)},
    "document_with_context": {"args": (1,)},
    "documents_for_run": {"args": ([1, 2, 3],)},
    "recent_fetch_runs": {"args": (20,)},
    "failed_extraction_rows": {"args": ()},
    "skipped_document_rows": {"args": ()},
    "cafr_coverage_summary": {"args": ()},
    "recent_cafr_refresh_runs": {"args": (20,)},
    "cafr_refresh_rows": {"args": ([],)},        # resolved from the data below
    "plan_labels": {"args": ()},
    "plans_by_id": {"args": ()},
    "video_sources": {"args": (None,)},
    "meeting_recordings": {"args": (None,)},
    "publications_by_status": {"args": (("published",),)},
    "drafts_awaiting_approval": {"args": ()},
}


def _resolve_dynamic_args(session) -> None:
    """Fill in arguments that only exist in the data.

    cafr_refresh_rows selects by run timestamp, so a hardcoded value would
    match nothing on either side and the case would compare two empty lists --
    passing while testing nothing.
    """
    runs = queries.recent_cafr_refresh_runs(session, 3)
    CASES["cafr_refresh_rows"]["args"] = ([r.run_at for r in runs],)


def compare(sqlite_path: str, other_url: str) -> dict:
    """Run every case against both databases. Never raises for one bad query.

    A run that dies partway looks indistinguishable from a clean run that
    covered less than it claimed, so a failing case is recorded and the
    remaining cases still run.
    """
    src = database.create_app_engine("sqlite:///%s" % sqlite_path)
    dst = database.create_app_engine(database.normalise_pg_url(other_url))
    SrcSession, DstSession = sessionmaker(bind=src), sessionmaker(bind=dst)

    matched: list[str] = []
    mismatched: dict[str, tuple] = {}
    errored: dict[str, str] = {}
    try:
        with SrcSession() as ss, DstSession() as ds:
            _resolve_dynamic_args(ss)
            for name, case in CASES.items():
                fn = getattr(queries, name)
                try:
                    left = normalise(fn(ss, *case["args"]))
                    right = normalise(fn(ds, *case["args"]))
                except Exception as exc:              # noqa: BLE001
                    errored[name] = "%s: %s" % (type(exc).__name__, exc)
                    continue
                if left == right:
                    matched.append(name)
                else:
                    mismatched[name] = (left, right)
    finally:
        src.dispose()
        dst.dispose()
    return {"matched": matched, "mismatched": mismatched, "errored": errored}


def _summarise(value, limit: int = 240) -> str:
    text = repr(value)
    return text if len(text) <= limit else text[:limit] + " ..."


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("sqlite_path")
    ap.add_argument("other_url", help="the second backend, usually Postgres")
    args = ap.parse_args(argv)

    result = compare(args.sqlite_path, args.other_url)
    for name in sorted(result["matched"]):
        print("  ok        %s" % name)
    for name, (left, right) in sorted(result["mismatched"].items()):
        print("  MISMATCH  %s" % name)
        print("      sqlite  : %s" % _summarise(left))
        print("      other   : %s" % _summarise(right))
    for name, message in sorted(result["errored"].items()):
        print("  ERROR     %s -- %s" % (name, message))

    print("\n%d matched, %d mismatched, %d errored (of %d cases)"
          % (len(result["matched"]), len(result["mismatched"]),
             len(result["errored"]), len(CASES)))
    return 0 if not (result["mismatched"] or result["errored"]) else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
