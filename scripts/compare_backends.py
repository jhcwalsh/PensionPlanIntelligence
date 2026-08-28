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

import sqlalchemy as sa
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
    # sa.Row is Sequence, NOT a tuple subclass, so a plain (list, tuple) check
    # misses every multi-column query result -- which is most of queries.py.
    # Left unhandled they fall through to `return value` and compare by
    # identity, so every such case reports a mismatch it cannot explain.
    if isinstance(value, (list, tuple, sa.Row)):
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
    "cafr_refresh_rows": {"args_fn": lambda s: (
        queries.recent_cafr_refresh_runs(s, 3),)},
    "plan_labels": {"args": ()},
    "plans_by_id": {"args": ()},
    "video_sources": {"args": (None,)},
    "meeting_recordings": {"args": (None,)},
    "publications_by_status": {"args": (("published",),)},
    "drafts_awaiting_approval": {"args": ()},
    # A wide window so the comparison is not vacuous on a fresh table.
    "api_spend_by_operation": {"args": (3650,)},
    "api_spend_by_model": {"args": (3650,)},
    "api_spend_total": {"args": (3650,)},
    # Default status filter, so this compares the same set the Weekly
    # archive tab shows and the monthly cascade gathers.
    "weekly_briefings": {"args": ()},
    # The asset-class map is loaded from disk by the caller, so pass an
    # empty one: the comparison is about backend parity in the SQL, and an
    # empty map still exercises every query and the total_fund path.
    "performance_report_rows": {"args": ({},)},
    # No asset-class map needed — unlike performance_report_rows this reads
    # scope only to filter to "total_fund", not to canonicalise classes.
    "quarterly_performance_rows": {"args": ()},
    # Default 30-day window, matching the CAFR page.
    "cafr_fiscal_year_counts": {"args": ()},
}


def case_args(case: dict, session):
    """The arguments for one case, against one session.

    Most cases carry literal args. A few select by a value that only exists in
    the data -- cafr_refresh_rows filters on run timestamps -- and a hardcoded
    value there would match nothing on either side, so the case would compare
    two empty lists and pass while testing nothing.

    Such a case resolves its args *per session*, not once from the source,
    because the two backends need them in different shapes. SQLite stores
    datetimes naive and compares them as strings; Postgres stores TIMESTAMPTZ.
    A naive value from SQLite passed into the Postgres query is interpreted in
    the session timezone and matches nothing, and an aware value passed into
    SQLite renders with a "+00:00" that no stored string carries. Resolving on
    each side gives each its native shape, and the comparison is of the
    results -- which is the thing under test.
    """
    if "args_fn" in case:
        return case["args_fn"](session)
    return case["args"]


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
            for name, case in CASES.items():
                fn = getattr(queries, name)
                try:
                    left = normalise(fn(ss, *case_args(case, ss)))
                    right = normalise(fn(ds, *case_args(case, ds)))
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


def first_difference(left, right, path: str = ""):
    """Where two normalised results first diverge, as (path, left, right).

    Printing a truncated prefix of each side is useless when the results agree
    for the first few hundred characters and differ deep inside element 40 --
    which is the common case, because the usual cause is ordering rather than
    content. Returns None when they are equal.
    """
    if left == right:
        return None
    if isinstance(left, list) and isinstance(right, list):
        if len(left) != len(right):
            return ("%s (length %d vs %d)" % (path, len(left), len(right)),
                    left[:1], right[:1])
        for i, (a, b) in enumerate(zip(left, right)):
            found = first_difference(a, b, "%s[%d]" % (path, i))
            if found:
                return found
    if isinstance(left, dict) and isinstance(right, dict):
        if set(left) != set(right):
            return ("%s (keys differ)" % path,
                    sorted(set(left) - set(right)), sorted(set(right) - set(left)))
        for key in left:
            found = first_difference(left[key], right[key],
                                     "%s.%s" % (path, key))
            if found:
                return found
    return (path or "<value>", left, right)


def _summarise(value, limit: int = 200) -> str:
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
        where, a, b = first_difference(left, right)
        print("      at      : %s" % where)
        print("      sqlite  : %s" % _summarise(a))
        print("      other   : %s" % _summarise(b))
    for name, message in sorted(result["errored"].items()):
        print("  ERROR     %s -- %s" % (name, message))

    print("\n%d matched, %d mismatched, %d errored (of %d cases)"
          % (len(result["matched"]), len(result["mismatched"]),
             len(result["errored"]), len(CASES)))
    return 0 if not (result["mismatched"] or result["errored"]) else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
