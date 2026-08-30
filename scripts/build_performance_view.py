"""Collate the asset-class returns this corpus already holds.

    python -m scripts.build_performance_view

Makes no API calls and extracts nothing. Two sources already carry returns
by asset class, and until now only one of them was ever shown:

  * ``cafr_performance``          fiscal-year returns from CAFR extraction
  * ``summaries.performance_data``  a by-product of ordinary summarising,
                                    4,233 data points across 925 documents
                                    in 2026 alone, surfaced nowhere

Newest wins per (plan, asset class), so a 2026 board document supersedes an
FY2024 CAFR for the same class rather than competing with it. Asset-class
names are canonicalised through data/asset_class_mappings.json -- the map
the allocation views already use -- so "US Equities", "Domestic Equity" and
"S&P 500" stop being three different things.

Rebuilt wholesale each run: the table is derived, so the build is the
definition. Nothing else is written, and no source row is modified.
"""
from __future__ import annotations

import argparse
import io
import json
import re
import sys
from datetime import date, datetime

from rich.console import Console
from sqlalchemy import text

import database
from database import (CafrExtract, CafrPerformance, Document,
                      PlanAssetClassPerformance, Summary)

console = Console(legacy_windows=False)

MAPPINGS = "data/asset_class_mappings.json"

# Labels that mean the whole fund rather than a class within it. Kept separate
# from the mapping file because the map is about asset classes and these are
# the absence of one.
TOTAL_LABELS = {"total fund", "total", "total plan", "overall",
                "total portfolio", "total fund composite", "composite"}

_YEAR = re.compile(r"(19|20)\d{2}")


def load_class_map() -> dict:
    with io.open(MAPPINGS, encoding="utf-8") as f:
        return json.load(f)


def canonical(raw: str, class_map: dict) -> str | None:
    """Canonical key for an asset-class label, or None to skip it.

    Absent from the map, or mapped to 'unmapped', means skip -- guessing
    would put a plan's hedge fund return in its private equity column, which
    is worse than a gap because a gap is visible.
    """
    if not raw:
        return None
    key = raw.strip()
    if key.lower() in TOTAL_LABELS:
        return "total"
    entry = class_map.get(key)
    if entry is None:
        # One retry on a squashed-whitespace match before giving up.
        squashed = " ".join(key.split())
        entry = class_map.get(squashed)
    if not entry:
        return None
    canon = entry.get("canonical")
    return None if not canon or canon == "unmapped" else canon


def _as_date(v) -> date | None:
    """One date type across both sources.

    CAFR rows synthesise a `date` from a fiscal year; `Document.meeting_date`
    is a `datetime`. Comparing them raises, and the comparison only happens
    when two sources actually collide on the same (plan, asset class) -- so
    left unnormalised this crashes on real data and passes on thin data.
    """
    # datetime is a SUBCLASS of date, so `isinstance(v, date)` is True for a
    # datetime and cannot be used to tell them apart. Test for datetime first.
    if isinstance(v, datetime):
        return v.date()
    return v if isinstance(v, date) else None


def _as_float(v) -> float | None:
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v.strip().rstrip("%"))
        except ValueError:
            return None
    return None


def collect_from_cafr(session, class_map) -> list[dict]:
    rows = []
    q = (session.query(CafrPerformance, CafrExtract)
         .join(CafrExtract, CafrPerformance.cafr_extract_id == CafrExtract.id))
    for perf, extract in q:
        canon = canonical(perf.scope or "", class_map)
        ret = _as_float(perf.return_pct)
        if not canon or ret is None:
            continue
        fy = extract.fiscal_year
        rows.append({
            "plan_id": extract.plan_id,
            "asset_class": canon,
            "return_pct": ret,
            "period_label": f"FY{fy}" if fy else None,
            # A fiscal year is not a date; June 30 is the common year end and
            # is only used to order sources against each other, never shown.
            "as_of_date": date(int(fy), 6, 30) if fy else None,
            "source": "cafr",
            "document_id": extract.document_id,
        })
    return rows


def collect_from_summaries(session, class_map, since: date | None) -> list[dict]:
    """Returns already parsed out of board documents by the summariser."""
    q = (session.query(Summary.performance_data, Document.plan_id,
                       Document.meeting_date, Document.id)
         .join(Document, Document.id == Summary.document_id)
         .filter(Summary.performance_data.isnot(None),
                 Summary.performance_data.notin_(("[]", ""))))
    if since:
        q = q.filter(Document.meeting_date >= since)

    rows = []
    for payload, plan_id, meeting_date, doc_id in q:
        try:
            items = json.loads(payload)
        except (TypeError, ValueError):
            continue
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            canon = canonical(item.get("asset_class") or "", class_map)
            ret = _as_float(item.get("return_pct"))
            if not canon or ret is None:
                continue
            rows.append({
                "plan_id": plan_id,
                "asset_class": canon,
                "return_pct": ret,
                "period_label": (item.get("period") or "")[:64] or None,
                "as_of_date": _as_date(meeting_date),
                "source": "board_doc",
                "document_id": doc_id,
            })
    return rows


def pick_latest(rows: list[dict]) -> list[dict]:
    """One row per (plan, asset class): the most recent wins.

    Undated rows lose to dated ones but beat nothing, so a plan whose only
    figure carries no date still appears rather than vanishing.
    """
    best: dict[tuple[str, str], dict] = {}
    for r in rows:
        key = (r["plan_id"], r["asset_class"])
        cur = best.get(key)
        if cur is None:
            best[key] = r
            continue
        a, b = r["as_of_date"], cur["as_of_date"]
        if a and (b is None or a > b):
            best[key] = r
    return list(best.values())


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--since", help="only board documents from this date (YYYY-MM-DD)")
    args = ap.parse_args()
    since = date.fromisoformat(args.since) if args.since else None

    database.init_db()
    class_map = load_class_map()
    session = database.SessionLocal()
    try:
        cafr = collect_from_cafr(session, class_map)
        board = collect_from_summaries(session, class_map, since)
        chosen = pick_latest(cafr + board)

        session.execute(text("DELETE FROM plan_asset_class_performance"))
        for r in chosen:
            session.add(PlanAssetClassPerformance(**r))
        session.commit()

        plans = len({r["plan_id"] for r in chosen})
        classes = len({r["asset_class"] for r in chosen})
        from_board = sum(1 for r in chosen if r["source"] == "board_doc")
        console.print(
            f"[green]{len(chosen)}[/green] rows across [green]{plans}[/green] "
            f"plans and {classes} asset classes "
            f"({from_board} from board documents, {len(chosen)-from_board} from CAFRs)"
        )
        console.print(f"[dim]candidates seen: {len(cafr)} CAFR, {len(board)} board[/dim]")
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
