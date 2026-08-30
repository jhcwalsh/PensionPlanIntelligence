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

# What a period label actually measures. Order matters: "FY2025 (1-Year ending
# 12/31/25)" is both, and "3-Year as of June 2026" contains a year too, so the
# narrower tests run first.
_INCEPTION = re.compile(r"(?i)since inception|since \d{4}|inception")
# Any N-year window other than one. "20-Year Forward" and "3-Year as of" both
# land here; a forward-looking assumption is not a return at all, but it is
# certainly not this year's.
_MULTI = re.compile(r"(?i)\b(?!1[- ]?year)(\d{1,2}|three|five|ten|twenty)[- ]?(year|yr)")
_MONTH_NAMES = (r"january|february|march|april|may|june|july|august|"
                r"september|october|november|december")
_MONTHLY = re.compile(
    r"(?i)\b1[- ]?month|\bmtd\b|\(1 month\)|month[- ]?to[- ]?date"
    rf"|^\s*({_MONTH_NAMES})\s+\d{{4}}\s*$")
_QUARTER = re.compile(r"(?i)\bq[1-4]\b|\b[1-4]q\b|quarter|\b3[- ]months?\b")
_PARTIAL = re.compile(r"(?i)\bf?ytd\b|to date|through")
_FISCAL = re.compile(r"(?i)\bfy\s?\d{4}|fiscal")
_ONE_YEAR = re.compile(
    r"(?i)1[- ]?year|12 month|twelve month|calendar year|\bcy\s?\d{4}")


def horizon_of(period_label: str | None) -> str:
    """What the number measures, so incomparable figures can be kept apart.

    A quarterly return and a fiscal-year return are both "the latest
    performance figure" and mean entirely different things. Recording which
    is which is the difference between a table you can read across and one
    that invites a wrong conclusion -- the same reasoning that keeps
    queries.PERFORMANCE_PERIODS restricted to fy and 1y.

    Order matters and is not arbitrary. "FY2026 (1-Year ending 12/31/25)"
    satisfies several patterns; "3-Year as of 6/30/2025" contains a year;
    "March 2026 (through March 13)" is both a month and a partial period.
    The narrowest and most misleading readings are tested first, because the
    cost of calling a one-month return "annual" is far higher than the cost
    of calling it "unclear".

    The label set came from the real corpus, not from imagination: bare
    "May 2026" is how three plans report a monthly number, and "1Q 2026",
    "3 Months Ending 03/31/2026" and "Since Inception (2013 vintage)" all
    appear verbatim.
    """
    if not period_label:
        return "unclear"
    t = period_label.strip()
    if _INCEPTION.search(t):
        return "inception"
    if _MONTHLY.search(t):
        return "month"
    if _MULTI.search(t):
        return "multi_year"
    if _QUARTER.search(t):
        return "quarter"
    if _PARTIAL.search(t):
        return "partial"
    if _FISCAL.search(t) or _ONE_YEAR.search(t):
        return "annual"
    return "unclear"


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
            # A CAFR figure is a fiscal year by construction.
            "horizon": "annual",
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
                "horizon": horizon_of(item.get("period")),
                "as_of_date": _as_date(meeting_date),
                "source": "board_doc",
                "document_id": doc_id,
            })
    return rows


def pick_latest(rows: list[dict]) -> list[dict]:
    """All of one plan's figures from a single document: the most recent.

    Selecting per (plan, asset class) produced rows that were each
    individually defensible and collectively meaningless -- a plan's equity
    return from an August board pack beside its private-equity return from
    an FY2024 CAFR, presented as one row. Every number was right; the row
    was not a portfolio.

    So the unit of selection is the *document*. One source per plan, the most
    recent one carrying performance data, and every figure in the row comes
    from it. That restores the property the CAFR-only view had and this one
    had traded away: a row you can read across, whose period and frequency
    are single facts about the row rather than per-cell footnotes.

    The cost is real and worth stating: a plan whose newest document reports
    only a total fund return shows only that, even when an older document
    holds a full asset-class breakdown. Recency and completeness genuinely
    conflict here, and mixing them is what produced the incoherent rows.

    Ties on date break towards the document with more asset classes.
    """
    # Grouped by document AND horizon, not document alone. A single board
    # pack often reports a fiscal-year total beside quarterly asset-class
    # figures; treating the document as the unit meant labelling that whole
    # row with one frequency, which is a coin toss between two true answers.
    # Splitting it gives two rows, each of which can be read across.
    by_doc: dict[tuple[str, int | None, str], list[dict]] = {}
    for r in rows:
        key = (r["plan_id"], r["document_id"], r.get("horizon") or "unclear")
        by_doc.setdefault(key, []).append(r)

    def rank(group: list[dict]):
        """Prefer a breakdown over a bare total, then recency, then breadth.

        Strict newest-first cost 27 plans their asset-class detail: their
        latest performance document reports a total-fund number and nothing
        else, so the row collapsed to one cell.
        """
        when = next((g["as_of_date"] for g in group if g["as_of_date"]), None)
        has_detail = any(g["asset_class"] != "total" for g in group)
        return (1 if has_detail else 0, when or date.min, len(group))

    # Two candidates per plan, because recency and comparability are
    # different questions and a plan can answer both from different
    # documents. An annual row is comparable across plans; the latest row is
    # the freshest thing that plan has published. Where the same document
    # wins both, the plan gets one row -- deduplicated below.
    best_annual: dict[str, list[dict]] = {}
    best_other: dict[str, list[dict]] = {}
    for (plan_id, _doc_id, horizon), group in by_doc.items():
        target = best_annual if horizon == "annual" else best_other
        cur = target.get(plan_id)
        if cur is None or rank(group) > rank(cur):
            target[plan_id] = group

    # At most two rows per plan: the comparable annual one, and the freshest
    # non-annual one. A second annual row would just be a staler version of
    # the first, leaving a reader to work out which to trust.
    chosen: list[list[dict]] = []
    for plan_id in set(best_annual) | set(best_other):
        for group in (best_annual.get(plan_id), best_other.get(plan_id)):
            if group is not None:
                chosen.append(group)

    # A single document can report the same class twice (a summary table and
    # a detail table). Keep the first: the table is unique on
    # (plan_id, document_id, asset_class), so a duplicate is an insert
    # failure, not a cosmetic issue.
    out, seen = [], set()
    for group in chosen:
        for r in group:
            key = (r["plan_id"], r["document_id"], r.get("horizon"), r["asset_class"])
            if key in seen:
                continue
            seen.add(key)
            out.append(r)
    return out


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
