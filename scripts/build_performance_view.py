"""Collate the asset-class returns this corpus already holds.

    python -m scripts.build_performance_view

Makes no API calls and extracts nothing. Three sources carry returns by
asset class:

  * ``cafr_performance``          fiscal-year returns from CAFR extraction
  * ``summaries.performance_data``  a by-product of ordinary summarising,
                                    4,233 data points across 925 documents
                                    in 2026 alone, surfaced nowhere
  * ``document_section_read``     figures read from a window chosen for
                                  holding numbers, for the documents the
                                  summariser saw only the first tenth of

Where a document has both a targeted read and a summariser reading, the
targeted read supersedes it: same table, but one of them actually saw it.

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
from sqlalchemy import inspect as sa_inspect
from sqlalchemy import text

import database
import queries
from database import (CafrExtract, CafrPerformance, Document,
                      DocumentSectionRead, PlanAssetClassHorizon,
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
# "ITD" is inception-to-date, this corpus's abbreviation for what "Since
# Inception" spells out (131 "ITD IRR%" rows). Word-bounded so it cannot fire
# inside an unrelated word.
_INCEPTION = re.compile(r"(?i)since inception|since \d{4}|inception|\bitd\b")
# Any N-year window other than one. "20-Year Forward" and "3-Year as of" both
# land here; a forward-looking assumption is not a return at all, but it is
# certainly not this year's.
#
# The negative lookahead originally spelled out only "1[- ]?year", never the
# "yr" abbreviation -- so "1 Yr" (572 rows in the corpus) satisfied this
# pattern (digit "1" + "yr") and was filed as multi_year, indistinguishable
# from an actual 3- or 10-year annualised figure. "(?:year|yr)" excludes
# both spellings of one year, the same way _ONE_YEAR below now recognises
# both when it picks up what this pattern excludes.
_MULTI = re.compile(r"(?i)\b(?!1[- ]?(?:year|yr))(\d{1,2}|three|five|ten|twenty)[- ]?(year|yr)")
_MONTH_NAMES = (r"january|february|march|april|may|june|july|august|"
                r"september|october|november|december")
# "Mo" is this corpus's abbreviation for "Month" (187 "1 Mo" rows) and "Last
# Month" is spelled out rather than numbered (285 rows) -- both as common as
# the "1-month" / "MTD" forms already handled.
_MONTHLY = re.compile(
    r"(?i)\b1[- ]?month|\b1[- ]?mo\b|\blast month\b|\bmtd\b|\(1 month\)"
    r"|month[- ]?to[- ]?date"
    # A column headed just "Month" (113 rows). Anchored to the whole label so
    # it cannot swallow "3 Month", which _QUARTER must keep.
    r"|^\s*month\s*$"
    rf"|^\s*({_MONTH_NAMES})\s+\d{{4}}\s*$")
# "Qtr" (640 "Last Qtr" rows) and "Mo" (180 "3 Mo" rows) are this corpus's
# abbreviations; QTD is bucketed with the other quarter-length readings
# rather than with "partial", unlike *YTD -- a deliberate choice, not an
# oversight, since a quarter-to-date figure is still a quarter-length read.
_QUARTER = re.compile(
    r"(?i)\bq[1-4]\b|\b[1-4]q\b|quarter|\bqtr\b|\bqtd\b"
    r"|\b3[- ]?months?\b|\b3[- ]?mo\b")
# Was `\bf?ytd\b`: the leading `\b` before the optional "f" meant only a bare
# "YTD" or "FYTD" could match, because "C" immediately before "YTD" is a
# word character and breaks the boundary. "CYTD" (calendar-YTD, 643 rows)
# needs any prefix letter accepted, not just "f" -- `\w*ytd\b` matches CYTD,
# FYTD and bare YTD alike, all still "partial".
_PARTIAL = re.compile(r"(?i)\w*ytd\b|to date|through")
# "FYE 6/30/25" / "FYE 6/30/24" (fiscal-year-*end*, followed by the end
# date) is a different shape from the bare "FY2025" the original regex
# expected -- there is a literal "E" between "fy" and the date, so
# `fy\s?\d{4}` never matched it. 396 + 381 rows.
_FISCAL = re.compile(r"(?i)\bfy\s?\d{4}|\bfye\b|fiscal")
# "CYE 12/31/24" is the calendar-year twin of FYE: calendar-year-*end*
# followed by the end date, not `cy` directly against a 4-digit year. 146
# rows.
# "1[- ]?yr" is the abbreviation _MULTI's negative lookahead now excludes
# (see the comment there); it has to be recognised here too, or those 572
# rows go from wrongly "multi_year" to wrongly "unclear" instead of landing
# on the "annual" they actually are. \b on both sides so it only matches the
# standalone token, not the tail of "21 Yr" (which _MULTI already claims
# first, since it runs before this in horizon_of's if-chain).
_ONE_YEAR = re.compile(
    r"(?i)1[- ]?year|\b1[- ]?yr\b|12 month|twelve month|calendar year"
    r"|\bcy\s?\d{4}|\bcye\b")
# A bare four-digit year and nothing else -- "2023", "2024" etc, 368+328+
# 317+316+299 rows in the corpus. Anchored to the whole (stripped) label so
# it never fires on a year embedded in a longer, more specific label that
# an earlier, narrower check should win instead (e.g. "3-Year as of 2023").
_BARE_YEAR = re.compile(r"^(19|20)\d{2}$")

# Not a period at all -- a forward-looking actuarial assumption the
# extractor filed under "period" anyway (240 "Long-Term Expected Real Rate
# of Return" rows). "Assumed" is the sibling term for the same concept
# elsewhere in this codebase (extract_cafr_actuarial.py's
# `assumed_return_pct`, kept separate from realized performance on
# purpose), so it is included on the same reasoning even though only
# "Expected" is attested in the measured corpus. Deliberately narrow -- it
# must match the "rate of return" phrase, not just "expected"/"assumed"
# alone, so a real but oddly-labelled return period (e.g. "Expected Q3
# Report") is never the one that gets dropped.
_NOT_A_RETURN_PERIOD = re.compile(
    r"(?i)\b(expected|assumed)\b.*\brate of return\b")


#: A period given as two dates rather than a length: "7/1/2024 - 6/30/2025".
#: Both endpoints are stated, so the length is arithmetic. En dash and em dash
#: appear as often as the hyphen, and a mangled PDF text layer turns either
#: into U+FFFD, so that is accepted as a separator too.
_DATE_RANGE = re.compile(
    r"(\d{1,2}/\d{1,2}/\d{2,4})\s*(?:-|–|—|�|to)\s*"
    r"(\d{1,2}/\d{1,2}/\d{2,4})")

#: Day counts a period of each length can plausibly span, allowing for
#: month-end and leap years. Deliberately narrow: a span that matches none of
#: these is left unclear rather than rounded into the nearest bucket, because
#: a six-month period has no bucket and calling it "annual" would be a
#: fabrication rather than an approximation.
_SPAN_BUCKETS = ((26, 35, "month"), (85, 96, "quarter"), (358, 375, "annual"))


def _horizon_from_span(label: str) -> str | None:
    """Length of an explicit date range, if it is one and the span is known.

    Returns None rather than guessing. "4/1/2022 - 6/30/2022" is a quarter and
    "7/1/2024 - 6/30/2025" a fiscal year, both by arithmetic; a 180-day range
    is a real period this vocabulary has no name for, so it stays unclear.
    """
    m = _DATE_RANGE.search(label)
    if not m:
        return None
    try:
        start, end = (datetime.strptime(g, "%m/%d/%Y").date()
                      if len(g.rsplit("/", 1)[-1]) == 4
                      else datetime.strptime(g, "%m/%d/%y").date()
                      for g in m.groups())
    except ValueError:
        return None
    days = (end - start).days
    if days <= 0:
        return None
    for low, high, bucket in _SPAN_BUCKETS:
        if low <= days <= high:
            return bucket
    return None


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

    A bare period-end date such as "12/31/24" is deliberately left
    "unclear" rather than guessed as "annual". It states only when a period
    ended, not how long it was -- pension performance tables report
    quarter-end and year-end figures from the same four dates a year
    (3/31, 6/30, 9/30, 12/31), so nothing in the string distinguishes a
    year's return from a quarter's. Calling it "annual" would silently
    mislabel every quarterly one of these; "unclear" is honest about not
    knowing, which is the same principle "FYE"/"CYE" rely on the other way
    -- those *do* carry an explicit year-length qualifier before the date,
    which is what earns them "annual".
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
    if _FISCAL.search(t) or _ONE_YEAR.search(t) or _BARE_YEAR.match(t):
        return "annual"
    # Last, because it is the only rule that measures rather than matches, and
    # every label above says what it is in words. A range gives both
    # endpoints, so its length is arithmetic rather than a guess -- which is
    # exactly what a bare end date like "12/31/24" cannot offer and why that
    # one stays unclear.
    spanned = _horizon_from_span(t)
    if spanned:
        return spanned
    return "unclear"


# Word forms attested alongside the digit forms in the corpus (e.g.
# "Three-Year", "Five Year"). Only the multi-year words horizon_of's _MULTI
# pattern recognises need an entry here.
_WORD_YEARS = {"three": 3, "five": 5, "ten": 10, "twenty": 20}


def horizon_key(period_label: str | None) -> str | None:
    """A finer horizon than horizon_of, for the per-asset-class view.

    horizon_of's "multi_year" bucket exists to keep a quarterly return out
    of the same column as an annual one -- but it does that by lumping every
    N-year annualised figure (3, 5, 10, 20, 30) into one bucket, which
    recreates the identical mistake one level down: a 3-year return and a
    10-year return are not comparable either, and the per-asset-class view
    is built entirely out of putting one horizon side by side across plans.

    Does NOT replace horizon_of -- the existing plan_asset_class_performance
    view and its 'horizon' column depend on horizon_of's coarser buckets
    unchanged, so this is a second, finer read of the same label rather than
    a modification of the first.

    Returns '3y', '5y', '10y' etc. for a multi-year label (digits and the
    words three/five/ten/twenty are both attested in the corpus), the
    unchanged horizon_of bucket name for anything else horizon_of can place,
    and None for 'unclear' -- nothing keyed on an unknown horizon is usable,
    so a caller building a table keyed on this value must be able to drop
    the row rather than invent a bucket for it.
    """
    h = horizon_of(period_label)
    if h == "unclear":
        return None
    if h != "multi_year":
        return h
    # period_label is not None here: horizon_of("unclear") only for a falsy
    # label, and multi_year requires _MULTI to have matched something.
    m = _MULTI.search(period_label.strip())
    if not m:
        return None
    token = m.group(1).lower()
    n = _WORD_YEARS.get(token)
    if n is None and token.isdigit():
        n = int(token)
    return f"{n}y" if n else None


class ClassMap(dict):
    """The mapping, plus a case-folded index of the labels it can fold safely.

    The map was built from mixed-case labels ("Public Equity"), but board
    documents shout their table headings ("PUBLIC EQUITY"), and an exact
    lookup misses every one. Measured on the targeted reads: 6,360 of 8,373
    rows failed to canonicalise, and the commonest misses were PUBLIC EQUITY,
    REAL ESTATE and PRIVATE EQUITY -- all present in the map in title case.

    Folding is not unconditionally safe, which is why this is an index rather
    than a `.lower()` on both sides. Three labels map to two different
    canonicals depending on case ("total fixed income" is 'total' in one
    casing and 'fixed_income_core' in another). Those are left out, so they
    keep today's behaviour of not matching rather than silently picking one.
    """

    def __init__(self, data):
        super().__init__(data)
        by_low: dict[str, dict] = {}
        for key, entry in data.items():
            canon = (entry or {}).get("canonical")
            slot = by_low.setdefault(key.lower(), {"canons": set(), "entry": entry})
            slot["canons"].add(canon)
        self.folded = {k: v["entry"] for k, v in by_low.items()
                       if len(v["canons"]) == 1}


def load_class_map() -> ClassMap:
    with io.open(MAPPINGS, encoding="utf-8") as f:
        return ClassMap(json.load(f))


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
    if entry is None:
        # Then case, which is a formatting choice in the source document and
        # not a distinction between asset classes.
        folded = getattr(class_map, "folded", None)
        if folded is None:
            folded = ClassMap(class_map).folded
        entry = folded.get(" ".join(key.split()).lower())
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


def _rows_from_payload(payload, plan_id, meeting_date, doc_id,
                       class_map, source: str) -> list[dict]:
    """Parse one JSON list of returns into view rows.

    Shared because the summariser and the targeted read emit the same shape
    on purpose -- that is what lets a targeted read drop into this builder
    with no new parser.
    """
    try:
        items = json.loads(payload)
    except (TypeError, ValueError):
        return []
    if not isinstance(items, list):
        return []

    rows = []
    for item in items:
        if not isinstance(item, dict):
            continue
        period = item.get("period") or ""
        if _NOT_A_RETURN_PERIOD.search(period):
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
            "source": source,
            "document_id": doc_id,
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
        rows += _rows_from_payload(payload, plan_id, meeting_date, doc_id,
                                   class_map, "board_doc")
    return rows


def collect_from_section_reads(session, class_map,
                               since: date | None) -> list[dict]:
    """Returns read from a located section rather than from the opening tenth.

    The summariser fills a ~50,000-character budget from the front of the
    document, chosen to write a good summary rather than to find a table. On
    a real board pack the performance headings begin 31% in. These rows come
    from a window picked for holding numbers, so for a truncated document
    they are strictly better evidence than the summariser's.
    """
    q = (session.query(DocumentSectionRead.returns_json, Document.plan_id,
                       Document.meeting_date, Document.id)
         .join(Document, Document.id == DocumentSectionRead.document_id)
         .filter(DocumentSectionRead.returns_json.isnot(None),
                 DocumentSectionRead.returns_json.notin_(("[]", ""))))
    if since:
        q = q.filter(Document.meeting_date >= since)

    rows = []
    for payload, plan_id, meeting_date, doc_id in q:
        rows += _rows_from_payload(payload, plan_id, meeting_date, doc_id,
                                   class_map, "targeted_read")
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


# Tie-break order for pick_best_per_cell when two readings share an as_of
# date: a targeted read saw the table the summariser only glimpsed in the
# first 50,000 characters, and a CAFR figure is at least audited, so both
# outrank the summariser. Higher wins.
_SOURCE_RANK = {"targeted_read": 2, "cafr": 1, "board_doc": 0}


def pick_best_per_cell(rows: list[dict]) -> list[dict]:
    """Best available reading per (plan, asset_class, horizon_key) cell.

    This is the selection rule for PlanAssetClassHorizon, and it is
    deliberately not pick_latest. pick_latest's unit of selection is the
    *document*, because plan_asset_class_performance is read across asset
    classes for one plan and a row that mixed documents there would silently
    claim to be a portfolio it is not. This table is read the other way --
    one asset class, across plans -- so a row here never makes that claim in
    the first place: mixing a 2024 CAFR's 10-year figure with a 2026 board
    pack's quarter figure for the same plan is not "a portfolio as of one
    date", it is two independent facts about that plan's real-estate
    program, each stamped with its own as_of_date so a reader sees exactly
    what is being mixed.

    So the unit of selection is the CELL, and a cell is per QUARTER: for each
    (plan_id, asset_class, horizon_key, period_end), keep the single reading
    with the most recent as_of_date. Ties break toward the more direct source
    -- targeted_read, then cafr, then the summariser (_SOURCE_RANK).

    period_end is in the key deliberately, and was not always. Without it the
    table held one reading per cell full stop -- the latest -- so a plan with
    both a 2025Q4 and a 2026Q1 private-equity figure kept only 2026Q1 and
    vanished from any sweep of 2025Q4. It was a snapshot being asked history
    questions: 26 plans reported a 2025Q4 private-equity return and the view
    could show 17. Keying on the quarter as well costs 2.7x the rows (2,514 ->
    6,761) and answers "how did everyone do in 2025Q4" properly.

    Two readings of the SAME quarter still collapse to one, which is the point
    of keeping the date and source tie-breaks: a figure restated in a later
    pack supersedes the earlier printing of the same period.

    A row with no horizon_key (period_label classifies as 'unclear') is
    dropped outright: nothing keyed on an unknown horizon is usable. A row
    with no resolvable period_end is kept under a NULL quarter rather than
    discarded -- it is one cell, not a series, and dropping it would lose a
    figure the previous key retained.
    """
    best: dict[tuple[str, str, str, str | None], dict] = {}
    for r in rows:
        key_h = horizon_key(r.get("period_label"))
        if key_h is None:
            continue
        period_end = queries.period_end_quarter(r.get("period_label"),
                                                r.get("as_of_date"))
        cell = (r["plan_id"], r["asset_class"], key_h, period_end)
        candidate = {
            "plan_id": r["plan_id"],
            "asset_class": r["asset_class"],
            "horizon_key": key_h,
            "return_pct": r["return_pct"],
            "period_label": r["period_label"],
            "as_of_date": r["as_of_date"],
            "period_end": period_end,
            "source": r["source"],
            "document_id": r["document_id"],
        }
        current = best.get(cell)
        if current is None:
            best[cell] = candidate
            continue
        cur_date = current["as_of_date"] or date.min
        new_date = candidate["as_of_date"] or date.min
        if new_date > cur_date:
            best[cell] = candidate
        elif new_date == cur_date and (
                _SOURCE_RANK.get(candidate["source"], 0)
                > _SOURCE_RANK.get(current["source"], 0)):
            best[cell] = candidate
    return list(best.values())


def _recreate_horizon_table_if_stale(session) -> bool:
    """Drop and recreate plan_asset_class_horizon when its columns have moved.

    There is no migration framework here, and `init_db()` cannot help:
    `create_all` skips a table that already exists, so a column added to the
    model appears in Python and not in Postgres, and the first insert fails on
    the missing column. For a *derived* table that is not a problem worth a
    migration -- nothing here is a source of record, every row is rebuilt from
    summaries, section reads and CAFR extracts a few lines above -- so the
    honest answer is to drop it and let create_all build the current shape.

    Deliberately narrow. It compares the live columns against the model and
    acts only when they differ, so an ordinary rebuild does not drop and
    recreate a table for no reason. It is also the only table in this repo
    where dropping is safe, which is why this is not a general helper.
    """
    inspector = sa_inspect(session.get_bind())
    if not inspector.has_table(PlanAssetClassHorizon.__tablename__):
        database.Base.metadata.create_all(
            session.get_bind(),
            tables=[PlanAssetClassHorizon.__table__])
        return True

    live = {c["name"] for c in
            inspector.get_columns(PlanAssetClassHorizon.__tablename__)}
    wanted = {c.name for c in PlanAssetClassHorizon.__table__.columns}
    if live == wanted:
        return False

    console.print(
        f"[yellow]plan_asset_class_horizon shape changed "
        f"(missing {sorted(wanted - live)}, extra {sorted(live - wanted)}) "
        f"-- dropping and recreating[/yellow]")
    session.commit()          # close any open transaction before DDL
    PlanAssetClassHorizon.__table__.drop(session.get_bind(), checkfirst=True)
    database.Base.metadata.create_all(
        session.get_bind(), tables=[PlanAssetClassHorizon.__table__])
    return True


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
        targeted = collect_from_section_reads(session, class_map, since)
        board = collect_from_summaries(session, class_map, since)

        # Where a document has both, the targeted read supersedes the
        # summariser outright rather than merging with it. pick_latest groups
        # by (plan, document, horizon), so leaving both in would put two
        # readings of the same table in one group, inflate the breadth tiebreak
        # with duplicates, and settle each clash by list order — which is not
        # a decision anyone made. The targeted read saw the table; the
        # summariser saw the first 50,000 characters.
        read_docs = {r["document_id"] for r in targeted}
        board = [r for r in board if r["document_id"] not in read_docs]

        pool = cafr + targeted + board
        chosen = pick_latest(pool)

        session.execute(text("DELETE FROM plan_asset_class_performance"))
        for r in chosen:
            session.add(PlanAssetClassPerformance(**r))
        session.commit()

        plans = len({r["plan_id"] for r in chosen})
        classes = len({r["asset_class"] for r in chosen})
        n = {}
        for r in chosen:
            n[r["source"]] = n.get(r["source"], 0) + 1
        console.print(
            f"[green]{len(chosen)}[/green] rows across [green]{plans}[/green] "
            f"plans and {classes} asset classes "
            f"({n.get('targeted_read', 0)} targeted reads, "
            f"{n.get('board_doc', 0)} summariser, {n.get('cafr', 0)} CAFR)"
        )
        console.print(f"[dim]candidates seen: {len(cafr)} CAFR, "
                      f"{len(targeted)} targeted, {len(board)} summariser "
                      f"(after {len(read_docs)} superseded documents)[/dim]")

        # Same pool, a different (finer, per-cell) selection rule -- see
        # pick_best_per_cell's docstring for why this is allowed to mix
        # documents across horizons where pick_latest is not.
        cells = pick_best_per_cell(pool)

        _recreate_horizon_table_if_stale(session)
        session.execute(text("DELETE FROM plan_asset_class_horizon"))
        for r in cells:
            session.add(PlanAssetClassHorizon(**r))
        session.commit()

        h_plans = len({r["plan_id"] for r in cells})
        h_classes = len({r["asset_class"] for r in cells})
        hn = {}
        for r in cells:
            hn[r["source"]] = hn.get(r["source"], 0) + 1
        console.print(
            f"[green]{len(cells)}[/green] per-asset-class-horizon cells across "
            f"[green]{h_plans}[/green] plans and {h_classes} asset classes "
            f"({hn.get('targeted_read', 0)} targeted reads, "
            f"{hn.get('board_doc', 0)} summariser, {hn.get('cafr', 0)} CAFR)"
        )
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
