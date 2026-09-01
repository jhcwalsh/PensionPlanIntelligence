"""Read layer: every query the UI needs, in one Streamlit-free module.

The rule (see docs/superpowers/specs/2026-08-16-low-maintenance-app-design.md
§2): **app.py contains no queries.** Each function here takes a session and
returns plain Python — dicts, lists, scalars — never a DataFrame and never a
Streamlit call. app.py wraps the result for display.

Two reasons this matters beyond tidiness:

1. It is what makes the phase-2 static-site port a front-end-only job. A build
   script or an API consumes exactly these functions.
2. It makes the queries testable without a Streamlit runtime, which app.py's
   own functions are not.

ORM objects are still returned by a few functions whose callers walk
relationships (``recent_summaries``, ``plans``). Converting those to dicts
means touching every attribute access in the templates, so it is deliberately
left for a follow-up rather than bundled into the extraction.
"""

from __future__ import annotations

import calendar
import json
import re
from datetime import date, datetime, timedelta
from pathlib import Path

from sqlalchemy import case, desc, distinct, func, or_

from database import (
    as_utc,
    utcnow,
    sort_key,
    CafrAllocation,
    CafrExtract,
    CafrPerformance,
    ApiUsage,
    CafrRefreshLog,
    Document,
    DocumentSkip,
    ExtractionDetail,
    FetchRun,
    MeetingRecording,
    PerformanceReportExtract,
    PerformanceReportReturn,
    Plan,
    PlanVideoSource,
    Publication,
    Summary,
    get_twin_index,
)

KNOWN_PLANS_PATH = Path(__file__).parent / "data" / "known_plans.json"


# ---------------------------------------------------------------------------
# Plans and headline counts
# ---------------------------------------------------------------------------

def plans(session) -> list[Plan]:
    """Every tracked plan, alphabetically. Returns ORM objects."""
    return session.query(Plan).order_by(Plan.name).all()


def recent_summaries(session, plan_id=None, limit: int = 20):
    """Most recent summarized documents, newest meeting first.

    Filters out CAFRs / performance reports and caps meeting_date at today +
    60 days so the by-document view of Activity matches the other two views —
    see ``database.get_new_meetings`` for the same filter rationale.

    Returns a list of (Document, Summary) ORM tuples.
    """
    future_cap = utcnow() + timedelta(days=60)
    q = (
        session.query(Document, Summary)
        .join(Summary, Document.id == Summary.document_id)
        .filter(Document.doc_type.notin_(["cafr", "performance"]))
        .filter((Document.meeting_date.is_(None)) |
                (Document.meeting_date <= future_cap))
    )
    if plan_id and plan_id != "All":
        q = q.filter(Document.plan_id == plan_id)
    return (q.order_by(Document.meeting_date.desc().nullslast(),
                       Document.id.desc())
            .limit(limit).all())


def corpus_stats(session) -> tuple[int, int, int, int]:
    """(plans, documents, downloaded, summarized) counts for the header."""
    return (
        session.query(Plan).count(),
        session.query(Document).count(),
        session.query(Document).filter(
            Document.extraction_status == "done").count(),
        session.query(Summary).count(),
    )


# ---------------------------------------------------------------------------
# Coverage tables
# ---------------------------------------------------------------------------

def plan_coverage_rows(session) -> list[dict]:
    """Per-plan document coverage for the Admin page, one dict per plan."""
    rows = (
        session.query(
            Plan.name.label("plan"),
            Plan.abbreviation.label("abbrev"),
            Plan.state.label("state"),
            func.count(distinct(Document.id)).label("downloaded"),
            func.sum(
                case((Document.extraction_status == "done", 1), else_=0)
            ).label("extracted"),
            func.count(distinct(Summary.id)).label("summarized"),
            func.max(Document.downloaded_at).label("last_download"),
        )
        .outerjoin(Document, Document.plan_id == Plan.id)
        .outerjoin(Summary, Summary.document_id == Document.id)
        .group_by(Plan.id)
        .order_by(Plan.name)
        .all()
    )
    return [
        {
            "Plan": r.plan,
            "Abbrev": r.abbrev or "",
            "State": r.state or "",
            "Downloaded": int(r.downloaded or 0),
            "Extracted": int(r.extracted or 0),
            "Summarized": int(r.summarized or 0),
            "Last download": (
                r.last_download.strftime("%Y-%m-%d %H:%M")
                if r.last_download else "—"
            ),
        }
        for r in rows
    ]


def plans_index_rows(session) -> list[dict]:
    """The Plans tab table: every plan, with twin metadata where it exists.

    Both queries are needed. ``get_twin_index`` inner-joins twin_snapshots, so
    it omits any plan that has no snapshot yet — a plan just added to
    known_plans.json, or one whose ``build_twin`` raised (twin_builder catches
    per-plan and moves on). Sourcing the row list from `plans` keeps those
    visible with an em-dash twin column.
    """
    twin_meta = {r["plan_id"]: r for r in get_twin_index(session)}
    plan_cols = (
        session.query(Plan.id, Plan.name, Plan.abbreviation,
                      Plan.state, Plan.aum_billions)
        .order_by(Plan.name)
        .all()
    )
    rows = []
    for plan_id, name, abbreviation, state, aum_billions in plan_cols:
        meta = twin_meta.get(plan_id)
        if meta:
            comp = meta["completeness"]
            completeness = f"{(sum(comp.values()) / len(comp)):.0%}" if comp else "—"
            # built_at is a datetime, not an ISO string — slicing it raises.
            twin_built = (meta["built_at"].strftime("%Y-%m-%d")
                          if meta["built_at"] else "—")
        else:
            completeness = "—"
            twin_built = "—"
        rows.append({
            "Plan": abbreviation or plan_id,
            "Name": name,
            "State": state or "—",
            "AUM ($B)": aum_billions,
            "Twin built": twin_built,
            "Completeness": completeness,
            "Twin": f"?plan={plan_id}",
        })
    return rows


# ---------------------------------------------------------------------------
# CAFR extraction
# ---------------------------------------------------------------------------

def _aggregator_plan_ids() -> set[str]:
    """Plans whose CAFR covers a system-of-systems (e.g. NYC Retirement).

    The structured extractor skips these deliberately — their asset-allocation
    tables do not map to a single plan — so they are bucketed separately
    rather than sitting forever as "Pending extract".

    Read from known_plans.json directly, not via fetcher.load_plans, so this
    module does not drag in the pipeline-side bs4 / Playwright dependencies
    that are not installed on the Render web service.
    """
    with open(KNOWN_PLANS_PATH, encoding="utf-8") as f:
        meta = json.load(f)
    return {m["id"] for m in meta
            if (m.get("cafr_format") or "").lower() == "aggregator"}


def cafr_coverage_rows(session) -> list[dict]:
    """Per-plan CAFR + extraction coverage, one dict per tracked plan.

    Picks the most recent CAFR per plan (highest fiscal_year, then most recent
    downloaded_at) and joins to its CafrExtract if one exists, plus allocation
    and performance row counts. The "latest per plan" reduction happens in
    Python to avoid SQL nullslast portability concerns.
    """
    cafr_rows = (
        session.query(Document)
        .filter(Document.doc_type == "cafr")
        .order_by(Document.plan_id)
        .all()
    )
    latest_cafr: dict[str, Document] = {}
    for d in cafr_rows:
        prev = latest_cafr.get(d.plan_id)
        if prev is None:
            latest_cafr[d.plan_id] = d
            continue
        prev_key = (prev.fiscal_year or 0, sort_key(prev.downloaded_at))
        d_key = (d.fiscal_year or 0, sort_key(d.downloaded_at))
        if d_key > prev_key:
            latest_cafr[d.plan_id] = d

    extracts = {e.document_id: e for e in session.query(CafrExtract).all()}
    alloc_counts = dict(
        session.query(CafrAllocation.cafr_extract_id,
                      func.count(CafrAllocation.id))
        .group_by(CafrAllocation.cafr_extract_id).all()
    )
    perf_counts = dict(
        session.query(CafrPerformance.cafr_extract_id,
                      func.count(CafrPerformance.id))
        .group_by(CafrPerformance.cafr_extract_id).all()
    )
    aggregator_ids = _aggregator_plan_ids()

    rows = []
    for p in session.query(Plan).order_by(Plan.name).all():
        doc = latest_cafr.get(p.id)
        if doc is None:
            status, cafr_fy, downloaded, url = "Missing CAFR", "", "", ""
            extracted, extract_fy, alloc, perf = "No", "", 0, 0
        else:
            cafr_fy = str(doc.fiscal_year) if doc.fiscal_year else ""
            downloaded = (doc.downloaded_at.strftime("%Y-%m-%d")
                          if doc.downloaded_at else "")
            url = doc.url or ""
            ext = extracts.get(doc.id)
            if p.id in aggregator_ids:
                status, extracted, extract_fy, alloc, perf = (
                    "Aggregator (skipped)", "N/A", "", 0, 0)
            elif ext is None:
                status, extracted, extract_fy, alloc, perf = (
                    "Pending extract", "No", "", 0, 0)
            else:
                status, extracted = "Extracted", "Yes"
                extract_fy = str(ext.fiscal_year) if ext.fiscal_year else ""
                alloc = int(alloc_counts.get(ext.id, 0))
                perf = int(perf_counts.get(ext.id, 0))

        rows.append({
            "plan_id": p.id,
            "Plan": p.abbreviation or p.name,
            "Name": p.name,
            "State": p.state or "",
            "FYE": p.fiscal_year_end or "",
            "Status": status,
            "CAFR FY": cafr_fy,
            "Source": url or None,
            "Extract FY": extract_fy,
            "# Asset classes": alloc,
            "# Perf rows": perf,
            "Downloaded": downloaded,
        })
    return rows


def cafr_plan_detail(session, plan_id: str) -> dict:
    """Latest CAFR extract for a plan, with allocations and performance.

    Returns {} when the plan is unknown, and {"plan": {...}} only when the
    plan exists but has no extract yet.
    """
    plan = session.query(Plan).filter_by(id=plan_id).first()
    if plan is None:
        return {}

    plan_dict = {"name": plan.name, "abbreviation": plan.abbreviation,
                 "state": plan.state}
    extract = (
        session.query(CafrExtract)
        .filter(CafrExtract.plan_id == plan_id)
        .order_by(CafrExtract.fiscal_year.desc(), CafrExtract.id.desc())
        .first()
    )
    if extract is None:
        return {"plan": plan_dict}

    allocations = (
        session.query(CafrAllocation)
        .filter(CafrAllocation.cafr_extract_id == extract.id)
        .order_by(CafrAllocation.id).all()
    )
    performance = (
        session.query(CafrPerformance)
        .filter(CafrPerformance.cafr_extract_id == extract.id)
        .order_by(CafrPerformance.scope, CafrPerformance.period).all()
    )
    document = session.query(Document).filter_by(id=extract.document_id).first()

    return {
        "plan": plan_dict,
        "extract": {
            "id": extract.id,
            "fiscal_year": extract.fiscal_year,
            "extracted_at": extract.extracted_at,
            "model_used": extract.model_used,
            "pages_used": extract.pages_used,
            "investment_policy_text": extract.investment_policy_text,
            "notes": extract.notes,
        },
        "document": {
            "id": document.id,
            "url": document.url,
            "filename": document.filename,
        } if document else None,
        "allocations": [
            {
                "Asset class": a.asset_class,
                "Target %": a.target_pct,
                "Actual %": a.actual_pct,
                "Range low %": a.target_range_low,
                "Range high %": a.target_range_high,
                "Notes": a.notes or "",
            }
            for a in allocations
        ],
        "performance": [
            {
                "Scope": p.scope,
                "Period": p.period,
                "Return %": p.return_pct,
                "Benchmark %": p.benchmark_return_pct,
                "Benchmark": p.benchmark_name or "",
                "Notes": p.notes or "",
            }
            for p in performance
        ],
    }


def cafr_extract_fy_range(session) -> tuple[int, int] | None:
    """Min/max fiscal_year across CAFR extracts, or None when there are none."""
    row = (
        session.query(func.min(CafrExtract.fiscal_year),
                      func.max(CafrExtract.fiscal_year))
        .filter(CafrExtract.fiscal_year.isnot(None))
        .one_or_none()
    )
    if not row or row[0] is None or row[1] is None:
        return None
    return int(row[0]), int(row[1])


ALLOCATION_COLUMNS = ("plan_id", "plan_name", "abbreviation", "state",
                      "fiscal_year", "asset_class", "target_pct", "actual_pct")


def allocation_rows(session, match_patterns: tuple, exclude_patterns: tuple,
                    min_fy: int | None = None) -> list[tuple]:
    """Latest-extract allocation rows matching an asset-class pattern set.

    Keeps only rows where both target_pct and actual_pct are populated.
    Returns raw tuples in ``ALLOCATION_COLUMNS`` order; per-plan
    de-duplication is a presentation concern and stays with the caller.

    The "latest extract per plan" subquery applies the same ``min_fy`` filter
    as the outer query. Otherwise a plan whose newest CAFR is older than
    min_fy correctly drops out, but a plan whose newest CAFR is newer would
    still be picked even when the user asked only for older data.
    """
    latest_extract_q = (
        session.query(func.max(CafrExtract.id))
        .filter(CafrExtract.plan_id == Plan.id)
    )
    if min_fy is not None:
        latest_extract_q = latest_extract_q.filter(
            CafrExtract.fiscal_year >= min_fy)
    latest_extract_id = latest_extract_q.correlate(Plan).scalar_subquery()

    asset_class_lower = func.lower(CafrAllocation.asset_class)
    query = (
        session.query(
            Plan.id, Plan.name, Plan.abbreviation, Plan.state,
            CafrExtract.fiscal_year, CafrAllocation.asset_class,
            CafrAllocation.target_pct, CafrAllocation.actual_pct,
        )
        .join(CafrExtract, CafrExtract.id == latest_extract_id)
        .join(CafrAllocation, CafrAllocation.cafr_extract_id == CafrExtract.id)
        .filter(or_(*[asset_class_lower.like(p) for p in match_patterns]))
        .filter(CafrAllocation.target_pct.isnot(None))
        .filter(CafrAllocation.actual_pct.isnot(None))
    )
    for pat in exclude_patterns:
        query = query.filter(~asset_class_lower.like(pat))
    return query.order_by(Plan.id, CafrAllocation.id).all()


# ---------------------------------------------------------------------------
# Documents and activity
# ---------------------------------------------------------------------------

def investment_action_docs(session, plan_id, cutoff):
    """Documents with non-empty investment_actions since ``cutoff``.

    Returns (Document, Summary) tuples, newest meeting first. The JSON in
    ``Summary.investment_actions`` is parsed by the caller.
    """
    q = (
        session.query(Document, Summary)
        .join(Summary, Document.id == Summary.document_id)
        .filter(Summary.investment_actions != "[]")
        .filter(Summary.investment_actions.isnot(None))
        .filter(Document.meeting_date >= cutoff)
    )
    if plan_id:
        q = q.filter(Document.plan_id == plan_id)
    return q.order_by(Document.meeting_date.desc()).all()


def documents_by_ids(session, doc_ids, limit: int = 20) -> list[Document]:
    """Documents for a set of ids, newest meeting first, capped at ``limit``."""
    return (
        session.query(Document)
        .filter(Document.id.in_(doc_ids))
        .order_by(Document.meeting_date.desc().nullslast(),
                  Document.id.desc())
        .limit(limit)
        .all()
    )


def document_with_context(session, doc_id):
    """(Document, Plan, Summary) for the ?doc=<id> deep link.

    Any of the three may be None: an unknown id, a document whose plan row is
    missing, or one that has not been summarised yet.
    """
    doc = session.query(Document).get(doc_id)
    if doc is None:
        return None, None, None
    plan = session.query(Plan).get(doc.plan_id) if doc.plan_id else None
    summary = session.query(Summary).filter_by(document_id=doc.id).first()
    return doc, plan, summary


def documents_for_run(session, doc_ids) -> list[tuple]:
    """(plan_name, filename, downloaded_at) for a fetch run's documents."""
    return (
        session.query(Plan.name, Document.filename, Document.downloaded_at)
        .join(Document, Document.plan_id == Plan.id)
        .filter(Document.id.in_(doc_ids))
        .order_by(Document.downloaded_at)
        .all()
    )


# ---------------------------------------------------------------------------
# Admin: runs, failures, CAFR coverage
# ---------------------------------------------------------------------------

def recent_fetch_runs(session, limit: int) -> list[FetchRun]:
    """Most recent pipeline runs, newest first."""
    return (
        session.query(FetchRun)
        .order_by(desc(FetchRun.started_at))
        .limit(limit)
        .all()
    )


def failed_extraction_rows(session) -> list[tuple]:
    """(plan_id, plan_name, doc_id, filename, reason) for failed extractions."""
    return (
        session.query(Plan.id, Plan.name, Document.id, Document.filename,
                      ExtractionDetail.reason)
        .join(Document, Document.plan_id == Plan.id)
        .outerjoin(ExtractionDetail,
                   ExtractionDetail.document_id == Document.id)
        .filter(Document.extraction_status == "failed")
        .order_by(Plan.id, Document.id)
        .all()
    )


def skipped_document_rows(session) -> list[tuple]:
    """(plan_id, plan_name, doc_id, filename, reason, error) for skips."""
    return (
        session.query(Plan.id, Plan.name, Document.id, Document.filename,
                      DocumentSkip.reason, DocumentSkip.error_message)
        .join(Document, Document.plan_id == Plan.id)
        .join(DocumentSkip, DocumentSkip.document_id == Document.id)
        .order_by(Plan.id, Document.id)
        .all()
    )


def cafr_coverage_summary(session) -> dict:
    """Inputs for the admin CAFR-coverage panel.

    ``latest_by_plan`` maps plan_id to its newest CAFR fiscal year (plans with
    no CAFR are absent, which is how the panel counts "none"); ``by_fiscal_year``
    counts CAFR documents per year across all plans, newest first.
    """
    latest_rows = (
        session.query(Document.plan_id,
                      func.max(Document.fiscal_year).label("latest_fy"))
        .filter(Document.doc_type == "cafr")
        .filter(Document.fiscal_year.isnot(None))
        .group_by(Document.plan_id)
        .all()
    )
    by_fy_rows = (
        session.query(Document.fiscal_year,
                      func.count(Document.id).label("n"))
        .filter(Document.doc_type == "cafr")
        .filter(Document.fiscal_year.isnot(None))
        .group_by(Document.fiscal_year)
        .order_by(Document.fiscal_year.desc())
        .all()
    )
    return {
        "latest_by_plan": {pid: fy for pid, fy in latest_rows},
        "plans": session.query(Plan).order_by(Plan.id).all(),
        "by_fiscal_year": by_fy_rows,
    }


def recent_cafr_refresh_runs(session, limit: int) -> list:
    """Distinct CAFR refresh run timestamps, newest first."""
    return [
        row[0] for row in
        session.query(CafrRefreshLog.run_at)
        .distinct()
        .order_by(desc(CafrRefreshLog.run_at))
        .limit(limit)
        .all()
    ]


def cafr_refresh_rows(session, run_ats) -> list[tuple]:
    """Every CAFR refresh log row for the given run timestamps."""
    return (
        session.query(
            CafrRefreshLog.run_at,
            CafrRefreshLog.plan_id,
            CafrRefreshLog.expected_year,
            CafrRefreshLog.status,
            CafrRefreshLog.url_tried,
            CafrRefreshLog.notes,
        )
        .filter(CafrRefreshLog.run_at.in_(run_ats))
        .order_by(desc(CafrRefreshLog.run_at), CafrRefreshLog.plan_id,
                  CafrRefreshLog.id)
        .all()
    )


def plan_labels(session) -> dict[str, tuple[str, str]]:
    """plan_id -> (abbreviation-or-id, name-or-id) for display."""
    return {p.id: (p.abbreviation or p.id, p.name or p.id)
            for p in session.query(Plan).all()}


def plans_by_id(session) -> dict[str, Plan]:
    """plan_id -> Plan, for lookups that would otherwise be N+1."""
    return {p.id: p for p in session.query(Plan).all()}


# ---------------------------------------------------------------------------
# Meeting recordings
# ---------------------------------------------------------------------------

def video_sources(session, plan_id=None) -> list[PlanVideoSource]:
    """Configured video sources, optionally scoped to one plan."""
    q = session.query(PlanVideoSource)
    if plan_id:
        q = q.filter(PlanVideoSource.plan_id == plan_id)
    return q.order_by(PlanVideoSource.plan_id, PlanVideoSource.platform,
                      PlanVideoSource.id).all()


def meeting_recordings(session, plan_id=None) -> list[MeetingRecording]:
    """Catalogued recordings, newest published first, undated last."""
    q = session.query(MeetingRecording)
    if plan_id:
        q = q.filter(MeetingRecording.plan_id == plan_id)
    return q.order_by(
        MeetingRecording.published_at.desc().nullslast(),
        MeetingRecording.discovered_at.desc(),
        MeetingRecording.id.desc(),
    ).all()


# ---------------------------------------------------------------------------
# Publications
# ---------------------------------------------------------------------------

def publications_by_status(session, statuses) -> list[Publication]:
    """Publications in any of ``statuses``, newest period first.

    Note the Drafts view passes ("awaiting_approval",), which no longer
    occurs: every cadence auto-publishes since 2026-08-16, so nothing new
    enters that state. The view is kept for historical rows.
    """
    return (
        session.query(Publication)
        .filter(Publication.status.in_(tuple(statuses)))
        .order_by(Publication.period_start.desc(), Publication.id.desc())
        .all()
    )


def drafts_awaiting_approval(session) -> list[Publication]:
    """Publications still awaiting approval, most recently composed first.

    Always empty for anything composed after 2026-08-16: every cadence
    auto-publishes now, so nothing new enters ``awaiting_approval``. Kept so
    the Drafts tab still shows historical rows rather than erroring.
    """
    return (
        session.query(Publication)
        .filter(Publication.status == "awaiting_approval")
        .order_by(Publication.composed_at.desc())
        .all()
    )


# ---------------------------------------------------------------------------
# API spend
# ---------------------------------------------------------------------------

def api_spend_by_operation(session, days: int = 30) -> list[tuple]:
    """(operation, calls, input_tokens, output_tokens, cost) over a window.

    Ordered by cost so the answer to "what should I optimise" is the first row
    rather than something to scan for.
    """
    cutoff = utcnow() - timedelta(days=days)
    return (
        session.query(
            ApiUsage.operation,
            func.count(ApiUsage.id),
            func.sum(ApiUsage.input_tokens),
            func.sum(ApiUsage.output_tokens),
            func.sum(ApiUsage.cost_usd),
        )
        .filter(ApiUsage.occurred_at >= cutoff)
        .group_by(ApiUsage.operation)
        .order_by(func.sum(ApiUsage.cost_usd).desc(), ApiUsage.operation)
        .all()
    )


def api_spend_by_model(session, days: int = 30) -> list[tuple]:
    """(model, calls, cost) over a window. The other axis of the same data."""
    cutoff = utcnow() - timedelta(days=days)
    return (
        session.query(
            ApiUsage.model,
            func.count(ApiUsage.id),
            func.sum(ApiUsage.cost_usd),
        )
        .filter(ApiUsage.occurred_at >= cutoff)
        .group_by(ApiUsage.model)
        .order_by(func.sum(ApiUsage.cost_usd).desc(), ApiUsage.model)
        .all()
    )


def api_spend_total(session, days: int = 30):
    """Total cost over a window, or 0 when nothing has been recorded yet."""
    cutoff = utcnow() - timedelta(days=days)
    total = (
        session.query(func.sum(ApiUsage.cost_usd))
        .filter(ApiUsage.occurred_at >= cutoff)
        .scalar()
    )
    return total or 0


def weekly_briefings(session, statuses: tuple = ("approved", "published")):
    """Weekly briefings for the archive tab, newest first.

    Reads the database rather than ``notes/``. Weekly composition sets
    ``archive=False`` (insights/weekly.py) because it exists to feed monthly,
    so no ``7day_highlights_*.md`` file has been written since 2026-05-24 --
    while 17 weekly publications with full ``draft_markdown`` accumulated in
    the table. The tab was globbing a filename nothing writes any more.

    Defaults to ``approved``/``published`` to match
    ``monthly._gather_approved_weeklies``: those are the weeks that actually
    feed the monthly, so the archive shows the same set the cascade counts.
    Pass a wider tuple to include the ones stranded by the removed approval
    gate.
    """
    return (
        session.query(Publication)
        .filter(Publication.cadence == "weekly")
        .filter(Publication.status.in_(statuses))
        .order_by(Publication.period_start.desc(), Publication.id.desc())
        .all()
    )


# Canonical asset classes surfaced by the performance table, in display order.
# These are the keys produced by data/asset_class_mappings.json, so the scope
# strings in cafr_performance ("Private Equity", "Private Equity Composite",
# "Real Assets"...) normalise through the same map the allocation views use
# rather than a second, drifting list of synonyms.
PERFORMANCE_CLASSES = (
    ("total", "Total plan"),
    ("private_equity", "Private equity"),
    ("private_credit", "Private credit"),
    ("real_assets_infrastructure", "Real assets"),
    ("real_estate", "Real estate"),
)

# Preference order for "the" headline return. ``fy`` is the fiscal-year figure
# the CAFR itself leads with; ``1y`` is the trailing-twelve-month equivalent
# some plans report instead. Longer windows are deliberately not substituted --
# showing a 3-year annualised return in a column labelled as the latest period
# would be wrong rather than merely incomplete.
PERFORMANCE_PERIODS = ("fy", "1y")


def performance_report_rows(session, class_map: dict) -> list[dict]:
    """One row per plan: latest CAFR's headline return by asset class.

    ``class_map`` is data/asset_class_mappings.json — ``{source_name:
    {"canonical": ...}}``. Scopes that map to ``unmapped``, or that are absent
    from the map entirely, are skipped rather than guessed at.

    Sourced from CAFRs, so the period is a **fiscal year, not a quarter**.
    The 48 ``doc_type='performance'`` documents hold true quarterly reports
    but have no structured extraction yet; when they do, this function is
    where a quarterly source would be merged in. The returned ``Period``
    column names what each row actually is so the distinction stays visible
    in the UI rather than living only here.
    """
    latest = _latest_cafr_extract_per_plan(session)

    rows: list[dict] = []
    for plan, extract, document in latest:
        perf = (
            session.query(CafrPerformance)
            .filter(CafrPerformance.cafr_extract_id == extract.id)
            .all()
        )
        if not perf:
            continue

        # Pick one period for the whole row so the columns are comparable.
        available = {p.period for p in perf}
        period = next((p for p in PERFORMANCE_PERIODS if p in available), None)
        if period is None:
            continue

        by_class: dict[str, float] = {}
        for p in perf:
            if p.period != period or p.return_pct is None:
                continue
            if p.scope == "total_fund":
                canonical = "total"
            else:
                # Entries are either {"canonical": ...} (the raw JSON) or a
                # plain string (twin_builder.load_asset_class_mappings
                # normalises them). Accept both rather than forcing callers
                # to agree on one — the same tolerance twin_builder has.
                entry = class_map.get(p.scope)
                if isinstance(entry, dict):
                    canonical = entry.get("canonical")
                else:
                    canonical = entry or None
            if not canonical or canonical == "unmapped":
                continue
            # First value wins: a plan reporting both "Private Equity" and
            # "Private Equity Composite" would otherwise overwrite one with
            # the other in whatever order the rows happen to come back.
            by_class.setdefault(canonical, p.return_pct)

        row = {
            "Plan": plan.abbreviation or plan.name,
            "plan_id": plan.id,
            "Fiscal year": extract.fiscal_year or document.fiscal_year,
            "Period": period,
        }
        for key, label in PERFORMANCE_CLASSES:
            row[label] = by_class.get(key)
        row["Source"] = document.url
        row["Source date"] = document.downloaded_at
        rows.append(row)

    rows.sort(key=lambda r: r["Plan"] or "")
    return rows


def _latest_cafr_extract_per_plan(session):
    """(Plan, CafrExtract, Document) for each plan's newest extracted CAFR.

    The reduction happens in Python for the same reason cafr_coverage_rows
    does it: picking the max fiscal_year with a NULL-safe tiebreak is a
    portability problem in SQL and a two-line loop here.
    """
    joined = (
        session.query(Plan, CafrExtract, Document)
        .join(CafrExtract, CafrExtract.document_id == Document.id)
        .join(Plan, Plan.id == Document.plan_id)
        .filter(Document.doc_type == "cafr")
        .all()
    )
    best: dict[str, tuple] = {}
    for plan, extract, document in joined:
        fy = extract.fiscal_year or document.fiscal_year or 0
        current = best.get(plan.id)
        if current is None or fy > (current[1].fiscal_year
                                    or current[2].fiscal_year or 0):
            best[plan.id] = (plan, extract, document)
    return list(best.values())


def quarterly_performance_rows(session) -> list[dict]:
    """One row per (plan, fund) latest periodic performance report.

    Sourced from ``performance_report_extract`` / ``_return`` — see
    ``extract_performance_reports.py``'s module docstring for why this is
    currently only ever populated for ``nycrs_comptroller``, and why it is
    one row per constituent fund rather than one row per plan: a single
    "New York City Retirement Systems" plan row would have to pick one of
    NYCERS/TRS/POLICE/FIRE/BERS's returns and label it as the plan's, which
    is not a real number. Kept as a separate table from
    ``performance_report_rows`` for the same reason — merging fund-level
    quarterly rows into the one-row-per-plan CAFR table would silently
    misattribute a sub-fund's return to the whole plan.
    """
    joined = (
        session.query(PerformanceReportExtract, Document, Plan)
        .join(Document, PerformanceReportExtract.document_id == Document.id)
        .join(Plan, Plan.id == PerformanceReportExtract.plan_id)
        .all()
    )

    best: dict[tuple[str, str | None], tuple] = {}
    for extract, document, plan in joined:
        key = (plan.id, extract.fund_scope)
        current = best.get(key)
        current_key = (current[0].as_of_date or "", current[1].downloaded_at) if current else None
        candidate_key = (extract.as_of_date or "", document.downloaded_at)
        if current is None or candidate_key > current_key:
            best[key] = (extract, document, plan)

    rows: list[dict] = []
    for extract, document, plan in best.values():
        returns = (
            session.query(PerformanceReportReturn)
            .filter(PerformanceReportReturn.extract_id == extract.id)
            .filter(PerformanceReportReturn.scope == "total_fund")
            .all()
        )
        by_period = {r.period: r.return_pct for r in returns if r.return_pct is not None}
        if not by_period:
            continue
        rows.append({
            "Plan": plan.abbreviation or plan.name,
            "plan_id": plan.id,
            "Fund": extract.fund_scope or "—",
            "As of": extract.as_of_date,
            "1 month": by_period.get("1mo"),
            "3 months": by_period.get("3mo"),
            "FYTD": by_period.get("fytd"),
            "Source": document.url,
            "Source date": document.downloaded_at,
        })

    rows.sort(key=lambda r: (r["Plan"] or "", r["Fund"] or ""))
    return rows


def cafr_fiscal_year_counts(session, prior_days: int = 30) -> list[dict]:
    """How many plans' latest CAFR is from each fiscal year, and the change.

    The "change since a month ago" is derived rather than stored: the same
    reduction is run twice, once over all CAFRs and once over only those
    downloaded more than ``prior_days`` ago. No snapshot table is needed
    because ``documents.downloaded_at`` already records when each CAFR
    entered the corpus.

    A caveat that matters when reading the deltas: this reconstructs what the
    *latest-CAFR-per-plan* picture looked like a month ago from today's rows.
    It cannot see CAFRs that were deleted since, and a plan whose FY2024 CAFR
    was superseded by FY2025 shows as -1 for 2024 and +1 for 2025 — the plan
    moved between buckets rather than anything being lost.

    Returns newest fiscal year first, so the most recent reporting year leads.
    """
    cutoff = utcnow() - timedelta(days=prior_days)

    docs = (
        session.query(Document.plan_id, Document.fiscal_year,
                      Document.downloaded_at)
        .filter(Document.doc_type == "cafr")
        .filter(Document.fiscal_year.isnot(None))
        .all()
    )

    def _latest_per_plan(rows):
        best: dict[str, int] = {}
        for plan_id, fy, _ in rows:
            if fy is None:
                continue
            if plan_id not in best or fy > best[plan_id]:
                best[plan_id] = fy
        counts: dict[int, int] = {}
        for fy in best.values():
            counts[fy] = counts.get(fy, 0) + 1
        return counts

    now_counts = _latest_per_plan(docs)
    prior_counts = _latest_per_plan(
        [d for d in docs if d[2] is not None and as_utc(d[2]) <= cutoff])

    return [
        {
            "Fiscal year": fy,
            "Plans": now_counts[fy],
            "Change (30d)": now_counts[fy] - prior_counts.get(fy, 0),
        }
        for fy in sorted(now_counts, reverse=True)
    ]


# --------------------------------------------------------------------------
# Which quarter does a figure refer to?
#
# The performance tables stored a verbatim period label and an as-of date, and
# neither answers that. The label is whatever the PDF said -- "FY2025",
# "1 Yr.", "12 months ended March 31, 2026" -- and 54% of the corpus states no
# date at all. The as-of date is the *document's* date: a board pack presented
# on 14 May 2026 reports figures through 31 March, so sorting on it files a
# 2026Q1 return under 2026Q2 and mixes it with the following quarter's.
#
# So resolve both into one period-end date and bucket that. The precedence is
# the whole design: what a label *states* always beats what a document date
# *implies*, and the implied fallback rounds back to the last quarter that had
# closed when the document was written -- never forward, so a figure is never
# claimed to be fresher than the paper carrying it.
#
# Computed here at read time rather than stored: both derived tables already
# carry period_label and as_of_date, this is a pure function of the two, and
# the alternative is a schema change to two tables for a display concern.
# --------------------------------------------------------------------------

_MONTH_NAMES = (
    "january february march april may june july august september "
    "october november december"
).split()
_MONTH_BY_PREFIX = {name[:3]: i + 1 for i, name in enumerate(_MONTH_NAMES)}
# Spelled out and abbreviated, because the corpus writes both -- "March 31,
# 2026" and "Mar 31, 2026". Longest first so the alternation prefers the full
# name, and matched with \b on both sides rather than a trailing [a-z]*, so
# "Mar" matches and "marketing" does not.
_MONTH_ALT = "|".join(sorted(
    _MONTH_NAMES + [n[:3] for n in _MONTH_NAMES] + ["sept"],
    key=len, reverse=True))

# 12/31/25, 09/30/2025, 6-30-2025. Deliberately greedy about which one wins:
# a range states both ends, and the period ends at the later one, so the LAST
# match in the label is the answer.
_NUMERIC_DATE = re.compile(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})\b")
# "March 31, 2026" / "Mar 31, 2026" / "June 30 2025"
_NAMED_DATE = re.compile(
    rf"\b({_MONTH_ALT})\.?\s+(\d{{1,2}})(?:st|nd|rd|th)?,?\s+((?:19|20)\d{{2}})\b",
    re.I)
# "ending May 2026" -- a month with no day ends when the month does.
_MONTH_YEAR = re.compile(rf"\b({_MONTH_ALT})\.?,?\s+((?:19|20)\d{{2}})\b", re.I)
# "Q1 2026" and "1Q 2026" -- both appear.
_QUARTER_YEAR = re.compile(
    r"\b(?:Q([1-4])|([1-4])Q)\s*,?\s*((?:19|20)\d{2})\b", re.I)
_YEAR_Q = re.compile(r"\b((?:19|20)\d{2})\s*Q([1-4])\b", re.I)
_FISCAL_YEAR = re.compile(
    r"\b(?:FY|fiscal\s+year(?:\s+end(?:ed|ing))?)\s*((?:19|20)\d{2})\b", re.I)
_CALENDAR_YEAR = re.compile(
    r"\b(?:CY|calendar\s+year)\s*((?:19|20)\d{2})\b", re.I)
_ANY_YEAR = re.compile(r"\b((?:19|20)\d{2})\b")
# "YTD 2026" names the year the period runs *in*, not a period that ends in
# December -- the run stops at whatever date the document was written. Reading
# it as a bare year dated thirteen of these to 2026Q4, a quarter that had not
# happened. When this fires, the year rules are skipped and the document's own
# date answers instead.
_YEAR_TO_DATE = re.compile(r"\b(?:[CF]?YTD|year[- ]to[- ]date)\b", re.I)

# June 30 is the dominant US public-pension fiscal year end, and is already
# what collect_from_cafr assumes when it stamps an as-of date on a CAFR row.
# The two must not disagree about what FY2025 means.
_FISCAL_YEAR_END = (6, 30)


def _end_of_month(year: int, month: int) -> date | None:
    try:
        return date(year, month, calendar.monthrange(year, month)[1])
    except (ValueError, calendar.IllegalMonthError):
        return None


def _last_quarter_end(d: date) -> date:
    """The most recent quarter end on or before ``d``.

    Accepts a ``datetime`` as well as a ``date``. The two derived tables store
    a ``Date`` column so production always passes a ``date``, but the same
    fallback is the obvious thing to hand a document's ``meeting_date``, which
    is a ``DateTime`` -- and comparing the two raises TypeError rather than
    doing anything sensible.
    """
    if isinstance(d, datetime):
        d = d.date()
    month = ((d.month - 1) // 3) * 3 + 3
    end = _end_of_month(d.year, month)
    if end is not None and end <= d:
        return end
    # d falls before its own quarter's end -- step back one quarter.
    month -= 3
    year = d.year
    if month < 1:
        month, year = 12, year - 1
    return _end_of_month(year, month)


def period_end(period_label: str | None, fallback: date | None) -> date | None:
    """The date a reported period ends, from the label if it says, else the
    quarter that had closed when ``fallback`` (the document's date) was
    written.

    Returns ``None`` only when the label names no period and there is no
    document date either.
    """
    label = (period_label or "").strip()

    if label:
        # A date spelled out beats every other reading, and the last one in
        # the label wins so a range resolves to its end.
        matches = list(_NUMERIC_DATE.finditer(label))
        for m in reversed(matches):
            month, day, year = (int(g) for g in m.groups())
            if year < 100:
                year += 2000
            try:
                return date(year, month, day)
            except ValueError:
                continue            # 31/31/2024 and friends: keep looking

        m = _NAMED_DATE.search(label)
        if m:
            month = _MONTH_BY_PREFIX[m.group(1)[:3].lower()]
            try:
                return date(int(m.group(3)), month, int(m.group(2)))
            except ValueError:
                pass

        m = _QUARTER_YEAR.search(label)
        if m:
            quarter = m.group(1) or m.group(2)
            return _end_of_month(int(m.group(3)), int(quarter) * 3)

        m = _YEAR_Q.search(label)
        if m:
            return _end_of_month(int(m.group(1)), int(m.group(2)) * 3)

        m = _MONTH_YEAR.search(label)
        if m:
            return _end_of_month(int(m.group(2)),
                                 _MONTH_BY_PREFIX[m.group(1)[:3].lower()])

        # Everything below reads a year as a whole period. A year-to-date
        # label names a year its period runs in but does not finish.
        if not _YEAR_TO_DATE.search(label):
            m = _FISCAL_YEAR.search(label)
            if m:
                return date(int(m.group(1)), *_FISCAL_YEAR_END)

            m = _CALENDAR_YEAR.search(label)
            if m:
                return date(int(m.group(1)), 12, 31)

            # A bare year, with nothing saying which kind. Calendar-year
            # returns are what a bare year means in these tables; "FY" and
            # "fiscal" are spelled out above when a plan means otherwise.
            m = _ANY_YEAR.search(label)
            if m:
                return date(int(m.group(1)), 12, 31)

    return _last_quarter_end(fallback) if fallback else None


def quarter_label(d: date | None) -> str | None:
    """``2026Q1`` -- the calendar quarter a period ends in.

    Sortable as a string, which is what makes it usable as a filter value.
    """
    if d is None:
        return None
    return f"{d.year}Q{(d.month - 1) // 3 + 1}"


def period_end_quarter(period_label: str | None,
                       fallback: date | None) -> str | None:
    return quarter_label(period_end(period_label, fallback))


# Figures older than this are not shown in either performance table.
#
# Both tables claim to be current -- "Latest return by asset class" says so in
# its title -- and the corpus reaches back to 1994Q4. A 2014 real-estate return
# sitting in a column beside a 2026 one is not stale data a reader can discount;
# it reads as this year's number unless they check the date on every row.
#
# A display rule, deliberately not a build rule: the underlying figures stay in
# plan_asset_class_performance and plan_asset_class_horizon, and nothing is
# deleted. Raise or lower this and the old readings come back.
EARLIEST_PERIOD_END = "2025Q1"


def _too_old(quarter: str | None) -> bool:
    """True for a quarter before the cutoff. Unknown is not old.

    A row whose period cannot be dated at all keeps its place: it is missing
    information, not evidence of age, and dropping it would hide a plan for a
    reason the reader cannot see.
    """
    return bool(quarter) and quarter < EARLIEST_PERIOD_END


def span_label(quarters) -> str | None:
    """One quarter, or the range a row's cells actually span.

    A per-asset-class row mixes documents across its horizon columns on
    purpose, so "which quarter is this row?" has no single answer. Showing
    ``2025Q2-2026Q1`` says so, rather than picking one end and implying the
    rest matches it. Quarter labels sort correctly as strings, which is why
    they are formatted the way they are.
    """
    unique = sorted({q for q in quarters if q})
    if not unique:
        return None
    if len(unique) == 1:
        return unique[0]
    return f"{unique[0]}-{unique[-1]}"


# What a return covers, in words. The frequency is the first thing a reader
# needs: a 2.1% quarter and a 2.1% year are not the same result, and the
# number alone cannot say which it is.
FREQUENCY_LABELS = {
    "annual": "Annual",
    "quarter": "Quarterly",
    "month": "Monthly",
    "partial": "Part-year",
    "multi_year": "Multi-year annualised",
    "inception": "Since inception",
    "unclear": "Unclear",
}

# Display order and labels for the collated asset-class view. Keys are the
# canonical values data/asset_class_mappings.json produces, so this list and
# the allocation views cannot drift apart.
COLLATED_CLASSES = (
    ("total", "Total plan"),
    ("public_equity_us", "US equity"),
    ("public_equity_non_us", "Non-US equity"),
    ("public_equity_global", "Global equity"),
    ("fixed_income_core", "Fixed income"),
    ("fixed_income_credit", "Credit"),
    ("private_equity", "Private equity"),
    ("private_credit", "Private credit"),
    ("real_estate", "Real estate"),
    ("real_assets_infrastructure", "Real assets"),
    ("hedge_funds_absolute_return", "Hedge funds"),
    ("opportunistic_other", "Opportunistic"),
    ("cash_short_term", "Cash"),
)


def collated_performance_rows(session) -> list[dict]:
    """One row per plan: latest return per asset class, whatever the source.

    Reads the derived ``plan_asset_class_performance`` table, which
    ``scripts/build_performance_view.py`` rebuilds. It is deliberately not
    computed here: half the underlying data lives in
    ``summaries.performance_data``, 2.2 MB of JSON across 2,087 rows, and
    parsing that behind a short Streamlit cache is the read shape that
    exhausted Neon's transfer quota on 2026-08-25.

    Distinct from ``performance_report_rows``, which is CAFR-only and reports
    a fiscal year. This one merges CAFR and board-document returns and takes
    the most recent per asset class, so a plan's row can mix a 2026 board
    figure for equities with an FY2024 CAFR figure for private equity. The
    per-plan ``As of`` and ``Sources`` columns say when and from what,
    because a blended row that hides its provenance invites false comparison.
    """
    from database import Document, Plan
    from database import PlanAssetClassPerformance as P

    rows = (session.query(P, Plan.name, Document.url, Document.filename)
            .join(Plan, Plan.id == P.plan_id)
            .outerjoin(Document, Document.id == P.document_id)
            .all())

    labels = dict(COLLATED_CLASSES)
    # Keyed on the document, not the plan: a plan can have two rows -- its
    # latest annual figures and its latest figures of any kind. Keying on
    # plan_id would silently merge them back into the incoherent single row
    # this design exists to avoid.
    by_source: dict[tuple[str, int | None, str], dict] = {}
    for rec, plan_name, doc_url, doc_name in rows:
        entry = by_source.setdefault(
            (rec.plan_id, rec.document_id, rec.horizon or "unclear"), {
            "Plan": plan_name,
            # Every figure in the row comes from one document, so period,
            # frequency and source are facts about the row rather than
            # per-cell footnotes.
            "Period": rec.period_label,
            # The same period, bucketed so it can be sorted and filtered on.
            # "Period" is verbatim and inconsistent; this is the column a
            # reader uses to keep 2026Q1 rows away from 2025Q4 ones.
            "Period end": period_end_quarter(rec.period_label, rec.as_of_date),
            "Frequency": FREQUENCY_LABELS.get(rec.horizon or "unclear", "Unclear"),
            "As of": rec.as_of_date.isoformat() if rec.as_of_date else None,
            "Source": doc_url,
            "Document": doc_name,
        })
        label = labels.get(rec.asset_class)
        if label:
            entry[label] = rec.return_pct

    # Every figure in one of these rows shares a period, so age is a property
    # of the whole row and the row goes or stays as one.
    out = [r for r in by_source.values() if not _too_old(r["Period end"])]
    # Annual first within a plan, so the comparable row is the one a reader
    # meets first; plans ordered by how recent their newest source is.
    out.sort(key=lambda r: (r["As of"] or "", r["Frequency"] == "Annual"),
             reverse=True)
    out.sort(key=lambda r: r["Plan"])
    return out


# Display order and labels for the per-asset-class view's horizon columns.
# Only the horizons a reader can usefully compare across plans side by side --
# 2y/7y/20y/30y exist in the corpus but are thin and are left off the picker,
# same reasoning COLLATED_CLASSES already applies to asset classes with no
# canonical mapping.
ASSET_CLASS_HORIZONS = (
    ("quarter", "Quarter"),
    ("annual", "1 year"),
    ("3y", "3 year"),
    ("5y", "5 year"),
    ("10y", "10 year"),
)


def asset_class_horizon_rows(session, asset_class: str) -> list[dict]:
    """One asset class, every plan, every horizon -- the mirror read of
    collated_performance_rows above, which fixes the plan and shows every
    asset class. This fixes the asset class and shows every plan.

    Reads the derived ``plan_asset_class_horizon`` table, which
    ``scripts/build_performance_view.py`` rebuilds alongside
    ``plan_asset_class_performance`` in the same run. Not computed here for
    the same reason that table is derived rather than queried live: half the
    source data lives in ``summaries.performance_data``, and parsing that
    behind a Streamlit cache is the read shape that exhausted Neon's
    transfer quota on 2026-08-25 (see CLAUDE.md).

    A row can mix documents across its horizon columns -- a plan's 1-year
    figure might come from a 2026 board pack while its 10-year figure comes
    from an FY2023 CAFR. That is deliberate (see
    ``build_performance_view.pick_best_per_cell``), so ``As of`` reports the
    newest of the cells actually used and ``Sources`` reports how many
    distinct documents the row draws on, rather than presenting the row as
    if it came from one paper.
    """
    from database import Document, Plan
    from database import PlanAssetClassHorizon as H

    rows = (session.query(H, Plan.name, Document.url, Document.filename)
            .join(Plan, Plan.id == H.plan_id)
            .outerjoin(Document, Document.id == H.document_id)
            .filter(H.asset_class == asset_class)
            .all())

    labels = dict(ASSET_CLASS_HORIZONS)
    by_plan: dict[str, dict] = {}
    newest: dict[str, tuple] = {}
    doc_ids: dict[str, set] = {}
    # Period end is per *cell* here, not per row: a row's 10-year figure can
    # be four years older than its 1-year one. Carried in a side dict under
    # "_period_ends" rather than as five more columns, which would double the
    # width of a table that is already thirteen columns of percentages.
    # Leading underscore because it is not a display column -- app.py pops it
    # to filter, then builds the DataFrame from what is left.
    cell_quarters: dict[str, dict[str, str]] = {}

    for rec, plan_name, doc_url, doc_name in rows:
        label = labels.get(rec.horizon_key)
        quarter = period_end_quarter(rec.period_label, rec.as_of_date)
        # Age is per cell here, not per row, so an old 10-year figure is
        # dropped without taking its row's current 1-year figure with it.
        if label and _too_old(quarter):
            continue

        entry = by_plan.setdefault(rec.plan_id, {"Plan": plan_name})
        if label:
            entry[label] = rec.return_pct
            if quarter:
                cell_quarters.setdefault(rec.plan_id, {})[label] = quarter

        if rec.document_id is not None:
            doc_ids.setdefault(rec.plan_id, set()).add(rec.document_id)

        if rec.as_of_date:
            cur = newest.get(rec.plan_id)
            if cur is None or rec.as_of_date > cur[0]:
                newest[rec.plan_id] = (rec.as_of_date, doc_url, doc_name)

    out = []
    display_labels = set(labels.values())
    for plan_id, entry in by_plan.items():
        # A plan can reach here with no displayable figure at all -- every
        # cell dropped for age, or its only readings on horizons this view
        # has no column for (monthly, since-inception). An all-blank row
        # reads as a plan that reported and did badly.
        if not display_labels & entry.keys():
            continue
        as_of, doc_url, doc_name = newest.get(plan_id, (None, None, None))
        quarters = cell_quarters.get(plan_id, {})
        entry["Period end"] = span_label(quarters.values())
        entry["_period_ends"] = quarters
        entry["As of"] = as_of.isoformat() if as_of else None
        entry["Sources"] = len(doc_ids.get(plan_id, ()))
        entry["Source"] = doc_url
        entry["Document"] = doc_name
        out.append(entry)

    one_year = labels["annual"]
    out.sort(key=lambda r: (r.get(one_year) is None,
                            -(r.get(one_year) or 0)))
    return out


def asset_class_horizon_quarters(session) -> list[str]:
    """Every period-end quarter in the horizon table, newest first.

    Deliberately **not** per asset class, and that is the whole point. The
    picker beside this table keeps its selection in Streamlit session state
    across an asset-class change, and Streamlit raises outright when a stored
    selection is absent from the options it is handed. Cash has no 2026Q2
    reading, so options derived from the selected class alone meant: choose
    2026Q2 on real estate, switch to Cash, crash.

    A stable list fixes that and is the better behaviour anyway -- "show me
    2026Q1" is a question a reader asks across asset classes, not one they
    expect to be silently forgotten when they change the class.
    """
    from database import PlanAssetClassHorizon as H

    rows = session.query(H.period_label, H.as_of_date).all()
    quarters = {period_end_quarter(label, as_of) for label, as_of in rows}
    return sorted((q for q in quarters if q and not _too_old(q)), reverse=True)
