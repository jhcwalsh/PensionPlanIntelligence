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

import json
from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy import case, distinct, func, or_

from database import (
    CafrAllocation,
    CafrExtract,
    CafrPerformance,
    Document,
    Plan,
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
    future_cap = datetime.utcnow() + timedelta(days=60)
    q = (
        session.query(Document, Summary)
        .join(Summary, Document.id == Summary.document_id)
        .filter(Document.doc_type.notin_(["cafr", "performance"]))
        .filter((Document.meeting_date.is_(None)) |
                (Document.meeting_date <= future_cap))
    )
    if plan_id and plan_id != "All":
        q = q.filter(Document.plan_id == plan_id)
    return q.order_by(Document.meeting_date.desc()).limit(limit).all()


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
        prev_key = (prev.fiscal_year or 0, prev.downloaded_at or datetime.min)
        d_key = (d.fiscal_year or 0, d.downloaded_at or datetime.min)
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
    return query.all()
