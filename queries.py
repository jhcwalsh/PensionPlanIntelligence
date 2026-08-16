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

from datetime import datetime, timedelta

from sqlalchemy import case, distinct, func

from database import Document, Plan, Summary, get_twin_index


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
