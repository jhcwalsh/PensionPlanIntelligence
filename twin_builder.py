"""Assemble and persist per-plan digital-twin snapshots.

v0: pure Python over data the pipeline already extracted — no LLM calls.
See docs/superpowers/specs/2026-07-10-digital-twin-design.md.

Runs inside the existing GHA daily-pipeline job (and the monthly CAFR
workflow) before the DB push — never as an independent writer.

Usage:
    python twin_builder.py                # all plans
    python twin_builder.py calpers nystrs # subset
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

from rich.console import Console

from database import (
    CafrAllocation, CafrExtract, CafrPerformance, Document, Plan, RFPRecord,
    Summary, TwinBuildRun, TwinSnapshot, get_session, get_twin_snapshot, init_db,
)

console = Console(legacy_windows=False)

TWIN_SCHEMA_VERSION = "twin_v0"
KEEP_RECENT = 8
GOVERNANCE_RFP_TYPES = ("Consultant", "Custodian", "Actuary", "Audit", "Legal")
MANAGER_MAPPINGS_PATH = Path(__file__).parent / "data" / "manager_mappings.json"


def _canonical_hash(facets: dict) -> str:
    return hashlib.sha256(
        json.dumps(facets, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def save_snapshot(session, plan_id: str, twin: dict) -> bool:
    """Persist a twin if its facets changed since the last snapshot.

    Returns True when a new row was written. Computes changed_facets
    against the previous snapshot and prunes old rows (keep the 8 most
    recent plus the earliest snapshot of each calendar month).
    """
    facets = twin["facets"]
    new_hash = _canonical_hash(facets)
    prev = get_twin_snapshot(session, plan_id)
    if prev is not None and prev.facets_hash == new_hash:
        return False

    changed = []
    if prev is not None:
        prev_facets = json.loads(prev.facets)["facets"]
        keys = sorted(set(prev_facets) | set(facets))
        changed = [k for k in keys
                   if json.dumps(prev_facets.get(k), sort_keys=True, default=str)
                   != json.dumps(facets.get(k), sort_keys=True, default=str)]

    row = TwinSnapshot(
        plan_id=plan_id,
        schema_version=TWIN_SCHEMA_VERSION,
        facets=json.dumps(twin, default=str),
        facets_hash=new_hash,
        changed_facets=json.dumps(changed),
        completeness=json.dumps(twin.get("completeness", {})),
        freshness=json.dumps(twin.get("freshness", {}), default=str),
    )
    session.add(row)
    session.commit()
    _prune(session, plan_id)
    return True


def _prune(session, plan_id: str) -> None:
    rows = (session.query(TwinSnapshot)
            .filter(TwinSnapshot.plan_id == plan_id)
            .order_by(TwinSnapshot.built_at.desc(), TwinSnapshot.id.desc())
            .all())
    keep = {r.id for r in rows[:KEEP_RECENT]}
    month_first: dict[str, TwinSnapshot] = {}
    for r in rows:
        key = r.built_at.strftime("%Y-%m")
        cur = month_first.get(key)
        if cur is None or r.built_at < cur.built_at:
            month_first[key] = r
    keep |= {r.id for r in month_first.values()}
    for r in rows:
        if r.id not in keep:
            session.delete(r)
    session.commit()


def _fact(v, as_of=None, table="", doc_id=None, row_id=None, url=None):
    return {"v": v, "as_of": as_of,
            "src": {"doc_id": doc_id, "table": table, "row_id": row_id, "url": url}}


def _fy_end(plan, fiscal_year):
    if not fiscal_year:
        return None
    mmdd = plan.fiscal_year_end or "06-30"
    return f"{fiscal_year}-{mmdd}"


def _load_manager_mappings() -> dict:
    try:
        raw = json.loads(MANAGER_MAPPINGS_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    out = {}
    for name, m in raw.items():
        out[name] = m.get("canonical", name) if isinstance(m, dict) else (m or name)
    return out


def _parse_json_list(text):
    if not text:
        return []
    try:
        val = json.loads(text)
        return val if isinstance(val, list) else []
    except (json.JSONDecodeError, TypeError):
        return []


def build_identity(plan) -> dict:
    """Plan-table facts; only aum_billions gets a provenance envelope."""
    return {
        "name": plan.name,
        "abbreviation": plan.abbreviation,
        "state": plan.state,
        "aum_billions": _fact(plan.aum_billions, as_of=None, table="plans"),
        "fiscal_year_end": plan.fiscal_year_end,
        "website": plan.website,
    }


def build_cafr_facets(session, plan):
    """policy, allocation, performance from the plan's latest CafrExtract."""
    extract = (
        session.query(CafrExtract)
        .filter(CafrExtract.plan_id == plan.id)
        .order_by(CafrExtract.fiscal_year.desc(), CafrExtract.id.desc())
        .first()
    )
    if extract is None:
        policy = {"investment_policy_text": None, "source_fiscal_year": None}
        allocation = {"fiscal_year": None, "as_of": None, "src": None, "rows": []}
        performance = {"fiscal_year": None, "as_of": None, "src": None, "rows": []}
        return policy, allocation, performance

    as_of = _fy_end(plan, extract.fiscal_year)
    doc = session.get(Document, extract.document_id)
    url = doc.url if doc else None
    src = {"doc_id": extract.document_id, "table": "cafr_extract",
           "row_id": extract.id, "url": url}

    policy = {
        "investment_policy_text": _fact(
            extract.investment_policy_text, as_of=as_of, table="cafr_extract",
            doc_id=extract.document_id, row_id=extract.id, url=url,
        ),
        "source_fiscal_year": extract.fiscal_year,
    }

    alloc_rows = (
        session.query(CafrAllocation)
        .filter(CafrAllocation.cafr_extract_id == extract.id)
        .order_by(CafrAllocation.id)
        .all()
    )
    allocation_rows = []
    for r in alloc_rows:
        drift_pct = None
        if r.actual_pct is not None and r.target_pct is not None:
            drift_pct = round(r.actual_pct - r.target_pct, 2)
        outside_range = None
        if (r.actual_pct is not None and r.target_range_low is not None
                and r.target_range_high is not None):
            outside_range = r.actual_pct < r.target_range_low or r.actual_pct > r.target_range_high
        allocation_rows.append({
            "asset_class_raw": r.asset_class,
            "target_pct": r.target_pct,
            "actual_pct": r.actual_pct,
            "range_low": r.target_range_low,
            "range_high": r.target_range_high,
            "drift_pct": drift_pct,
            "outside_range": outside_range,
        })
    allocation = {"fiscal_year": extract.fiscal_year, "as_of": as_of, "src": src,
                  "rows": allocation_rows}

    perf_rows = (
        session.query(CafrPerformance)
        .filter(CafrPerformance.cafr_extract_id == extract.id)
        .order_by(CafrPerformance.scope, CafrPerformance.period)
        .all()
    )
    performance_rows = [{
        "scope": r.scope, "period": r.period, "return_pct": r.return_pct,
        "benchmark_return_pct": r.benchmark_return_pct, "benchmark_name": r.benchmark_name,
    } for r in perf_rows]
    performance = {"fiscal_year": extract.fiscal_year, "as_of": as_of, "src": src,
                   "rows": performance_rows}

    return policy, allocation, performance


def build_roster_and_timeline(session, plan, mappings):
    """manager_roster + activity_timeline in one pass over the plan's summaries."""
    rows = (
        session.query(Summary, Document)
        .join(Document, Summary.document_id == Document.id)
        .filter(Document.plan_id == plan.id)
        .all()
    )

    managers: dict[str, dict] = {}
    timeline_items = []

    for summary, doc in rows:
        doc_date = doc.meeting_date.date().isoformat() if doc.meeting_date else None

        for act in _parse_json_list(summary.investment_actions):
            if not isinstance(act, dict):
                continue
            timeline_items.append({
                "date": doc_date, "kind": "action",
                "action": act.get("action"), "manager": act.get("manager"),
                "asset_class_raw": act.get("asset_class"),
                "amount_millions": act.get("amount_millions"),
                "description": act.get("description"), "vote": None,
                "doc_id": doc.id, "url": doc.url,
            })

            manager_raw = act.get("manager")
            if not manager_raw or not str(manager_raw).strip() \
                    or str(manager_raw).strip().upper() == "N/A":
                continue
            canonical = mappings.get(manager_raw, manager_raw)
            entry = managers.setdefault(canonical, {
                "name_raw": manager_raw, "name_canonical": canonical,
                "mention_count": 0, "action_types": Counter(),
                "first_seen": None, "last_seen": None,
                "doc_ids": set(), "_dated_actions": [],
            })
            entry["mention_count"] += 1
            action_type = act.get("action")
            if action_type:
                entry["action_types"][action_type] += 1
            entry["doc_ids"].add(doc.id)
            entry["_dated_actions"].append((doc.meeting_date, action_type))
            if doc_date:
                if entry["first_seen"] is None or doc_date < entry["first_seen"]:
                    entry["first_seen"] = doc_date
                if entry["last_seen"] is None or doc_date > entry["last_seen"]:
                    entry["last_seen"] = doc_date

        for dec in _parse_json_list(summary.decisions):
            if not isinstance(dec, dict):
                continue
            timeline_items.append({
                "date": doc_date, "kind": "decision",
                "action": None, "manager": None, "asset_class_raw": None,
                "amount_millions": None,
                "description": dec.get("description"), "vote": dec.get("vote"),
                "doc_id": doc.id, "url": doc.url,
            })

    now = datetime.utcnow()
    entries = []
    for canonical, entry in managers.items():
        dated = sorted((d, a) for d, a in entry["_dated_actions"] if d is not None)
        status = "unknown"
        if dated and dated[-1][1] == "fire":
            status = "terminated"
        elif entry["last_seen"]:
            last_seen_dt = datetime.fromisoformat(entry["last_seen"])
            if last_seen_dt >= now - timedelta(days=730):
                status = "current"
        entries.append({
            "name_raw": entry["name_raw"],
            "name_canonical": entry["name_canonical"],
            "mention_count": entry["mention_count"],
            "action_types": dict(entry["action_types"]),
            "first_seen": entry["first_seen"],
            "last_seen": entry["last_seen"],
            "status": status,
            "doc_ids": sorted(entry["doc_ids"])[:20],
        })
    entries.sort(key=lambda e: e["name_canonical"])

    timeline_count = len(timeline_items)
    dated_items = [i for i in timeline_items if i["date"] is not None]
    undated_items = [i for i in timeline_items if i["date"] is None]
    dated_items.sort(key=lambda i: i["date"], reverse=True)
    timeline_items = dated_items + undated_items

    manager_roster = {"entries": entries}
    activity_timeline = {"count": timeline_count, "items": timeline_items[:100]}
    return manager_roster, activity_timeline


def build_rfp_facets(session, plan):
    """rfp_state + governance_people from the plan's RFPRecord rows."""
    rows = session.query(RFPRecord).filter(RFPRecord.plan_id == plan.id).all()
    records = []
    by_status: Counter = Counter()
    relationships = []
    seen = set()

    for row in rows:
        try:
            rec = json.loads(row.record)
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(rec, dict):
            continue

        out_rec = {k: rec.get(k) for k in (
            "rfp_type", "status", "title", "asset_class", "mandate_size_usd_millions",
            "release_date", "response_due_date", "award_date",
            "incumbent_manager", "awarded_manager",
        )}
        out_rec["doc_id"] = row.document_id
        records.append(out_rec)

        status = rec.get("status")
        if status:
            by_status[status] += 1

        rfp_type = rec.get("rfp_type")
        if rfp_type in GOVERNANCE_RFP_TYPES:
            for field, basis in (("awarded_manager", "rfp_awarded"),
                                 ("incumbent_manager", "rfp_incumbent")):
                name = rec.get(field)
                if not name:
                    continue
                key = (rfp_type, name)
                if key in seen:
                    continue
                seen.add(key)
                relationships.append({"role": rfp_type, "name": name,
                                      "basis": basis, "doc_id": row.document_id})

    rfp_state = {"by_status": dict(by_status), "records": records}
    governance_people = {"relationships": relationships}
    return rfp_state, governance_people


def _completeness(facets: dict) -> dict:
    pol = facets["policy"].get("investment_policy_text")
    timeline = facets["activity_timeline"]
    return {
        "identity": 1.0,
        "policy": 1.0 if (pol and pol.get("v")) else 0.0,
        "allocation": 1.0 if facets["allocation"]["rows"] else 0.0,
        "performance": 1.0 if facets["performance"]["rows"] else 0.0,
        "manager_roster": round(min(1.0, len(facets["manager_roster"]["entries"]) / 5), 2),
        "activity_timeline": round(min(1.0, timeline["count"] / 5), 2),
        "rfp_state": round(min(1.0, len(facets["rfp_state"]["records"]) / 5), 2),
        "governance_people": round(
            min(1.0, len(facets["governance_people"]["relationships"]) * 0.2), 2),
        "funding_actuarial": 0.0,
    }


def _freshness(facets: dict) -> dict:
    pol = facets["policy"].get("investment_policy_text")
    roster_dates = [e["last_seen"] for e in facets["manager_roster"]["entries"] if e.get("last_seen")]
    timeline_dates = [i["date"] for i in facets["activity_timeline"]["items"] if i.get("date")]
    rfp_dates = [
        v for rec in facets["rfp_state"]["records"]
        for v in (rec.get("release_date"), rec.get("response_due_date"), rec.get("award_date"))
        if v
    ]
    rfp_freshness = max(rfp_dates) if rfp_dates else None

    # Governance freshness: only from governance-type records (Consultant, Custodian, etc.)
    governance_dates = [
        v for rec in facets["rfp_state"]["records"]
        if rec.get("rfp_type") in GOVERNANCE_RFP_TYPES
        for v in (rec.get("release_date"), rec.get("response_due_date"), rec.get("award_date"))
        if v
    ]
    governance_freshness = max(governance_dates) if governance_dates and facets["governance_people"]["relationships"] else None

    return {
        "identity": None,
        "policy": pol["as_of"] if pol else None,
        "allocation": facets["allocation"].get("as_of"),
        "performance": facets["performance"].get("as_of"),
        "manager_roster": max(roster_dates) if roster_dates else None,
        "activity_timeline": max(timeline_dates) if timeline_dates else None,
        "rfp_state": rfp_freshness,
        "governance_people": governance_freshness,
        "funding_actuarial": None,
    }


def build_twin(session, plan) -> dict:
    mappings = _load_manager_mappings()
    identity = build_identity(plan)
    policy, allocation, performance = build_cafr_facets(session, plan)
    roster, timeline = build_roster_and_timeline(session, plan, mappings)
    rfp_state, governance = build_rfp_facets(session, plan)
    facets = {
        "identity": identity, "policy": policy, "allocation": allocation,
        "performance": performance, "manager_roster": roster,
        "activity_timeline": timeline, "rfp_state": rfp_state,
        "governance_people": governance,
        "funding_actuarial": {"status": "not_captured"},
    }
    return {
        "schema_version": TWIN_SCHEMA_VERSION,
        "plan_id": plan.id,
        "built_at": datetime.utcnow().isoformat(),
        "facets": facets,
        "completeness": _completeness(facets),
        "freshness": _freshness(facets),
    }


def run_builder(plan_ids=None) -> None:
    init_db()
    session = get_session()
    run = TwinBuildRun()
    session.add(run); session.commit()
    errors = []
    try:
        q = session.query(Plan).order_by(Plan.id)
        if plan_ids:
            q = q.filter(Plan.id.in_(plan_ids))
        plans = q.all()
        run.plans_total = len(plans)
        written = 0
        for plan in plans:
            try:
                if save_snapshot(session, plan.id, build_twin(session, plan)):
                    written += 1
                    console.print(f"  [green]snapshot[/green] {plan.id}")
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{plan.id}: {exc}")
                console.print(f"  [red]failed[/red] {plan.id}: {exc}")
        run.snapshots_written = written
        run.errors = json.dumps(errors)
        run.status = "succeeded" if not errors else "failed"
        run.completed_at = datetime.utcnow()
        session.commit()
        console.print(f"[bold]{written}/{len(plans)} snapshots written[/bold]")
    finally:
        session.close()


def main():
    parser = argparse.ArgumentParser(prog="twin_builder")
    parser.add_argument("plan_ids", nargs="*", help="subset of plan ids")
    args = parser.parse_args()
    run_builder(args.plan_ids or None)


if __name__ == "__main__":
    main()
