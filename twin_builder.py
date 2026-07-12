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
    CafrActuarial, CafrAllocation, CafrExtract, CafrPerformance, Document,
    IpsAllocation, IpsDocument, IpsExtract, Plan, PlanManagerRoster, RFPRecord,
    Summary, TwinBuildRun, TwinSnapshot, get_session, get_twin_snapshot, init_db,
)

console = Console(legacy_windows=False)

TWIN_SCHEMA_VERSION = "twin_v1"
KEEP_RECENT = 8
GOVERNANCE_RFP_TYPES = ("Consultant", "Custodian", "Actuary", "Audit", "Legal")
MANAGER_MAPPINGS_PATH = Path(__file__).parent / "data" / "manager_mappings.json"
ASSET_CLASS_MAPPINGS_PATH = Path(__file__).parent / "data" / "asset_class_mappings.json"

# Every non-metadata payload column on CafrActuarial; surfaced verbatim (with
# None values included) as facets["funding_actuarial"]["metrics"].
FUNDING_METRIC_FIELDS = (
    "funded_ratio_pct", "market_funded_ratio_pct",
    "actuarial_value_assets_millions", "actuarial_accrued_liability_millions",
    "unfunded_aal_millions", "net_pension_liability_millions",
    "discount_rate_pct", "assumed_return_pct", "inflation_pct",
    "payroll_growth_pct", "amortization_years",
    "employer_contribution_rate_pct", "employee_contribution_rate_pct",
    "adc_millions", "adc_pct_contributed", "members_active", "members_retired",
    "actuary_firm", "valuation_date",
)


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
        # `m.get("canonical", name)` only falls back to `name` when the key is
        # *missing*; entries with an explicit `"canonical": null` (real data
        # has ~179 of these) return None here, which then propagates as
        # entries[i]["name_canonical"] = None and breaks the str sort below.
        out[name] = (m.get("canonical") or name) if isinstance(m, dict) else (m or name)
    return out


def load_asset_class_mappings() -> dict[str, str]:
    """Load asset-class label mappings from data/asset_class_mappings.json.

    Returns a dict mapping raw label strings to canonical asset-class names.
    Entry values may be dicts with {"canonical": ...} or plain strings;
    both are normalized to strings. Missing file returns {}.
    """
    try:
        raw = json.loads(ASSET_CLASS_MAPPINGS_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    out = {}
    for label, m in raw.items():
        # Handle both dict entries {"canonical": "..."} and plain strings
        out[label] = m.get("canonical") if isinstance(m, dict) else m
    return out


def canonical_asset_class(raw: str | None, mappings: dict[str, str]) -> str:
    """Map a raw asset-class label to canonical form.

    Args:
        raw: raw asset-class label string (may be None or empty)
        mappings: dict from raw labels to canonical names

    Returns:
        A canonical asset-class name from ASSET_CLASS_CANONICAL, or "unmapped".
        Returns "unmapped" for None/empty input, unmapped labels, or labels
        that map to values not in ASSET_CLASS_CANONICAL.
    """
    from database import ASSET_CLASS_CANONICAL

    if not raw or not str(raw).strip():
        return "unmapped"

    raw_str = str(raw).strip()
    canonical = mappings.get(raw_str)

    if canonical is None or canonical not in ASSET_CLASS_CANONICAL:
        return "unmapped"

    return canonical


def _parse_json_list(text):
    if not text:
        return []
    try:
        val = json.loads(text)
        return val if isinstance(val, list) else []
    except (json.JSONDecodeError, TypeError):
        return []


def _parse_json_dict(text):
    if not text:
        return {}
    try:
        val = json.loads(text)
        return val if isinstance(val, dict) else {}
    except (json.JSONDecodeError, TypeError):
        return {}


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


def build_cafr_facets(session, plan, asset_mappings):
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
            "asset_class_canonical": canonical_asset_class(r.asset_class, asset_mappings),
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


def build_ips_facets(session, plan, asset_mappings):
    """policy["ips"], allocation["ips_targets"], and an optional consultant
    governance relationship from the plan's latest IpsExtract.

    Returns (ips_policy, ips_targets, relationship) — all None when the plan
    has no IpsExtract row.
    """
    extract = (
        session.query(IpsExtract)
        .filter(IpsExtract.plan_id == plan.id)
        .order_by(IpsExtract.extracted_at.desc(), IpsExtract.id.desc())
        .first()
    )
    if extract is None:
        return None, None, None

    ips_doc = session.get(IpsDocument, extract.ips_document_id)
    as_of = extract.effective_date
    if not as_of and ips_doc is not None and ips_doc.fetched_at is not None:
        as_of = ips_doc.fetched_at.date().isoformat()

    src = {"doc_id": ips_doc.id if ips_doc else None, "table": "ips_extract",
           "row_id": extract.id, "url": ips_doc.url if ips_doc else None}

    permitted_prohibited = _parse_json_dict(extract.permitted_prohibited)

    ips_policy = {
        "target_return_pct": extract.target_return_pct,
        "rebalancing_policy": _parse_json_dict(extract.rebalancing_policy),
        "permitted": permitted_prohibited.get("permitted") or [],
        "prohibited": permitted_prohibited.get("prohibited") or [],
        "effective_date": extract.effective_date,
        "as_of": as_of,
        "src": src,
    }

    alloc_rows = (
        session.query(IpsAllocation)
        .filter(IpsAllocation.ips_extract_id == extract.id)
        .order_by(IpsAllocation.id)
        .all()
    )
    ips_targets = {
        "as_of": as_of,
        "src": src,
        "rows": [{
            "asset_class_raw": r.asset_class,
            "asset_class_canonical": canonical_asset_class(r.asset_class, asset_mappings),
            "target_pct": r.target_pct,
            "range_low": r.range_low,
            "range_high": r.range_high,
        } for r in alloc_rows],
    }

    governance = _parse_json_dict(extract.governance)
    consultant_name = governance.get("consultant_name")
    relationship = None
    if consultant_name and str(consultant_name).strip():
        relationship = {
            "role": "Consultant", "name": str(consultant_name).strip(),
            "basis": "ips", "doc_id": ips_doc.id if ips_doc else None,
            "_date": as_of,
        }

    return ips_policy, ips_targets, relationship


def build_actuarial_facets(session, plan):
    """funding_actuarial + an optional actuary governance relationship from
    the plan's latest CafrActuarial row.

    Returns (facet, relationship) — facet is {"status": "not_captured"} and
    relationship is None when the plan has no CafrActuarial row.
    """
    row = (
        session.query(CafrActuarial)
        .filter(CafrActuarial.plan_id == plan.id)
        .order_by(CafrActuarial.fiscal_year.desc(), CafrActuarial.id.desc())
        .first()
    )
    if row is None:
        return {"status": "not_captured"}, None

    doc = session.get(Document, row.document_id)
    src = {"doc_id": row.document_id, "table": "cafr_actuarial",
           "row_id": row.id, "url": doc.url if doc else None}
    metrics = {field: getattr(row, field) for field in FUNDING_METRIC_FIELDS}

    facet = {
        "status": "captured", "as_of": row.valuation_date, "src": src,
        "fiscal_year": row.fiscal_year, "metrics": metrics,
    }

    relationship = None
    if row.actuary_firm and str(row.actuary_firm).strip():
        relationship = {
            "role": "Actuary", "name": str(row.actuary_firm).strip(),
            "basis": "cafr_actuarial", "doc_id": row.document_id,
            "_date": row.valuation_date,
        }

    return facet, relationship


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
            canonical = mappings.get(manager_raw, manager_raw) or manager_raw
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
        # Sort by date only: `action_type` (the second tuple element) can be
        # None for some rows, and a plain tuple sort falls through to compare
        # it whenever two actions share the exact same meeting_date (common
        # when one document logs several actions for the same manager).
        dated = sorted(
            ((d, a) for d, a in entry["_dated_actions"] if d is not None),
            key=lambda t: t[0],
        )
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
    # None-safe defense in depth: name_canonical should always be a string
    # now that _load_manager_mappings() never emits None, but sort on a
    # (is_none, value) key so a future bad mapping can't crash the build.
    entries.sort(key=lambda e: (e["name_canonical"] is None, e["name_canonical"] or ""))

    timeline_count = len(timeline_items)
    dated_items = [i for i in timeline_items if i["date"] is not None]
    undated_items = [i for i in timeline_items if i["date"] is None]
    dated_items.sort(key=lambda i: i["date"], reverse=True)
    timeline_items = dated_items + undated_items

    roster_rows = (
        session.query(PlanManagerRoster)
        .filter(PlanManagerRoster.plan_id == plan.id)
        .order_by(PlanManagerRoster.canonical_name, PlanManagerRoster.role)
        .all()
    )
    if roster_rows:
        entries = []
        for r in roster_rows:
            evidence = _parse_json_dict(r.evidence)
            doc_ids = evidence.get("doc_ids") or evidence.get("rfp_doc_ids") or []
            action_types = evidence.get("action_types")
            action_types = action_types if isinstance(action_types, dict) else {}
            entries.append({
                "name_canonical": r.canonical_name,
                "role": r.role,
                "asset_class_raw": r.asset_class_raw,
                "asset_class_canonical": r.asset_class_canonical,
                "status": r.status,
                "first_seen": r.first_seen,
                "last_seen": r.last_seen,
                "confidence": r.confidence,
                "doc_ids": doc_ids,
                "mention_count": sum(action_types.values()),
                "action_types": action_types,
            })
        entries.sort(key=lambda e: (
            e["name_canonical"] is None,
            e["name_canonical"] or "",
            e["role"] or "",
        ))

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
            rec_dates = [v for v in (rec.get("release_date"), rec.get("response_due_date"),
                                     rec.get("award_date")) if v]
            rec_date = max(rec_dates) if rec_dates else None
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
                                      "basis": basis, "doc_id": row.document_id,
                                      "_date": rec_date})

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
        "funding_actuarial": 1.0 if facets["funding_actuarial"].get("status") == "captured" else 0.0,
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

    # Governance freshness: the max date attached to each relationship as it
    # was actually emitted (rfp record dates for rfp-basis, ips as_of for
    # ips-basis, valuation_date for actuary-basis) -- not a facet-wide scan.
    governance_dates = [
        rel["_date"] for rel in facets["governance_people"]["relationships"]
        if rel.get("_date")
    ]
    governance_freshness = max(governance_dates) if governance_dates else None

    ips_targets = facets["allocation"].get("ips_targets")
    allocation_dates = [
        v for v in (facets["allocation"].get("as_of"),
                    ips_targets.get("as_of") if ips_targets else None)
        if v
    ]
    allocation_freshness = max(allocation_dates) if allocation_dates else None

    return {
        "identity": None,
        "policy": pol["as_of"] if pol else None,
        "allocation": allocation_freshness,
        "performance": facets["performance"].get("as_of"),
        "manager_roster": max(roster_dates) if roster_dates else None,
        "activity_timeline": max(timeline_dates) if timeline_dates else None,
        "rfp_state": rfp_freshness,
        "governance_people": governance_freshness,
        "funding_actuarial": facets["funding_actuarial"].get("as_of"),
    }


def build_twin(session, plan) -> dict:
    mappings = _load_manager_mappings()
    asset_mappings = load_asset_class_mappings()

    identity = build_identity(plan)
    policy, allocation, performance = build_cafr_facets(session, plan, asset_mappings)
    ips_policy, ips_targets, ips_relationship = build_ips_facets(session, plan, asset_mappings)
    funding_actuarial, actuary_relationship = build_actuarial_facets(session, plan)
    roster, timeline = build_roster_and_timeline(session, plan, mappings)
    rfp_state, governance = build_rfp_facets(session, plan)

    policy["ips"] = ips_policy
    allocation["ips_targets"] = ips_targets

    relationships = governance["relationships"]
    seen = {(rel["role"], rel["name"]) for rel in relationships}
    for rel in (ips_relationship, actuary_relationship):
        if rel is None:
            continue
        key = (rel["role"], rel["name"])
        if key in seen:
            continue
        seen.add(key)
        relationships.append(rel)

    facets = {
        "identity": identity, "policy": policy, "allocation": allocation,
        "performance": performance, "manager_roster": roster,
        "activity_timeline": timeline, "rfp_state": rfp_state,
        "governance_people": governance,
        "funding_actuarial": funding_actuarial,
    }

    completeness = _completeness(facets)
    freshness = _freshness(facets)

    # "_date" is an internal bookkeeping field used only to compute
    # governance_people freshness precisely per-relationship; strip it
    # before the facets are hashed/persisted/returned.
    for rel in relationships:
        rel.pop("_date", None)

    return {
        "schema_version": TWIN_SCHEMA_VERSION,
        "plan_id": plan.id,
        "built_at": datetime.utcnow().isoformat(),
        "facets": facets,
        "completeness": completeness,
        "freshness": freshness,
    }


def run_builder(plan_ids=None) -> None:
    init_db()
    session = get_session()
    run = TwinBuildRun()
    session.add(run); session.commit()
    errors = []
    written = 0
    plans = []
    try:
        q = session.query(Plan).order_by(Plan.id)
        if plan_ids:
            q = q.filter(Plan.id.in_(plan_ids))
        plans = q.all()
        run.plans_total = len(plans)
        for plan in plans:
            try:
                if save_snapshot(session, plan.id, build_twin(session, plan)):
                    written += 1
                    console.print(f"  [green]snapshot[/green] {plan.id}")
            except Exception as exc:  # noqa: BLE001
                # A failure inside save_snapshot (e.g. database locked) can
                # leave the session in a pending-rollback state; without
                # rolling back here, every later plan's first session use
                # raises PendingRollbackError instead of its own error.
                session.rollback()
                errors.append(f"{plan.id}: {exc}")
                console.print(f"  [red]failed[/red] {plan.id}: {exc}")
    finally:
        try:
            run.snapshots_written = written
            run.errors = json.dumps(errors)
            run.status = "succeeded" if not errors else "failed"
            run.completed_at = datetime.utcnow()
            session.commit()
            console.print(f"[bold]{written}/{len(plans)} snapshots written[/bold]")
        except Exception as exc:  # noqa: BLE001
            # Never let bookkeeping itself blow up the build run: roll back
            # and try once more so the run row still gets finalized.
            try:
                session.rollback()
                run.snapshots_written = written
                run.errors = json.dumps(errors)
                run.status = "succeeded" if not errors else "failed"
                run.completed_at = datetime.utcnow()
                session.commit()
            except Exception as exc2:  # noqa: BLE001
                print(f"twin_builder: failed to finalize run {run.run_id}: {exc2}",
                      file=sys.stderr)
        finally:
            session.close()


def main():
    parser = argparse.ArgumentParser(prog="twin_builder")
    parser.add_argument("plan_ids", nargs="*", help="subset of plan ids")
    args = parser.parse_args()
    run_builder(args.plan_ids or None)


if __name__ == "__main__":
    main()
