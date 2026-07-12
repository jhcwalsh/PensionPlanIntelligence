"""Deterministic manager-roster reconciliation into ``plan_manager_roster``.

v0: pure Python over data the pipeline already extracted — no LLM calls.
Reuses the summary-parsing / skip / status-heuristic rules from
``twin_builder.build_roster_and_timeline`` so the roster stays consistent
with the twin's on-the-fly ``manager_roster`` facet, but materializes a
reconciled table (one row per plan/canonical_name/role) that also folds in
governance and manager relationships surfaced by the RFP pipeline.

Two source families feed each plan's roster:

1. Summary-derived "manager" rows — grouped by canonical manager name from
   ``Summary.investment_actions``. ``asset_class_raw`` is the most frequent
   non-null raw label seen for that manager; status follows the same v0
   heuristic as the twin (latest dated action ``fire`` -> terminated; last
   seen within 730 days -> current; else unknown). Confidence is 0.9 when
   mentioned >= 3 times, else 0.6.
2. RFP-derived rows from ``RFPRecord`` — governance-type records
   (Consultant/Custodian/Actuary; Audit/Legal are skipped, they aren't
   investment relationships) produce consultant/custodian/actuary rows from
   ``awarded_manager`` (confidence 0.8) and ``incumbent_manager`` (confidence
   0.5), always status "current". Manager-type records' ``awarded_manager``
   merges into the matching summary-derived "manager" row when the
   canonical names collide (evidence union, keep the higher confidence); if
   there's no matching summary row it becomes a new "manager" row.

Usage:
    python -m scripts.build_manager_roster                # all plans
    python -m scripts.build_manager_roster calpers nystrs  # subset
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timedelta

from database import (
    Document, Plan, PlanManagerRoster, RFPRecord, Summary, get_session, init_db,
)
from twin_builder import (
    GOVERNANCE_RFP_TYPES,
    _load_manager_mappings,
    _parse_json_list,
    canonical_asset_class,
    load_asset_class_mappings,
)

# rfp_type -> roster role for governance-type RFP records. Audit and Legal
# are intentionally excluded — they're not investment relationships.
GOVERNANCE_ROLE_MAP = {"Consultant": "consultant", "Custodian": "custodian",
                        "Actuary": "actuary"}

EVIDENCE_DOC_ID_CAP = 20


def _merge_or_create(entries: dict, key: tuple, *, status: str, confidence: float,
                      evidence: dict, first_seen: str | None = None,
                      last_seen: str | None = None, asset_class_raw: str | None = None,
                      asset_class_canonical: str | None = None) -> None:
    """Insert a new roster entry at ``key``, or merge into an existing one.

    On merge: evidence dicts are unioned key-by-key (list values are
    deduped+sorted, dict values are summed), confidence keeps the max of
    the two, and status/asset_class/first_seen/last_seen from the *existing*
    entry win — only summary-derived entries carry those fields
    meaningfully; an RFP merge should not clobber them.
    """
    existing = entries.get(key)
    if existing is None:
        entries[key] = {
            "status": status,
            "confidence": confidence,
            "evidence": evidence,
            "first_seen": first_seen,
            "last_seen": last_seen,
            "asset_class_raw": asset_class_raw,
            "asset_class_canonical": asset_class_canonical,
        }
        return

    merged_evidence = dict(existing["evidence"] or {})
    for k, v in (evidence or {}).items():
        if isinstance(v, list):
            merged_evidence[k] = sorted(set(merged_evidence.get(k, [])) | set(v))
        elif isinstance(v, dict):
            cur = dict(merged_evidence.get(k, {}))
            for kk, vv in v.items():
                cur[kk] = cur.get(kk, 0) + vv
            merged_evidence[k] = cur
        else:
            merged_evidence[k] = v
    existing["evidence"] = merged_evidence
    existing["confidence"] = max(existing["confidence"], confidence)


def _summary_manager_entries(session, plan_id: str, manager_mappings: dict,
                              asset_mappings: dict) -> dict:
    """Group the plan's summary investment_actions by canonical manager name.

    Mirrors twin_builder.build_roster_and_timeline's parsing/skip rules
    (manager null/empty/"N/A" skipped; canonical via manager_mappings.json)
    and its v0 status heuristic, but additionally tracks the raw
    asset-class label frequency needed for roster's asset_class_raw.
    """
    rows = (
        session.query(Summary, Document)
        .join(Document, Summary.document_id == Document.id)
        .filter(Document.plan_id == plan_id)
        .all()
    )

    managers: dict[str, dict] = {}
    for summary, doc in rows:
        doc_date = doc.meeting_date.date().isoformat() if doc.meeting_date else None

        for act in _parse_json_list(summary.investment_actions):
            if not isinstance(act, dict):
                continue

            manager_raw = act.get("manager")
            if not manager_raw or not str(manager_raw).strip() \
                    or str(manager_raw).strip().upper() == "N/A":
                continue

            canonical = manager_mappings.get(manager_raw, manager_raw) or manager_raw
            entry = managers.setdefault(canonical, {
                "mention_count": 0, "action_types": Counter(),
                "asset_class_labels": Counter(),
                "first_seen": None, "last_seen": None,
                "doc_ids": set(), "_dated_actions": [],
            })
            entry["mention_count"] += 1
            action_type = act.get("action")
            if action_type:
                entry["action_types"][action_type] += 1
            asset_class_raw = act.get("asset_class")
            if asset_class_raw and str(asset_class_raw).strip():
                entry["asset_class_labels"][str(asset_class_raw).strip()] += 1
            entry["doc_ids"].add(doc.id)
            entry["_dated_actions"].append((doc.meeting_date, action_type))
            if doc_date:
                if entry["first_seen"] is None or doc_date < entry["first_seen"]:
                    entry["first_seen"] = doc_date
                if entry["last_seen"] is None or doc_date > entry["last_seen"]:
                    entry["last_seen"] = doc_date

    now = datetime.utcnow()
    result: dict[str, dict] = {}
    for canonical, m in managers.items():
        # Sort by date only — action_type can be None and a plain tuple
        # sort would fall through to compare it on same-date ties.
        dated = sorted(
            ((d, a) for d, a in m["_dated_actions"] if d is not None),
            key=lambda t: t[0],
        )
        status = "unknown"
        if dated and dated[-1][1] == "fire":
            status = "terminated"
        elif m["last_seen"]:
            last_seen_dt = datetime.fromisoformat(m["last_seen"])
            if last_seen_dt >= now - timedelta(days=730):
                status = "current"

        asset_class_raw = None
        if m["asset_class_labels"]:
            asset_class_raw = m["asset_class_labels"].most_common(1)[0][0]
        asset_class_canonical = canonical_asset_class(asset_class_raw, asset_mappings)

        confidence = 0.9 if m["mention_count"] >= 3 else 0.6
        evidence = {
            "doc_ids": sorted(m["doc_ids"])[:EVIDENCE_DOC_ID_CAP],
            "action_types": dict(m["action_types"]),
        }
        result[canonical] = {
            "status": status,
            "confidence": confidence,
            "evidence": evidence,
            "first_seen": m["first_seen"],
            "last_seen": m["last_seen"],
            "asset_class_raw": asset_class_raw,
            "asset_class_canonical": asset_class_canonical,
        }
    return result


def _apply_rfp_entries(session, plan_id: str, manager_mappings: dict,
                        entries: dict) -> None:
    """Fold RFPRecord governance/manager relationships into ``entries`` in place."""
    rows = session.query(RFPRecord).filter(RFPRecord.plan_id == plan_id).all()

    for row in rows:
        try:
            rec = json.loads(row.record)
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(rec, dict):
            continue

        rfp_type = rec.get("rfp_type")
        if rfp_type in GOVERNANCE_RFP_TYPES:
            role = GOVERNANCE_ROLE_MAP.get(rfp_type)
            if role is None:
                continue  # Audit, Legal -> not investment relationships
            for field, confidence in (("awarded_manager", 0.8), ("incumbent_manager", 0.5)):
                name = rec.get(field)
                if not name or not str(name).strip():
                    continue
                name = str(name).strip()
                _merge_or_create(
                    entries, (name, role),
                    status="current", confidence=confidence,
                    evidence={"rfp_doc_ids": [row.document_id]},
                )
        elif rfp_type == "Manager":
            name = rec.get("awarded_manager")
            if not name or not str(name).strip():
                continue
            name = str(name).strip()
            canonical = manager_mappings.get(name, name) or name
            _merge_or_create(
                entries, (canonical, "manager"),
                status="current", confidence=0.8,
                evidence={"rfp_doc_ids": [row.document_id]},
            )


def build_roster_for_plan(session, plan_id: str) -> int:
    """Rebuild the reconciled manager roster for one plan.

    Deletes any existing plan_manager_roster rows for ``plan_id`` and
    inserts the freshly-computed set. Returns the number of rows written.
    Deterministic — no LLM calls.
    """
    manager_mappings = _load_manager_mappings()
    asset_mappings = load_asset_class_mappings()

    entries: dict[tuple[str, str], dict] = {}
    for canonical, data in _summary_manager_entries(
            session, plan_id, manager_mappings, asset_mappings).items():
        entries[(canonical, "manager")] = data

    _apply_rfp_entries(session, plan_id, manager_mappings, entries)

    session.query(PlanManagerRoster).filter(PlanManagerRoster.plan_id == plan_id).delete()
    for (canonical_name, role), data in entries.items():
        session.add(PlanManagerRoster(
            plan_id=plan_id,
            canonical_name=canonical_name,
            role=role,
            asset_class_raw=data.get("asset_class_raw"),
            asset_class_canonical=data.get("asset_class_canonical"),
            status=data["status"],
            first_seen=data.get("first_seen"),
            last_seen=data.get("last_seen"),
            evidence=json.dumps(data.get("evidence") or {}),
            confidence=data.get("confidence"),
        ))
    session.commit()
    return len(entries)


def main() -> None:
    parser = argparse.ArgumentParser(prog="scripts.build_manager_roster")
    parser.add_argument("plan_ids", nargs="*", help="subset of plan ids (default: all)")
    args = parser.parse_args()

    init_db()
    session = get_session()
    total_rows = 0
    plan_ids: list[str] = []
    try:
        q = session.query(Plan.id).order_by(Plan.id)
        if args.plan_ids:
            q = q.filter(Plan.id.in_(args.plan_ids))
        plan_ids = [row.id for row in q.all()]

        for plan_id in plan_ids:
            try:
                n = build_roster_for_plan(session, plan_id)
                total_rows += n
                print(f"  {plan_id}: {n} roster rows")
            except Exception as exc:  # noqa: BLE001
                # Roll back so a single bad plan doesn't poison the session
                # for the rest of the run (mirrors twin_builder.run_builder).
                session.rollback()
                print(f"  {plan_id}: FAILED: {exc}", file=sys.stderr)
    finally:
        session.close()

    print(f"Done. {total_rows} roster rows written across {len(plan_ids)} plans.")


if __name__ == "__main__":
    main()
