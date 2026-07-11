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
from datetime import datetime

from rich.console import Console

from database import (
    Plan, TwinBuildRun, TwinSnapshot, get_session, get_twin_snapshot, init_db,
)

console = Console(legacy_windows=False)

TWIN_SCHEMA_VERSION = "twin_v0"
KEEP_RECENT = 8


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


if __name__ == "__main__":
    pass
