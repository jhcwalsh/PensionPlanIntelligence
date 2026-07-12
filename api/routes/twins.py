"""GET /api/v1/twins and GET /api/v1/twin/{plan_id}."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

import database as db
from api.schemas import TwinIndexResponse, TwinIndexRow, TwinResponse

router = APIRouter(prefix="/api/v1", tags=["twins"])


def _session() -> Session:
    s = db.get_session()
    try:
        yield s
    finally:
        s.close()


def _to_response(row: db.TwinSnapshot) -> TwinResponse:
    twin = json.loads(row.facets)
    return TwinResponse(
        plan_id=row.plan_id,
        built_at=row.built_at,
        schema_version=row.schema_version,
        facets=twin["facets"],
        completeness=json.loads(row.completeness or "{}"),
        freshness=json.loads(row.freshness or "{}"),
        changed_facets=json.loads(row.changed_facets or "[]"),
    )


@router.get("/twins", response_model=TwinIndexResponse)
def twin_index(session: Session = Depends(_session)) -> TwinIndexResponse:
    rows = [
        TwinIndexRow(
            plan_id=r["plan_id"], name=r["name"], state=r["state"],
            aum_billions=r["aum_billions"], built_at=r["built_at"],
            schema_version=r["schema_version"],
            completeness=r["completeness"], freshness=r["freshness"],
        )
        for r in db.get_twin_index(session)
    ]
    return TwinIndexResponse(results=rows, total=len(rows))


@router.get("/twin/{plan_id}", response_model=TwinResponse)
def twin_detail(
    plan_id: str,
    as_of: Optional[str] = Query(None, description="YYYY-MM-DD; snapshot as known at this date"),
    session: Session = Depends(_session),
) -> TwinResponse:
    as_of_dt = None
    if as_of:
        try:
            as_of_dt = datetime.fromisoformat(as_of).replace(hour=23, minute=59, second=59)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail="as_of must be YYYY-MM-DD") from exc
    snap = db.get_twin_snapshot(session, plan_id, as_of=as_of_dt)
    if snap is None:
        raise HTTPException(status_code=404, detail=f"no twin snapshot for {plan_id}")
    return _to_response(snap)
