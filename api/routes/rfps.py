"""GET /api/v1/rfps and GET /api/v1/rfps/stats."""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

import database as db
from api.schemas import (
    PipelineHealth, RFPListResponse, RFPResponse, RFPStatsResponse,
    RFPStatus, RFPType,
)

router = APIRouter(prefix="/api/v1", tags=["rfps"])


def _session() -> Session:
    s = db.get_session()
    try:
        yield s
    finally:
        s.close()


_BASELINE_PATH_ENV = "EVAL_BASELINE_PATH"
_DEFAULT_BASELINE = Path(__file__).resolve().parents[2] / "fixtures" / "eval_baseline.json"


def _read_field_accuracy() -> float | None:
    path = Path(os.environ.get(_BASELINE_PATH_ENV, str(_DEFAULT_BASELINE)))
    if not path.exists():
        return None
    try:
        with path.open() as f:
            data = json.load(f)
        return data.get("overall_accuracy")
    except (OSError, json.JSONDecodeError):
        return None


def _to_response(row: db.RFPRecord) -> RFPResponse:
    payload = json.loads(row.record)
    return RFPResponse(
        **payload,
        needs_review=row.needs_review,
        extracted_at=row.extracted_at,
        prompt_version=row.prompt_version,
    )


def _date_in_year(date_str: Optional[str], year: int) -> bool:
    if not date_str:
        return False
    try:
        return datetime.fromisoformat(date_str).year == year
    except ValueError:
        return False


def _filter_by_year(record: dict, year: int, extracted_at: datetime | None = None) -> bool:
    """
    Year matches if any of (release, response_due, award) date falls in it.

    For records with all three internal dates null (common for in-flight
    Manager searches where dates haven't been announced yet) we fall back to
    extracted_at so the record is still visible under the year it was
    discovered. Without this fallback, dateless RFPs would disappear from
    the default frontend view.
    """
    keys = ("release_date", "response_due_date", "award_date")
    if any(record.get(k) for k in keys):
        return any(_date_in_year(record.get(k), year) for k in keys)
    return extracted_at is not None and extracted_at.year == year


@router.get("/rfps", response_model=RFPListResponse)
def list_rfps(
    year: Optional[int] = Query(None, description="Filter by year on any of release/response_due/award dates"),
    rfp_type: Optional[RFPType] = Query(None),
    plan_id: Optional[str] = Query(None),
    include_review: bool = Query(False),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    session: Session = Depends(_session),
) -> RFPListResponse:
    q = session.query(db.RFPRecord)
    if not include_review:
        q = q.filter(db.RFPRecord.needs_review.is_(False))
    if plan_id:
        q = q.filter(db.RFPRecord.plan_id == plan_id)

    rows = q.order_by(db.RFPRecord.extracted_at.desc()).all()
    filtered = []
    for row in rows:
        payload = json.loads(row.record)
        if rfp_type and payload.get("rfp_type") != rfp_type:
            continue
        if year is not None and not _filter_by_year(payload, year, row.extracted_at):
            continue
        filtered.append(row)

    total = len(filtered)
    page = filtered[offset : offset + limit]

    last_run = (
        session.query(db.PipelineRun)
        .order_by(db.PipelineRun.completed_at.desc().nullslast())
        .first()
    )
    pending_review = session.query(db.RFPRecord).filter(
        db.RFPRecord.needs_review.is_(True)
    ).count()

    return RFPListResponse(
        results=[_to_response(r) for r in page],
        total=total,
        pipeline_health=PipelineHealth(
            last_scan_at=last_run.completed_at if last_run else None,
            field_accuracy=_read_field_accuracy(),
            records_pending_review=pending_review,
        ),
    )


@router.get("/rfps/stats", response_model=RFPStatsResponse)
def stats(
    year: Optional[int] = Query(None),
    session: Session = Depends(_session),
) -> RFPStatsResponse:
    rows = session.query(db.RFPRecord).filter(db.RFPRecord.needs_review.is_(False)).all()
    by_type: dict[str, int] = {}
    total = 0
    for row in rows:
        payload = json.loads(row.record)
        if year is not None and not _filter_by_year(payload, year, row.extracted_at):
            continue
        by_type[payload["rfp_type"]] = by_type.get(payload["rfp_type"], 0) + 1
        total += 1
    return RFPStatsResponse(total=total, by_type=by_type)
