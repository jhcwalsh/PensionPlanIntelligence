"""Pydantic response models for the RFP API."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field

RFPType = Literal["Consultant", "Manager", "Custodian", "Actuary", "Audit", "Legal"]
RFPStatus = Literal["Planned", "Issued", "ResponsesReceived",
                    "FinalistsNamed", "Awarded", "Withdrawn"]


class SourceDocument(BaseModel):
    url: str
    page_number: int
    document_id: int


class RFPResponse(BaseModel):
    """Mirrors lib/rfp_schema.json plus operational metadata."""

    rfp_id: str
    plan_id: str
    rfp_type: RFPType
    title: str
    status: RFPStatus
    release_date: Optional[str] = None
    response_due_date: Optional[str] = None
    award_date: Optional[str] = None
    mandate_size_usd_millions: Optional[float] = None
    asset_class: Optional[str] = None
    incumbent_manager: Optional[str] = None
    incumbent_manager_id: None = None
    shortlisted_managers: list[str] = Field(default_factory=list)
    awarded_manager: Optional[str] = None
    source_document: SourceDocument
    source_quote: str
    extraction_confidence: float

    # Operational metadata layered on top of the schema body
    needs_review: bool
    extracted_at: datetime
    prompt_version: str


class PipelineHealth(BaseModel):
    last_scan_at: Optional[datetime] = None
    field_accuracy: Optional[float] = None
    records_pending_review: int = 0


class RFPListResponse(BaseModel):
    results: list[RFPResponse]
    total: int
    pipeline_health: PipelineHealth


class RFPStatsResponse(BaseModel):
    total: int
    by_type: dict[str, int]
