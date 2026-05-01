"""
SQLite database schema and helper functions using SQLAlchemy.
"""

import gzip
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import (
    Column, Integer, String, Text, Float, DateTime, Boolean, Date, JSON,
    LargeBinary, ForeignKey, create_engine, UniqueConstraint, Index
)
from sqlalchemy.orm import DeclarativeBase, Session, relationship, sessionmaker
from sqlalchemy.types import TypeDecorator


class GzippedText(TypeDecorator):
    """Transparent gzip wrapper for large text columns.

    Callers see str both ways; on disk values are gzipped UTF-8 bytes.
    Legacy uncompressed str rows are returned as-is until rewritten,
    which keeps the migration idempotent and lets the model change
    land before the data is rewritten.
    """
    impl = LargeBinary
    cache_ok = True

    GZIP_MAGIC = b"\x1f\x8b"

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if isinstance(value, (bytes, bytearray)):
            return bytes(value)
        return gzip.compress(str(value).encode("utf-8"))

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if isinstance(value, (bytes, bytearray)):
            b = bytes(value)
            if b.startswith(self.GZIP_MAGIC):
                return gzip.decompress(b).decode("utf-8")
            return b.decode("utf-8", errors="replace")
        return value

DB_PATH = os.environ.get(
    "DB_PATH",
    os.path.join(os.path.dirname(__file__), "db", "pension.db"),
)
DATABASE_URL = f"sqlite:///{DB_PATH}"

engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class Plan(Base):
    __tablename__ = "plans"

    id = Column(String, primary_key=True)           # e.g. "calpers"
    name = Column(String, nullable=False)
    abbreviation = Column(String)
    state = Column(String(2))
    aum_billions = Column(Float)
    website = Column(String)
    materials_url = Column(String)
    fiscal_year_end = Column(String(5))             # MM-DD, e.g. "06-30"

    meetings = relationship("Meeting", back_populates="plan", cascade="all, delete-orphan")
    documents = relationship("Document", back_populates="plan", cascade="all, delete-orphan")


class Meeting(Base):
    __tablename__ = "meetings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    plan_id = Column(String, ForeignKey("plans.id"), nullable=False)
    meeting_date = Column(DateTime)
    meeting_type = Column(String)       # board, investment, audit, etc.
    title = Column(String)
    source_url = Column(String)

    plan = relationship("Plan", back_populates="meetings")
    documents = relationship("Document", back_populates="meeting", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("plan_id", "meeting_date", "meeting_type", name="uq_meeting"),
    )


class Document(Base):
    __tablename__ = "documents"

    id = Column(Integer, primary_key=True, autoincrement=True)
    plan_id = Column(String, ForeignKey("plans.id"), nullable=False)
    meeting_id = Column(Integer, ForeignKey("meetings.id"), nullable=True)
    url = Column(String, nullable=False, unique=True)
    filename = Column(String)
    doc_type = Column(String)           # agenda, board_pack, minutes, performance
    local_path = Column(String)         # path to downloaded file
    file_size_bytes = Column(Integer)
    downloaded_at = Column(DateTime)
    extracted_text = Column(GzippedText)
    extraction_status = Column(String, default="pending")   # pending, done, failed
    page_count = Column(Integer)
    meeting_date = Column(DateTime)     # parsed from document or filename
    fiscal_year = Column(Integer)       # CAFR/ACFR fiscal year (e.g. 2024); null for non-CAFR docs

    plan = relationship("Plan", back_populates="documents")
    meeting = relationship("Meeting", back_populates="documents")
    summary = relationship("Summary", back_populates="document", uselist=False,
                           cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_documents_plan_id", "plan_id"),
        Index("ix_documents_meeting_date", "meeting_date"),
        Index("ix_documents_plan_fy", "plan_id", "doc_type", "fiscal_year"),
    )


class CafrRefreshLog(Base):
    """Per-plan outcome from each monthly CAFR refresh run."""
    __tablename__ = "cafr_refresh_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    plan_id = Column(String, ForeignKey("plans.id"), nullable=False)
    run_at = Column(DateTime, nullable=False)
    expected_year = Column(Integer)            # the FY we were checking for
    status = Column(String, nullable=False)
    # status values: "saved" | "already_have" | "not_yet_published" | "url_failed"
    #              | "validation_failed" | "no_strategy" | "error"
    url_tried = Column(String)
    document_id = Column(Integer, ForeignKey("documents.id"))  # set when saved
    notes = Column(Text)

    __table_args__ = (
        Index("ix_refresh_plan_run", "plan_id", "run_at"),
    )


class CafrExtract(Base):
    """One row per CAFR document we've extracted Investment Section data from."""
    __tablename__ = "cafr_extract"

    id = Column(Integer, primary_key=True, autoincrement=True)
    plan_id = Column(String, ForeignKey("plans.id"), nullable=False)
    document_id = Column(Integer, ForeignKey("documents.id"), nullable=False, unique=True)
    fiscal_year = Column(Integer)
    investment_policy_text = Column(Text)
    extracted_at = Column(DateTime)
    model_used = Column(String)
    pages_used = Column(String)        # e.g. "45-78" — which PDF pages fed the extraction
    text_hash = Column(String)         # MD5 of the section text — skip re-extraction
    notes = Column(Text)

    plan = relationship("Plan")
    document = relationship("Document")
    allocations = relationship("CafrAllocation", back_populates="extract",
                               cascade="all, delete-orphan")
    performance = relationship("CafrPerformance", back_populates="extract",
                               cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_cafr_extract_plan_fy", "plan_id", "fiscal_year"),
    )


class CafrAllocation(Base):
    """Long-form asset-allocation row: one per (CAFR, asset class)."""
    __tablename__ = "cafr_allocation"

    id = Column(Integer, primary_key=True, autoincrement=True)
    cafr_extract_id = Column(Integer, ForeignKey("cafr_extract.id"), nullable=False)
    asset_class = Column(String, nullable=False)
    target_pct = Column(Float)
    actual_pct = Column(Float)
    target_range_low = Column(Float)
    target_range_high = Column(Float)
    notes = Column(String)

    extract = relationship("CafrExtract", back_populates="allocations")

    __table_args__ = (
        Index("ix_cafr_alloc_extract", "cafr_extract_id"),
    )


class CafrPerformance(Base):
    """Long-form performance row: one per (CAFR, scope, period)."""
    __tablename__ = "cafr_performance"

    id = Column(Integer, primary_key=True, autoincrement=True)
    cafr_extract_id = Column(Integer, ForeignKey("cafr_extract.id"), nullable=False)
    scope = Column(String, nullable=False)         # "total_fund" or asset class name
    period = Column(String, nullable=False)        # "fy" | "1y" | "3y" | "5y" | "10y" | "since_inception"
    return_pct = Column(Float)
    benchmark_return_pct = Column(Float)
    benchmark_name = Column(String)
    notes = Column(String)

    extract = relationship("CafrExtract", back_populates="performance")

    __table_args__ = (
        Index("ix_cafr_perf_extract", "cafr_extract_id"),
        Index("ix_cafr_perf_lookup", "cafr_extract_id", "scope", "period"),
    )


class Publication(Base):
    """One row per scheduled CIO Insights publication, regardless of status.

    Lifecycle: generating → awaiting_approval → (approved | rejected | expired)
                                              ↓ approved
                                          published

    `(cadence, period_start)` is unique — re-running a cycle for the same
    period reuses the existing row rather than creating a duplicate.
    """
    __tablename__ = "publications"

    id = Column(Integer, primary_key=True, autoincrement=True)
    cadence = Column(String, nullable=False)        # 'weekly' | 'monthly' | 'annual'
    period_start = Column(Date, nullable=False)
    period_end = Column(Date, nullable=False)
    status = Column(String, nullable=False, default="generating")
    # status values: generating, awaiting_approval, approved, rejected,
    #                published, expired, failed

    draft_markdown = Column(Text)               # populated after compose
    pdf_path = Column(String)                   # populated after render
    composed_at = Column(DateTime)
    approved_at = Column(DateTime)
    rejected_at = Column(DateTime)
    published_at = Column(DateTime)
    expires_at = Column(DateTime)               # 7 days after composed_at
    error_message = Column(Text)                # if status='failed'
    reminder_sent_at = Column(DateTime)         # 72-hour nudge

    # For monthly/annual: the publication ids of the lower-cadence inputs
    source_publication_ids = Column(JSON)       # list[int] or null

    tokens = relationship("ApprovalToken", back_populates="publication",
                          cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("cadence", "period_start", name="uq_publication_cadence_period"),
        Index("ix_publication_status", "status"),
    )


class ApprovalToken(Base):
    """Magic-link tokens for one-click approval/rejection.

    Two tokens are issued per publication — one for approve, one for reject.
    Tokens expire after `APPROVAL_TOKEN_TTL_DAYS` (default 7) and are
    single-use (consumed_at is set atomically with the action).
    """
    __tablename__ = "approval_tokens"

    id = Column(Integer, primary_key=True, autoincrement=True)
    publication_id = Column(Integer, ForeignKey("publications.id"), nullable=False)
    token_hash = Column(String, nullable=False, unique=True)  # sha256 of raw token
    action = Column(String, nullable=False)     # 'approve' | 'reject'
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    expires_at = Column(DateTime, nullable=False)
    consumed_at = Column(DateTime)

    publication = relationship("Publication", back_populates="tokens")

    __table_args__ = (
        Index("ix_approval_token_pub", "publication_id"),
    )


class WeeklyRun(Base):
    """One row per weekly scrape/extract run, for resumability and audit."""
    __tablename__ = "weekly_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    period_start = Column(Date, nullable=False)
    period_end = Column(Date, nullable=False)
    started_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    completed_at = Column(DateTime)
    status = Column(String, nullable=False, default="running")
    # status: running, succeeded, failed, partial

    plans_total = Column(Integer)
    plans_completed = Column(Integer, default=0)
    documents_fetched = Column(Integer, default=0)
    records_extracted = Column(Integer, default=0)
    error_message = Column(Text)

    plan_runs = relationship("WeeklyRunPlan", back_populates="run",
                             cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("period_start", name="uq_weekly_run_period"),
        Index("ix_weekly_run_status", "status"),
    )


class WeeklyRunPlan(Base):
    """One row per (run, plan) — supports per-plan resumability."""
    __tablename__ = "weekly_run_plans"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, ForeignKey("weekly_runs.id"), nullable=False)
    plan_id = Column(String, ForeignKey("plans.id"), nullable=False)
    status = Column(String, nullable=False, default="pending")
    # status: pending, fetching, extracting, succeeded, failed, skipped
    error_message = Column(Text)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    documents_fetched = Column(Integer, default=0)

    run = relationship("WeeklyRun", back_populates="plan_runs")

    __table_args__ = (
        UniqueConstraint("run_id", "plan_id", name="uq_weekly_run_plan"),
        Index("ix_weekly_run_plan_status", "run_id", "status"),
    )


class Summary(Base):
    __tablename__ = "summaries"

    id = Column(Integer, primary_key=True, autoincrement=True)
    document_id = Column(Integer, ForeignKey("documents.id"), nullable=False, unique=True)
    summary_text = Column(Text)
    key_topics = Column(Text)           # JSON list of topic strings
    investment_actions = Column(Text)   # JSON list: manager hires/fires, allocation changes
    decisions = Column(Text)            # JSON list of formal decisions/votes
    performance_data = Column(Text)     # JSON: returns by asset class if present
    generated_at = Column(DateTime)
    model_used = Column(String)
    text_hash = Column(String)          # MD5 of extracted_text — skip re-summarizing duplicates

    document = relationship("Document", back_populates="summary")


# ---------------------------------------------------------------------------
# RFP pipeline tables (added without Alembic; init_db() creates if missing)
# ---------------------------------------------------------------------------

# Confidence threshold below which a record is held back from the default
# API view and surfaces only via ?include_review=true.
RFP_REVIEW_CONFIDENCE_THRESHOLD = 0.70

# Bumped when the prompt, schema, or extraction logic changes in a way that
# invalidates prior extractions. Old records remain in the table for
# regression analysis; the orchestrator re-extracts when prompt_version
# changes.
RFP_PROMPT_VERSION = "rfp_v1"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _new_run_id() -> str:
    return uuid.uuid4().hex


class DocumentHealth(Base):
    """
    Stage-1 PDF-quality diagnostic verdict for one document. One row per
    (document, prompt_version) so a prompt bump triggers re-diagnosis +
    re-extraction; the row also doubles as the "this doc has been processed"
    marker used by get_documents_pending_rfp_extraction.
    """

    __tablename__ = "document_health"

    document_id = Column(Integer, ForeignKey("documents.id"), primary_key=True)
    prompt_version = Column(String, primary_key=True, default=RFP_PROMPT_VERSION)
    stage1_verdict = Column(String, nullable=False)   # STAGE_1_HEALTHY | STAGE_1_SUSPECTED | NO_TASK_CONTENT
    blank_pages = Column(Integer, default=0)
    scanned_pages = Column(Integer, default=0)
    garbled_pages = Column(Integer, default=0)
    task_relevant_pages = Column(Integer, default=0)
    structure_score = Column(Float)
    rationale = Column(Text)                          # JSON list of strings
    evaluated_at = Column(DateTime, default=_utcnow, nullable=False)


class RFPRecord(Base):
    """One structured RFP extracted from a document, validated against rfp_schema.json."""

    __tablename__ = "rfp_records"

    rfp_id = Column(String(16), primary_key=True)
    document_id = Column(Integer, ForeignKey("documents.id"), nullable=False)
    plan_id = Column(String, ForeignKey("plans.id"), nullable=False)
    record = Column(Text, nullable=False)             # JSON-serialized full record
    extraction_confidence = Column(Float, nullable=False)
    needs_review = Column(Boolean, nullable=False, default=False)
    prompt_version = Column(String, nullable=False, default=RFP_PROMPT_VERSION)
    extracted_at = Column(DateTime, default=_utcnow, nullable=False)

    __table_args__ = (
        Index("ix_rfp_plan", "plan_id"),
        Index("ix_rfp_extracted_at", "extracted_at"),
        Index("ix_rfp_needs_review", "needs_review"),
    )


class PipelineRun(Base):
    """One row per RFP-extraction run, for observability and the API health block."""

    __tablename__ = "pipeline_runs"

    run_id = Column(String(32), primary_key=True, default=_new_run_id)
    started_at = Column(DateTime, default=_utcnow, nullable=False)
    completed_at = Column(DateTime)
    documents_discovered = Column(Integer, default=0)
    documents_processed = Column(Integer, default=0)
    records_extracted = Column(Integer, default=0)
    errors = Column(Text, default="[]")               # JSON list of error strings
    status = Column(String, nullable=False, default="running")  # running|succeeded|failed


# ---------------------------------------------------------------------------
# Init / helpers
# ---------------------------------------------------------------------------

def init_db():
    """Create all tables if they don't exist."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    Base.metadata.create_all(engine)


def get_session() -> Session:
    return SessionLocal()


def upsert_plan(session: Session, plan_data: dict) -> Plan:
    plan = session.get(Plan, plan_data["id"])
    if plan is None:
        plan = Plan(**{k: v for k, v in plan_data.items()
                       if k in Plan.__table__.columns.keys()})
        session.add(plan)
    return plan


def get_or_create_meeting(session: Session, plan_id: str, meeting_date: Optional[datetime],
                           meeting_type: str, title: str = None,
                           source_url: str = None) -> Meeting:
    q = session.query(Meeting).filter_by(plan_id=plan_id, meeting_date=meeting_date,
                                          meeting_type=meeting_type)
    meeting = q.first()
    if meeting is None:
        meeting = Meeting(plan_id=plan_id, meeting_date=meeting_date,
                          meeting_type=meeting_type, title=title, source_url=source_url)
        session.add(meeting)
        session.flush()
    return meeting


def document_exists(session: Session, url: str) -> bool:
    return session.query(Document).filter_by(url=url).first() is not None


def get_unextracted_documents(session: Session) -> list[Document]:
    return session.query(Document).filter_by(extraction_status="pending").all()


def summary_exists_for_hash(session: Session, text_hash: str) -> Summary | None:
    """Return an existing Summary with the same text hash, or None."""
    return session.query(Summary).filter_by(text_hash=text_hash).first()


def get_unsummarized_documents(session: Session) -> list[Document]:
    return (
        session.query(Document)
        .filter(Document.extraction_status == "done")
        .filter(~Document.id.in_(
            session.query(Summary.document_id)
        ))
        .all()
    )


def get_new_meetings(session: Session, days: int = 7) -> list[dict]:
    """
    Return meetings that have documents downloaded within the last N days.
    Groups by (plan_id, meeting_date). For each meeting returns:
      plan, meeting_date, agenda_doc, agenda_summary, all_docs
    Sorted by meeting_date descending.
    """
    cutoff = datetime.utcnow() - timedelta(days=days)
    recent_docs = (
        session.query(Document)
        .filter(Document.downloaded_at >= cutoff)
        .order_by(Document.meeting_date.desc())
        .all()
    )

    # Group by (plan_id, meeting_date)
    seen: dict[tuple, dict] = {}
    for doc in recent_docs:
        key = (doc.plan_id, doc.meeting_date)
        if key not in seen:
            plan = session.get(Plan, doc.plan_id)
            seen[key] = {
                "plan": plan,
                "meeting_date": doc.meeting_date,
                "all_docs": [],
                "agenda_doc": None,
                "agenda_summary": None,
            }
        seen[key]["all_docs"].append(doc)
        # Prefer agenda; fall back to board_pack
        entry = seen[key]
        if doc.doc_type == "agenda":
            entry["agenda_doc"] = doc
        elif entry["agenda_doc"] is None and doc.doc_type in ("board_pack", "minutes"):
            entry["agenda_doc"] = doc

    # Attach summaries
    for entry in seen.values():
        if entry["agenda_doc"]:
            entry["agenda_summary"] = (
                session.query(Summary)
                .filter_by(document_id=entry["agenda_doc"].id)
                .first()
            )

    return sorted(seen.values(), key=lambda e: e["meeting_date"] or datetime.min, reverse=True)


def search_summaries(session: Session, query: str, plan_id: str = None,
                     limit: int = 20) -> list[tuple[Document, Summary]]:
    """Simple full-text search across summary_text and key_topics."""
    q = (
        session.query(Document, Summary)
        .join(Summary, Document.id == Summary.document_id)
        .filter(Summary.summary_text.ilike(f"%{query}%") |
                Summary.key_topics.ilike(f"%{query}%") |
                Summary.investment_actions.ilike(f"%{query}%"))
    )
    if plan_id:
        q = q.filter(Document.plan_id == plan_id)
    return q.order_by(Document.meeting_date.desc()).limit(limit).all()


def get_documents_pending_rfp_extraction(
    session: Session,
    prompt_version: str = RFP_PROMPT_VERSION,
    plan_ids: list[str] | None = None,
) -> list[Document]:
    """
    Documents that have been text-extracted but not yet processed by the RFP
    pipeline at the current prompt version. Re-running the orchestrator after
    a prompt bump picks up everything automatically.
    """
    from sqlalchemy import select
    already_diagnosed = (
        select(DocumentHealth.document_id)
        .where(DocumentHealth.prompt_version == prompt_version)
        .distinct()
    )
    q = (
        session.query(Document)
        .filter(Document.extraction_status == "done")
        .filter(~Document.id.in_(already_diagnosed))
    )
    if plan_ids:
        q = q.filter(Document.plan_id.in_(plan_ids))
    return q.order_by(Document.meeting_date.desc().nullslast()).all()


def upsert_rfp_record(
    session: Session,
    *,
    rfp_id: str,
    document_id: int,
    plan_id: str,
    record_json: str,
    extraction_confidence: float,
    prompt_version: str = RFP_PROMPT_VERSION,
) -> RFPRecord:
    """Insert or update an RFP record by deterministic rfp_id."""
    existing = session.get(RFPRecord, rfp_id)
    needs_review = extraction_confidence < RFP_REVIEW_CONFIDENCE_THRESHOLD
    if existing is None:
        existing = RFPRecord(
            rfp_id=rfp_id,
            document_id=document_id,
            plan_id=plan_id,
            record=record_json,
            extraction_confidence=extraction_confidence,
            needs_review=needs_review,
            prompt_version=prompt_version,
        )
        session.add(existing)
    else:
        existing.document_id = document_id
        existing.plan_id = plan_id
        existing.record = record_json
        existing.extraction_confidence = extraction_confidence
        existing.needs_review = needs_review
        existing.prompt_version = prompt_version
        existing.extracted_at = _utcnow()
    return existing


def upsert_document_health(
    session: Session,
    *,
    document_id: int,
    verdict: str,
    blank_pages: int,
    scanned_pages: int,
    garbled_pages: int,
    task_relevant_pages: int,
    structure_score: float,
    rationale_json: str,
    prompt_version: str = RFP_PROMPT_VERSION,
) -> DocumentHealth:
    existing = session.get(DocumentHealth, (document_id, prompt_version))
    if existing is None:
        existing = DocumentHealth(
            document_id=document_id,
            prompt_version=prompt_version,
        )
        session.add(existing)
    existing.stage1_verdict = verdict
    existing.blank_pages = blank_pages
    existing.scanned_pages = scanned_pages
    existing.garbled_pages = garbled_pages
    existing.task_relevant_pages = task_relevant_pages
    existing.structure_score = structure_score
    existing.rationale = rationale_json
    existing.evaluated_at = _utcnow()
    return existing


if __name__ == "__main__":
    init_db()
    print(f"Database initialised at {DB_PATH}")
