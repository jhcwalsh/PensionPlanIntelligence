"""
SQLite database schema and helper functions using SQLAlchemy.
"""

import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import (
    Column, Integer, String, Text, Float, DateTime, Boolean,
    ForeignKey, create_engine, UniqueConstraint, Index
)
from sqlalchemy.orm import DeclarativeBase, Session, relationship, sessionmaker

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
    extracted_text = Column(Text)
    extraction_status = Column(String, default="pending")   # pending, done, failed
    page_count = Column(Integer)
    meeting_date = Column(DateTime)     # parsed from document or filename

    plan = relationship("Plan", back_populates="documents")
    meeting = relationship("Meeting", back_populates="documents")
    summary = relationship("Summary", back_populates="document", uselist=False,
                           cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_documents_plan_id", "plan_id"),
        Index("ix_documents_meeting_date", "meeting_date"),
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
