"""
SQLite database schema and helper functions using SQLAlchemy.
"""

import os
from datetime import datetime, timedelta
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
    extracted_text = Column(Text)
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


if __name__ == "__main__":
    init_db()
    print(f"Database initialised at {DB_PATH}")
