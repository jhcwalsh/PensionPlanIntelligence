"""
SQLite database schema and helper functions using SQLAlchemy.
"""

import os
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Column, Integer, String, Text, Float, DateTime, Boolean,
    ForeignKey, create_engine, UniqueConstraint, Index
)
from sqlalchemy.orm import DeclarativeBase, Session, relationship, sessionmaker

DB_PATH = os.path.join(os.path.dirname(__file__), "db", "pension.db")
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


def get_unsummarized_documents(session: Session) -> list[Document]:
    return (
        session.query(Document)
        .filter(Document.extraction_status == "done")
        .filter(~Document.id.in_(
            session.query(Summary.document_id)
        ))
        .all()
    )


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
