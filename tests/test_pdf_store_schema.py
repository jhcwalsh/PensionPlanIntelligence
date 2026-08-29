"""The three retention columns on documents.

Deliberately columns on an existing table rather than a new table: a
document has at most one stored object, so a join would buy nothing.

content_sha256 and r2_uploaded_at record success. retention_status records
why a null content_sha256 is still null -- without it the corpus cannot tell
"not yet backfilled" from "gone forever", which is the distinction spec §5
asks for.
"""
from __future__ import annotations

from datetime import datetime, timezone

import database
from database import Document, Plan, get_session


def test_document_carries_retention_columns(tmp_db):
    session = get_session()
    try:
        session.add(Plan(id="p1", name="P", abbreviation="P", state="CA"))
        doc = Document(
            plan_id="p1", url="https://x/a.pdf", filename="a.pdf",
            doc_type="board_pack",
            content_sha256="a" * 64,
            r2_uploaded_at=datetime(2026, 8, 29, tzinfo=timezone.utc),
        )
        session.add(doc)
        session.commit()

        got = session.query(Document).one()
        assert got.content_sha256 == "a" * 64
        assert got.r2_uploaded_at.year == 2026
    finally:
        session.close()


def test_retention_columns_default_to_null(tmp_db):
    """A document with no stored object is the normal pre-backfill state and
    must not require the columns to be set."""
    session = get_session()
    try:
        session.add(Plan(id="p2", name="P2", abbreviation="P2", state="CA"))
        session.add(Document(plan_id="p2", url="https://x/b.pdf",
                             filename="b.pdf", doc_type="agenda"))
        session.commit()
        got = session.query(Document).one()
        assert got.content_sha256 is None
        assert got.r2_uploaded_at is None
        assert got.retention_status is None
    finally:
        session.close()


def test_retention_status_records_why_a_document_is_unstored(tmp_db):
    """The marker spec §5 asks for: null content_sha256 alone cannot say
    whether a document is merely un-backfilled or permanently lost."""
    session = get_session()
    try:
        session.add(Plan(id="p3", name="P3", abbreviation="P3", state="CA"))
        session.add(Document(plan_id="p3", url="https://x/c.pdf",
                             filename="c.pdf", doc_type="agenda",
                             retention_status="unrecoverable"))
        session.commit()
        got = session.query(Document).one()
        assert got.retention_status == "unrecoverable"
        assert got.content_sha256 is None
    finally:
        session.close()
