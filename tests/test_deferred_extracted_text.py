"""``documents.extracted_text`` must not ride along on ordinary queries.

The column is half the database — 33.7 MB gzipped across 4,257 rows — and
before this change every ``session.query(Document)`` dragged it over the
wire whether or not the caller ever read it.

On SQLite that was a local file read and cost nothing measurable. Against
Neon it is the entire bill: ``queries.cafr_coverage_rows()`` pulled 140 CAFR
documents — 5.2 MB of blob — purely to count fiscal years and allocation
rows, and on a 300-second cache TTL that one function can move 1.5 GB a day.
Four days after the Postgres cutover the project hit Neon's 5 GB transfer
quota, compute was suspended, and every consumer went down at once: the
Streamlit service, all eight GHA crons, and local shells.

So the column is mapper-level deferred. Callers that genuinely want the text
still get it — transparently, on attribute access — but they now pay for it
explicitly, and the ones looping over many documents say ``undefer`` so the
laziness does not turn into an N+1 storm.
"""

from __future__ import annotations

import pathlib

import pytest
from sqlalchemy import event, inspect
from sqlalchemy import text as sa_text
from sqlalchemy.orm import undefer

import database

ROOT = pathlib.Path(__file__).resolve().parents[1]

TEXT = "the quick brown fox " * 500


@pytest.fixture()
def doc_id(tmp_db):
    """One document carrying real text, committed and expunged."""
    session = database.get_session()
    try:
        doc = database.Document(
            plan_id="calpers",
            url="https://example.invalid/board_pack.pdf",
            filename="board_pack.pdf",
            doc_type="board_pack",
            extracted_text=TEXT,
        )
        session.add(doc)
        session.commit()
        return doc.id
    finally:
        session.close()


@pytest.fixture()
def emitted_sql(tmp_db):
    """Every statement the engine executes, captured verbatim."""
    statements: list[str] = []

    def record(conn, cursor, statement, params, context, executemany):
        statements.append(statement)

    event.listen(database.engine, "before_cursor_execute", record)
    yield statements
    event.remove(database.engine, "before_cursor_execute", record)


def test_a_plain_query_leaves_the_text_unloaded(doc_id):
    """The whole point: the blob does not come back unless asked for."""
    session = database.get_session()
    try:
        doc = session.query(database.Document).filter_by(id=doc_id).one()
        assert "extracted_text" in inspect(doc).unloaded, (
            "extracted_text was loaded eagerly — every query(Document) is "
            "still shipping the blob")
    finally:
        session.close()


def test_the_sql_itself_omits_the_column(doc_id, emitted_sql):
    """Stronger than the mapper check: prove it never reaches the wire.

    ``unloaded`` could in principle be satisfied by an ORM that fetched the
    column and discarded it. The bytes are what cost money, so assert on the
    statement actually sent.
    """
    session = database.get_session()
    try:
        session.query(database.Document).filter_by(id=doc_id).one()
    finally:
        session.close()

    selects = [s for s in emitted_sql if s.lstrip().upper().startswith("SELECT")]
    assert selects, "no SELECT was captured — the fixture is not wired up"
    assert not any("extracted_text" in s for s in selects), (
        "extracted_text appears in the emitted SQL:\n  "
        + "\n  ".join(selects))


def test_the_text_is_still_correct_on_access(doc_id):
    """Deferral must be transparent — summarizer.py reads this attribute."""
    session = database.get_session()
    try:
        doc = session.query(database.Document).filter_by(id=doc_id).one()
        assert doc.extracted_text == TEXT
        assert "extracted_text" not in inspect(doc).unloaded
    finally:
        session.close()


def test_undefer_brings_it_back_eagerly(doc_id, emitted_sql):
    """The escape hatch the bulk readers use, so they avoid an N+1."""
    session = database.get_session()
    try:
        doc = (session.query(database.Document)
               .options(undefer(database.Document.extracted_text))
               .filter_by(id=doc_id).one())
        assert "extracted_text" not in inspect(doc).unloaded
        assert doc.extracted_text == TEXT
    finally:
        session.close()

    assert any("extracted_text" in s for s in emitted_sql), (
        "undefer() did not put the column in the query")


def test_writing_the_column_still_works(tmp_db):
    """extractor.py assigns to it; deferral must not break the write path."""
    session = database.get_session()
    try:
        doc = database.Document(plan_id="calpers", doc_type="agenda",
                                  url="https://example.invalid/x.pdf",
                                  filename="x.pdf")
        session.add(doc)
        session.commit()
        doc.extracted_text = "fresh text"
        session.commit()
        new_id = doc.id
    finally:
        session.close()

    session = database.get_session()
    try:
        assert session.get(database.Document, new_id).extracted_text == "fresh text"
    finally:
        session.close()


def test_gzip_roundtrip_survives_deferral(doc_id):
    """GzippedText is a TypeDecorator; deferred loading uses the same path."""
    session = database.get_session()
    try:
        doc = session.get(database.Document, doc_id)
        assert doc.extracted_text == TEXT
        # Raw SQL, so the GzippedText TypeDecorator does not decompress on
        # the way out -- this is the on-disk representation.
        stored = session.execute(
            sa_text("SELECT extracted_text FROM documents WHERE id = :i"),
            {"i": doc_id}).scalar_one()
        assert isinstance(stored, (bytes, bytearray)), type(stored)
        assert stored[:2] == b"\x1f\x8b", "not gzipped on disk any more"
    finally:
        session.close()


# ---------------------------------------------------------------------------
# The N+1 guard
#
# Deferral turns "one query, too many bytes" into "one query per document"
# for anyone looping over documents and reading the text. These three do
# exactly that, so each must undefer explicitly.
# ---------------------------------------------------------------------------

_BULK_TEXT_READERS = ("fetch_cafr.py", "fetch_ips.py", "discover_video_sources.py")


@pytest.mark.parametrize("module", _BULK_TEXT_READERS)
def test_bulk_text_readers_undefer(module):
    src = (ROOT / module).read_text(encoding="utf-8")
    assert "undefer" in src, (
        "%s loops over documents reading extracted_text — without undefer() "
        "it issues one extra SELECT per document" % module)


def test_the_summarise_path_undefers(tmp_db):
    """Behavioural, not a source grep.

    The summariser reads extracted_text for every document it is handed, so
    leaving the column lazy there would swap one query for N+1 round trips.
    Same bytes, far more latency -- and the N+1 is invisible on SQLite,
    which is exactly how it would reach production unnoticed.
    """
    session = database.get_session()
    try:
        for i in range(3):
            session.add(database.Document(
                plan_id="calpers",
                url="https://example.invalid/%d.pdf" % i,
                filename="%d.pdf" % i,
                doc_type="agenda",
                extraction_status="done",
                extracted_text=TEXT))
        session.commit()
    finally:
        session.close()

    session = database.get_session()
    try:
        docs = database.get_unsummarized_documents(session)
        assert len(docs) == 3, "fixture did not produce unsummarised documents"
        for doc in docs:
            assert "extracted_text" not in inspect(doc).unloaded, (
                "get_unsummarized_documents left the text deferred — the "
                "summariser will issue one extra SELECT per document")
    finally:
        session.close()
