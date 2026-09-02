"""Re-extracting the documents cut off at the old 150,000-character cap.

The cap was a consequence of the database being a SQLite file in git. It is
2,000,000 now and the PDFs are retained, so the text can be read again -- 449
documents, no model, no cost.

The property under test above all others is that this **never shrinks a
document**. A truncated 150,000 characters is worth more than a complete 400,
and a PDF that re-reads short is the signature of a bad retained copy or a
changed source -- something to investigate, never something to overwrite. The
standing rule in this repo is that extracts are not thrown away.
"""
from datetime import date

import pytest

import database
from database import Document, Plan
from scripts import reextract_truncated as rx


CAP = rx.OLD_CAP


def _text(n: int) -> str:
    """`n` characters that compress roughly like real extracted text.

    Not `"x" * n`. extracted_text is gzipped on disk and the candidate query
    narrows on compressed size, so a string of one repeated character shrinks
    to a few hundred bytes and falls outside the band every real document sits
    in -- the fixtures would then be testing nothing. Hex compresses at about
    two characters per byte, the low end of the 1.98-6.63 range measured
    across this corpus.
    """
    import secrets
    return secrets.token_hex(n // 2)[:n]


@pytest.fixture
def session(tmp_db):
    s = database.get_session()
    s.add(Plan(id="mcera", name="MCERA", state="CA"))
    s.commit()
    yield s
    s.close()


def _doc(session, text, name="pack.pdf", status="done"):
    d = Document(plan_id="mcera", url=f"https://x/{name}", filename=name,
                 meeting_date=date(2026, 5, 14), extraction_status=status,
                 extracted_text=text)
    session.add(d)
    session.commit()
    return d


# --------------------------------------------------------------------------
# Which documents are candidates
# --------------------------------------------------------------------------

def test_a_document_at_the_cap_is_a_candidate(session):
    _doc(session, _text(CAP))
    assert len(rx.candidates(session)) == 1


def test_a_document_comfortably_under_the_cap_is_left_alone(session):
    _doc(session, _text(40_000))
    assert rx.candidates(session) == []


def test_a_document_far_over_the_cap_is_left_alone(session):
    """Already re-extracted, or extracted after the cap was raised. Long for
    honest reasons, and re-reading it would be work for nothing."""
    _doc(session, _text(900_000))
    assert rx.candidates(session) == []


def test_a_failed_document_is_not_a_candidate(session):
    """Those need OCR and are a different, priced piece of work."""
    _doc(session, _text(CAP), status="failed")
    assert rx.candidates(session) == []


def test_an_empty_document_is_not_a_candidate(session):
    _doc(session, None)
    _doc(session, "", name="b.pdf")
    assert rx.candidates(session) == []


def test_the_shortlist_is_narrowed_in_sql_before_any_text_is_read(session):
    """Not a micro-optimisation. Measuring len() on every extracted document
    pulls the whole corpus's text across the network to answer a question
    about lengths -- the read shape that exhausted Neon's transfer quota on
    2026-08-25, and that crashed this script's own first live run."""
    _doc(session, _text(CAP))
    _doc(session, _text(40_000), name="small.pdf")

    from sqlalchemy import event

    seen = []
    engine = session.get_bind()

    @event.listens_for(engine, "before_cursor_execute")
    def record(conn, cursor, statement, params, context, executemany):
        seen.append(statement)

    try:
        rx.candidates(session)
    finally:
        event.remove(engine, "before_cursor_execute", record)

    assert any("octet_length" in s.lower() for s in seen), (
        "no length filter reached the database -- the shortlist is being "
        "built in Python over every document's text")


def test_limit_stops_early(session):
    for i in range(4):
        _doc(session, _text(CAP), name=f"{i}.pdf")
    assert len(rx.candidates(session, limit=2)) == 2


# --------------------------------------------------------------------------
# The shrink guard -- the property that matters most
# --------------------------------------------------------------------------

def _run(monkeypatch, session, doc, new_text, status="done", pages=10):
    """Drive main() with the PDF read stubbed to return `new_text`."""
    import contextlib

    @contextlib.contextmanager
    def fake_pdf(document, cfg=None):
        yield "ignored.pdf"

    monkeypatch.setattr(rx.pdf_store, "document_pdf", fake_pdf)
    monkeypatch.setattr(
        rx.extractor, "_extract_from_path",
        lambda d, p: rx.extractor.ExtractOutcome(
            text=new_text, pages=pages, status=status))
    monkeypatch.setattr(rx.database, "get_session", lambda: session)
    monkeypatch.setattr(session, "close", lambda: None)
    rx.main(["--apply"])
    session.refresh(doc)


def test_a_shorter_re_read_never_overwrites(monkeypatch, session):
    """The whole point. A retained copy that reads back as 400 characters
    must not replace 150,000 real ones."""
    doc = _doc(session, _text(CAP))

    _run(monkeypatch, session, doc, "tiny")

    assert len(doc.extracted_text) == CAP


def test_a_longer_re_read_is_written(monkeypatch, session):
    doc = _doc(session, _text(CAP))

    _run(monkeypatch, session, doc, _text(900_000))

    assert len(doc.extracted_text) == 900_000
    assert doc.page_count == 10


def test_a_failed_re_read_keeps_the_stored_text(monkeypatch, session):
    """An image-only re-read returns almost nothing and reports its status.
    Refusing on the status says why, rather than letting the shrink guard
    catch it silently."""
    doc = _doc(session, _text(CAP))

    _run(monkeypatch, session, doc, "", status="failed")

    assert len(doc.extracted_text) == CAP


def test_a_missing_pdf_is_counted_not_raised(monkeypatch, session):
    """The backfill's problem, not this script's. It must keep going."""
    import contextlib

    doc = _doc(session, _text(CAP))

    @contextlib.contextmanager
    def missing(document, cfg=None):
        raise FileNotFoundError("neither on disk nor retained")
        yield  # pragma: no cover

    monkeypatch.setattr(rx.pdf_store, "document_pdf", missing)
    monkeypatch.setattr(rx.database, "get_session", lambda: session)
    monkeypatch.setattr(session, "close", lambda: None)

    assert rx.main(["--apply"]) == 0
    session.refresh(doc)
    assert len(doc.extracted_text) == CAP


def test_a_dry_run_writes_nothing(monkeypatch, session):
    import contextlib

    doc = _doc(session, _text(CAP))

    @contextlib.contextmanager
    def fake_pdf(document, cfg=None):
        yield "ignored.pdf"

    monkeypatch.setattr(rx.pdf_store, "document_pdf", fake_pdf)
    monkeypatch.setattr(
        rx.extractor, "_extract_from_path",
        lambda d, p: rx.extractor.ExtractOutcome(
            text=_text(900_000), pages=10, status="done"))
    monkeypatch.setattr(rx.database, "get_session", lambda: session)
    monkeypatch.setattr(session, "close", lambda: None)

    rx.main([])                      # no --apply
    session.expire(doc)

    assert len(doc.extracted_text) == CAP


def test_rerunning_after_success_finds_nothing(monkeypatch, session):
    """Idempotent by construction: a grown document no longer matches the
    candidate query, so a resume picks up only what is left."""
    doc = _doc(session, _text(CAP))

    _run(monkeypatch, session, doc, _text(900_000))

    assert rx.candidates(session) == []
