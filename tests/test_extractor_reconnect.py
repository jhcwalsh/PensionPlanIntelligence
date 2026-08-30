"""One dropped connection must not strand a plan's whole extraction run.

Extraction sits between commits, so the database connection idles for as long
as the parse takes -- minutes on a large board pack. Neon closes it in that
window and the write lands on a dead socket. Found live on a 2,596-page MCERA
packet: it failed reproducibly and took the nine documents queued behind it
down with it, because the loop has no per-document guard.
"""
import pytest
from sqlalchemy.exc import InternalError, OperationalError

import extractor
from database import Document, ExtractionDetail, Plan


@pytest.fixture
def session(tmp_db):
    import database
    s = database.get_session()
    s.add(Plan(id="mcera", name="MCERA", state="CA"))
    s.commit()
    yield s
    s.close()


def _doc(session, **kw):
    d = Document(plan_id="mcera", url=kw.pop("url", "https://x/a.pdf"),
                 filename="a.pdf", extraction_status="pending", **kw)
    session.add(d)
    session.commit()
    return d


def _outcome(text="hello world", pages=3, status="done", reason=None):
    return extractor.ExtractOutcome(
        text=text, pages=pages, status=status, reason=reason, pages_ocred=None)


def test_persists_normally_when_the_connection_is_healthy(session):
    doc = _doc(session)
    extractor._persist_outcome(session, doc, _outcome())

    stored = session.get(Document, doc.id)
    assert stored.extraction_status == "done"
    assert stored.extracted_text == "hello world"
    assert stored.page_count == 3


def test_a_dropped_connection_is_retried_not_fatal(session, monkeypatch):
    doc = _doc(session)

    calls = {"n": 0}
    real_commit = session.commit

    def flaky_commit():
        calls["n"] += 1
        if calls["n"] == 1:
            raise OperationalError(
                "UPDATE documents SET extracted_text=...", {},
                Exception("SSL connection has been closed unexpectedly"))
        return real_commit()

    monkeypatch.setattr(session, "commit", flaky_commit)
    extractor._persist_outcome(session, doc, _outcome(text="recovered"))
    monkeypatch.undo()

    assert calls["n"] == 2, "should have retried exactly once"
    stored = session.get(Document, doc.id)
    assert stored.extraction_status == "done"
    assert stored.extracted_text == "recovered"


def test_idle_in_transaction_timeout_is_also_retried(session, monkeypatch):
    """The second face of the same fault.

    The idle window ends in one of two exceptions depending on who notices
    the dead connection first: psycopg reports a closed SSL socket as
    OperationalError, while Postgres killing the session for idling in a
    transaction arrives as InternalError. Catching only the first passes its
    test and then fails on the next large document.
    """
    doc = _doc(session)

    calls = {"n": 0}
    real_commit = session.commit

    def flaky_commit():
        calls["n"] += 1
        if calls["n"] == 1:
            raise InternalError(
                "UPDATE documents SET extracted_text=...", {},
                Exception("terminating connection due to "
                          "idle-in-transaction timeout"))
        return real_commit()

    monkeypatch.setattr(session, "commit", flaky_commit)
    extractor._persist_outcome(session, doc, _outcome(text="survived the timeout"))
    monkeypatch.undo()

    assert calls["n"] == 2
    assert session.get(Document, doc.id).extracted_text == "survived the timeout"


def test_a_second_failure_still_raises(session, monkeypatch):
    """One retry, not an infinite loop -- a genuinely dead database must surface."""
    doc = _doc(session)

    def always_fails():
        raise OperationalError("UPDATE documents", {}, Exception("gone"))

    monkeypatch.setattr(session, "commit", always_fails)
    with pytest.raises(OperationalError):
        extractor._persist_outcome(session, doc, _outcome())


def test_failed_outcome_does_not_clobber_existing_text(session):
    """The rule the original loop encoded: never overwrite good text with ''."""
    doc = _doc(session, extracted_text="kept from an earlier pass")
    extractor._persist_outcome(session, doc, _outcome(text="", status="failed"))

    stored = session.get(Document, doc.id)
    assert stored.extracted_text == "kept from an earlier pass"
    assert stored.extraction_status == "failed"


def test_reason_is_recorded_and_cleared(session):
    doc = _doc(session)

    extractor._persist_outcome(session, doc, _outcome(status="failed", reason="ocr_empty"))
    assert session.query(ExtractionDetail).filter_by(document_id=doc.id).count() == 1

    extractor._persist_outcome(session, doc, _outcome())
    assert session.query(ExtractionDetail).filter_by(document_id=doc.id).count() == 0
