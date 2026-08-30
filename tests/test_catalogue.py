"""The catalogue builder, and the guarantee that it cannot spend by accident.

On 2026-08-29 a run whose entire purpose was to avoid spending made 472 paid
calls, because `--extract-only` also summarises. The lesson is that a
guarantee living in a flag's name is not a guarantee. So the first test here
asserts no API client is ever constructed on the unapproved path -- not that
the cost came out at zero, which is what you check when you are hoping.
"""
import json

import pytest

import database
from database import Document, DocumentCatalogue, ExtractionDetail, Plan
from scripts import catalogue


@pytest.fixture
def session(tmp_db):
    s = database.get_session()
    s.add(Plan(id="mcera", name="MCERA", state="CA"))
    s.commit()
    yield s
    s.close()


def _partial_doc(session, n=1, text="AGENDA\n1. Investment performance report"):
    made = []
    for i in range(n):
        d = Document(plan_id="mcera", url=f"https://x/{i}.pdf", filename=f"{i}.pdf",
                     extraction_status="done", extracted_text=text)
        session.add(d)
        session.commit()
        session.add(ExtractionDetail(document_id=d.id, reason="ocr_partial",
                                     pages_total=200, pages_ocred=100))
        session.commit()
        made.append(d)
    return made


def _no_client(monkeypatch):
    """Make constructing a client an error, so any paid path fails loudly."""
    import summarizer

    def boom():
        raise AssertionError("an API client was constructed on a no-spend path")

    monkeypatch.setattr(summarizer, "_get_client", boom)


def test_without_approve_no_client_is_ever_built(session, monkeypatch, capsys):
    """The 2026-08-29 test. Not 'spend was zero' -- 'spending was unreachable'."""
    _partial_doc(session, n=3)
    _no_client(monkeypatch)
    monkeypatch.setattr("sys.argv", ["catalogue", "--backlog"])

    assert catalogue.main() == 0
    assert session.query(DocumentCatalogue).count() == 0
    assert "Nothing spent" in capsys.readouterr().out


def test_approve_builds_entries_without_touching_the_document(session, monkeypatch):
    docs = _partial_doc(session, n=2)
    original = docs[0].extracted_text

    monkeypatch.setattr(catalogue, "build_entry", lambda d: (
        {"contains": ["performance"], "sections": ["1. Performance"],
         "page_hints": "Tab 7, pp. 12-30"}, __import__("decimal").Decimal("0.001")))
    monkeypatch.setattr("sys.argv", ["catalogue", "--backlog", "--approve"])
    catalogue.main()

    rows = session.query(DocumentCatalogue).all()
    assert len(rows) == 2
    assert json.loads(rows[0].contains) == ["performance"]
    assert rows[0].page_hints == "Tab 7, pp. 12-30"
    assert rows[0].source == "existing_text"

    # The constraint James set: extracted material is not discarded.
    session.expire_all()
    assert session.get(Document, docs[0].id).extracted_text == original


def test_budget_is_a_hard_stop(session, monkeypatch):
    _partial_doc(session, n=10)
    from decimal import Decimal

    monkeypatch.setattr(catalogue, "build_entry", lambda d: (
        {"contains": [], "sections": [], "page_hints": ""}, Decimal("0.10")))
    monkeypatch.setattr("sys.argv",
                        ["catalogue", "--backlog", "--approve", "--budget", "0.30"])
    catalogue.main()

    # Stops rather than warning: at 10c a document, a 30c ceiling buys 3-4.
    n = session.query(DocumentCatalogue).count()
    assert 0 < n <= 4, f"budget ceiling not enforced ({n} built)"


def test_already_catalogued_documents_are_not_rebuilt(session, monkeypatch):
    docs = _partial_doc(session, n=2)
    session.add(DocumentCatalogue(document_id=docs[0].id, source="existing_text",
                                  contains="[]", sections="[]"))
    session.commit()

    calls = []
    from decimal import Decimal
    monkeypatch.setattr(catalogue, "build_entry", lambda d: (
        calls.append(d.id) or ({"contains": [], "sections": [], "page_hints": ""},
                               Decimal("0.001"))))
    monkeypatch.setattr("sys.argv", ["catalogue", "--backlog", "--approve"])
    catalogue.main()

    assert calls == [docs[1].id], "re-catalogued a document that already had an entry"


def test_only_reads_the_opening_pages(session, monkeypatch):
    """Sending a 150,000-char document to be told what its first page says
    would cost ~25x more per document and answer the same question."""
    long_text = "AGENDA\n1. Performance report\n" + ("filler line\n" * 40_000)
    _partial_doc(session, n=1, text=long_text)

    seen = {}
    from decimal import Decimal

    def capture(doc):
        seen["chars"] = len((doc.extracted_text or "")[:catalogue.HEAD_CHARS])
        return {"contains": [], "sections": [], "page_hints": ""}, Decimal("0.001")

    monkeypatch.setattr(catalogue, "build_entry", capture)
    monkeypatch.setattr("sys.argv", ["catalogue", "--backlog", "--approve"])
    catalogue.main()

    assert seen["chars"] == catalogue.HEAD_CHARS
    assert catalogue.HEAD_CHARS < len(long_text) / 10
