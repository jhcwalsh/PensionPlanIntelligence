"""The targeted-read CLI, and the guarantee it cannot spend by accident.

Mirrors tests/test_catalogue.py. The first test asserts no API client is ever
constructed on the unapproved path — not that the cost came out zero, which
is what you check when you are hoping.
"""
from decimal import Decimal

import pytest

import database
from database import Document, DocumentSectionRead, Plan
from scripts import read_sections

# Long enough to be selected: the CLI only looks at documents the summariser
# truncated, and SMART_TRUNCATE_TARGET is 50,000.
#
# The filler is varied rather than repeated, because the SQL pre-filter
# measures the *gzipped* column. "preamble line\n" * 5000 is 70,000 characters
# that compress to a few hundred bytes, so a repeated fixture is excluded by a
# threshold that admits every real document — the test would fail for a reason
# that could never occur in production.
def _filler(n_lines: int) -> str:
    import random
    rnd = random.Random(20260830)
    letters = "abcdefghijklmnopqrstuvwxyz "
    return "".join(
        "".join(rnd.choice(letters) for _ in range(60)) + "\n"
        for _ in range(n_lines))


TABLE = ("\nTotal Rates of Return (%)\n"
         + "Domestic Equity 12.4 11.8 9.2\n" * 40)
LONG = _filler(1_200) + TABLE + _filler(100)


@pytest.fixture
def session(tmp_db):
    s = database.get_session()
    s.add(Plan(id="mcera", name="MCERA", state="CA"))
    s.commit()
    yield s
    s.close()


def _docs(session, n=1, text=LONG):
    made = []
    for i in range(n):
        d = Document(plan_id="mcera", url=f"https://x/{i}.pdf",
                     filename=f"{i}.pdf", extraction_status="done",
                     extracted_text=text)
        session.add(d)
        session.commit()
        made.append(d)
    return made


def test_without_approve_no_client_is_ever_built(session, monkeypatch, capsys):
    """The 2026-08-29 test. Not 'spend was zero' — 'spending was unreachable'."""
    _docs(session, n=3)

    def boom(**kw):
        raise AssertionError("an API client was constructed on a no-spend path")

    import llm_openrouter
    monkeypatch.setattr(llm_openrouter, "_raw_call", boom)
    monkeypatch.setattr("sys.argv", ["read_sections"])

    assert read_sections.main() == 0
    assert session.query(DocumentSectionRead).count() == 0
    assert "Nothing spent" in capsys.readouterr().out


def test_approve_writes_reads_without_touching_the_document(session, monkeypatch):
    docs = _docs(session, n=2)
    original = docs[0].extracted_text

    monkeypatch.setattr(read_sections, "extract_window", lambda t, c: (
        {"returns": [{"asset_class": "US Equity", "return_pct": 12.4,
                      "period": "FY2026"}]}, Decimal("0.0008")))
    monkeypatch.setattr("sys.argv", ["read_sections", "--approve"])
    read_sections.main()

    rows = session.query(DocumentSectionRead).all()
    assert len(rows) == 2
    assert "US Equity" in rows[0].returns_json
    assert rows[0].model == read_sections.MODEL
    assert rows[0].cost_usd > 0

    # The constraint James set: extracted material is not discarded.
    session.expire_all()
    assert session.get(Document, docs[0].id).extracted_text == original


def test_budget_is_a_hard_stop(session, monkeypatch):
    _docs(session, n=10)
    monkeypatch.setattr(read_sections, "extract_window",
                        lambda t, c: ({"returns": []}, Decimal("0.10")))
    monkeypatch.setattr("sys.argv",
                        ["read_sections", "--approve", "--budget", "0.30"])
    read_sections.main()

    n = session.query(DocumentSectionRead).count()
    assert 0 < n <= 4, f"budget ceiling not enforced ({n} written)"


def test_already_read_documents_are_skipped(session, monkeypatch):
    docs = _docs(session, n=2)
    session.add(DocumentSectionRead(document_id=docs[0].id, offset=1,
                                    returns_json="[]"))
    session.commit()

    calls = []
    monkeypatch.setattr(read_sections, "extract_window", lambda t, c: (
        calls.append(t) or ({"returns": []}, Decimal("0.0008"))))
    monkeypatch.setattr("sys.argv", ["read_sections", "--approve"])
    read_sections.main()

    assert len(calls) == 1, "re-read a document that already had a section read"


def test_short_documents_are_not_selected(session, monkeypatch, capsys):
    """The premise is truncation. A document the summariser read in full has
    nothing for a targeted read to add, and paying for it is pure waste."""
    _docs(session, n=1, text="short document\n" + TABLE)
    monkeypatch.setattr("sys.argv", ["read_sections"])
    read_sections.main()
    assert "Nothing to read" in capsys.readouterr().out


def test_documents_with_no_candidate_are_reported_not_guessed_at(
        session, monkeypatch, capsys):
    """A long document with no returns heading is a real and common case.

    Reporting the count keeps it visible; picking an arbitrary window for it
    would spend money to extract nothing and look like a model failure.
    """
    # Long and incompressible, so it clears the pre-filter, but with no
    # returns heading anywhere in it.
    _docs(session, n=1, text=_filler(1_200))
    monkeypatch.setattr("sys.argv", ["read_sections"])
    read_sections.main()
    out = capsys.readouterr().out
    assert "no candidate" in out.lower()


def test_a_failed_read_does_not_stop_the_run(session, monkeypatch):
    """One plan's quirk must not cost the other 1,013 documents."""
    _docs(session, n=3)
    seen = {"n": 0}

    def flaky(t, c):
        seen["n"] += 1
        if seen["n"] == 1:
            raise RuntimeError("no tool call; finish_reason=stop")
        return {"returns": []}, Decimal("0.0008")

    monkeypatch.setattr(read_sections, "extract_window", flaky)
    monkeypatch.setattr("sys.argv", ["read_sections", "--approve"])
    read_sections.main()

    assert session.query(DocumentSectionRead).count() == 2
