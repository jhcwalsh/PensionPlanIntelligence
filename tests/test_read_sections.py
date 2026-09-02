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
    """One worker, so the ceiling is exact and the assertion is unambiguous."""
    _docs(session, n=10)
    monkeypatch.setattr(read_sections, "extract_window",
                        lambda t, c: ({"returns": []}, Decimal("0.10")))
    monkeypatch.setattr("sys.argv",
                        ["read_sections", "--approve", "--budget", "0.30",
                         "--workers", "1"])
    read_sections.main()

    n = session.query(DocumentSectionRead).count()
    assert 0 < n <= 4, f"budget ceiling not enforced ({n} written)"


def test_concurrency_does_not_blow_through_the_budget(session, monkeypatch):
    """The ceiling must survive parallelism, not merely serial execution.

    Submission accounts for calls already in flight, so W workers cannot each
    be mid-request when the limit lands. Without that term this test writes
    ten rows against a three-row budget.
    """
    _docs(session, n=10)
    monkeypatch.setattr(read_sections, "extract_window",
                        lambda t, c: ({"returns": []}, Decimal("0.10")))
    monkeypatch.setattr("sys.argv",
                        ["read_sections", "--approve", "--budget", "0.30",
                         "--workers", "8"])
    read_sections.main()

    n = session.query(DocumentSectionRead).count()
    assert 0 < n <= 4, f"in-flight spend not counted against budget ({n} written)"


def test_every_window_is_read_when_the_budget_allows(session, monkeypatch):
    """Concurrency must not drop jobs: the sliding-window submitter is easy to
    get wrong in the direction of quietly reading fewer documents than asked."""
    _docs(session, n=9)
    monkeypatch.setattr(read_sections, "extract_window",
                        lambda t, c: ({"returns": []}, Decimal("0.0001")))
    # --per-plan 0 because this is a test about the submitter, not about
    # document selection; all nine documents belong to one plan.
    monkeypatch.setattr("sys.argv",
                        ["read_sections", "--approve", "--workers", "4",
                         "--per-plan", "0"])
    read_sections.main()

    assert session.query(DocumentSectionRead).count() == 9


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


def test_reread_buys_only_offsets_not_already_read(session, monkeypatch):
    """The point of --reread: text that grew past its read, not a second bill.

    The document has two candidate tables. One offset is already recorded, so
    --reread must buy the other and only the other. Without --reread the whole
    document is invisible, which is what hid the 122.8M characters the
    2026-09-01 re-extraction recovered.
    """
    two_tables = _filler(1_200) + TABLE + _filler(1_200) + TABLE + _filler(100)
    doc, = _docs(session, n=1, text=two_tables)
    offsets = [c.offset for c in
               read_sections.section_finder.find_candidates(two_tables)]
    assert len(offsets) >= 2, "fixture must offer at least two candidates"

    session.add(DocumentSectionRead(document_id=doc.id, offset=offsets[0],
                                    returns_json="[]"))
    session.commit()

    bought = []
    monkeypatch.setattr(read_sections, "extract_window", lambda t, c: (
        bought.append(c.offset) or ({"returns": []}, Decimal("0.0008"))))
    monkeypatch.setattr("sys.argv",
                        ["read_sections", "--approve", "--reread", "--top", "5"])
    read_sections.main()

    assert offsets[0] not in bought, "paid again for a passage already read"
    assert set(bought) == set(offsets[1:])


def test_reread_skips_a_document_whose_every_candidate_is_read(
        session, monkeypatch, capsys):
    """Finished, not blank — and not billed."""
    doc, = _docs(session, n=1)
    for c in read_sections.section_finder.find_candidates(LONG):
        session.add(DocumentSectionRead(document_id=doc.id, offset=c.offset,
                                        returns_json="[]"))
    session.commit()

    def boom(*a, **kw):
        raise AssertionError("spent on a document with nothing new to read")

    monkeypatch.setattr(read_sections, "extract_window", boom)
    monkeypatch.setattr("sys.argv",
                        ["read_sections", "--approve", "--reread", "--top", "9"])
    read_sections.main()

    out = capsys.readouterr().out
    assert "already read at every candidate offset" in out
    assert "no candidate section" not in out, "reported a real table as absent"


def test_reread_does_not_duplicate_a_document_with_several_reads(
        session, monkeypatch):
    """Dropping the outer join, not just its filter.

    Keeping the join and only relaxing its NULL test returns one Document row
    per recorded read, so a document read three times would be priced and
    bought three times over.
    """
    two_tables = _filler(1_200) + TABLE + _filler(1_200) + TABLE + _filler(100)
    doc, = _docs(session, n=1, text=two_tables)
    cands = read_sections.section_finder.find_candidates(two_tables)
    for c in cands[:2]:
        session.add(DocumentSectionRead(document_id=doc.id, offset=c.offset,
                                        returns_json="[]"))
    session.commit()

    ids = [d.id for d in
           read_sections.backlog_documents(session, reread=True).all()]
    assert ids == [doc.id]


def test_only_the_newest_documents_per_plan_are_read(session, monkeypatch, capsys):
    """The view shows one document per plan; reading forty is paying to hide 39.

    Measured on the first corpus run: 510 documents read across 121 plans,
    111 windows on one plan alone, and 85% of what was read never reached the
    view. 31% of the spend went on documents dated before 2025.
    """
    from datetime import date
    for i in range(6):
        d = Document(plan_id="mcera", url=f"https://x/{i}.pdf",
                     filename=f"{i}.pdf", extraction_status="done",
                     extracted_text=LONG,
                     meeting_date=date(2020 + i, 1, 1))
        session.add(d)
    session.commit()

    monkeypatch.setattr(read_sections, "extract_window",
                        lambda t, c: ({"returns": []}, Decimal("0.0001")))
    monkeypatch.setattr("sys.argv", ["read_sections", "--approve",
                                     "--per-plan", "2", "--workers", "1"])
    read_sections.main()

    rows = session.query(DocumentSectionRead).all()
    assert len(rows) == 2, f"per-plan cap not applied ({len(rows)} read)"

    # And it must keep the *newest*, not the first two the query happened to
    # return — an old document is exactly what the cap exists to skip.
    read_ids = {r.document_id for r in rows}
    newest = {d.id for d in session.query(Document)
              .order_by(Document.meeting_date.desc()).limit(2)}
    assert read_ids == newest


def test_the_per_plan_cap_counts_plans_separately(session, monkeypatch):
    """Two plans with one document each are both read under a cap of 1."""
    from datetime import date
    session.add(Plan(id="other", name="Other", state="TX"))
    session.commit()
    for plan in ("mcera", "other"):
        session.add(Document(plan_id=plan, url=f"https://x/{plan}.pdf",
                             filename=f"{plan}.pdf", extraction_status="done",
                             extracted_text=LONG, meeting_date=date(2026, 1, 1)))
    session.commit()

    monkeypatch.setattr(read_sections, "extract_window",
                        lambda t, c: ({"returns": []}, Decimal("0.0001")))
    monkeypatch.setattr("sys.argv", ["read_sections", "--approve",
                                     "--per-plan", "1", "--workers", "1"])
    read_sections.main()

    assert session.query(DocumentSectionRead).count() == 2


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


def test_an_unprintable_heading_does_not_end_the_run(session, monkeypatch):
    """A PDF text layer carries private-use glyphs — the Symbol bullet arrives
    as \\uf0a7. Printed to a cp1252 stdout that raises UnicodeEncodeError, and
    on the first full corpus run it killed the process after twelve windows,
    after the money was spent. A progress line must never end a paid run.
    """
    _docs(session, n=2)
    monkeypatch.setattr(read_sections, "extract_window",
                        lambda t, c: ({"returns": []}, Decimal("0.0001")))

    real = read_sections.console.print

    def exploding(msg, *a, **kw):
        # Fail on a progress line specifically — the one carrying document
        # text, which is where the real fault came from.
        if "rets" in str(msg):
            raise UnicodeEncodeError("charmap", "x", 0, 1, "undefined")
        return real(msg, *a, **kw)

    monkeypatch.setattr(read_sections.console, "print", exploding)
    monkeypatch.setattr("sys.argv", ["read_sections", "--approve",
                                     "--workers", "1"])
    read_sections.main()

    assert session.query(DocumentSectionRead).count() == 2


def test_printable_survives_a_private_use_glyph():
    assert read_sections._printable("Total  Return")
    assert read_sections._printable(None) == ""


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
    monkeypatch.setattr("sys.argv", ["read_sections", "--approve",
                                     "--per-plan", "0"])
    read_sections.main()

    assert session.query(DocumentSectionRead).count() == 2
