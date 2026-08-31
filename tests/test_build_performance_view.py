"""The performance view builder, and how it ranks its three sources.

The builder is derived data: the build *is* the definition, so a source it
does not read is a source that does not exist as far as the app is concerned.
document_section_read was added without a consumer, which would have made the
whole targeted read invisible.
"""
import json
from datetime import date

import pytest

import database
from database import (Document, DocumentSectionRead,
                      PlanAssetClassPerformance, Plan, Summary)
from scripts import build_performance_view as bpv


@pytest.fixture
def session(tmp_db):
    s = database.get_session()
    s.add(Plan(id="mcera", name="MCERA", state="CA"))
    s.commit()
    yield s
    s.close()


def _doc(session, when=date(2026, 8, 26), name="pack.pdf"):
    d = Document(plan_id="mcera", url=f"https://x/{name}", filename=name,
                 meeting_date=when, extraction_status="done")
    session.add(d)
    session.commit()
    return d


def _payload(cls, pct, period="1 Year"):
    return json.dumps([{"asset_class": cls, "return_pct": pct,
                        "period": period}])


def test_a_targeted_read_reaches_the_view(session, monkeypatch):
    """Without a collector for it, the whole targeted read is invisible."""
    d = _doc(session)
    session.add(DocumentSectionRead(document_id=d.id, offset=200_000,
                                    returns_json=_payload("Domestic Equity", 12.4)))
    session.commit()

    monkeypatch.setattr("sys.argv", ["build_performance_view"])
    bpv.main()

    rows = session.query(PlanAssetClassPerformance).all()
    assert [r.source for r in rows] == ["targeted_read"]
    assert float(rows[0].return_pct) == 12.4


def test_a_targeted_read_supersedes_the_summariser_for_the_same_document(
        session, monkeypatch):
    """Same document, same table, two readings — one of which saw it.

    Left in together they land in one pick_latest group, so the winner is
    decided by list order rather than by anyone's decision.
    """
    d = _doc(session)
    session.add(Summary(document_id=d.id,
                        performance_data=_payload("Domestic Equity", 9.9)))
    session.add(DocumentSectionRead(document_id=d.id, offset=200_000,
                                    returns_json=_payload("Domestic Equity", 12.4)))
    session.commit()

    monkeypatch.setattr("sys.argv", ["build_performance_view"])
    bpv.main()

    rows = session.query(PlanAssetClassPerformance).all()
    assert len(rows) == 1, "the same class was recorded twice for one document"
    assert rows[0].source == "targeted_read"
    assert float(rows[0].return_pct) == 12.4


def test_the_summariser_still_wins_where_there_was_no_targeted_read(
        session, monkeypatch):
    """Superseding is per document, not global. 300 long documents have no
    candidate section and 4,116 shorter ones were never in scope — their
    summariser figures must survive untouched.

    Two plans, not two documents: pick_latest deliberately keeps one document
    per plan per horizon, so same-plan documents compete rather than coexist.
    """
    session.add(Plan(id="other", name="Other", state="TX"))
    session.commit()

    read = _doc(session, name="read.pdf")
    unread = Document(plan_id="other", url="https://x/unread.pdf",
                      filename="unread.pdf", meeting_date=date(2026, 8, 25),
                      extraction_status="done")
    session.add(unread)
    session.commit()

    session.add(DocumentSectionRead(document_id=read.id, offset=1,
                                    returns_json=_payload("Domestic Equity", 12.4)))
    session.add(Summary(document_id=unread.id,
                        performance_data=_payload("Real Estate", 3.3)))
    session.commit()

    monkeypatch.setattr("sys.argv", ["build_performance_view"])
    bpv.main()

    got = {r.source for r in session.query(PlanAssetClassPerformance).all()}
    assert got == {"targeted_read", "board_doc"}


def test_an_empty_read_contributes_nothing(session, monkeypatch):
    """A window with no returns in it is a real and common outcome — 300 of
    810 documents. It must not create a row, and must not crash the build."""
    d = _doc(session)
    session.add(DocumentSectionRead(document_id=d.id, offset=1,
                                    returns_json="[]"))
    session.commit()

    monkeypatch.setattr("sys.argv", ["build_performance_view"])
    bpv.main()

    assert session.query(PlanAssetClassPerformance).count() == 0
