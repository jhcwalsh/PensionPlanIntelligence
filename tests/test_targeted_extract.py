"""Storing and extracting what a targeted read of a document finds."""
from decimal import Decimal

import pytest

import database
from database import Document, DocumentSectionRead, Plan
from section_finder import Candidate


def test_section_reads_sit_beside_the_document(tmp_db):
    s = database.get_session()
    s.add(Plan(id="mcera", name="MCERA", state="CA")); s.commit()
    d = Document(plan_id="mcera", url="https://x/a.pdf", filename="a.pdf",
                 extracted_text="the whole pack")
    s.add(d); s.commit()
    before = d.extracted_text

    s.add(DocumentSectionRead(document_id=d.id, offset=200_881,
                              heading="Total Rates of Return (%)",
                              returns_json='[{"asset_class":"US Equity","return_pct":12.4}]'))
    s.commit(); s.expire_all()

    assert s.get(Document, d.id).extracted_text == before
    assert s.query(DocumentSectionRead).one().offset == 200_881
    s.close()


def test_the_same_passage_cannot_be_charged_for_twice(tmp_db):
    """UNIQUE(document_id, offset) is what makes a re-run a no-op.

    Without it, re-running the CLI over a document whose read failed to be
    recorded charges again for the identical window.
    """
    import pytest
    from sqlalchemy.exc import IntegrityError

    s = database.get_session()
    s.add(Plan(id="mcera", name="MCERA", state="CA")); s.commit()
    d = Document(plan_id="mcera", url="https://x/a.pdf", filename="a.pdf")
    s.add(d); s.commit()

    s.add(DocumentSectionRead(document_id=d.id, offset=1000, returns_json="[]"))
    s.commit()
    s.add(DocumentSectionRead(document_id=d.id, offset=1000, returns_json="[]"))
    with pytest.raises(IntegrityError):
        s.commit()
    s.rollback(); s.close()


# --- Task 4: extracting from a chosen window ---------------------------------

def test_returns_parsed_rows_and_cost(monkeypatch):
    import targeted_extract
    monkeypatch.setattr(targeted_extract, "_call_model", lambda w: (
        {"returns": [{"asset_class": "US Equity", "return_pct": 12.4,
                      "period": "FY2026"}]}, Decimal("0.004")))
    data, cost = targeted_extract.extract_window(
        "…", Candidate(200_881, "Total Rates of Return (%)", 2.4))
    assert data["returns"][0]["asset_class"] == "US Equity"
    assert cost == Decimal("0.004")


def test_a_truncated_response_raises_rather_than_saving_nothing(monkeypatch):
    """extract_performance_reports saved 30 documents with zero rows and no
    error when max_tokens cut the tool call short. A silent empty result is
    worse than a failure, because nobody re-runs it."""
    import targeted_extract

    def truncated(window):
        raise targeted_extract.ResponseTruncated("in=58236 out=4096")

    monkeypatch.setattr(targeted_extract, "_call_model", truncated)
    with pytest.raises(targeted_extract.ResponseTruncated):
        targeted_extract.extract_window("…", Candidate(0, "x", 1.0))


def test_the_window_is_the_slice_the_candidate_points_at(monkeypatch):
    """The offset is stored so a figure can be checked against the source.

    If the window handed to the model is not the window the offset names, that
    verification checks the wrong passage and proves nothing.
    """
    import targeted_extract
    seen = {}

    def capture(window):
        seen["w"] = window
        return {"returns": []}, Decimal(0)

    monkeypatch.setattr(targeted_extract, "_call_model", capture)

    text = "A" * 10_000 + "TARGET" + "B" * 60_000
    targeted_extract.extract_window(text, Candidate(10_000, "h", 3.0))

    assert seen["w"].startswith("A" * 500 + "TARGET"), \
        "window must open 500 chars before the heading"
    assert len(seen["w"]) <= 30_000


def test_the_prompt_refuses_to_read_an_allocation_weight_as_a_return():
    """Measured on inv-202412.pdf: the top-ranked window is Total Fund Asset
    Allocation — a grid of 31.9% / 22.7% / 14.5% *weights*, not returns. A
    schema asking for return_pct pointed at that table will happily record
    weights unless the prompt forbids it.
    """
    import targeted_extract
    p = targeted_extract.SYSTEM.lower()
    assert "weight" in p and "allocation" in p
