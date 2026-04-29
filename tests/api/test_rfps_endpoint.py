"""FastAPI endpoint tests using the TestClient + the integration fixtures."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import database as db
from rfp.orchestrator import run_rfp_extraction


FIXTURE_DOCS = Path(__file__).resolve().parents[2] / "fixtures" / "documents"
FIXTURE_RESPONSES = Path(__file__).resolve().parents[2] / "fixtures" / "llm_responses"


@pytest.fixture
def populated_db(seeded_session, monkeypatch):
    """Run the orchestrator once so we have RFP records in the DB."""
    monkeypatch.setenv("LLM_FIXTURE_DIR", str(FIXTURE_RESPONSES))
    fixtures = [
        (1, "calpers", "calpers_2024_board.txt",
         "https://www.calpers.ca.gov/board/2024-03/packet.pdf"),
        (2, "calstrs", "calstrs_2024_investment.txt",
         "https://www.calstrs.com/board/2024-04/packet.pdf"),
        (3, "calpers", "calpers_2024_governance.txt",
         "https://www.calpers.ca.gov/governance/2024-02/packet.pdf"),
    ]
    for doc_id, plan_id, fname, url in fixtures:
        text = (FIXTURE_DOCS / fname).read_text()
        seeded_session.add(db.Document(
            id=doc_id, plan_id=plan_id, url=url, filename=fname,
            doc_type="board_pack",
            local_path="/nonexistent/path/" + fname,
            extracted_text=text, extraction_status="done",
            page_count=text.count("[Page "),
            meeting_date=datetime(2024, 3, 15),
        ))
    seeded_session.commit()
    run_rfp_extraction()
    return seeded_session


@pytest.fixture
def client(populated_db):
    """Build the FastAPI app after the DB is populated and bind it to our test engine."""
    from api.main import app
    return TestClient(app)


def test_health_endpoint(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_list_rfps_default_returns_all_three(client):
    r = client.get("/api/v1/rfps")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 3
    assert len(body["results"]) == 3
    assert "pipeline_health" in body
    assert body["pipeline_health"]["records_pending_review"] == 0


def test_filter_by_year(client):
    # Two of the three fixture records have explicit 2024 dates (the
    # Consultant RFP and the Actuary award). The Manager search has all
    # dates null → falls back to extracted_at year, which is "now" in
    # tests, so it doesn't match 2024.
    r = client.get("/api/v1/rfps?year=2024")
    assert r.json()["total"] == 2

    r2 = client.get("/api/v1/rfps?year=2099")
    assert r2.json()["total"] == 0


def test_dateless_records_match_current_year(client):
    """The Manager record (no dates) appears under 'now's year via fallback."""
    from datetime import datetime, timezone
    current_year = datetime.now(timezone.utc).year
    r = client.get(f"/api/v1/rfps?year={current_year}&rfp_type=Manager")
    assert r.json()["total"] == 1


def test_filter_by_rfp_type(client):
    r = client.get("/api/v1/rfps?rfp_type=Consultant")
    body = r.json()
    assert body["total"] == 1
    assert body["results"][0]["rfp_type"] == "Consultant"


def test_filter_by_plan_id(client):
    r = client.get("/api/v1/rfps?plan_id=calstrs")
    body = r.json()
    assert body["total"] == 1
    assert body["results"][0]["plan_id"] == "calstrs"


def test_pagination(client):
    r = client.get("/api/v1/rfps?limit=2&offset=0")
    body = r.json()
    assert body["total"] == 3
    assert len(body["results"]) == 2

    r2 = client.get("/api/v1/rfps?limit=2&offset=2")
    body2 = r2.json()
    assert len(body2["results"]) == 1


def test_include_review_gates_low_confidence(seeded_session, monkeypatch):
    """Records with confidence < 0.70 default-hidden; ?include_review=true reveals them."""
    monkeypatch.setenv("LLM_FIXTURE_DIR", str(FIXTURE_RESPONSES))

    db.upsert_rfp_record(
        seeded_session, rfp_id="0000aaaa11112222",
        document_id=99, plan_id="calpers",
        record_json=json.dumps({
            "rfp_id": "0000aaaa11112222",
            "plan_id": "calpers",
            "rfp_type": "Audit",
            "title": "Low-confidence audit",
            "status": "Planned",
            "release_date": "2024-08-15",
            "response_due_date": None, "award_date": None,
            "mandate_size_usd_millions": None, "asset_class": None,
            "incumbent_manager": None, "incumbent_manager_id": None,
            "shortlisted_managers": [], "awarded_manager": None,
            "source_document": {
                "url": "https://example.com/x.pdf",
                "page_number": 1, "document_id": 99,
            },
            "source_quote": "Audit RFP planned for fall 2024.",
            "extraction_confidence": 0.5,
        }),
        extraction_confidence=0.5,
    )
    seeded_session.add(db.Document(
        id=99, plan_id="calpers", url="https://example.com/x.pdf",
        filename="x.pdf", extraction_status="done",
        extracted_text="[Page 1]\nplaceholder",
    ))
    seeded_session.commit()

    from api.main import app
    c = TestClient(app)
    r = c.get("/api/v1/rfps")
    body = r.json()
    assert all(not rec["needs_review"] for rec in body["results"])
    assert body["pipeline_health"]["records_pending_review"] == 1
    assert "0000aaaa11112222" not in {rec["rfp_id"] for rec in body["results"]}

    r2 = c.get("/api/v1/rfps?include_review=true")
    body2 = r2.json()
    assert "0000aaaa11112222" in {rec["rfp_id"] for rec in body2["results"]}


def test_stats_endpoint(client):
    # Year-less query covers all three.
    r = client.get("/api/v1/rfps/stats")
    body = r.json()
    assert body["total"] == 3
    assert body["by_type"]["Consultant"] == 1
    assert body["by_type"]["Manager"] == 1
    assert body["by_type"]["Actuary"] == 1

    # 2024 query covers the two records with explicit 2024 dates.
    r2 = client.get("/api/v1/rfps/stats?year=2024")
    assert r2.json()["total"] == 2


def test_each_response_has_provenance(client):
    r = client.get("/api/v1/rfps")
    for rec in r.json()["results"]:
        assert rec["source_document"]["url"]
        assert rec["source_document"]["page_number"] >= 1
        assert len(rec["source_quote"]) >= 10
        assert rec["prompt_version"] == "rfp_v1"
