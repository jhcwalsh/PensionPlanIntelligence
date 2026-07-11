"""Facet assembly from seeded source rows."""
import json
from datetime import datetime

from database import (
    CafrAllocation, CafrExtract, CafrPerformance, Document, Plan,
    RFPRecord, Summary, TwinBuildRun, get_session, get_twin_snapshot,
)
import twin_builder


def _seed(session):
    plan = Plan(id="testplan", name="Test Plan", abbreviation="TP",
                state="CA", aum_billions=10.0, fiscal_year_end="06-30")
    session.add(plan)
    doc = Document(plan_id="testplan", url="https://x/pack.pdf", filename="pack.pdf",
                   doc_type="board_pack", extraction_status="done",
                   meeting_date=datetime(2026, 6, 17))
    cafr_doc = Document(plan_id="testplan", url="https://x/cafr.pdf", filename="cafr.pdf",
                        doc_type="cafr", extraction_status="done", fiscal_year=2025)
    session.add_all([doc, cafr_doc]); session.commit()
    ext = CafrExtract(plan_id="testplan", document_id=cafr_doc.id, fiscal_year=2025,
                      investment_policy_text="Prudent person rule.")
    session.add(ext); session.commit()
    session.add_all([
        CafrAllocation(cafr_extract_id=ext.id, asset_class="Global Equity",
                       target_pct=40.0, actual_pct=45.0, target_range_low=35.0, target_range_high=44.0),
        CafrPerformance(cafr_extract_id=ext.id, scope="total_fund",
                        period="1y", return_pct=9.3, benchmark_return_pct=9.1,
                        benchmark_name="Policy Index"),
        Summary(document_id=doc.id, summary_text="s",
                investment_actions=json.dumps([{"action": "hire", "manager": "BlackRock",
                                                "asset_class": "Private Credit",
                                                "amount_millions": 150,
                                                "description": "hired BlackRock"}]),
                decisions=json.dumps([{"description": "Approved budget", "vote": "9-0"}])),
        RFPRecord(rfp_id="ab" * 8, document_id=doc.id, plan_id="testplan",
                  record=json.dumps({"rfp_type": "Consultant", "status": "Awarded",
                                     "title": "General consultant", "asset_class": None,
                                     "mandate_size_usd_millions": None,
                                     "release_date": None, "response_due_date": None,
                                     "award_date": "2026-05-02",
                                     "incumbent_manager": None,
                                     "awarded_manager": "Meketa"}),
                extraction_confidence=0.9, needs_review=False, prompt_version="rfp_v1"),
    ])
    session.commit()
    return plan


def test_build_twin_facets(tmp_db):
    session = get_session()
    plan = _seed(session)
    twin = twin_builder.build_twin(session, plan)
    f = twin["facets"]
    assert twin["schema_version"] == "twin_v0"
    assert f["identity"]["aum_billions"]["v"] == 10.0
    assert f["policy"]["investment_policy_text"]["v"] == "Prudent person rule."
    assert f["policy"]["investment_policy_text"]["as_of"] == "2025-06-30"
    row = f["allocation"]["rows"][0]
    assert row["drift_pct"] == 5.0 and row["outside_range"] is True
    assert f["performance"]["rows"][0]["return_pct"] == 9.3
    roster = f["manager_roster"]["entries"]
    assert roster[0]["name_raw"] == "BlackRock" and roster[0]["status"] == "current"
    kinds = {i["kind"] for i in f["activity_timeline"]["items"]}
    assert kinds == {"action", "decision"}
    assert f["rfp_state"]["by_status"] == {"Awarded": 1}
    assert f["governance_people"]["relationships"][0] == {
        "role": "Consultant", "name": "Meketa", "basis": "rfp_awarded",
        "doc_id": f["rfp_state"]["records"][0]["doc_id"]}
    assert f["funding_actuarial"] == {"status": "not_captured"}
    assert twin["completeness"]["funding_actuarial"] == 0.0
    assert twin["freshness"]["allocation"] == "2025-06-30"
    session.close()


def test_run_builder_writes_snapshot_and_run_row(tmp_db):
    session = get_session()
    _seed(session)
    session.close()
    twin_builder.run_builder(["testplan"])
    session = get_session()
    snap = get_twin_snapshot(session, "testplan")
    assert snap is not None and snap.schema_version == "twin_v0"
    run = session.query(TwinBuildRun).one()
    assert run.status == "succeeded" and run.snapshots_written == 1
    session.close()


def test_builder_tolerates_empty_plan(tmp_db):
    session = get_session()
    session.add(Plan(id="bare", name="Bare Plan", abbreviation="BP",
                     state="TX", aum_billions=None))
    session.commit(); session.close()
    twin_builder.run_builder(["bare"])
    session = get_session()
    snap = get_twin_snapshot(session, "bare")
    facets = json.loads(snap.facets)["facets"]
    assert facets["allocation"]["rows"] == []
    assert facets["funding_actuarial"] == {"status": "not_captured"}
    session.close()


def test_governance_freshness_scoped_to_governance_types(tmp_db):
    """Verify governance_people freshness ignores non-governance RFP types."""
    session = get_session()
    plan = Plan(id="testplan2", name="Test Plan 2", abbreviation="TP2",
                state="CA", aum_billions=10.0, fiscal_year_end="06-30")
    session.add(plan)
    doc = Document(plan_id="testplan2", url="https://x/pack.pdf", filename="pack.pdf",
                   doc_type="board_pack", extraction_status="done",
                   meeting_date=datetime(2026, 6, 17))
    session.add(doc); session.commit()

    # Governance-type RFP with older award date
    consultant_rec = RFPRecord(
        rfp_id="governance_id", document_id=doc.id, plan_id="testplan2",
        record=json.dumps({
            "rfp_type": "Consultant", "status": "Awarded",
            "title": "Consultant search", "asset_class": None,
            "mandate_size_usd_millions": None,
            "release_date": None, "response_due_date": None,
            "award_date": "2026-01-01",
            "incumbent_manager": None,
            "awarded_manager": "Meketa"
        }),
        extraction_confidence=0.9, needs_review=False, prompt_version="rfp_v1"
    )

    # Non-governance RFP (Manager) with newer release date
    manager_rec = RFPRecord(
        rfp_id="manager_id", document_id=doc.id, plan_id="testplan2",
        record=json.dumps({
            "rfp_type": "Manager", "status": "In Progress",
            "title": "Manager search", "asset_class": "Global Equity",
            "mandate_size_usd_millions": 500,
            "release_date": "2026-06-01", "response_due_date": None,
            "award_date": None,
            "incumbent_manager": None,
            "awarded_manager": None
        }),
        extraction_confidence=0.9, needs_review=False, prompt_version="rfp_v1"
    )

    session.add_all([consultant_rec, manager_rec])
    session.commit()

    twin = twin_builder.build_twin(session, plan)
    freshness = twin["freshness"]

    # governance_people should only look at Consultant record (2026-01-01)
    assert freshness["governance_people"] == "2026-01-01", \
        f"Expected governance_people freshness to be 2026-01-01, got {freshness['governance_people']}"

    # rfp_state should see both records, so the max date is from Manager (2026-06-01)
    assert freshness["rfp_state"] == "2026-06-01", \
        f"Expected rfp_state freshness to be 2026-06-01, got {freshness['rfp_state']}"

    session.close()
