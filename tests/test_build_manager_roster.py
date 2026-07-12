"""Deterministic roster reconciliation into plan_manager_roster."""
import json
from datetime import datetime

from database import Document, Plan, PlanManagerRoster, RFPRecord, Summary, get_session
from scripts.build_manager_roster import build_roster_for_plan


def test_roster_rebuild(tmp_db):
    session = get_session()
    session.add(Plan(id="p1", name="P", abbreviation="P", state="CA"))
    doc = Document(plan_id="p1", url="https://x/a.pdf", filename="a.pdf",
                   doc_type="minutes", extraction_status="done",
                   meeting_date=datetime(2026, 6, 1))
    session.add(doc); session.commit()
    session.add(Summary(document_id=doc.id, summary_text="s",
                        investment_actions=json.dumps([
                            {"action": "hire", "manager": "BlackRock",
                             "asset_class": "Private Credit"}])))
    session.add(RFPRecord(rfp_id="cd" * 8, document_id=doc.id, plan_id="p1",
                          record=json.dumps({"rfp_type": "Consultant",
                                             "status": "Awarded",
                                             "awarded_manager": "Meketa"}),
                          extraction_confidence=0.9, needs_review=False,
                          prompt_version="rfp_v1"))
    session.commit()

    n = build_roster_for_plan(session, "p1")
    assert n == 2
    rows = {(r.canonical_name, r.role): r
            for r in session.query(PlanManagerRoster).all()}
    mgr = rows[("BlackRock", "manager")]
    assert mgr.status == "current" and mgr.asset_class_raw == "Private Credit"
    assert rows[("Meketa", "consultant")].confidence == 0.8

    # rebuild is idempotent (delete+insert)
    assert build_roster_for_plan(session, "p1") == 2
    assert session.query(PlanManagerRoster).count() == 2
    session.close()
