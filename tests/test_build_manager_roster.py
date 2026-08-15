"""Deterministic roster reconciliation into plan_manager_roster."""
import json
from datetime import datetime

import pytest

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
    # "Meketa" is canonicalized through manager_mappings.json like any other
    # name — the column is canonical_name and the table is UNIQUE on it.
    assert rows[("Meketa Investment Group", "consultant")].confidence == 0.8

    # rebuild is idempotent (delete+insert)
    assert build_roster_for_plan(session, "p1") == 2
    assert session.query(PlanManagerRoster).count() == 2
    session.close()


def _plan_with_doc(session, plan_id="p1"):
    session.add(Plan(id=plan_id, name="P", abbreviation="P", state="CA"))
    doc = Document(plan_id=plan_id, url=f"https://x/{plan_id}.pdf", filename="a.pdf",
                   doc_type="minutes", extraction_status="done",
                   meeting_date=datetime(2026, 6, 1))
    session.add(doc); session.commit()
    return doc


def _rfp(doc, plan_id, rfp_id, **record):
    return RFPRecord(rfp_id=rfp_id, document_id=doc.id, plan_id=plan_id,
                     record=json.dumps(record), extraction_confidence=0.9,
                     needs_review=False, prompt_version="rfp_v1")


def test_governance_names_are_canonicalized_into_one_row(tmp_db):
    """Two spellings of one consultant must collapse to a single roster row.

    The governance branch used to key on the raw name while the Manager
    branch canonicalized, so "Meketa" and "Meketa Investment Group" produced
    two rows that UNIQUE(plan_id, canonical_name, role) cannot collapse —
    double-counting the consultant.
    """
    session = get_session()
    doc = _plan_with_doc(session)
    session.add_all([
        _rfp(doc, "p1", "a" * 16, rfp_type="Consultant", status="Awarded",
             awarded_manager="Meketa"),
        _rfp(doc, "p1", "b" * 16, rfp_type="Consultant", status="Awarded",
             awarded_manager="Meketa Investment Group"),
    ])
    session.commit()

    assert build_roster_for_plan(session, "p1") == 1
    row = session.query(PlanManagerRoster).one()
    assert (row.canonical_name, row.role) == ("Meketa Investment Group", "consultant")
    # Both records are kept as evidence and both count as mentions.
    assert json.loads(row.evidence)["mention_count"] == 2
    session.close()


def test_withdrawn_rfp_award_is_not_asserted_as_current(tmp_db):
    """A Withdrawn search's awarded_manager is not the plan's current provider."""
    session = get_session()
    doc = _plan_with_doc(session)
    session.add(_rfp(doc, "p1", "c" * 16, rfp_type="Custodian", status="Withdrawn",
                     awarded_manager="BNY Mellon"))
    session.commit()

    build_roster_for_plan(session, "p1")
    row = session.query(PlanManagerRoster).one()
    assert row.status == "unknown"
    session.close()


def test_incumbent_stays_current_when_search_is_in_flight(tmp_db):
    """`incumbent_manager` names the provider already in place, whatever the
    search status — including a withdrawn one, which means it was retained."""
    session = get_session()
    doc = _plan_with_doc(session)
    session.add_all([
        _rfp(doc, "p1", "d" * 16, rfp_type="Consultant", status="Planned",
             incumbent_manager="Callan"),
        _rfp(doc, "p1", "e" * 16, rfp_type="Consultant", status="Withdrawn",
             incumbent_manager="Callan"),
    ])
    session.commit()

    build_roster_for_plan(session, "p1")
    row = session.query(PlanManagerRoster).filter_by(canonical_name="Callan").one()
    assert row.status == "current"
    session.close()


@pytest.mark.parametrize("order", [("Withdrawn", "Awarded"), ("Awarded", "Withdrawn")])
def test_rfp_status_resolution_is_order_independent(tmp_db, order):
    """A live award must win over a withdrawn one regardless of query order.

    _merge_or_create keeps the *existing* entry's status, so folding RFP
    records straight into the shared dict let whichever record was seen first
    pin the status permanently.
    """
    session = get_session()
    doc = _plan_with_doc(session)
    session.add_all([
        _rfp(doc, "p1", chr(102 + i) * 16, rfp_type="Actuary", status=status,
             awarded_manager="Segal")
        for i, status in enumerate(order)
    ])
    session.commit()

    build_roster_for_plan(session, "p1")
    row = session.query(PlanManagerRoster).one()
    assert row.status == "current"
    session.close()


def test_mention_count_persisted_and_counts_null_action_rows(tmp_db):
    """evidence["mention_count"] is the true count, not sum(action_types).

    Actions with a null `action` field are real mentions but contribute
    nothing to action_types, and RFP-derived rows have no action_types at all.
    """
    session = get_session()
    doc = _plan_with_doc(session)
    session.add(Summary(document_id=doc.id, summary_text="s",
                        investment_actions=json.dumps([
                            {"action": "hire", "manager": "BlackRock"},
                            {"action": None, "manager": "BlackRock"},
                            {"manager": "BlackRock"},
                        ])))
    session.add(_rfp(doc, "p1", "g" * 16, rfp_type="Manager", status="Awarded",
                     awarded_manager="BlackRock"))
    session.commit()

    build_roster_for_plan(session, "p1")
    row = session.query(PlanManagerRoster).one()
    evidence = json.loads(row.evidence)
    assert evidence["action_types"] == {"hire": 1}
    # 3 summary mentions + 1 RFP award, none of them lost.
    assert evidence["mention_count"] == 4
    session.close()


def test_main_exits_nonzero_when_a_plan_fails(tmp_db, monkeypatch, capsys):
    """A failed roster build must not report success to the GHA step.

    Safe for daily-pipeline.yml: every step after "Rebuild manager rosters"
    carries `if: ${{ !cancelled() }}`, so the DB push/commit still runs.
    """
    import scripts.build_manager_roster as brm

    session = get_session()
    session.add_all([Plan(id="good", name="G", abbreviation="G", state="CA"),
                     Plan(id="bad", name="B", abbreviation="B", state="CA")])
    session.commit(); session.close()

    real = brm.build_roster_for_plan

    def flaky(session, plan_id):
        if plan_id == "bad":
            raise RuntimeError("boom")
        return real(session, plan_id)

    monkeypatch.setattr(brm, "build_roster_for_plan", flaky)
    monkeypatch.setattr("sys.argv", ["build_manager_roster"])

    with pytest.raises(SystemExit) as exc:
        brm.main()
    assert exc.value.code == 1
    assert "bad" in capsys.readouterr().err

    # ...and stays zero-exit when every plan succeeds.
    monkeypatch.setattr(brm, "build_roster_for_plan", real)
    brm.main()  # must not raise
