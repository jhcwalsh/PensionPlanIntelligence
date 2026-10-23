"""Building fund_commitments from summaries (funds/build.py), mock-mode LLM."""
import json
from datetime import datetime

import pytest

import database
from database import Document, Fund, FundActionExtract, FundCommitment, Plan, Summary
from funds import build
from funds.extract import action_hash


@pytest.fixture
def s(monkeypatch):
    monkeypatch.setattr(build, "load_manager_mappings", lambda: {})
    monkeypatch.setattr(build, "load_fund_mappings", lambda: {"aliases": {}, "separate": []})
    sess = database.get_session()
    yield sess
    sess.close()


def _seed(s, plan, doc_id, actions, doc_type="minutes", date="2026-03-20", model="claude-haiku"):
    if not s.get(Plan, plan):
        s.add(Plan(id=plan, name=plan.upper(), aum_billions=10.0))
    s.add(Document(id=doc_id, plan_id=plan, url=f"https://example.test/{doc_id}",
                   doc_type=doc_type,
                   meeting_date=datetime.fromisoformat(date) if date else None))
    s.add(Summary(document_id=doc_id, investment_actions=json.dumps(actions), model_used=model))
    s.commit()


def _commit(desc, manager, amount=None, status="approved", action="commitment"):
    return {"action": action, "description": desc, "manager": manager,
            "amount_millions": amount, "_mock_status": status}


def _build(s):
    build.extract_pending(s, None)
    return build.rebuild(s)


def test_rebuild_dedupes_across_documents(s):
    _seed(s, "p", 1, [_commit("GTCR Fund XV", "GTCR", 100, "proposed")], "agenda", "2026-03-01")
    _seed(s, "p", 2, [_commit("GTCR Fund XV, L.P.", "GTCR", 75, "approved")], "minutes", "2026-03-20")
    _build(s)
    rows = s.query(FundCommitment).all()
    assert len(rows) == 1
    r = rows[0]
    assert (r.status, r.amount_millions, r.first_seen, r.last_seen) == ("approved", 75, "2026-03-01", "2026-03-20")
    assert sorted(e["document_id"] for e in json.loads(r.evidence)) == [1, 2]


def test_two_plans_same_fund_two_spellings(s):
    _seed(s, "p1", 1, [_commit("Francisco Partners VIII", "Francisco Partners", 50)])
    _seed(s, "p2", 2, [_commit("Francisco Partners Fund VIII, L.P.", "Francisco Partners", 60)])
    _build(s)
    assert s.query(Fund).count() == 1
    assert s.query(FundCommitment).count() == 2


def test_rebuild_unknown_manager_never_merges(s, monkeypatch):
    monkeypatch.setattr(build, "load_manager_mappings", lambda: {"Various": None})
    _seed(s, "p1", 1, [_commit("Fund III", "Various", 50)])
    _seed(s, "p2", 2, [_commit("Fund III", "Various", 60)])
    counts = _build(s)
    assert s.query(Fund).count() == 0
    assert counts["dropped"] == 2


def test_rebuild_bundle_does_not_copy_total(s):
    action = _commit("KKR GII V and Actis Energy 6", "Multiple", 500)
    _seed(s, "p", 1, [action])
    s.add(FundActionExtract(action_hash=action_hash(action), model="test", items_json=json.dumps([
        {"fund_name": "KKR Global Infrastructure Investors V", "manager": "KKR",
         "amount_millions": None, "status": "approved", "asset_class": None},
        {"fund_name": "Actis Energy 6", "manager": "Actis",
         "amount_millions": None, "status": "approved", "asset_class": None},
    ])))
    s.commit()
    build.rebuild(s)
    amounts = [r.amount_millions for r in s.query(FundCommitment).all()]
    assert amounts == [None, None]


def test_hire_without_fund_dropped_commitment_without_fund_manager_only(s, monkeypatch):
    real = build.extract_batch

    def no_fund_names(client, batch):
        out = real(client, batch)
        for items in out.values():
            for it in items:
                it["fund_name"] = None
        return out

    monkeypatch.setattr(build, "extract_batch", no_fund_names)
    _seed(s, "p", 1, [_commit("Hired Acme for a mandate", "Acme", action="hire"),
                      _commit("Repeat commitment with Almanac", "Almanac")])
    counts = _build(s)
    rows = s.query(FundCommitment).all()
    assert [(r.fund_id, r.manager_canonical, r.match_confidence) for r in rows] == [(None, "Almanac", "manager_only")]
    assert counts["manager_only"] == 1 and counts["dropped"] == 1


def test_alias_applied(s, monkeypatch):
    monkeypatch.setattr(build, "load_fund_mappings", lambda: {
        "aliases": {"blackstone|bcp|9": "blackstone|capital partners|9"}, "separate": []})
    _seed(s, "p1", 1, [_commit("Blackstone BCP IX", "Blackstone")])
    _seed(s, "p2", 2, [_commit("Blackstone Capital Partners IX", "Blackstone")])
    _build(s)
    assert s.query(Fund).count() == 1
    assert {r.match_confidence for r in s.query(FundCommitment)} == {"alias", "exact"}


def test_extract_pending_skips_cached_and_dedup_text(s):
    a = _commit("TPG Partners X", "TPG", 40)
    _seed(s, "p", 1, [a])
    _seed(s, "p", 2, [a], model="dedup:claude-haiku")
    assert build.extract_pending(s, None) == (1, 0)
    assert build.extract_pending(s, None) == (0, 0)
    build.rebuild(s)
    r = s.query(FundCommitment).one()
    assert len(json.loads(r.evidence)) == 2


def test_rebuild_idempotent(s):
    _seed(s, "p1", 1, [_commit("GTCR Fund XV", "GTCR", 50)])
    _seed(s, "p2", 2, [_commit("Wellington Emerging Markets Fund", "Wellington", 60)])
    first = _build(s)
    second = build.rebuild(s)
    assert first == second
    assert s.query(Fund).count() == 2


def test_unnumbered_marked(s):
    _seed(s, "p", 1, [_commit("Wellington Emerging Markets Fund", "Wellington", 60)])
    _build(s)
    assert s.query(FundCommitment).one().match_confidence == "unnumbered"


def test_cli_mock_run_exits_zero(s):
    from scripts.build_fund_commitments import main
    _seed(s, "p", 1, [_commit("GTCR Fund XV", "GTCR", 50)])
    assert main([]) == 0
    assert s.query(FundCommitment).count() == 1


@pytest.mark.parametrize("a, b", [
    (("Carlyle Realty Partners VIII", "Carlyle Realty Partners"), ("Carlyle Partners VIII", "Carlyle Partners")),
    (("Blackstone Growth II", "Blackstone Growth II"), ("Blackstone Capital Partners II", "Blackstone Capital Partners")),
])
def test_series_bearing_raw_manager_does_not_merge_funds(s, monkeypatch, a, b):
    monkeypatch.setattr(build, "load_manager_mappings", lambda: {
        "Carlyle Realty Partners": "The Carlyle Group", "Carlyle Partners": "The Carlyle Group",
        "Blackstone Growth II": "Blackstone", "Blackstone Capital Partners": "Blackstone"})
    _seed(s, "p1", 1, [_commit(a[0], a[1])])
    _seed(s, "p2", 2, [_commit(b[0], b[1])])
    _build(s)
    assert s.query(Fund).count() == 2


def test_alternative_manager_spelling_still_strips(s, monkeypatch):
    monkeypatch.setattr(build, "load_manager_mappings", lambda: {"Kohlberg Kravis Roberts & Co.": "KKR"})
    _seed(s, "p1", 1, [_commit("KKR North America Fund XIV", "KKR")])
    _seed(s, "p2", 2, [_commit("Kohlberg Kravis Roberts North America Fund XIV", "Kohlberg Kravis Roberts & Co.")])
    _build(s)
    assert s.query(Fund).count() == 1


def test_generic_fund_name_becomes_manager_only(s):
    _seed(s, "p1", 1, [_commit("Fund III", "Audax")])
    _seed(s, "p2", 2, [_commit("Audax Fund III", "Audax")])
    _build(s)
    rows = {r.plan_id: r for r in s.query(FundCommitment)}
    assert rows["p1"].fund_id is None and rows["p1"].match_confidence == "manager_only"
    assert rows["p2"].fund_id is not None


def test_single_item_without_amount_does_not_take_action_total(s):
    action = _commit("Commitments including KKR GII V", "KKR", 500)
    _seed(s, "p", 1, [action])
    s.add(FundActionExtract(action_hash=action_hash(action), model="test", items_json=json.dumps([
        {"fund_name": "KKR Global Infrastructure Investors V", "manager": "KKR",
         "amount_millions": None, "status": "approved", "asset_class": None}])))
    s.commit()
    build.rebuild(s)
    assert s.query(FundCommitment).one().amount_millions is None
