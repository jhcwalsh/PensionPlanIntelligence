"""Fuzzy fund-merge proposals (scripts/propose_fund_mappings.py) — they only
ever edit JSON for James to review, never the database."""
import json

import database
from database import Fund
from funds import build
from scripts import propose_fund_mappings as pfm


def test_candidate_pairs_same_manager_number_subset_series():
    keys = ["blackstone|capital partners|9", "blackstone|bcp capital partners|9",
            "blackstone|capital partners|8", "kkr|capital partners|9",
            "blackstone|real estate|9", "wellington|emerging markets|"]
    assert pfm.candidate_pairs(keys, {"aliases": {}, "separate": []}) == [
        ("blackstone|bcp capital partners|9", "blackstone|capital partners|9")]


def test_candidate_pairs_skip_known():
    keys = ["a|x y|1", "a|x|1", "b|p q|2", "b|p|2"]
    known = {"aliases": {"a|x y|1": "a|x|1"}, "separate": [["b|p|2", "b|p q|2"]]}
    assert pfm.candidate_pairs(keys, known) == []


def test_mock_mode_writes_separate_only(tmp_path, monkeypatch):
    path = tmp_path / "fund_mappings.json"
    path.write_text(json.dumps({"aliases": {}, "separate": []}))
    monkeypatch.setattr(build, "FUND_MAPPINGS_PATH", path)
    s = database.get_session()
    s.add_all([Fund(fund_key="a|x y|1", canonical_name="A X Y Fund I", manager_canonical="A"),
               Fund(fund_key="a|x|1", canonical_name="A X Fund I", manager_canonical="A")])
    s.commit()
    s.close()
    assert pfm.main([]) == 0
    data = json.loads(path.read_text())
    assert data["aliases"] == {}
    assert data["separate"] == [["a|x y|1", "a|x|1"]]


def test_same_name_pairs_across_managers():
    names = {"tpg partners||10": "TPG Partners X, L.P.",
             "hamilton lane|tpg partners|10": "TPG Partners X, L.P.",
             "tpg capital bd|partners|10": "TPG Partners X",
             "tpg partners||9": "TPG Partners IX",
             "acme||3": "Fund III",
             "other||3": "Fund III"}
    assert pfm.same_name_pairs(names, {"aliases": {}, "separate": []}) == [
        ("hamilton lane|tpg partners|10", "tpg partners||10"),
        ("tpg capital bd|partners|10", "tpg partners||10")]


def test_report_lists_proposals_by_name(tmp_path, monkeypatch):
    path = tmp_path / "fund_mappings.json"
    path.write_text(json.dumps({"aliases": {}, "separate": []}))
    report = tmp_path / "report.md"
    monkeypatch.setattr(build, "FUND_MAPPINGS_PATH", path)
    monkeypatch.setattr(pfm, "_ask_same", lambda client, a, b: True)
    monkeypatch.setattr(pfm, "_client", lambda: object())
    s = database.get_session()
    s.add_all([Fund(fund_key="a|x y|1", canonical_name="A X Y Fund I", manager_canonical="A"),
               Fund(fund_key="a|x|1", canonical_name="A X Fund I", manager_canonical="A")])
    s.commit()
    s.close()
    assert pfm.main(["--report", str(report), "--skip-managers"]) == 0
    assert "A X Y Fund I -> A X Fund I" in report.read_text()
    assert json.loads(path.read_text())["aliases"] == {"a|x y|1": "a|x|1"}


def test_pair_prompt_names_separate_pools():
    for word in ("co-invest", "annex", "sidecar", "growth"):
        assert word in pfm._PAIR_PROMPT.lower()
