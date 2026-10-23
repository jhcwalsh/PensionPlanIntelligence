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
