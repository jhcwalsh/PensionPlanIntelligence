"""Haiku's per-fund split of investment actions (funds/extract.py)."""
from datetime import datetime
from types import SimpleNamespace

from funds.extract import ActionInput, action_hash, extract_batch


def _inp(action, doc_type="minutes"):
    return ActionInput(action_hash(action), action, doc_type, "2026-03-20")


def test_mock_mode_one_item_per_action():
    a = {"action": "commitment", "description": "GTCR Fund XV", "manager": "GTCR", "amount_millions": 50}
    b = {"action": "commitment", "description": "TPG Partners X", "manager": "TPG"}
    out = extract_batch(None, [_inp(a), _inp(b)])
    assert set(out) == {action_hash(a), action_hash(b)}
    item = out[action_hash(a)][0]
    assert item["fund_name"] == "GTCR Fund XV"
    assert item["status"] == "approved"
    assert item["amount_millions"] == 50


def test_action_hash_stable_and_order_insensitive():
    assert action_hash({"a": 1, "b": "x"}) == action_hash({"b": "x", "a": 1})
    assert action_hash({"a": 1}) != action_hash({"a": 2})


class _FakeClient:
    def __init__(self, text):
        self.text = text
        self.calls = []
        self.messages = self

    def create(self, **kw):
        self.calls.append(kw)
        return SimpleNamespace(content=[SimpleNamespace(type="text", text=self.text)])


def test_live_response_parsed_by_index(monkeypatch):
    monkeypatch.delenv("LLM_MODE", raising=False)
    client = _FakeClient(
        '```json\n{"0":[{"fund_name":"A Fund I","manager":"A","amount_millions":5,'
        '"status":"approved","asset_class":null}],"7":[]}\n```'
    )
    a = {"action": "commitment", "description": "Committed $5M to A Fund I"}
    b = {"action": "commitment", "description": "Something else"}
    out = extract_batch(client, [_inp(a), _inp(b)])
    assert list(out) == [action_hash(a)]
    assert out[action_hash(a)][0]["fund_name"] == "A Fund I"
    assert len(client.calls) == 1


def test_summarizer_prompt_asks_for_fund_name():
    from summarizer import build_extraction_prompt
    doc = SimpleNamespace(meeting_date=datetime(2026, 3, 20), doc_type="minutes")
    assert '"fund_name"' in build_extraction_prompt(doc, "Test Plan", "text")


def test_action_hash_changes_with_the_prompt(monkeypatch):
    import funds.extract as fx
    a = {"action": "commitment", "description": "GTCR Fund XV"}
    before = action_hash(a)
    monkeypatch.setattr(fx, "_SYSTEM", fx._SYSTEM + "\nA new rule.")
    assert action_hash(a) != before
