"""The precision-review sheet (scripts/fund_review_sheet.py)."""
from scripts.fund_review_sheet import _quote


def test_quote_is_taken_from_the_approving_document():
    ev = [{"status": "pipeline", "quote": "Realterm V commitment under review"},
          {"status": "approved", "quote": "Committed $40 million to Realterm V"}]
    assert _quote(ev) == "Committed $40 million to Realterm V"
    assert _quote([]) == ""
