"""The OpenRouter client, and the silent-zero trap it exists to prevent."""
from decimal import Decimal

import pytest

import costs
import llm_openrouter


class _OpenAIUsage:
    """The shape OpenRouter returns — deliberately not Anthropic's."""
    prompt_tokens = 7_500
    completion_tokens = 800


def test_usage_is_translated_not_passed_through():
    """The silent-zero trap. costs.cost_usd reads Anthropic field names, so
    handing it an OpenAI usage block prices every token at nothing and the
    spend vanishes — the one direction of error nobody questions."""
    raw = _OpenAIUsage()
    assert costs.cost_usd(llm_openrouter.MODEL, raw) == Decimal(0), (
        "if this fails the field names now match and this guard is obsolete")

    adapted = llm_openrouter.adapt_usage(raw)
    cost = costs.cost_usd(llm_openrouter.MODEL, adapted)
    assert cost > 0
    # 7500 in @ $0.0886/M + 800 out @ $0.1772/M = $0.00080628
    assert Decimal("0.0008") < cost < Decimal("0.00081")


def test_the_model_has_a_price():
    assert llm_openrouter.MODEL in costs.PRICES


def _resp(finish_reason, tool_calls=None):
    class _Fn:
        arguments = '{"returns": [{"asset_class": "US Equity"}]}'
        name = "record"

    class _Call:
        function = _Fn()

    class _Msg:
        pass

    class _Choice:
        pass

    class _Resp:
        pass

    msg = _Msg()
    msg.tool_calls = [_Call()] if tool_calls else None
    choice = _Choice()
    choice.message, choice.finish_reason = msg, finish_reason
    resp = _Resp()
    resp.choices, resp.usage = [choice], _OpenAIUsage()
    return resp


def test_truncated_response_raises(monkeypatch):
    """max_tokens cutting a tool call short must not look like an empty table.

    extract_performance_reports saved thirty documents with zero rows and no
    error this way. A silent empty result is worse than a failure, because
    nobody re-runs it.
    """
    monkeypatch.setattr(llm_openrouter, "_raw_call",
                        lambda **kw: _resp("length"))
    with pytest.raises(llm_openrouter.ResponseTruncated):
        llm_openrouter.call_tool("sys", "user", {"type": "object"}, "record")


def test_missing_tool_call_raises_rather_than_returning_empty(monkeypatch):
    monkeypatch.setattr(llm_openrouter, "_raw_call",
                        lambda **kw: _resp("stop"))
    with pytest.raises(RuntimeError):
        llm_openrouter.call_tool("sys", "user", {"type": "object"}, "record")


def test_a_good_call_returns_parsed_arguments_and_a_positive_cost(monkeypatch):
    monkeypatch.setattr(llm_openrouter, "_raw_call",
                        lambda **kw: _resp("tool_calls", tool_calls=True))
    data, cost = llm_openrouter.call_tool(
        "sys", "user", {"type": "object"}, "record")
    assert data["returns"][0]["asset_class"] == "US Equity"
    assert cost > 0


def test_truncation_that_does_not_announce_itself_still_raises(monkeypatch):
    """Two providers in the first corpus run returned arguments cut off
    mid-string — one at 50,651 characters — while reporting finish_reason
    "stop". The length check passed and json.loads failed instead. Same
    fault, so the caller should see the same exception."""
    resp = _resp("stop", tool_calls=True)
    resp.choices[0].message.tool_calls[0].function.arguments = (
        '{"returns": [{"asset_class": "US Equ')

    monkeypatch.setattr(llm_openrouter, "_raw_call", lambda **kw: resp)
    with pytest.raises(llm_openrouter.ResponseTruncated, match="malformed"):
        llm_openrouter.call_tool("sys", "user", {"type": "object"}, "record")


def test_no_api_key_means_no_call(monkeypatch):
    """The key is never optional and never falls back to the Anthropic one."""
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="OPENROUTER_API_KEY"):
        llm_openrouter._raw_call(model="x", messages=[])
