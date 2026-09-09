"""Sonnet 4.6 -> Sonnet 5 and Opus 4.6 -> Opus 5.

Both successors reject non-default sampling parameters, run adaptive
thinking when the parameter is omitted, and tokenise about 30% heavier, so
the migration is three things per call site: the id, the sampling and
thinking parameters, and output headroom. Structured extraction and
summaries run thinking-off; the editorial briefings keep adaptive thinking
at medium effort, which means the text block is no longer content[0].
"""

import pathlib
import types
from datetime import date
from decimal import Decimal

import pytest

import costs
import summarizer

ROOT = pathlib.Path(__file__).resolve().parent.parent

SONNET = "claude-sonnet-5"
OPUS = "claude-opus-5"


def _text(t):
    return types.SimpleNamespace(type="text", text=t)


def _thinking():
    return types.SimpleNamespace(type="thinking", thinking="")


def _usage():
    return types.SimpleNamespace(input_tokens=10, output_tokens=5,
                                 cache_creation_input_tokens=0,
                                 cache_read_input_tokens=0)


class Capture:
    """A client whose create() records kwargs and returns a fixed message."""

    def __init__(self, content):
        self.kwargs = []
        self.content = content

    def create(self, **kwargs):
        self.kwargs.append(kwargs)
        return types.SimpleNamespace(content=self.content, stop_reason="end_turn",
                                     usage=_usage())

    @property
    def client(self):
        return types.SimpleNamespace(messages=self)


# ---------------------------------------------------------------------------
# Identifiers and prices
# ---------------------------------------------------------------------------

def test_the_sonnet_and_opus_constants_name_the_current_generation():
    import generate_notes
    import extract_cafr_actuarial
    import extract_cafr_investments
    import extract_ips
    import extract_performance_reports

    assert summarizer.MODEL_SONNET == SONNET
    assert generate_notes.MODEL_OPUS == OPUS
    for module in (extract_cafr_actuarial, extract_cafr_investments,
                   extract_ips, extract_performance_reports):
        assert module.MODEL == SONNET, module.__name__


def test_no_source_file_names_the_previous_generation_except_the_price_table():
    """costs.PRICES keeps the old rows so historical api_usage stays priceable."""
    offenders = []
    for path in ROOT.rglob("*.py"):
        rel = path.relative_to(ROOT).as_posix()
        if rel.startswith((".", "tests/", "docs/")) or rel == "costs.py":
            continue
        src = path.read_text(encoding="utf-8", errors="ignore")
        if "claude-sonnet-4-6" in src or "claude-opus-4-6" in src:
            offenders.append(rel)
    assert offenders == []


def test_the_successors_are_priced():
    assert costs.PRICES[SONNET] == costs._p("2", "10", "2.50", "0.20")
    assert costs.PRICES[OPUS] == costs._p("5", "25", "6.25", "0.50")


def test_the_pinned_sdk_knows_the_thinking_parameter():
    import inspect
    import anthropic
    pin = [l for l in (ROOT / "requirements-pipeline.txt").read_text().splitlines()
           if l.startswith("anthropic==")]
    assert pin == ["anthropic==0.125.0"]
    params = inspect.signature(anthropic.Anthropic(api_key="x").messages.create).parameters
    assert "thinking" in params and "output_config" in params


# ---------------------------------------------------------------------------
# Summariser
# ---------------------------------------------------------------------------

def test_sonnet_summaries_run_thinking_off_with_more_output_room():
    params = summarizer.request_params("prompt", summarizer.MODEL_SONNET)
    assert params["model"] == SONNET
    assert params["thinking"] == {"type": "disabled"}
    assert "temperature" not in params
    assert params["max_tokens"] >= 5300     # 4096 x 1.3 for the new tokenizer


def test_haiku_summaries_send_no_thinking_parameter():
    params = summarizer.request_params("prompt", summarizer.MODEL_HAIKU)
    assert "thinking" not in params


def test_message_text_skips_a_leading_thinking_block():
    message = types.SimpleNamespace(content=[_thinking(), _text("{}")],
                                    stop_reason="end_turn", usage=_usage())
    assert summarizer.message_text(message) == "{}"


# ---------------------------------------------------------------------------
# Briefings
# ---------------------------------------------------------------------------

def test_generate_note_uses_adaptive_thinking_and_reads_the_text_block(monkeypatch):
    import generate_notes
    cap = Capture([_thinking(), _text("# note")])
    monkeypatch.setattr(generate_notes, "_get_client", lambda: cap.client)

    out = generate_notes.generate_note("p", 100, model=generate_notes.MODEL_OPUS)

    (kw,) = cap.kwargs
    assert out == "# note"
    assert kw["model"] == OPUS
    assert "temperature" not in kw
    assert kw["thinking"] == {"type": "adaptive"}
    assert kw["output_config"] == {"effort": "medium"}


def test_weekly_output_cap_has_room_for_thinking_and_the_heavier_tokenizer():
    import generate_notes
    assert generate_notes.MAX_TOKENS_HIGHLIGHTS >= 8000


@pytest.mark.parametrize("cadence, model, floor", [
    ("monthly", SONNET, 8000),
    ("quarterly", OPUS, 16000),
    ("annual", OPUS, 16000),
])
def test_compose_calls_use_the_successor_without_sampling_params(
        monkeypatch, cadence, model, floor):
    monkeypatch.setenv("INSIGHTS_MODE", "live")
    from insights import compose
    cap = Capture([_thinking(), _text("# briefing")])
    monkeypatch.setattr(summarizer, "_get_client", lambda: cap.client)

    start, end = date(2026, 1, 1), date(2026, 3, 31)
    if cadence == "monthly":
        out = compose.compose_monthly(["# w1"], start, end)
    elif cadence == "quarterly":
        out = compose.compose_quarterly([(start, "# m1")], start, end)
    else:
        out = compose.compose_annual([(start, "# m1")], start, end)

    (kw,) = cap.kwargs
    assert out == "# briefing"
    assert kw["model"] == model
    assert "temperature" not in kw
    assert kw["thinking"] == {"type": "adaptive"}
    assert kw["output_config"] == {"effort": "medium"}
    assert kw["max_tokens"] >= floor


# ---------------------------------------------------------------------------
# Structured extraction
# ---------------------------------------------------------------------------

def _tool_use(name):
    return types.SimpleNamespace(type="tool_use", name=name, input={"ok": True})


@pytest.mark.parametrize("module_name, tool, call, old_cap", [
    ("extract_cafr_investments", "record_investment_data",
     lambda m: m.call_claude("Plan", 2025, "section"), 16384),
    ("extract_cafr_actuarial", "record_actuarial_data",
     lambda m: m.call_claude("Plan", 2025, "section"), 8192),
    ("extract_ips", "record_ips_data",
     lambda m: m.call_claude("Plan", "ips text"), 8192),
    ("extract_performance_reports", "record_performance_data",
     lambda m: m.call_claude("Plan", None, "report"), 16384),
])
def test_extractors_run_thinking_off_on_sonnet_5_with_wider_caps(
        monkeypatch, module_name, tool, call, old_cap):
    monkeypatch.setenv("LLM_MODE", "live")
    module = __import__(module_name)
    cap = Capture([_tool_use(tool)])
    monkeypatch.setattr(module, "_get_client", lambda: cap.client)

    assert call(module) == {"ok": True}

    (kw,) = cap.kwargs
    assert kw["model"] == SONNET
    assert kw["thinking"] == {"type": "disabled"}
    assert "temperature" not in kw
    assert kw["max_tokens"] >= int(old_cap * 1.3)


def test_manager_normaliser_runs_thinking_off_on_sonnet_5():
    from scripts import normalize_managers
    cap = Capture([_text("{}")])

    assert normalize_managers._classify_batch(cap.client, ["Acme Capital"]) == {}

    (kw,) = cap.kwargs
    assert kw["model"] == SONNET
    assert kw["thinking"] == {"type": "disabled"}
    assert "temperature" not in kw
    assert kw["max_tokens"] >= int(8192 * 1.3)


def test_pending_spend_prices_summaries_at_the_successor_rate():
    from scripts import pending_spend
    src = (ROOT / "scripts" / "pending_spend.py").read_text(encoding="utf-8")
    assert "claude-sonnet-4-6" not in src
    assert pending_spend._price(SONNET, 1_000_000, 0) == Decimal("2")
