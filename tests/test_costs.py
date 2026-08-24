"""Pricing arithmetic, checked against the published numbers.

Money, so Decimal rather than float, and a table rather than a formula: prices
change per model and are not derivable from anything.

Prices verified 2026-08-24 at
https://platform.claude.com/docs/en/about-claude/pricing
"""

from __future__ import annotations

import types
from decimal import Decimal

import pytest

from costs import PRICES, UnknownModelError, cost_usd

HAIKU = "claude-haiku-4-5-20251001"
SONNET = "claude-sonnet-4-6"
OPUS = "claude-opus-4-6"


def _usage(input_tokens=0, output_tokens=0,
           cache_creation_input_tokens=0, cache_read_input_tokens=0):
    return types.SimpleNamespace(
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cache_creation_input_tokens=cache_creation_input_tokens,
        cache_read_input_tokens=cache_read_input_tokens)


def test_a_million_input_tokens_costs_the_headline_price():
    assert cost_usd(HAIKU, _usage(input_tokens=1_000_000)) == Decimal("1")
    assert cost_usd(SONNET, _usage(input_tokens=1_000_000)) == Decimal("3")
    assert cost_usd(OPUS, _usage(input_tokens=1_000_000)) == Decimal("5")


def test_output_is_priced_separately_and_higher():
    assert cost_usd(HAIKU, _usage(output_tokens=1_000_000)) == Decimal("5")
    assert cost_usd(SONNET, _usage(output_tokens=1_000_000)) == Decimal("15")
    assert cost_usd(OPUS, _usage(output_tokens=1_000_000)) == Decimal("25")


def test_cache_reads_cost_a_tenth_of_input():
    assert cost_usd(HAIKU, _usage(cache_read_input_tokens=1_000_000)) \
        == Decimal("0.10")
    assert cost_usd(SONNET, _usage(cache_read_input_tokens=1_000_000)) \
        == Decimal("0.30")


def test_cache_writes_cost_a_quarter_more_than_input():
    """The 5-minute write multiplier is 1.25x base."""
    assert cost_usd(HAIKU, _usage(cache_creation_input_tokens=1_000_000)) \
        == Decimal("1.25")
    assert cost_usd(SONNET, _usage(cache_creation_input_tokens=1_000_000)) \
        == Decimal("3.75")


def test_the_four_categories_are_summed_not_nested():
    """input_tokens excludes cache reads and writes in the Anthropic API.

    Treating cache tokens as a subset of input_tokens would under-count every
    CAFR extraction, which is the one place caching is already used.
    """
    total = cost_usd(SONNET, _usage(
        input_tokens=1_000_000, output_tokens=1_000_000,
        cache_creation_input_tokens=1_000_000,
        cache_read_input_tokens=1_000_000))
    assert total == Decimal("3") + Decimal("15") + Decimal("3.75") + Decimal("0.30")


def test_a_realistic_summary_costs_about_one_and_a_half_cents():
    """Sanity anchor at the real working size: ~12.5k input, ~600 output.

    12,500 x $1/MTok = $0.0125, plus 600 x $5/MTok = $0.003. Worth pinning
    because it is the number that makes the wider point: at roughly 186 real
    API calls a month, document summarisation costs single-digit dollars, so
    it is not where the bill comes from.
    """
    cost = cost_usd(HAIKU, _usage(input_tokens=12_500, output_tokens=600))
    assert cost == Decimal("0.0155"), cost


def test_an_unknown_model_raises_rather_than_costing_zero():
    """Silently pricing a new model at zero would make the table lie in the
    one direction that matters — spend appearing to fall after a model bump."""
    with pytest.raises(UnknownModelError, match="claude-future-9"):
        cost_usd("claude-future-9", _usage(input_tokens=100))


def test_missing_cache_fields_are_treated_as_zero():
    """Older SDK responses omit them entirely."""
    bare = types.SimpleNamespace(input_tokens=1000, output_tokens=100)
    assert cost_usd(HAIKU, bare) > 0


def test_every_model_the_code_actually_uses_is_priced():
    """A model referenced in the codebase but absent from PRICES would raise
    at run time, inside the wrapper, on a real job."""
    import generate_notes
    import summarizer

    used = {summarizer.MODEL_HAIKU, summarizer.MODEL_SONNET,
            generate_notes.MODEL_OPUS}
    assert used <= set(PRICES), used - set(PRICES)


# ---------------------------------------------------------------------------
# Attribution
# ---------------------------------------------------------------------------

def test_calls_are_unattributed_by_default():
    """The honest default. An unlabelled row still records the spend."""
    import costs
    assert costs.current_attribution() == ("unattributed", None)


def test_track_labels_the_calls_inside_it():
    import costs
    with costs.track("summarize", run_id="run-7"):
        assert costs.current_attribution() == ("summarize", "run-7")


def test_the_label_is_restored_afterwards():
    import costs
    with costs.track("summarize", run_id="run-7"):
        pass
    assert costs.current_attribution() == ("unattributed", None)


def test_the_label_is_restored_even_when_the_block_raises():
    """Otherwise one failed job mislabels every call after it."""
    import costs
    with pytest.raises(ValueError):
        with costs.track("cafr_extract", run_id="2025"):
            raise ValueError("boom")
    assert costs.current_attribution() == ("unattributed", None)


def test_tracks_nest():
    import costs
    with costs.track("outer", run_id="a"):
        with costs.track("inner", run_id="b"):
            assert costs.current_attribution() == ("inner", "b")
        assert costs.current_attribution() == ("outer", "a")
