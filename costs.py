"""What a Claude call costs, and which job made it.

Kept separate from database.py so the arithmetic is testable without a session,
and separate from summarizer.py so the insights package and the three CAFR/IPS
extractors can use it without importing the document summariser.

Prices are USD per million tokens, verified 2026-08-24 at
https://platform.claude.com/docs/en/about-claude/pricing
"""

from __future__ import annotations

import contextvars
import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

logger = logging.getLogger(__name__)


class UnknownModelError(Exception):
    """A model with no price.

    Raised rather than costed at zero: a silent zero makes spend appear to
    *fall* when a model is upgraded, which is the one direction of error that
    would go unquestioned.
    """


@dataclass(frozen=True)
class Price:
    input: Decimal
    output: Decimal
    cache_write_5m: Decimal
    cache_read: Decimal


def _p(i: str, o: str, w: str, r: str) -> Price:
    return Price(Decimal(i), Decimal(o), Decimal(w), Decimal(r))


PRICES: dict[str, Price] = {
    "claude-haiku-4-5-20251001": _p("1", "5", "1.25", "0.10"),
    "claude-sonnet-4-6":         _p("3", "15", "3.75", "0.30"),
    "claude-opus-4-6":           _p("5", "25", "6.25", "0.50"),
}

MILLION = Decimal(1_000_000)


def cost_usd(model: str, usage) -> Decimal:
    """Cost of one call, from its usage block.

    The four token categories are summed independently: the API reports
    ``input_tokens`` *excluding* cache reads and writes, so treating cache
    tokens as a subset of input would under-count every cached call — which
    today means every CAFR extraction.
    """
    try:
        price = PRICES[model]
    except KeyError:
        raise UnknownModelError(
            "no price for %r — add it to costs.PRICES" % model) from None

    def n(attr: str) -> Decimal:
        return Decimal(getattr(usage, attr, 0) or 0)

    return (
        n("input_tokens") * price.input
        + n("output_tokens") * price.output
        + n("cache_creation_input_tokens") * price.cache_write_5m
        + n("cache_read_input_tokens") * price.cache_read
    ) / MILLION


# ---------------------------------------------------------------------------
# Attribution
# ---------------------------------------------------------------------------

_OPERATION: contextvars.ContextVar[str] = contextvars.ContextVar(
    "claude_operation", default="unattributed")
_RUN_ID: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "claude_run_id", default=None)


@contextmanager
def track(operation: str, run_id: Optional[str] = None):
    """Label every Claude call made inside this block.

    A contextvar rather than a parameter: there are thirteen call sites across
    six modules, and threading a label through all of them would touch far more
    code than the measurement is worth — and would be silently forgotten at the
    fourteenth. Reset in a finally so a raising job does not mislabel every
    call that follows it.
    """
    op_token = _OPERATION.set(operation)
    run_token = _RUN_ID.set(run_id)
    try:
        yield
    finally:
        _OPERATION.reset(op_token)
        _RUN_ID.reset(run_token)


def current_attribution() -> tuple[str, Optional[str]]:
    return _OPERATION.get(), _RUN_ID.get()


# ---------------------------------------------------------------------------
# Instrumentation
# ---------------------------------------------------------------------------

def mock_mode() -> bool:
    """True when this run must not touch the API, so has no spend to record."""
    return "mock" in (os.environ.get("LLM_MODE", ""),
                      os.environ.get("INSIGHTS_MODE", ""),
                      os.environ.get("IPS_MODE", ""))


class _RecordingMessages:
    """``messages.create`` plus a usage row. Transparent to the caller."""

    def __init__(self, inner):
        self._inner = inner

    def create(self, **kwargs):
        message = self._inner.create(**kwargs)
        try:
            usage = getattr(message, "usage", None)
            if usage is not None and not mock_mode():
                import database
                database.record_api_usage(kwargs.get("model", "unknown"), usage)
        except Exception as exc:                       # noqa: BLE001
            # Measurement is subordinate to the work. record_api_usage already
            # swallows its own failures; this catches anything above it, such
            # as the import itself.
            logger.warning("API usage not recorded: %s", exc)
        return message

    def __getattr__(self, name):
        return getattr(self._inner, name)


class _RecordingClient:
    """An Anthropic client that records what its calls cost.

    Applied in all four ``_get_client`` factories — the summariser and the
    three CAFR/IPS extractors, which each build their own. Instrumenting only
    the summariser would miss the Sonnet calls over 100+ page PDFs, which are
    the most likely answer to where the money goes.
    """

    def __init__(self, inner):
        self._inner = inner
        self.messages = _RecordingMessages(inner.messages)

    def __getattr__(self, name):
        return getattr(self._inner, name)


def instrument(client):
    """Wrap a client so every call records its usage. Idempotent."""
    if isinstance(client, _RecordingClient):
        return client
    return _RecordingClient(client)
