# Claude API Spend Visibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Record what every Claude call costs, attributed to the job that made it, so the next optimisation targets measured spend instead of a guess.

**Architecture:** One instrumented wrapper around the shared client, not thirteen edits at thirteen call sites. A `contextvar` carries "which job is running" so a usage row can be attributed without threading an argument through every function. Pricing is a pure table with pure arithmetic, so it can be tested against the published numbers without an API call.

**Tech Stack:** anthropic SDK, SQLAlchemy 2.x, Neon Postgres, Streamlit.

**Spec:** `docs/superpowers/specs/2026-08-16-low-maintenance-app-design.md` §6, control 4. **This plan corrects that section** — see below.

## Global Constraints

- **Never reload `database.py` in a test.** `conftest.py` rebinds `engine`/`SessionLocal` by monkeypatch; a reload orphans the ORM classes.
- **Never write SQL `ALTER TABLE` migrations.** Add the model, call `init_db()`.
- **Do not run `git add .`** — stage by name.
- **Recording must never break a job.** A failure to write a usage row is a lost measurement, not a lost summary. Every record path is wrapped and logged.
- **Sessions stay short.** Neon's `idle_in_transaction_session_timeout` is 5 minutes, and the pipeline does minutes of I/O between database touches (see `pipeline.py`'s `log_session`).
- `LLM_MODE=mock` and `INSIGHTS_MODE=mock` must record nothing — mock runs have no spend.

## What this plan corrects in the spec

Measured on 2026-08-24 before writing this, because the spec's §6 predates work that has since landed:

- **Control 1 (Haiku-first summarisation) is already implemented.** `summarizer.choose_model()` routes to Haiku by default and reserves Sonnet for large investment packs. Last 30 days: 57.9% `dedup:haiku` (no API call at all), 31.3% Haiku, 9.4% Sonnet. The spec's "largest single saving" is already banked.
- **Control 3 (prompt caching on the summariser) would do nothing.** `SYSTEM_PROMPT` is **62 tokens**. The minimum cacheable prefix is 2,048 for Haiku and 1,024 for Sonnet — it is 30× below the floor, so no cache entry would ever be created. The CAFR extractors, which do have large prompts, already cache correctly.
- **Controls 2 and 4 are real and undone.** There is no per-run cap, and `message.usage` is discarded on every call except inside an error string.

The remaining cost candidates are therefore **unmeasured**: vision OCR in `extractor.py` (354 `ocr_partial` extractions in 30 days at a stated $0.02–0.05/page), CAFR extraction (Sonnet over 100+ page PDFs, bursty — 115 in April), and Opus for the monthly/quarterly/annual briefings. Control 2's cap should be sized from this data, so it comes after.

## Prices

Per [platform.claude.com pricing](https://platform.claude.com/docs/en/about-claude/pricing), fetched 2026-08-24. USD per million tokens.

| Model in use | Input | Output | 5m cache write | Cache read |
|---|---|---|---|---|
| `claude-haiku-4-5-20251001` | $1 | $5 | $1.25 | $0.10 |
| `claude-sonnet-4-6` | $3 | $15 | $3.75 | $0.30 |
| `claude-opus-4-6` | $5 | $25 | $6.25 | $0.50 |

`usage.input_tokens` **excludes** cache reads and writes, so the four are summed independently rather than nested.

## File structure

| File | Responsibility |
|---|---|
| `costs.py` (create) | Price table, `cost_usd()`, the `track()` context |
| `tests/test_costs.py` (create) | Arithmetic against the published prices |
| `database.py` (modify) | `ApiUsage` model, `record_api_usage()` |
| `tests/test_api_usage.py` (create) | Recording, attribution, failure isolation |
| `costs.py` | also holds `instrument(client)` |
| `summarizer.py`, `extract_cafr_investments.py`, `extract_cafr_actuarial.py`, `extract_ips.py` (modify) | each `_get_client()` returns an instrumented client |
| `insights/daily.py` (modify) | Route its bare `Anthropic()` through the factory |
| `tests/test_usage_instrumentation.py` (create) | Every call site records, mock records nothing |
| `pipeline.py`, `insights/scheduler.py`, `refresh_cafrs.py` (modify) | Wrap their work in `track()` |
| `app.py` (modify) | A Spend sub-tab in Admin |

---

## Task 1: Pricing arithmetic

**Files:**
- Create: `costs.py`
- Test: `tests/test_costs.py`

**Interfaces:**
- Produces: `PRICES: dict[str, Price]`, `cost_usd(model: str, usage) -> Decimal`, `UnknownModelError`, `track(operation, run_id)`, `current_attribution()`

- [ ] **Step 1: Write the failing tests**

```python
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


def test_a_realistic_summary_costs_a_fraction_of_a_cent():
    """Sanity anchor: ~12.5k input, ~600 output on Haiku."""
    cost = cost_usd(HAIKU, _usage(input_tokens=12_500, output_tokens=600))
    assert Decimal("0.0001") < cost < Decimal("0.01"), cost


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
    import summarizer
    used = {summarizer.MODEL_HAIKU, summarizer.MODEL_SONNET}
    import generate_notes
    used.add(generate_notes.MODEL_OPUS)
    assert used <= set(PRICES), used - set(PRICES)
```

- [ ] **Step 2: Run them and watch them fail**

Run: `LLM_MODE=mock python -m pytest tests/test_costs.py -q`
Expected: FAIL, `ModuleNotFoundError: No module named 'costs'`

- [ ] **Step 3: Implement `costs.py`**

```python
"""What a Claude call costs, and which job made it.

Kept separate from database.py so the arithmetic is testable without a
session, and separate from summarizer.py so the insights package can use it
without importing the document summariser.

Prices are USD per million tokens, verified 2026-08-24 at
https://platform.claude.com/docs/en/about-claude/pricing
"""

from __future__ import annotations

import contextvars
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal


class UnknownModelError(Exception):
    """A model with no price. Raised rather than costed at zero, because a
    silent zero makes spend appear to *fall* when a model is upgraded."""


@dataclass(frozen=True)
class Price:
    input: Decimal
    output: Decimal
    cache_write_5m: Decimal
    cache_read: Decimal


def _p(i, o, w, r) -> Price:
    return Price(Decimal(i), Decimal(o), Decimal(w), Decimal(r))


PRICES: dict[str, Price] = {
    "claude-haiku-4-5-20251001": _p("1", "5", "1.25", "0.10"),
    "claude-sonnet-4-6":         _p("3", "15", "3.75", "0.30"),
    "claude-opus-4-6":           _p("5", "25", "6.25", "0.50"),
}

MILLION = Decimal(1_000_000)


def cost_usd(model: str, usage) -> Decimal:
    """Cost of one call from its usage block.

    The four token categories are summed independently: the API reports
    input_tokens *excluding* cache reads and writes, so treating cache tokens
    as a subset would under-count every cached call.
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


# --- attribution -----------------------------------------------------------

_OPERATION: contextvars.ContextVar[str] = contextvars.ContextVar(
    "claude_operation", default="unattributed")
_RUN_ID: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "claude_run_id", default=None)


@contextmanager
def track(operation: str, run_id: str | None = None):
    """Label every Claude call made inside this block.

    A contextvar rather than an argument: there are thirteen call sites across
    six modules, and threading a label through all of them would touch far
    more code than the measurement is worth — and would be silently forgotten
    at the fourteenth.
    """
    op_token = _OPERATION.set(operation)
    run_token = _RUN_ID.set(run_id)
    try:
        yield
    finally:
        _OPERATION.reset(op_token)
        _RUN_ID.reset(run_token)


def current_attribution() -> tuple[str, str | None]:
    return _OPERATION.get(), _RUN_ID.get()
```

- [ ] **Step 4: Run the tests**

Run: `LLM_MODE=mock python -m pytest tests/test_costs.py -q`
Expected: PASS (9 tests)

- [ ] **Step 5: Commit**

```bash
git add costs.py tests/test_costs.py
git commit -m "Add costs.py: per-call pricing and job attribution"
```

---

## Task 2: The `api_usage` table

**Files:**
- Modify: `database.py`
- Test: `tests/test_api_usage.py`

**Interfaces:**
- Consumes: `costs.cost_usd`, `costs.current_attribution`
- Produces: `ApiUsage` model, `record_api_usage(model, usage) -> ApiUsage | None`

- [ ] **Step 1: Write the failing tests**

```python
"""Usage rows, and the rule that recording never breaks a job.

A lost usage row is a lost measurement. A summary lost because the
measurement failed is a lost day of work, so every failure here is swallowed
and logged.
"""

from __future__ import annotations

import logging
import types
from decimal import Decimal

import pytest

import costs
import database
from database import get_session


def _usage(i=1000, o=100, cw=0, cr=0):
    return types.SimpleNamespace(
        input_tokens=i, output_tokens=o,
        cache_creation_input_tokens=cw, cache_read_input_tokens=cr)


def test_a_call_is_recorded_with_its_cost(tmp_db):
    row = database.record_api_usage("claude-haiku-4-5-20251001", _usage())
    assert row is not None
    assert row.input_tokens == 1000 and row.output_tokens == 100
    assert Decimal(str(row.cost_usd)) > 0


def test_the_operation_label_is_taken_from_the_context(tmp_db):
    with costs.track("summarize", run_id="run-7"):
        row = database.record_api_usage("claude-haiku-4-5-20251001", _usage())
    assert row.operation == "summarize"
    assert row.run_id == "run-7"


def test_calls_outside_a_track_block_are_still_recorded(tmp_db):
    """Unattributed spend is the spend most worth seeing — it is the call
    nobody remembered to label."""
    row = database.record_api_usage("claude-haiku-4-5-20251001", _usage())
    assert row.operation == "unattributed"


def test_an_unknown_model_does_not_break_the_caller(tmp_db, caplog):
    """A model bump must not take the pipeline down for want of a price."""
    with caplog.at_level(logging.WARNING, logger="database"):
        row = database.record_api_usage("claude-future-9", _usage())
    assert row is None
    assert "no price" in caplog.text.lower()


def test_a_database_failure_does_not_break_the_caller(tmp_db, monkeypatch,
                                                      caplog):
    """The whole point: measurement is subordinate to the work."""
    def boom(*a, **k):
        raise RuntimeError("database on fire")

    monkeypatch.setattr(database, "get_session", boom)
    with caplog.at_level(logging.WARNING, logger="database"):
        assert database.record_api_usage(
            "claude-haiku-4-5-20251001", _usage()) is None
    assert "database on fire" in caplog.text


def test_cache_tokens_are_stored_separately(tmp_db):
    """So a later question — 'is caching paying for itself?' — is answerable."""
    row = database.record_api_usage("claude-sonnet-4-6", _usage(cw=5000, cr=20000))
    assert row.cache_write_tokens == 5000
    assert row.cache_read_tokens == 20000


def test_occurred_at_is_aware_utc(tmp_db):
    row = database.record_api_usage("claude-haiku-4-5-20251001", _usage())
    assert row.occurred_at.tzinfo is not None
```

- [ ] **Step 2: Run and watch fail**

Expected: `AttributeError: module 'database' has no attribute 'record_api_usage'`

- [ ] **Step 3: Implement — add to `database.py`**

```python
class ApiUsage(Base):
    """One row per Claude API call: tokens, cost, and which job made it.

    Exists because `message.usage` was discarded on every call, so the only
    answer to "where does the money go" was a guess. Rows are cheap — a few
    hundred a day at ~100 bytes.
    """

    __tablename__ = "api_usage"

    id = Column(Integer, primary_key=True, autoincrement=True)
    occurred_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    model = Column(String, nullable=False)
    operation = Column(String, nullable=False)   # "summarize", "cafr_extract", ...
    run_id = Column(String)                      # fetch_runs.id, publication id, ...
    input_tokens = Column(Integer, nullable=False, default=0)
    output_tokens = Column(Integer, nullable=False, default=0)
    cache_write_tokens = Column(Integer, nullable=False, default=0)
    cache_read_tokens = Column(Integer, nullable=False, default=0)
    cost_usd = Column(Numeric(12, 6), nullable=False)

    __table_args__ = (
        Index("ix_api_usage_occurred", "occurred_at"),
        Index("ix_api_usage_operation", "operation", "occurred_at"),
    )


def record_api_usage(model: str, usage) -> "ApiUsage | None":
    """Record one call. Never raises.

    Measurement is subordinate to the work: a lost row costs a data point, an
    exception here would cost the summary, the CAFR extraction, or the day's
    briefing. Returns None on any failure, having logged it.
    """
    import costs
    try:
        cost = costs.cost_usd(model, usage)
        operation, run_id = costs.current_attribution()
        session = get_session()
        try:
            row = ApiUsage(
                model=model, operation=operation, run_id=run_id,
                input_tokens=getattr(usage, "input_tokens", 0) or 0,
                output_tokens=getattr(usage, "output_tokens", 0) or 0,
                cache_write_tokens=getattr(
                    usage, "cache_creation_input_tokens", 0) or 0,
                cache_read_tokens=getattr(
                    usage, "cache_read_input_tokens", 0) or 0,
                cost_usd=cost,
            )
            session.add(row)
            session.commit()
            session.refresh(row)
            return row
        finally:
            session.close()
    except Exception as exc:                      # noqa: BLE001 — see docstring
        logger.warning("API usage not recorded (%s): %s", model, exc)
        return None
```

`Numeric` must be added to the `sqlalchemy` import at `database.py:14`.

- [ ] **Step 4: Run tests; confirm the table creates on Postgres**

```bash
LLM_MODE=mock python -m pytest tests/test_api_usage.py -q
TEST_POSTGRES_URL="$(cat .test_pg_url)" LLM_MODE=mock python -m pytest tests/postgres/ -q
```

- [ ] **Step 5: Commit**

```bash
git add database.py tests/test_api_usage.py
git commit -m "Add the api_usage table"
```

---

## Task 3: Instrument the client

**Files:**
- Modify: `costs.py` (add `instrument`), `summarizer.py`,
  `extract_cafr_investments.py`, `extract_cafr_actuarial.py`, `extract_ips.py`,
  `insights/daily.py:314`
- Test: `tests/test_usage_instrumentation.py`

**Interfaces:**
- Produces: `costs.instrument(client) -> client`

Thirteen call sites across six modules, but only **four** client factories —
and they are not one factory. `summarizer._get_client()` is the one most
modules share, but `extract_cafr_investments`, `extract_cafr_actuarial` and
`extract_ips` each define their own, with duplicated credential logic.

That distinction is the point of this task. Those three are the Sonnet calls
over 100+ page CAFR PDFs — the most likely answer to "where does the money
go" — so instrumenting only the summariser would have measured everything
**except** the thing this plan exists to find. The wrapper therefore lives in
`costs.py` and is applied in all four.

Consolidating the four into one factory is the better end state and is
deliberately not done here: it touches credential handling on the live path,
which does not belong in the same commit as a measurement feature.

- [ ] **Step 1: Write the failing tests**

```python
"""Every Claude call records usage, because the client records it.

Instrumenting the shared client rather than the call sites is the whole
design: there are thirteen `messages.create(` calls across six modules, and
the next one added would not be instrumented if this were per-call-site.
"""

from __future__ import annotations

import pathlib
import re
import types

import pytest

import database
import summarizer


class _FakeMessages:
    def __init__(self):
        self.calls = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return types.SimpleNamespace(
            content=[types.SimpleNamespace(text="{}")],
            stop_reason="end_turn",
            usage=types.SimpleNamespace(
                input_tokens=1234, output_tokens=56,
                cache_creation_input_tokens=0, cache_read_input_tokens=0))


class _FakeClient:
    def __init__(self):
        self.messages = _FakeMessages()


def test_a_call_through_the_wrapper_records_usage(tmp_db, monkeypatch):
    fake = _FakeClient()
    monkeypatch.setattr(summarizer, "_client", None)
    monkeypatch.setattr(summarizer, "_build_client", lambda: fake)

    client = summarizer._get_client()
    client.messages.create(model="claude-haiku-4-5-20251001",
                           max_tokens=10, messages=[])

    rows = database.get_session().query(database.ApiUsage).all()
    assert len(rows) == 1
    assert rows[0].input_tokens == 1234 and rows[0].output_tokens == 56


def test_the_response_is_returned_unchanged(tmp_db, monkeypatch):
    """The wrapper must be transparent — callers read .content[0].text."""
    fake = _FakeClient()
    monkeypatch.setattr(summarizer, "_client", None)
    monkeypatch.setattr(summarizer, "_build_client", lambda: fake)

    msg = summarizer._get_client().messages.create(
        model="claude-haiku-4-5-20251001", max_tokens=10, messages=[])
    assert msg.content[0].text == "{}"
    assert msg.usage.input_tokens == 1234


def test_arguments_pass_through_untouched(tmp_db, monkeypatch):
    fake = _FakeClient()
    monkeypatch.setattr(summarizer, "_client", None)
    monkeypatch.setattr(summarizer, "_build_client", lambda: fake)

    summarizer._get_client().messages.create(
        model="claude-sonnet-4-6", max_tokens=99, system="sys", messages=[])
    assert fake.messages.calls[0]["max_tokens"] == 99
    assert fake.messages.calls[0]["system"] == "sys"


def test_a_recording_failure_does_not_break_the_call(tmp_db, monkeypatch):
    """The work survives a broken measurement."""
    fake = _FakeClient()
    monkeypatch.setattr(summarizer, "_client", None)
    monkeypatch.setattr(summarizer, "_build_client", lambda: fake)
    monkeypatch.setattr(database, "record_api_usage",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("x")))

    msg = summarizer._get_client().messages.create(
        model="claude-haiku-4-5-20251001", max_tokens=10, messages=[])
    assert msg.content[0].text == "{}"


# The four modules that build their own Anthropic client. Every one must
# instrument it, or its spend is invisible — and three of them are the
# expensive ones.
_CLIENT_FACTORIES = (
    "summarizer.py",
    "extract_cafr_investments.py",
    "extract_cafr_actuarial.py",
    "extract_ips.py",
)


@pytest.mark.parametrize("module", _CLIENT_FACTORIES)
def test_every_client_factory_instruments_its_client(module):
    """Instrumenting only the summariser would miss the CAFR extractors.

    They define their own _get_client with duplicated credential logic, and
    they are the Sonnet-over-100-page-PDF calls this plan exists to measure.
    """
    root = pathlib.Path(__file__).resolve().parents[1]
    src = (root / module).read_text(encoding="utf-8")
    assert "def _get_client" in src, "%s no longer builds a client" % module
    assert "instrument(" in src, (
        "%s builds an Anthropic client without costs.instrument() — its spend "
        "would not be recorded" % module)


def test_no_new_module_builds_an_uninstrumented_client():
    """Catches the fifth factory before it is written."""
    root = pathlib.Path(__file__).resolve().parents[1]
    offenders = []
    for path in root.rglob("*.py"):
        rel = path.relative_to(root).as_posix()
        if any(rel.startswith(p) for p in
               (".venv/", ".claude/", "tests/", "scripts/")):
            continue
        if rel in _CLIENT_FACTORIES:
            continue
        if re.search(r"\bAnthropic\(",
                     path.read_text(encoding="utf-8", errors="replace")):
            offenders.append(rel)
    assert offenders == [], (
        "these construct an Anthropic client directly, so their spend is "
        "invisible — use an instrumented factory: %s" % offenders)
```

- [ ] **Step 2: Run and watch fail**

Expected: FAIL — `_build_client` does not exist, and `insights/daily.py` is an offender.

- [ ] **Step 3: Implement**

Put the wrapper in `costs.py`, so all four factories reach it without
importing the summariser. In `summarizer.py`, split the existing `_get_client`
body into `_build_client()` (unchanged credential logic, returning a raw
`anthropic.Anthropic`). In each of the three extractors, wrap the client where
it is assigned to their `_client` global — `_client = costs.instrument(...)`.

In `costs.py`:

```python
class _RecordingMessages:
    """messages.create + a usage row. Transparent to the caller."""

    def __init__(self, inner):
        self._inner = inner

    def create(self, **kwargs):
        message = self._inner.create(**kwargs)
        try:
            usage = getattr(message, "usage", None)
            if usage is not None and not _mock_mode():
                import database
                database.record_api_usage(kwargs.get("model", "unknown"), usage)
        except Exception:                      # noqa: BLE001
            # Never let measurement break the work. record_api_usage already
            # swallows its own failures; this catches anything above it.
            pass
        return message

    def __getattr__(self, name):
        return getattr(self._inner, name)


class _RecordingClient:
    def __init__(self, inner):
        self._inner = inner
        self.messages = _RecordingMessages(inner.messages)

    def __getattr__(self, name):
        return getattr(self._inner, name)


def _get_client():
    """The shared client, instrumented.

    Every Claude call in the project goes through here so that spend is
    recorded in one place rather than at thirteen call sites — and so the
    fourteenth is recorded without anyone remembering to.
    """
    global _client
    if _client is None:
        _client = _RecordingClient(_build_client())
    return _client
```

`_mock_mode()` returns True when `LLM_MODE` or `INSIGHTS_MODE` is `mock`.

Then change `insights/daily.py:314` from `client = Anthropic()` to
`from summarizer import _get_client; client = _get_client()`.

- [ ] **Step 4: Run the tests, then the full suite**

```bash
LLM_MODE=mock python -m pytest tests/test_usage_instrumentation.py -q
LLM_MODE=mock INSIGHTS_MODE=mock python -m pytest tests/ -q --ignore=tests/postgres
```

- [ ] **Step 5: Commit**

```bash
git add summarizer.py insights/daily.py tests/test_usage_instrumentation.py
git commit -m "Record usage in the shared client, not at each call site"
```

---

## Task 4: Attribute spend to jobs

**Files:**
- Modify: `pipeline.py`, `refresh_cafrs.py`, `insights/scheduler.py`, `extractor.py`
- Test: `tests/test_usage_instrumentation.py` (extend)

Without this every row reads `unattributed`, which answers "how much" but not
"on what" — and "on what" is the question this whole plan exists to answer.

- [ ] **Step 1: Wrap each entry point**

| Module | Block | `operation` | `run_id` |
|---|---|---|---|
| `pipeline.py` | around the summarize step | `"summarize"` | `str(fetch_run_id)` |
| `extractor.py` | around the vision-OCR call | `"ocr"` | `str(doc.id)` |
| `refresh_cafrs.py` | around the extraction pass | `"cafr_extract"` | fiscal year |
| `insights/scheduler.py` | around each cadence | `"insights:<cadence>"` | period start |

Each is `with costs.track(...):` around the existing call — no other change.

- [ ] **Step 2: Add the coverage test**

```python
@pytest.mark.parametrize("module,label", [
    ("pipeline", "summarize"),
    ("extractor", "ocr"),
    ("refresh_cafrs", "cafr_extract"),
    ("insights/scheduler", "insights:"),
])
def test_each_spending_entry_point_labels_its_calls(module, label):
    """Unlabelled spend answers 'how much' but not 'on what'."""
    import pathlib
    root = pathlib.Path(__file__).resolve().parents[1]
    src = (root / (module + ".py")).read_text(encoding="utf-8")
    assert "costs.track(" in src or "from costs import track" in src, module
    assert label in src, "%s does not use the %r label" % (module, label)
```

- [ ] **Step 3: Run the suite and commit**

```bash
LLM_MODE=mock INSIGHTS_MODE=mock python -m pytest tests/ -q --ignore=tests/postgres
git add pipeline.py extractor.py refresh_cafrs.py insights/scheduler.py tests/test_usage_instrumentation.py
git commit -m "Attribute Claude spend to the job that incurred it"
```

---

## Task 5: Surface it

**Files:**
- Modify: `queries.py`, `app.py`
- Test: `tests/test_queries.py` (extend)

- [ ] **Step 1: Add the read**

```python
def api_spend_by_operation(session, days: int = 30) -> list[tuple]:
    """(operation, calls, input_tokens, output_tokens, cost) newest window."""
    cutoff = utcnow() - timedelta(days=days)
    return (
        session.query(
            ApiUsage.operation,
            func.count(ApiUsage.id),
            func.sum(ApiUsage.input_tokens),
            func.sum(ApiUsage.output_tokens),
            func.sum(ApiUsage.cost_usd),
        )
        .filter(ApiUsage.occurred_at >= cutoff)
        .group_by(ApiUsage.operation)
        .order_by(func.sum(ApiUsage.cost_usd).desc())
        .all()
    )
```

Add a matching case to `scripts/compare_backends.py`'s `CASES` — the coverage
test there fails otherwise, by design.

- [ ] **Step 2: Add a `Spend` sub-tab to `page_admin`**

Total for 30 days, the per-operation table above, and a per-model breakdown.
Follow the existing sub-tab pattern.

- [ ] **Step 3: Run, commit**

```bash
LLM_MODE=mock INSIGHTS_MODE=mock python -m pytest tests/ -q --ignore=tests/postgres
git add queries.py app.py scripts/compare_backends.py tests/test_queries.py
git commit -m "Surface API spend in the Admin tab"
```

---

## Verification summary

| Claim | Evidence |
|---|---|
| The arithmetic matches the published prices | Task 1 — a million tokens costs the headline figure, per model |
| Cache tokens are not double- or under-counted | Task 1 — the four categories sum independently |
| Measurement never breaks a job | Tasks 2 and 3 — unknown model, dead database, and a raising recorder all leave the call intact |
| Every call site is covered | Task 3 — all **four** client factories wrapped, plus a guard against a fifth |
| Spend is attributable | Task 4 — each entry point labelled, asserted in source |
| Mock runs record nothing | Task 3 — `_mock_mode()` short-circuits |

## What this plan deliberately does not do

- **Set a per-run cap.** That is control 2, and it should be sized from a week of this data rather than picked now.
- **Add prompt caching to the summariser.** Measured at 62 tokens against a 2,048-token floor — it would create no cache entries. The spec should be corrected rather than implemented.
- **Optimise anything.** The point is to find out what to optimise. My expectation is vision OCR, and the plan is worth running precisely because that is a guess.
- **Backfill historical spend.** The token counts were never recorded and cannot be reconstructed.
