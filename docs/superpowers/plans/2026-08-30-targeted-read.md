# Targeted Read Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Read the part of a document that holds the numbers, instead of the first tenth of it.

**Architecture:** Two phases against one idea. Phase A covers documents whose text we already hold — the large majority — where locating a section is a free text search and the only paid step is reading the slice we chose. Phase B covers scanned documents, where the same idea needs OCR and therefore needs a page offset resolved first.

**Tech Stack:** Python, SQLAlchemy, **DeepSeek V4 Flash via OpenRouter** (OpenAI-compatible tool calling), `costs` for pricing, PyMuPDF for Phase B.

**Why not Anthropic here.** This step is mechanical: a schema-constrained read of one 30,000-character window. DeepSeek V4 Flash costs **$0.0886/M input and $0.1772/M output** against Haiku 4.5's $1.00/$5.00 — about a twentieth, which takes the full run from **$9.30 to $0.41**. (Prices read from OpenRouter's live `/models` catalogue on 2026-08-30; an earlier draft of this plan quoted $0.068/$0.168, which was stale.) It supports `tools`/`tool_choice` and JSON-schema structured output, which the design depends on, confirmed against the live catalogue's `supported_parameters`. Routing asks for `provider.require_parameters` so only providers that actually implement those parameters are used. (An earlier draft asked for `route: "exacto"`; OpenRouter rejects it — `route` accepts only `"fallback"`, and the catalogue has no exacto variant of this model.)

Deliberately scoped to this extractor. Summarising stays on Anthropic: that text feeds the briefings people read, and swapping it is a quality decision needing its own comparison, not a cost decision.

**Spec:** `docs/superpowers/specs/2026-08-30-relevance-gating-design.md`

## Why this was rewritten

The first version of this plan covered only the 354 scanned documents. Chasing a different question — why eleven plans have documents but no performance figures — showed that scoping was wrong, and wrong about the bigger number.

Those eleven plans mostly do not publish performance material at all: Nashville's meeting pages offer one document each, the agenda; Atlanta's ten "board packs" are one-page meeting notices misfiled by `guess_doc_type`; across their 641 documents only ten run past twenty pages. No scraper fixes that.

But those ten long documents *are* held, extracted and summarised, and still yield nothing. The summariser compresses every document to ~50,000 characters before Claude sees it (`summarizer.SMART_TRUNCATE_TARGET`). That is not naive — head, investment-keyword windows, tail — but it fills its budget from the front, and an allocation table is a dense numeric grid that sits deep. Measured on a real board pack, the genuine performance headings begin **31% of the way in**.

This is not a ten-document problem:

| | |
|---|---|
| Documents with stored text | 4,926 |
| **Truncated before summarising (>50k chars)** | **810** |
| Heavily truncated (>250k chars) | 79 |
| **Plans affected** | **136 of 148** |

So the ceiling on performance coverage is not fetching, and not the scanned tail. It is that a fifth of the corpus is read in part, and the part is chosen to write a good summary rather than to find a table.

## Global Constraints

- **Nothing is discarded.** No task writes to `documents.extracted_text` or deletes any row. Extracted figures go to their own table; a test asserts `extracted_text` is byte-identical after a run.
- **No paid call is reachable without `--approve`.** Follow `scripts/catalogue.py`: the client constructor is never entered on an unapproved path, and the test asserts *that*, not that cost came out zero.
- **`--budget` is a hard stop**, checked before each call against spend so far.
- **`OPENROUTER_API_KEY` lives in `.env` (gitignored) and nowhere else.** This is a local one-off backfill over documents that already exist — no workflow runs it, and CI never needs the key because the tests monkeypatch `_raw_call`. Do not add it to GitHub Actions secrets; if a later cadence ever calls this path, that is when it earns one. `llm_openrouter` raises if the variable is unset; it must never fall back to the Anthropic key or to an unauthenticated call.
- **Every paid call is recorded** via `database.record_api_usage`, the same as every other spending path in this repo. A run that produces rows but no `api_usage` entries is a bug, not a saving.
- **Never write an ALTER TABLE migration system.** Add the model, run `init_db()`.
- **`documents.extracted_text` is `deferred()`** — bulk readers need `.options(undefer(...))`; never loop reading it without one.
- Baseline: `LLM_MODE=mock pytest tests/ -q` green at 651 passed / 30 skipped.

## Measured inputs (2026-08-30, live corpus)

Section search over the four largest stored documents:

| Document | Chars | Heading hits | First genuine hit |
|---|---|---|---|
| `inv-202412.pdf` | 641,965 | 26 | **31% in** |
| `brd-202411p.pdf` | 1,341,713 | 39 | 1% (prose, not a table) |
| Rhode Island pack | 1,158,684 | 99 | 2% (prose) |
| `Gov%202025-04%20V2_0.pdf` | **2,000,000** | **0** | — capped at `MAX_STORED_CHARS` |

Two things follow, and they shape Tasks 1 and 2. Hits are **noisy** — most are prose mentions ("provide holistic asset allocation"), not table headings — so a raw first-match rule would read the wrong slice. And a document sitting at exactly 2,000,000 characters was truncated at *storage*, so its tail is not on disk at all; those are a Phase B/R2 problem, not this one.

## File Structure

- `section_finder.py` (new) — pure text search producing ranked candidate windows. No I/O, no API.
- `database.py` (modify) — add `DocumentSectionRead`.
- `llm_openrouter.py` (new) — a metered OpenRouter client. Its own module because everything else here talks to Anthropic, and the two differ in both call shape and usage accounting.
- `costs.py` (modify) — a `PRICES` entry for the DeepSeek model.
- `requirements-pipeline.txt` (modify) — add `openai` (not currently a dependency).
- `targeted_extract.py` (new) — the only module that spends: rank candidates, then extract from the chosen window.
- `scripts/read_sections.py` (new) — CLI: priced worklist, `--approve`, `--budget`.
- `tests/test_section_finder.py`, `tests/test_targeted_extract.py` (new).
- Phase B (`page_hints.py`, `targeted_read.py`) — deferred; see Task 7.

---

### Task 1: Find candidate sections in stored text

**Files:**
- Create: `section_finder.py`
- Test: `tests/test_section_finder.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `find_candidates(text: str, max_candidates: int = 12) -> list[Candidate]`, where `Candidate` is a frozen dataclass `(offset: int, heading: str, score: float)`; and the module constant `WINDOW = 30_000`, the slice length a candidate's offset opens. Tasks 4 and 5 consume both.

- [ ] **Step 1: Write the failing test**

```python
from section_finder import find_candidates, Candidate

def test_finds_a_heading_on_its_own_line():
    text = "preamble\n" * 100 + "\nASSET ALLOCATION\n" + "table rows\n" * 50
    got = find_candidates(text)
    assert got and "ASSET ALLOCATION" in got[0].heading

def test_prose_mentions_rank_below_headings():
    """The measured failure mode: 99 hits on a real pack, nearly all prose.

    'we provide holistic asset allocation advice' is a sentence; 'Asset
    Allocation' alone on a line is a heading. Taking the first match reads
    the wrong slice of a 1.1 MB document.
    """
    text = ("The consultant will provide holistic asset allocation advice "
            "to the board in due course.\n") * 20 + "\nAsset Allocation\n" + "x\n" * 40
    got = find_candidates(text)
    assert "Asset Allocation" == got[0].heading.strip()

def test_numeric_density_lifts_a_real_table():
    """A heading followed by numbers beats one followed by prose."""
    prose = "\nRates of Return\n" + "discussion of philosophy\n" * 30
    table = "\nRates of Return\n" + "Domestic Equity 12.4 11.8 9.2\n" * 30
    got = find_candidates(prose + table)
    assert got[0].offset > len(prose) - 1

def test_returns_nothing_when_there_is_nothing():
    assert find_candidates("minutes of a routine meeting\n" * 200) == []

def test_caps_the_number_of_candidates():
    text = ("\nAsset Allocation\n" + "1.0 2.0 3.0\n" * 5) * 40
    assert len(find_candidates(text, max_candidates=6)) <= 6
```

- [ ] **Step 2: Run it and watch it fail**

Run: `LLM_MODE=mock pytest tests/test_section_finder.py -v`
Expected: FAIL, `ModuleNotFoundError: No module named 'section_finder'`

- [ ] **Step 3: Implement**

```python
"""Locate the parts of a long document that might hold a returns table.

Free: pure text, no API, no I/O. That matters because the alternative --
sending a 1.3 MB document to a model to be told where its tables are --
costs more than reading the tables.

Scoring is the whole content of this module. A naive keyword search over a
real board pack returns 99 hits of which nearly all are prose ("the
consultant will provide holistic asset allocation advice"). Reading the
first match reads the wrong slice.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

WINDOW = 30_000          # chars handed to the extractor around a hit
_HEADINGS = re.compile(
    r"(?im)^(.{0,80}?)(asset allocation|total fund performance"
    r"|performance summary|investment performance|rates? of return"
    r"|manager performance|portfolio performance)(.{0,60})$")
_NUMBER = re.compile(r"-?\d+\.\d")


@dataclass(frozen=True)
class Candidate:
    offset: int
    heading: str
    score: float


def _numeric_density(text: str) -> float:
    """Fraction of lines that look like table rows."""
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        return 0.0
    return sum(1 for ln in lines if len(_NUMBER.findall(ln)) >= 2) / len(lines)


def find_candidates(text: str, max_candidates: int = 12) -> list[Candidate]:
    if not text:
        return []
    scored: list[Candidate] = []
    for m in _HEADINGS.finditer(text):
        before, term, after = m.group(1), m.group(2), m.group(3)
        line = m.group(0).strip()

        # A heading is short and mostly the term itself. A sentence that
        # happens to contain the term is long and has words either side.
        clutter = len(before.strip()) + len(after.strip())
        score = 1.0 if clutter <= 4 else 1.0 / (1 + clutter / 10)

        # Numbers just below it are the strongest signal that this is a table.
        following = text[m.end():m.end() + 4_000]
        score += 2.0 * _numeric_density(following)

        scored.append(Candidate(offset=m.start(), heading=line, score=score))

    scored.sort(key=lambda c: (-c.score, c.offset))
    # Drop near-duplicates: one table produces several adjacent headings.
    kept: list[Candidate] = []
    for c in scored:
        if any(abs(c.offset - k.offset) < WINDOW // 2 for k in kept):
            continue
        kept.append(c)
        if len(kept) >= max_candidates:
            break
    return [c for c in kept if c.score >= 0.5]


def window_for(text: str, candidate: Candidate) -> str:
    """The slice to hand a model: the heading and what follows it."""
    start = max(0, candidate.offset - 500)
    return text[start:start + WINDOW]
```

- [ ] **Step 4: Run the tests**

Run: `LLM_MODE=mock pytest tests/test_section_finder.py -v`
Expected: PASS

- [ ] **Step 5: Check it against the real documents that motivated it**

```bash
python -c "
import database, section_finder
from database import Document
from sqlalchemy.orm import undefer
from sqlalchemy import text as sql
s = database.SessionLocal()
ids = [r[0] for r in s.execute(sql('''SELECT id FROM documents
  WHERE octet_length(extracted_text) > 46000
  ORDER BY octet_length(extracted_text) DESC LIMIT 5''')).fetchall()]
for did in ids:
    d = s.query(Document).options(undefer(Document.extracted_text)).get(did)
    t = d.extracted_text or ''
    for c in section_finder.find_candidates(t)[:3]:
        print(f'{did} {c.score:.2f} @{100*c.offset//max(len(t),1):>3}%  {c.heading[:60]}')
s.close()"
```

Expected: the top candidate for `inv-202412.pdf` lands near **31% in** (`Asset Allocation, Portfolio Strategy` / `Total Rates of Return (%)`), not on the 1-2% prose mentions. If prose still ranks first, raise the numeric-density weight before continuing — the whole plan depends on this ranking being right.

- [ ] **Step 6: Commit**

```bash
git add section_finder.py tests/test_section_finder.py
git commit -m "Locate returns tables in stored text, free"
```

---

### Task 2: Somewhere to put what a targeted read finds

**Files:**
- Modify: `database.py`
- Test: `tests/test_targeted_extract.py` (schema test only)

**Interfaces:**
- Produces: `DocumentSectionRead(document_id, offset, heading, returns_json, model, cost_usd, created_at)`, unique on `(document_id, offset)`.

- [ ] **Step 1: Write the failing test**

```python
import database
from database import Document, DocumentSectionRead, Plan

def test_section_reads_sit_beside_the_document(tmp_db):
    s = database.get_session()
    s.add(Plan(id="mcera", name="MCERA", state="CA")); s.commit()
    d = Document(plan_id="mcera", url="https://x/a.pdf", filename="a.pdf",
                 extracted_text="the whole pack")
    s.add(d); s.commit()
    before = d.extracted_text

    s.add(DocumentSectionRead(document_id=d.id, offset=200_881,
                              heading="Total Rates of Return (%)",
                              returns_json='[{"asset_class":"US Equity","return_pct":12.4}]'))
    s.commit(); s.expire_all()

    assert s.get(Document, d.id).extracted_text == before
    assert s.query(DocumentSectionRead).one().offset == 200_881
    s.close()
```

- [ ] **Step 2: Run and watch it fail**

Expected: `ImportError: cannot import name 'DocumentSectionRead'`

- [ ] **Step 3: Add the model**

Place after `DocumentCatalogue` in `database.py`:

```python
class DocumentSectionRead(Base):
    """Figures read from one located section of a document.

    Separate from ``summaries.performance_data``, which holds whatever the
    summariser happened to see in the ~50,000 characters it was given. This
    holds what a targeted read of the right slice found, and records which
    slice, so a disagreement between the two is investigable rather than
    mysterious.

    ``offset`` is a character position in ``documents.extracted_text`` at the
    time of the read. Unique with ``document_id`` so a re-run is a no-op
    rather than a second charge for the same passage.
    """

    __tablename__ = "document_section_read"

    id = Column(Integer, primary_key=True)
    document_id = Column(Integer, ForeignKey("documents.id"),
                         nullable=False, index=True)
    offset = Column(Integer, nullable=False)
    heading = Column(String(200))
    returns_json = Column(Text)          # same shape as summaries.performance_data
    model = Column(String(64))
    cost_usd = Column(Numeric(10, 6))
    created_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint("document_id", "offset", name="uq_section_read"),
    )
```

- [ ] **Step 4: Run the test** — Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add database.py tests/test_targeted_extract.py
git commit -m "Store targeted section reads beside the document"
```

---

### Task 3: A metered OpenRouter client

**Files:**
- Create: `llm_openrouter.py`
- Modify: `costs.py` (one `PRICES` entry), `requirements-pipeline.txt`
- Test: `tests/test_openrouter_client.py`

**Interfaces:**
- Produces: `MODEL` (the OpenRouter model id), `call_tool(system, user, schema, tool_name) -> tuple[dict, Decimal]`, and `ResponseTruncated`. Task 4 consumes these.

**The trap this task exists to avoid.** `costs.cost_usd` reads `input_tokens`, `output_tokens`, `cache_creation_input_tokens`, `cache_read_input_tokens` — Anthropic's names — via `getattr(usage, attr, 0)`. OpenAI-shaped responses call them `prompt_tokens` and `completion_tokens`, so every field misses and **every call costs a silent zero**. `costs.py`'s own docstring names this as the one direction of error that goes unquestioned: spend appears to fall. The adapter below exists to make that impossible, and the first test asserts it.

- [ ] **Step 1: Write the failing test**

```python
from decimal import Decimal
import pytest
import costs, llm_openrouter


class _OpenAIUsage:
    """The shape OpenRouter returns — deliberately not Anthropic's."""
    prompt_tokens = 7_500
    completion_tokens = 800


def test_usage_is_translated_not_passed_through():
    """The silent-zero trap. costs.cost_usd reads Anthropic field names, so
    handing it an OpenAI usage block returns 0.00 and the spend vanishes."""
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


def test_truncated_response_raises(monkeypatch):
    class _Msg:
        tool_calls = None
    class _Choice:
        message, finish_reason = _Msg(), "length"
    class _Resp:
        choices, usage = [_Choice()], _OpenAIUsage()

    monkeypatch.setattr(llm_openrouter, "_raw_call", lambda **kw: _Resp())
    with pytest.raises(llm_openrouter.ResponseTruncated):
        llm_openrouter.call_tool("sys", "user", {"type": "object"}, "record")


def test_missing_tool_call_raises_rather_than_returning_empty(monkeypatch):
    class _Msg:
        tool_calls = None
    class _Choice:
        message, finish_reason = _Msg(), "stop"
    class _Resp:
        choices, usage = [_Choice()], _OpenAIUsage()

    monkeypatch.setattr(llm_openrouter, "_raw_call", lambda **kw: _Resp())
    with pytest.raises(RuntimeError):
        llm_openrouter.call_tool("sys", "user", {"type": "object"}, "record")
```

- [ ] **Step 2: Run and watch it fail**

Run: `LLM_MODE=mock pytest tests/test_openrouter_client.py -v`
Expected: FAIL, `ModuleNotFoundError: No module named 'llm_openrouter'`

- [ ] **Step 3: Add the price**

In `costs.py`, alongside the Anthropic entries:

```python
    # OpenRouter, DeepSeek V4 Flash. Both cache columns are zero deliberately.
    # DeepSeek does have a cache-read price (~$0.0177/M), but adapt_usage
    # reports every prompt token as uncached, so each call is priced at the
    # full input rate. That over-states a cached call and never under-states
    # one -- the safe direction, and the opposite of the usage-name mismatch
    # this whole module exists to prevent.
    "deepseek/deepseek-v4-flash": _p("0.0886", "0.1772", "0", "0"),
```

- [ ] **Step 4: Implement the client**

```python
"""DeepSeek V4 Flash through OpenRouter, metered the same way as Anthropic.

Its own module because OpenRouter is OpenAI-shaped and everything else here
is Anthropic-shaped, and the differences are not cosmetic:

  * the tool call arrives as a JSON *string* in
    ``choices[0].message.tool_calls[0].function.arguments``, not as a parsed
    ``tool_use`` block;
  * truncation shows as ``finish_reason == "length"``, not
    ``stop_reason == "max_tokens"``;
  * usage is ``prompt_tokens``/``completion_tokens``, which ``costs.cost_usd``
    does not recognise -- see ``adapt_usage``.

Reads OPENROUTER_API_KEY. Used only for schema-constrained extraction;
summarising stays on Anthropic.
"""
from __future__ import annotations

import json
import os
from decimal import Decimal
from types import SimpleNamespace

import costs
import database

MODEL = "deepseek/deepseek-v4-flash"
BASE_URL = "https://openrouter.ai/api/v1"
MAX_OUTPUT_TOKENS = 16_384


class ResponseTruncated(RuntimeError):
    """finish_reason was 'length'. The arguments JSON is incomplete and must
    not be saved as if it were a result."""


def adapt_usage(usage):
    """OpenAI token names -> the names costs.cost_usd reads.

    Without this every call costs 0.00 and the spend silently disappears.
    """
    return SimpleNamespace(
        input_tokens=getattr(usage, "prompt_tokens", 0) or 0,
        output_tokens=getattr(usage, "completion_tokens", 0) or 0,
        cache_creation_input_tokens=0,
        cache_read_input_tokens=0,
    )


def _raw_call(**kwargs):
    from openai import OpenAI
    key = os.environ.get("OPENROUTER_API_KEY")
    if not key:
        raise RuntimeError("OPENROUTER_API_KEY not set")
    client = OpenAI(api_key=key, base_url=BASE_URL)
    return client.chat.completions.create(**kwargs)


def call_tool(system: str, user: str, schema: dict,
              tool_name: str) -> tuple[dict, Decimal]:
    resp = _raw_call(
        model=MODEL,
        max_tokens=MAX_OUTPUT_TOKENS,
        messages=[{"role": "system", "content": system},
                  {"role": "user", "content": user}],
        tools=[{"type": "function",
                "function": {"name": tool_name, "parameters": schema}}],
        tool_choice={"type": "function", "function": {"name": tool_name}},
        # The whole design rests on the tool call being well-formed, so route
        # only to providers that actually implement the parameters sent --
        # without this, OpenRouter may fall back to one that ignores `tools`
        # and returns prose, which arrives here as "no tool call".
        extra_body={"provider": {"require_parameters": True}},
    )
    choice = resp.choices[0]
    usage = adapt_usage(resp.usage)
    cost = costs.cost_usd(MODEL, usage)

    # Record before raising. A truncated call is still a billed call, and the
    # failures are exactly the ones worth seeing in api_usage afterwards.
    if not costs.mock_mode():
        database.record_api_usage(MODEL, usage)

    if choice.finish_reason == "length":
        raise ResponseTruncated(
            f"in={usage.input_tokens} out={usage.output_tokens}")
    calls = getattr(choice.message, "tool_calls", None)
    if not calls:
        raise RuntimeError(
            f"no tool call; finish_reason={choice.finish_reason}")
    return json.loads(calls[0].function.arguments), cost
```

`record_api_usage` puts this spend in `api_usage` alongside every other paid path, which is what keeps `scripts/pending_spend.py` honest. The `costs.mock_mode()` guard mirrors `costs._RecordingMessages`. Import `database` at module top beside `costs`.

Add `openai` to `requirements-pipeline.txt`, **not** `requirements.txt` — the Streamlit service never calls a model, and `moto` being in the wrong file was the same mistake earlier on this branch.

- [ ] **Step 5: Run the tests** — Expected: PASS

- [ ] **Step 6: One real call, to prove the wiring** — *ask before running it*

This is the first paid call in the plan. It is about **$0.000002**, but it is real, and the standing rule on this project is that spending is authorised explicitly rather than inferred from the size of the number. Ask, then run.

```bash
python -c "
import llm_openrouter as m
data, cost = m.call_tool(
  'Extract the returns.',
  'Total Fund 8.4%\nUS Equity 12.1%\nReal Estate -3.2%',
  {'type':'object','properties':{'returns':{'type':'array','items':{
     'type':'object','properties':{'asset_class':{'type':'string'},
     'return_pct':{'type':'number'}}}}},'required':['returns']},
  'record_returns')
print(data); print('cost \$', cost)"
```

Expected: three rows, and a cost **greater than zero** — a zero here means `adapt_usage` is not on the path, which is the whole point of the task.

- [ ] **Step 7: Commit**

```bash
git add llm_openrouter.py costs.py requirements-pipeline.txt tests/test_openrouter_client.py
git commit -m "Metered OpenRouter client for schema-constrained extraction"
```

---

### Task 4: Extract returns from a chosen window

**Files:**
- Create: `targeted_extract.py`
- Test: `tests/test_targeted_extract.py` (extend)

**Interfaces:**
- Consumes: `section_finder.Candidate` and `section_finder.WINDOW` (Task 1), `llm_openrouter.call_tool` and `ResponseTruncated` (Task 3).
- Produces: `extract_window(text: str, candidate: Candidate) -> tuple[dict, Decimal]`, re-exporting `ResponseTruncated`. Task 5 consumes both, and writes the `DocumentSectionRead` row from Task 2 — this module does no I/O.

All model mechanics live in `llm_openrouter` — this module builds the prompt and the schema and interprets the result. Do **not** reach for `summarizer._get_client` here; that is the Anthropic path and it is deliberately not used for this step.

The tool schema mirrors `extract_performance_reports.py`'s: a list of `{asset_class, return_pct, period, benchmark_pct}` — the same shape as `summaries.performance_data`, so `scripts/build_performance_view.py` consumes it with no new parser.

**The scar this carries.** `extract_performance_reports.py` ran with `MAX_OUTPUT_TOKENS = 4096`, which silently cut every tool call short: thirty documents saved with zero rows, no error, discovered only by reading raw output. Task 3's client raises `ResponseTruncated` rather than returning a partial payload; this module must let that propagate, never catch it and write an empty result.

- [ ] **Step 1: Write the failing test**

```python
from decimal import Decimal
import targeted_extract
from section_finder import Candidate

def test_returns_parsed_rows_and_cost(monkeypatch):
    monkeypatch.setattr(targeted_extract, "_call_model", lambda w: (
        {"returns": [{"asset_class": "US Equity", "return_pct": 12.4,
                      "period": "FY2026"}]}, Decimal("0.004")))
    data, cost = targeted_extract.extract_window(
        "…", Candidate(200_881, "Total Rates of Return (%)", 2.4))
    assert data["returns"][0]["asset_class"] == "US Equity"
    assert cost == Decimal("0.004")

def test_a_truncated_response_raises_rather_than_saving_nothing(monkeypatch):
    """extract_performance_reports saved 30 documents with zero rows and no
    error when max_tokens cut the tool call short. A silent empty result is
    worse than a failure, because nobody re-runs it."""
    def truncated(window):
        raise targeted_extract.ResponseTruncated("in=58236 out=4096")
    monkeypatch.setattr(targeted_extract, "_call_model", truncated)
    import pytest
    with pytest.raises(targeted_extract.ResponseTruncated):
        targeted_extract.extract_window("…", Candidate(0, "x", 1.0))
```

- [ ] **Step 2: Run and watch it fail**

- [ ] **Step 3: Implement**

Mirror `extract_performance_reports.py`'s prompt and schema, but not its client. Required elements:

```python
from decimal import Decimal

from llm_openrouter import call_tool, ResponseTruncated  # re-exported for callers
from section_finder import WINDOW

TOOL_NAME = "record_returns"

SYSTEM = (
    "You are reading one excerpt from a public pension fund board document. "
    "Record every asset-class return you can see in the excerpt. Copy the "
    "numbers exactly as printed; do not compute, convert or infer any figure "
    "that is not there. If the excerpt holds no returns table, record none."
)

RETURNS_SCHEMA = {
    "type": "object",
    "properties": {
        "returns": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "asset_class": {"type": "string"},
                    "return_pct": {"type": "number"},
                    "period": {
                        "type": "string",
                        "description": "as printed, e.g. 'FY2026', 'Q1 2026', '3 Year'",
                    },
                    "benchmark_pct": {"type": "number"},
                },
                "required": ["asset_class", "return_pct", "period"],
            },
        }
    },
    "required": ["returns"],
}


def _call_model(window: str) -> tuple[dict, Decimal]:
    """One seam, so the tests above can replace the only paid call."""
    return call_tool(SYSTEM, window, RETURNS_SCHEMA, TOOL_NAME)


def extract_window(text: str, candidate) -> tuple[dict, Decimal]:
    return _call_model(text[candidate.offset:candidate.offset + WINDOW])
```

`MODEL`, `MAX_OUTPUT_TOKENS` and the truncation check all belong to `llm_openrouter` (Task 3). Do not redeclare them here — a second copy of the token cap is exactly how `extract_performance_reports.py` came to run at 4096 without anyone noticing.

The row shape — `{asset_class, return_pct, period, benchmark_pct}` — is the same shape as `summaries.performance_data`, so `scripts/build_performance_view.py` consumes it with no new parser. `benchmark_pct` is deliberately optional: most tables print it, some do not, and a required field would push the model into inventing one.

- [ ] **Step 4: Run the tests** — Expected: PASS

- [ ] **Step 5: Commit**

---

### Task 5: The CLI — priced worklist, approval, hard budget

**Files:**
- Create: `scripts/read_sections.py`
- Test: `tests/test_targeted_extract.py` (extend)

Model it on `scripts/catalogue.py`, which is the reference for the no-spend contract.

Behaviour:
- Selects documents whose stored text exceeds `summarizer.SMART_TRUNCATE_TARGET` and that have no `DocumentSectionRead` row.
- Orders newest-first by `meeting_date`, nulls last.
- Runs `find_candidates` (free) and reports: documents, candidate windows, estimated cost, and separately the count with **no** candidates — those are reported, never guessed at.
- Without `--approve`: prints the worklist, prints `Nothing spent.`, returns 0, constructs no client.
- With `--approve`: extracts the top candidate per document, writes a `DocumentSectionRead`, commits per document, checks `--budget` before each call.
- `--top N` to read more than one window per document (default 1).

- [ ] **Step 1: Write the failing tests** — mirror `tests/test_catalogue.py`: no-client-on-unapproved-path; budget is a hard stop; already-read documents are skipped; `extracted_text` unchanged after a run.

- [ ] **Step 2: Run and watch them fail**

- [ ] **Step 3: Implement**

- [ ] **Step 4: Run the full suite** — Expected: PASS, ~670 passed (651 at baseline plus the roughly twenty added across Tasks 1-5)

- [ ] **Step 5: Price the real run**

Run: `python -m scripts.read_sections`

Measured on the live corpus: **810** documents exceed the character limit, of which **510** have a candidate section and **300** have none. 510 windows come to **$0.41** at top-1, or **$0.77** for 958 windows at top-3. The same work on Haiku 4.5 would have been $9.30.

The 1,014 an earlier draft of this plan quoted was wrong — almost certainly `LENGTH(extracted_text)` measuring gzipped bytes rather than characters, which is the trap CLAUDE.md warns about. The 810 was confirmed by decompressing every document down to 2,000 compressed bytes, far below the query's own threshold, so nothing is being silently dropped.

Two numbers to check rather than one, because the cheap model changes what a wrong estimate means. If the estimate exceeds **$3**, the window or the candidate cap is too generous — fix that before spending. If it comes out below **$0.10**, the worklist is far smaller than 1,014 documents and the *selection* is wrong; do not let a comfortable price hide a query that found nothing.

- [ ] **Step 6: Commit**

---

### Task 6: Verify on real documents, then a bounded first run

- [ ] **Step 1: Read three documents from three plans**

```bash
python -m scripts.read_sections --limit 3 --approve --budget 0.05
```

Three windows on DeepSeek is about **$0.0024**. The budget is a ceiling, not an estimate — if the run stops on it, something is calling the model far more often than once per document.

- [ ] **Step 2: Check the figures against the source**

For each, print the stored `returns_json` and open the document at that offset. **The numbers must appear in the document at that position.** This is the step that catches a plausible-looking hallucination, and it is the reason the offset is stored.

- [ ] **Step 3: Compare against what the summariser got**

For the same documents, print `summaries.performance_data`. The targeted read should find *more* — that is the entire premise. If it finds the same or less, stop: the ranking in Task 1 is picking the wrong window and no amount of running it wider will help.

- [ ] **Step 4: Confirm nothing was overwritten** — original `extracted_text` character counts unchanged.

- [ ] **Step 5: Rebuild the performance view and measure the gain**

```bash
python -m scripts.build_performance_view
```

Record plans with asset-class detail before and after. Baseline today: **110 of 148**.

- [x] **Step 6: Commit the verification**

## Outcome (2026-08-31)

**951 windows read across all 510 documents that had one, for $1.0980.**
Five failed, all on the same fault — output over the 16,384-token cap — and
all five raised rather than saving a partial table.

Verification on the first three documents, before the corpus run:

| | Summariser | Targeted |
|---|---|---|
| `Aug_26_2026_public_materials.pdf` | 0 | **102** |
| CERS Investment Committee | 15 | **77** |
| `BOT_Packet.pdf` | 2 | **16** |

All 195 figures appear verbatim in the window they were read from — zero
hallucinated numbers. The decisive check was doc 5815, whose top-ranked
window opens on an `Asset Allocation` heading listing 28.94 / 17.17 / 15.28:
portfolio **weights**. The model skipped every one and read the
`Comparative Performance` table on the following page instead, which is what
the prompt hardening was built for and what the 30,000-character window
made reachable.

Coverage, measured with and without the targeted reads:

| | Without | With |
|---|---|---|
| Plans with any data | 126 | 127 |
| **Plans with asset-class detail** | **110** | **116** |
| Asset-class cells filled | 597 | **683** (+14%) |
| Plans losing detail | — | **0** |

**Read this gain honestly.** It is smaller than this plan implied when it
speculated about "something near 140". Extraction is no longer the
constraint: the targeted read yields 7,236 canonicalisable rows against the
summariser's 4,069. What caps the view is `pick_latest`, which keeps one
document per plan per horizon — a deliberate choice, made because rows
blending an August equity figure with an FY2024 private-equity figure were
individually defensible and collectively meaningless. Better extraction
mostly improves documents already represented rather than adding plans.

So the binding limit has moved from "we cannot read the table" to "we show
one document per plan". That is a product decision, not an extraction
problem, and it should be re-opened deliberately rather than by loosening
extraction further.

Two things worth knowing before Phase B:

- **Only 29% of extracted rows canonicalise, and that is correct.** The rest
  are benchmarks (MSCI ACWI, Russell 2000, Policy Benchmark) and individual
  manager mandates (Oaktree High Yield, Eaton Vance High Yield) — real
  figures that are not asset classes. They stay in `document_section_read`
  for a later question to reach.
- **300 of the 810 long documents have no candidate section at all.** They
  are reported, never handed an arbitrary window.

---

### Task 7: Phase B — scanned documents (deferred, do not start here)

The original version of this plan covered the 354 scanned documents: parsing free-text page hints from `document_catalogue`, resolving the printed folio against the PDF page index, and OCR-ing only the resolved range. That work is still valid and still wanted, and its detail is preserved in git at `67cf502`.

It is deferred behind Phase A for three reasons, all measured: it is **810 documents against 354**; it costs **nothing per document to locate** where Phase B needs OCR to find anything at all; and it has **no page-offset problem**, which is the one part of Phase B that fails silently and expensively.

Do not start Task 7 until Task 6's before/after number exists. If Phase A moves coverage from 110 plans to something near 140, Phase B's remaining value is small and should be re-priced before it is built.
