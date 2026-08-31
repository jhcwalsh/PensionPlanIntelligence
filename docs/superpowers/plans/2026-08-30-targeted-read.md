# Targeted Read Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Read the part of a document that holds the numbers, instead of the first tenth of it.

**Architecture:** Two phases against one idea. Phase A covers documents whose text we already hold — the large majority — where locating a section is a free text search and the only paid step is reading the slice we chose. Phase B covers scanned documents, where the same idea needs OCR and therefore needs a page offset resolved first.

**Tech Stack:** Python, SQLAlchemy, Anthropic tool-use via `summarizer._get_client`, `costs` for pricing, PyMuPDF for Phase B.

**Spec:** `docs/superpowers/specs/2026-08-30-relevance-gating-design.md`

## Why this was rewritten

The first version of this plan covered only the 354 scanned documents. Chasing a different question — why eleven plans have documents but no performance figures — showed that scoping was wrong, and wrong about the bigger number.

Those eleven plans mostly do not publish performance material at all: Nashville's meeting pages offer one document each, the agenda; Atlanta's ten "board packs" are one-page meeting notices misfiled by `guess_doc_type`; across their 641 documents only ten run past twenty pages. No scraper fixes that.

But those ten long documents *are* held, extracted and summarised, and still yield nothing. The summariser compresses every document to ~50,000 characters before Claude sees it (`summarizer.SMART_TRUNCATE_TARGET`). That is not naive — head, investment-keyword windows, tail — but it fills its budget from the front, and an allocation table is a dense numeric grid that sits deep. Measured on a real board pack, the genuine performance headings begin **31% of the way in**.

This is not a ten-document problem:

| | |
|---|---|
| Documents with stored text | 4,926 |
| **Truncated before summarising (>50k chars)** | **1,014** |
| Heavily truncated (>250k chars) | 79 |
| **Plans affected** | **136 of 148** |

So the ceiling on performance coverage is not fetching, and not the scanned tail. It is that a fifth of the corpus is read in part, and the part is chosen to write a good summary rather than to find a table.

## Global Constraints

- **Nothing is discarded.** No task writes to `documents.extracted_text` or deletes any row. Extracted figures go to their own table; a test asserts `extracted_text` is byte-identical after a run.
- **No paid call is reachable without `--approve`.** Follow `scripts/catalogue.py`: the client constructor is never entered on an unapproved path, and the test asserts *that*, not that cost came out zero.
- **`--budget` is a hard stop**, checked before each call against spend so far.
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
- `targeted_extract.py` (new) — the only module that spends: rank candidates, then extract from the chosen window.
- `scripts/read_sections.py` (new) — CLI: priced worklist, `--approve`, `--budget`.
- `tests/test_section_finder.py`, `tests/test_targeted_extract.py` (new).
- Phase B (`page_hints.py`, `targeted_read.py`) — deferred; see Task 6.

---

### Task 1: Find candidate sections in stored text

**Files:**
- Create: `section_finder.py`
- Test: `tests/test_section_finder.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `find_candidates(text: str, max_candidates: int = 12) -> list[Candidate]`, where `Candidate` is a frozen dataclass `(offset: int, heading: str, score: float)`. Task 2 consumes this.

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

### Task 3: Extract returns from a chosen window

**Files:**
- Create: `targeted_extract.py`
- Test: `tests/test_targeted_extract.py` (extend)

**Interfaces:**
- Consumes: `section_finder.Candidate`, `DocumentSectionRead`.
- Produces: `extract_window(text: str, candidate, model: str) -> tuple[dict, Decimal]`.

Reuse the tool-use shape from `extract_performance_reports.py:190-215` — system prompt, `tools=[SCHEMA]`, `tool_choice={"type":"tool", ...}`, read `block.input` from the `tool_use` block.

**Two things that file learned the hard way, and this must not repeat:**
- `MAX_OUTPUT_TOKENS` must be generous (that file uses 16384). At 4096 it silently truncated every call and saved thirty documents with zero rows and no error.
- Check `msg.stop_reason == "max_tokens"` and surface it, rather than trusting the payload.

- [ ] **Step 1: Write the failing test**

```python
from decimal import Decimal
import targeted_extract
from section_finder import Candidate

def test_returns_parsed_rows_and_cost(monkeypatch):
    monkeypatch.setattr(targeted_extract, "_call_model", lambda w, m: (
        {"returns": [{"asset_class": "US Equity", "return_pct": 12.4,
                      "period": "FY2026"}]}, Decimal("0.004")))
    data, cost = targeted_extract.extract_window(
        "…", Candidate(200_881, "Total Rates of Return (%)", 2.4), "m")
    assert data["returns"][0]["asset_class"] == "US Equity"
    assert cost == Decimal("0.004")

def test_a_truncated_response_raises_rather_than_saving_nothing(monkeypatch):
    """extract_performance_reports saved 30 documents with zero rows and no
    error when max_tokens cut the tool call short. A silent empty result is
    worse than a failure, because nobody re-runs it."""
    def truncated(window, model):
        raise targeted_extract.ResponseTruncated("in=58236 out=4096")
    monkeypatch.setattr(targeted_extract, "_call_model", truncated)
    import pytest
    with pytest.raises(targeted_extract.ResponseTruncated):
        targeted_extract.extract_window("…", Candidate(0, "x", 1.0), "m")
```

- [ ] **Step 2: Run and watch it fail**

- [ ] **Step 3: Implement**

Mirror `extract_performance_reports.py`. Required elements:

```python
MODEL = "claude-haiku-4-5-20251001"   # a 30k window, one table: Haiku is enough
MAX_OUTPUT_TOKENS = 16_384


class ResponseTruncated(RuntimeError):
    """The model hit max_tokens. The payload is incomplete and must not be
    saved as if it were a result."""
```

The tool schema records a list of `{asset_class, return_pct, period, benchmark_pct}` — the same shape as `summaries.performance_data`, so `scripts/build_performance_view.py` can consume it with no new parser.

- [ ] **Step 4: Run the tests** — Expected: PASS

- [ ] **Step 5: Commit**

---

### Task 4: The CLI — priced worklist, approval, hard budget

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

- [ ] **Step 4: Run the full suite** — Expected: PASS, ~665 passed

- [ ] **Step 5: Price the real run**

Run: `python -m scripts.read_sections`
Expected: on the order of 1,014 documents. At roughly 7,500 input tokens per 30k-char window plus ~800 output, Haiku puts this near **$10-15** for the whole corpus. If the estimate exceeds $30, the window or the candidate cap is too generous — fix that before spending.

- [ ] **Step 6: Commit**

---

### Task 5: Verify on real documents, then a bounded first run

- [ ] **Step 1: Read three documents from three plans**

```bash
python -m scripts.read_sections --limit 3 --approve --budget 0.20
```

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

- [ ] **Step 6: Commit the verification**, recording the three documents, what was found, and the before/after coverage.

---

### Task 6: Phase B — scanned documents (deferred, do not start here)

The original version of this plan covered the 354 scanned documents: parsing free-text page hints from `document_catalogue`, resolving the printed folio against the PDF page index, and OCR-ing only the resolved range. That work is still valid and still wanted, and its detail is preserved in git at `67cf502`.

It is deferred behind Phase A for three reasons, all measured: it is **1,014 documents against 354**; it costs **nothing per document to locate** where Phase B needs OCR to find anything at all; and it has **no page-offset problem**, which is the one part of Phase B that fails silently and expensively.

Do not start Task 6 until Task 5's before/after number exists. If Phase A moves coverage from 110 plans to something near 140, Phase B's remaining value is small and should be re-priced before it is built.
