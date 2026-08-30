# Targeted Read Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Read the pages a catalogue entry points at, and only those pages, so recovering a performance table costs cents rather than the price of a whole scanned board pack.

**Architecture:** A pure parser turns free-text page hints into candidate ranges. A resolver reconciles the *printed* page numbers those hints use against the PDF's own indices, because board packs paginate themselves and an unverified offset silently returns the wrong pages. OCR then runs over the resolved range only, and the text lands in a new table beside `extracted_text` rather than on top of it.

**Tech Stack:** Python, SQLAlchemy, PyMuPDF (`fitz`), Anthropic vision via the existing `extractor.extract_pdf_ocr` machinery, `costs` for pricing.

**Spec:** `docs/superpowers/specs/2026-08-30-relevance-gating-design.md`

## Global Constraints

- **Nothing is discarded.** No task writes to `documents.extracted_text`, truncates it, or deletes any row. Targeted text is stored in a new table. A test asserts `extracted_text` is byte-identical after a run.
- **No paid call is reachable without `--approve`.** Follow `scripts/catalogue.py`: the client constructor is never entered on an unapproved path, and the test asserts that rather than asserting cost is zero.
- **`--budget` is a hard stop**, checked before each call against spend so far. It stops the run; it does not warn.
- **Never write an ALTER TABLE migration system.** Add the model, run `init_db()`.
- **`documents.extracted_text` is `deferred()`** — bulk readers need `.options(undefer(...))`, and single-document reads must not loop.
- **OCR bills per page**: 1.3¢ measured. Every page read must be one a hint asked for.
- **Don't run `git add .`** — stage by explicit path.
- Baseline: `LLM_MODE=mock pytest tests/ -q` green at 648 passed / 30 skipped.

## Measured inputs (from the 354-document catalogue, 2026-08-30)

| | |
|---|---|
| documents with relevant material | 253 |
| ...carrying page hints | 172 |
| ...whose hints parse to page numbers | **119** |
| hints giving only agenda-item ids (`Item 9.7`, `VII.A`) | 53 |
| page ranges extracted | 133 |
| median span / largest span | **5 pages** / 204 pages |

## File Structure

- `page_hints.py` (new) — pure parsing of hint text to ranges. No I/O, no DB, no API. Its own file because it is the one piece that is fully testable without anything else, and it carries all the format variation.
- `database.py` (modify) — add `DocumentPageText`.
- `targeted_read.py` (new) — resolve ranges against a PDF, OCR them, store. The only module that spends.
- `scripts/read_targets.py` (new) — the CLI: worklist, `--approve`, `--budget`.
- `tests/test_page_hints.py`, `tests/test_targeted_read.py` (new).

---

### Task 1: Parse page hints into ranges

**Files:**
- Create: `page_hints.py`
- Test: `tests/test_page_hints.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `parse_hints(text: str) -> list[PageRange]` where `PageRange` is a frozen dataclass `(start: int, end: int, label: str)`. `start`/`end` are **printed** page numbers, 1-based, inclusive. Task 3 consumes this.

- [ ] **Step 1: Write the failing test**

```python
from page_hints import parse_hints, PageRange

def test_parses_the_common_forms():
    assert parse_hints("pp. 22-23; ETI Quarterly Report pp. 92-93") == [
        PageRange(22, 23, "pp. 22-23"),
        PageRange(92, 93, "pp. 92-93"),
    ]

def test_single_page_becomes_a_one_page_range():
    assert parse_hints("presented by Wilshire on page 3") == [PageRange(3, 3, "page 3")]

def test_handles_Page_N_colon_label_lists():
    got = parse_hints("Page 4: May Returns; Page 5: Asset Allocation; Page 10: Trustee Report")
    assert [(r.start, r.end) for r in got] == [(4, 4), (5, 5), (10, 10)]

def test_agenda_item_ids_yield_nothing():
    # 53 of 172 hints look like this. They are not page numbers and must not
    # be guessed at -- Item 9.7 is not page 9.
    for h in ["Item 9.7, Item 10.1", "VII.A", "ITEM 9", "Item 6.1, Item 6.2"]:
        assert parse_hints(h) == []

def test_ignores_dates_and_times_that_look_like_numbers():
    assert parse_hints("Memo dated April 24, 2026; Item 3 (9:15-9:30 a.m.)") == []

def test_merges_overlapping_and_adjacent_ranges():
    assert parse_hints("pp. 21-22, Pages 22-25") == [PageRange(21, 25, "pp. 21-22")]

def test_drops_implausible_spans():
    # "pp. 22-225" is 204 pages: a real hint, but reading it is not a
    # targeted read. Task 4 reports these rather than spending on them.
    assert parse_hints("pp. 22-225") == []
```

- [ ] **Step 2: Run it and watch it fail**

Run: `LLM_MODE=mock pytest tests/test_page_hints.py -v`
Expected: FAIL, `ModuleNotFoundError: No module named 'page_hints'`

- [ ] **Step 3: Implement**

```python
"""Turn a catalogue entry's free-text page hint into page ranges.

Pure: no I/O, no database, no API. All the format variation lives here,
measured from the 172 real hints the 2026-08-30 catalogue produced.

Page numbers here are the ones PRINTED in the document, which is not the
same as the PDF's page index -- see targeted_read.resolve_offset.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

MAX_SPAN = 60          # beyond this it is not a targeted read
MAX_PAGE = 3_000

_RANGE = re.compile(
    r"(?i)\b(?:pp?\.|pages?)\s*(\d{1,4})\s*(?:[-–]\s*(\d{1,4}))?")


@dataclass(frozen=True)
class PageRange:
    start: int
    end: int
    label: str


def parse_hints(text: str) -> list[PageRange]:
    if not text:
        return []
    found: list[PageRange] = []
    for m in _RANGE.finditer(text):
        start = int(m.group(1))
        end = int(m.group(2)) if m.group(2) else start
        if end < start or start < 1 or end > MAX_PAGE:
            continue
        if end - start + 1 > MAX_SPAN:
            continue
        found.append(PageRange(start, end, m.group(0).strip()))
    return _merge(found)


def _merge(ranges: list[PageRange]) -> list[PageRange]:
    """Overlapping and adjacent ranges become one, keeping the first label."""
    if not ranges:
        return []
    ordered = sorted(ranges, key=lambda r: (r.start, r.end))
    out = [ordered[0]]
    for r in ordered[1:]:
        last = out[-1]
        if r.start <= last.end + 1:
            out[-1] = PageRange(last.start, max(last.end, r.end), last.label)
        else:
            out.append(r)
    return out
```

- [ ] **Step 4: Run the tests**

Run: `LLM_MODE=mock pytest tests/test_page_hints.py -v`
Expected: PASS

- [ ] **Step 5: Check it against the real hints**

Run:
```bash
python -c "
import database, page_hints
from sqlalchemy import text
s = database.SessionLocal()
rows = s.execute(text(\"SELECT page_hints FROM document_catalogue WHERE page_hints <> '' AND contains <> '[]'\")).fetchall()
hit = sum(1 for (h,) in rows if page_hints.parse_hints(h))
print(f'{hit} of {len(rows)} hints yield ranges')
s.close()"
```
Expected: on the order of 110-125 of 172. If it is far below 100, a real format is being missed — print the misses and add it. If it is above 140, the agenda-item guard is leaking; check that `Item 9.7` still yields nothing.

- [ ] **Step 6: Commit**

```bash
git add page_hints.py tests/test_page_hints.py
git commit -m "Parse catalogue page hints into ranges"
```

---

### Task 2: Somewhere to put targeted text that is not on top of existing text

**Files:**
- Modify: `database.py` (add `DocumentPageText` next to `DocumentCatalogue`)
- Test: `tests/test_targeted_read.py` (schema test only in this task)

**Interfaces:**
- Consumes: nothing.
- Produces: `DocumentPageText(document_id, page_start, page_end, text, source, model, cost_usd, created_at)`, unique on `(document_id, page_start, page_end)`. Task 3 writes it.

- [ ] **Step 1: Write the failing test**

```python
import database
from database import Document, DocumentPageText, Plan

def test_targeted_text_is_stored_beside_the_document_not_on_it(tmp_db):
    s = database.get_session()
    s.add(Plan(id="mcera", name="MCERA", state="CA")); s.commit()
    d = Document(plan_id="mcera", url="https://x/a.pdf", filename="a.pdf",
                 extracted_text="pages 1-100 as OCRd long ago")
    s.add(d); s.commit()
    before = d.extracted_text

    s.add(DocumentPageText(document_id=d.id, page_start=340, page_end=372,
                           text="the allocation table", source="ocr_targeted"))
    s.commit()
    s.expire_all()

    assert s.get(Document, d.id).extracted_text == before
    row = s.query(DocumentPageText).one()
    assert (row.page_start, row.page_end) == (340, 372)
    s.close()
```

- [ ] **Step 2: Run it and watch it fail**

Run: `LLM_MODE=mock pytest tests/test_targeted_read.py -v`
Expected: FAIL, `ImportError: cannot import name 'DocumentPageText'`

- [ ] **Step 3: Add the model**

Place immediately after `DocumentCatalogue` in `database.py`:

```python
class DocumentPageText(Base):
    """Text from a specific page range, read because something asked for it.

    Separate from ``documents.extracted_text`` on purpose. That column holds
    whatever the original extraction produced -- for the 354 catalogued
    documents, pages 1-100, truncated at the old 150,000-character cap. A
    targeted read of pages 340-372 is a different, later, narrower act, and
    writing it into the same column would either overwrite material or grow
    a blob nobody can attribute. Both lose information.

    Unique on (document_id, page_start, page_end) so a re-run is a no-op
    rather than a second charge for the same pages.
    """

    __tablename__ = "document_page_text"

    id = Column(Integer, primary_key=True)
    document_id = Column(Integer, ForeignKey("documents.id"),
                         nullable=False, index=True)
    page_start = Column(Integer, nullable=False)   # PDF index, 1-based
    page_end = Column(Integer, nullable=False)
    text = Column(GzippedText)
    source = Column(String(32), nullable=False)    # 'ocr_targeted'
    model = Column(String(64))
    cost_usd = Column(Numeric(10, 6))
    created_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint("document_id", "page_start", "page_end",
                         name="uq_page_text_span"),
    )
```

- [ ] **Step 4: Run the test**

Run: `LLM_MODE=mock pytest tests/test_targeted_read.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add database.py tests/test_targeted_read.py
git commit -m "Store targeted page text beside the document, never on it"
```

---

### Task 3: Resolve printed page numbers to PDF indices, and read them

**Files:**
- Create: `targeted_read.py`
- Test: `tests/test_targeted_read.py` (extend)

**Interfaces:**
- Consumes: `page_hints.PageRange` (Task 1), `DocumentPageText` (Task 2).
- Produces:
  - `resolve_offset(pdf_path: str, probe_pages: list[int]) -> int | None` — how far the PDF index runs ahead of the printed number, or `None` when it cannot be established.
  - `read_range(pdf_path, start, end, model) -> tuple[str, Decimal]` — OCR of a PDF-index range.

**Why an offset step exists at all:** a board pack's cover, agenda and tabs are unnumbered, so "page 22" in a contents entry is rarely PDF page 22. Reading the unadjusted index returns the wrong pages and stores them as if they were right — a silent corruption, and the expensive kind, because nobody re-reads a page they believe they already have.

- [ ] **Step 1: Write the failing test**

```python
from decimal import Decimal
import targeted_read

def test_offset_is_found_from_a_printed_folio(monkeypatch):
    # PDF page 25 carries the printed folio "22" -> offset 3
    monkeypatch.setattr(targeted_read, "_ocr_single_page",
                        lambda p, i: ("... quarterly review ...\n22\n", Decimal("0.013")))
    assert targeted_read.resolve_offset("x.pdf", [25]) == 3

def test_offset_is_none_when_no_folio_is_found(monkeypatch):
    monkeypatch.setattr(targeted_read, "_ocr_single_page",
                        lambda p, i: ("no numbers here at all", Decimal("0.013")))
    assert targeted_read.resolve_offset("x.pdf", [25]) is None

def test_unresolved_offset_reads_nothing(monkeypatch):
    """Refusing to guess is the whole point: a wrong offset stores wrong
    pages under a correct-looking label."""
    monkeypatch.setattr(targeted_read, "resolve_offset", lambda *a, **k: None)
    called = []
    monkeypatch.setattr(targeted_read, "read_range",
                        lambda *a, **k: called.append(a) or ("", Decimal(0)))
    out = targeted_read.read_target("x.pdf", start=22, end=23, model="m")
    assert out.status == "offset_unresolved"
    assert called == []
```

- [ ] **Step 2: Run and watch it fail**

Run: `LLM_MODE=mock pytest tests/test_targeted_read.py -v`
Expected: FAIL, `ModuleNotFoundError: No module named 'targeted_read'`

- [ ] **Step 3: Implement**

```python
"""Read the pages a catalogue entry points at, and only those.

Reuses extractor's vision OCR per page. The one thing it adds is the offset:
"page 22" in a contents entry means the printed folio, and a board pack's
cover, agenda and tab dividers are unnumbered, so the PDF index runs ahead.
Reading the unadjusted index returns plausible-looking wrong pages and stores
them as if correct -- which nobody re-checks, because the row exists.

When the offset cannot be established, this reads nothing and says so.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal

MAX_OFFSET = 40
_FOLIO = re.compile(r"(?m)^\s*(\d{1,4})\s*$")


@dataclass
class ReadResult:
    status: str                 # 'ok' | 'offset_unresolved' | 'empty'
    text: str = ""
    cost: Decimal = Decimal(0)
    offset: int | None = None


def _ocr_single_page(pdf_path: str, index: int) -> tuple[str, Decimal]:
    """One page, one vision call. Split out so tests can replace it."""
    import fitz
    from costs import cost_usd
    from extractor import _ocr_page_image        # existing per-page helper
    doc = fitz.open(pdf_path)
    try:
        text, usage = _ocr_page_image(doc[index - 1])
        return text, cost_usd("claude-sonnet-4-6", usage)
    finally:
        doc.close()


def resolve_offset(pdf_path: str, probe_pages: list[int]) -> int | None:
    """PDF index minus printed folio, or None when no folio is legible."""
    for index in probe_pages:
        text, _ = _ocr_single_page(pdf_path, index)
        folios = [int(m) for m in _FOLIO.findall(text)]
        for folio in folios:
            offset = index - folio
            if 0 <= offset <= MAX_OFFSET:
                return offset
    return None


def read_range(pdf_path: str, start: int, end: int,
               model: str) -> tuple[str, Decimal]:
    parts, total = [], Decimal(0)
    for index in range(start, end + 1):
        text, cost = _ocr_single_page(pdf_path, index)
        parts.append(text)
        total += cost
    return "\n".join(parts), total


def read_target(pdf_path: str, start: int, end: int, model: str) -> ReadResult:
    """Printed page `start`-`end` -> text, or a status explaining why not."""
    offset = resolve_offset(pdf_path, [start, start + 1])
    if offset is None:
        return ReadResult(status="offset_unresolved")
    text, cost = read_range(pdf_path, start + offset, end + offset, model)
    if not text.strip():
        return ReadResult(status="empty", cost=cost, offset=offset)
    return ReadResult(status="ok", text=text, cost=cost, offset=offset)
```

**`extractor._ocr_page_image` does not exist yet — verified.** The per-page render-and-call is inline in `_extract_pdf_ocr` at `extractor.py:193-210`: it builds a pixmap with `page.get_pixmap(matrix=mat)`, base64-encodes it as PNG, and calls `client.messages.create`. Your first move in this task is to lift lines 193-210's body into

```python
def _ocr_page_image(page) -> tuple[str, object]:
    """One rendered page -> (text, usage). Extracted from _extract_pdf_ocr so
    targeted reads can call a single page without the 100-page loop."""
```

leaving `_extract_pdf_ocr` calling it in the same order with identical behaviour. Run `LLM_MODE=mock pytest tests/ -q` after that refactor and before writing anything new — the existing OCR tests are the proof it is behaviour-preserving, and separating the two changes is what makes a later bisect readable.

- [ ] **Step 4: Run the tests**

Run: `LLM_MODE=mock pytest tests/test_targeted_read.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add targeted_read.py tests/test_targeted_read.py
git commit -m "Resolve printed page numbers to PDF indices before reading"
```

---

### Task 4: The CLI — priced worklist, approval, hard budget

**Files:**
- Create: `scripts/read_targets.py`
- Test: `tests/test_targeted_read.py` (extend)

**Interfaces:**
- Consumes: everything above.
- Produces: `python -m scripts.read_targets [--approve] [--budget N] [--limit N] [--plan ID]`

Model it on `scripts/catalogue.py` — same structure, same guarantees, same output shape. That file is the reference for the no-spend contract.

- [ ] **Step 1: Write the failing test**

```python
def test_without_approve_no_ocr_is_reachable(session, monkeypatch, capsys):
    _catalogued_doc(session, hints="pp. 22-23")
    monkeypatch.setattr(targeted_read, "_ocr_single_page",
                        lambda *a: (_ for _ in ()).throw(
                            AssertionError("OCR reached on a no-spend path")))
    monkeypatch.setattr("sys.argv", ["read_targets"])
    assert read_targets.main() == 0
    assert "Nothing spent" in capsys.readouterr().out

def test_worklist_prices_pages_not_documents(session, monkeypatch, capsys):
    _catalogued_doc(session, hints="pp. 22-23")       # 2 pages
    _catalogued_doc(session, hints="Item 9.7")        # unparseable
    monkeypatch.setattr("sys.argv", ["read_targets"])
    read_targets.main()
    out = capsys.readouterr().out
    assert "2 pages" in out
    assert "1 document with hints that do not parse" in out

def test_budget_stops_the_run(session, monkeypatch):
    for _ in range(10):
        _catalogued_doc(session, hints="pp. 1-5")
    monkeypatch.setattr(targeted_read, "read_target",
                        lambda *a, **k: targeted_read.ReadResult(
                            status="ok", text="x", cost=Decimal("0.10"), offset=0))
    monkeypatch.setattr("sys.argv",
                        ["read_targets", "--approve", "--budget", "0.30"])
    read_targets.main()
    assert 0 < session.query(DocumentPageText).count() <= 4
```

- [ ] **Step 2: Run and watch it fail**

Run: `LLM_MODE=mock pytest tests/test_targeted_read.py -v`
Expected: FAIL on the import of `scripts.read_targets`

- [ ] **Step 3: Implement**

Follow `scripts/catalogue.py` closely. Required behaviour:

- Selects catalogue rows where `contains <> '[]'` and `page_hints <> ''`, whose document has a local file, and which have no `DocumentPageText` row covering the same span.
- Runs `page_hints.parse_hints` on each; documents whose hints yield no ranges are **counted and reported, never guessed at**.
- Orders newest-first by `documents.meeting_date`, nulls last — the spec's date-as-prioritiser.
- Prints a priced worklist: documents, total pages, estimated cost at 1.3c/page, and separately the count whose hints did not parse.
- Without `--approve`: prints the worklist, prints `Nothing spent.`, returns 0, and constructs no client.
- With `--approve`: reads each range, stores a `DocumentPageText` row, commits per document, and checks `--budget` before each document.
- Reports `offset_unresolved` documents separately at the end — they are the ones needing a human, not a retry.

- [ ] **Step 4: Run the tests**

Run: `LLM_MODE=mock pytest tests/ -q`
Expected: PASS, ~656 passed

- [ ] **Step 5: Price the real run**

Run: `python -m scripts.read_targets`
Expected: a worklist of roughly 119 documents and on the order of 600-900 pages (median span 5, plus multi-range documents), estimated at **$8-12** — against $375 to read the same documents whole. If the estimate lands far above $20, the span cap in Task 1 is too loose; check what is being included before spending anything.

- [ ] **Step 6: Commit**

```bash
git add scripts/read_targets.py tests/test_targeted_read.py
git commit -m "Priced worklist and approval gate for targeted reads"
```

---

### Task 5: Verify against three real documents before any bulk run

**Files:**
- Test: manual, recorded in the commit message.

Nothing here has touched a real PDF. The offset logic in particular is a hypothesis about how board packs paginate, and it is the piece that fails silently.

- [ ] **Step 1: Pick three documents with parseable hints, from different plans**

```bash
python -c "
import database, page_hints
from sqlalchemy import text
s = database.SessionLocal()
rows = s.execute(text('''
  SELECT dc.document_id, d.plan_id, d.filename, dc.page_hints
  FROM document_catalogue dc JOIN documents d ON d.id = dc.document_id
  WHERE dc.contains <> '[]' AND dc.page_hints <> '' AND d.local_path IS NOT NULL
''')).fetchall()
seen = set()
for did, plan, fn, h in rows:
    if plan in seen: continue
    r = page_hints.parse_hints(h)
    if r:
        seen.add(plan)
        print(did, plan, fn, r[:2])
    if len(seen) == 3: break
s.close()"
```

- [ ] **Step 2: Read one range from each, approved, with a tight budget**

```bash
python -m scripts.read_targets --limit 3 --approve --budget 0.50
```

- [ ] **Step 3: Confirm the pages are the right pages**

For each result, print the stored text and check it against what the hint promised. A hint saying "Asset Allocation Report - Page 36" must return text about asset allocation. **If it returns an unrelated page, the offset is wrong** — stop, and do not run in bulk. That is the failure this task exists to catch, and it will not announce itself.

- [ ] **Step 4: Confirm nothing was overwritten**

Substitute the three document ids from Step 1:

```bash
python -c "
import database
from database import Document, DocumentPageText
from sqlalchemy.orm import undefer
IDS = [111, 222, 333]        # <- the three ids printed by Step 1
s = database.SessionLocal()
for d in s.query(Document).options(undefer(Document.extracted_text)).filter(Document.id.in_(IDS)):
    n = s.query(DocumentPageText).filter_by(document_id=d.id).count()
    print(f'{d.id}: {len(d.extracted_text or \"\"):,} chars still in documents, {n} targeted rows alongside')
s.close()"
```

Every one must still report its original character count — the same number as before the run. A shrunk count means a targeted read wrote over the original extraction, which is the constraint this whole design exists to hold.

- [ ] **Step 5: Commit the verification**

```bash
git commit --allow-empty -m "Verify targeted reads return the promised pages"
```

Record in the message: the three documents, what each hint promised, and what came back.
