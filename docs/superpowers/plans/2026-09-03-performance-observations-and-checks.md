# Performance Observations and Data Checks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Store every extracted performance figure as a durable, queryable row, then use that table to find the ones that are wrong.

**Architecture:** A new `performance_observation` table holds one immutable row per number an extractor produced — including the ~27% that carry a benchmark and the ones whose asset class does not map, both of which are discarded today. `scripts/build_performance_view.py` writes it as a first stage and keeps building the existing derived views unchanged, so nothing that works stops working. A new `scripts/check_performance.py` then runs SQL checks over the fact table and records findings in `performance_flag`, surfaced on the Admin tab.

**Tech Stack:** Python 3.12, SQLAlchemy 2.x ORM, Neon Postgres (SQLite in tests), pytest, Streamlit.

**Spec:** `nextsteps.md`, entries **D19** ("Are the numbers right? — checks, not hope") and **D20** ("Make the data usable — by charts, and by an agent"). Read both before starting; they carry the measurements this plan assumes.

## Global Constraints

- **Never write `ALTER TABLE`.** Add the SQLAlchemy model class and call `init_db()`. Existing-row backfill is a one-off script. (`CLAUDE.md`)
- **`documents.extracted_text` is `deferred()` and gzipped.** Never `SELECT` it in bulk; loading it in bulk exhausted Neon's transfer quota on 2026-08-25. Nothing in this plan needs it.
- **Do not use `LENGTH()`/`octet_length` as a character count** on gzipped columns.
- **Tests set `LLM_MODE=mock` and `INSIGHTS_MODE=mock`** via autouse fixtures in `tests/conftest.py`. Use the existing `tmp_db` fixture for DB isolation; never build your own engine.
- **No API calls anywhere in this plan.** Every check is arithmetic over stored rows. If a task seems to need a model, it is the wrong task.
- **Never auto-delete or auto-correct a flagged figure.** Flag, store, display. A silent delete is the same failure as a silent wrong number, minus the evidence.
- Run the suite with `LLM_MODE=mock pytest tests/ -q`.
- Commit messages: no `Generated with` footer; use the repo's existing style.

## Scope

**In scope:** the `performance_observation` fact table, the write path, four checks, the `performance_flag` table, and an Admin queue.

**Out of scope, deliberately** — each is a separate plan, and none of them can start before this one lands:
- `allocation_observation` (the same shape for weights; D20 names it).
- The daily Parquet/CSV export (D20).
- A read-only MCP server (D20).
- Rebuilding `plan_asset_class_horizon` *from* the fact table rather than from the blobs. That refactor is the eventual point, but doing it in the same change as introducing the table means a bug in either is indistinguishable from a bug in the other.

## File Structure

| File | Responsibility |
|---|---|
| `database.py` | Add `PerformanceObservation` and `PerformanceFlag` model classes. Nothing else. |
| `scripts/build_performance_view.py` | Add `observations_from_payload()` and `write_observations()`; call them from `main()` before the existing view build. Existing functions untouched. |
| `scripts/check_performance.py` | **New.** The check ladder and its CLI. Pure reads plus writes to `performance_flag`. |
| `app.py` | One new Admin sub-tab rendering the open flag queue. |
| `tests/test_performance_observations.py` | **New.** Model, parser, write path. |
| `tests/test_check_performance.py` | **New.** Each check, with fixtures that make the arithmetic obvious. |

---

### Task 1: The `performance_observation` and `performance_flag` tables

**Files:**
- Modify: `database.py` (append two model classes near `PlanAssetClassHorizon`, around line 1046)
- Test: `tests/test_performance_observations.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `database.PerformanceObservation` with columns `id, plan_id, document_id, source, asset_class_raw, asset_class, horizon_key, period_label, period_end, as_of_date, return_pct, benchmark_pct, offset, model, extracted_at`. `database.PerformanceFlag` with columns `id, check_name, observation_id, plan_id, asset_class, horizon_key, period_end, value, expected, detail, flagged_at, resolved_at, resolution`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_performance_observations.py
"""The fact table: one durable row per extracted number.

Today an observation lives in a TEXT column as JSON and is reconstituted by a
build script into a table that gets dropped. 66,041 raw observations collapse
to ~8,000 cells and the losers are unrecoverable, which is why benchmark_pct
was discardable without anyone noticing. These tests pin the properties that
stop being true if that happens again.
"""
from datetime import date, datetime, timezone

import database
from database import PerformanceObservation, PerformanceFlag, Plan


def test_an_observation_keeps_what_the_view_throws_away(tmp_db):
    s = database.get_session()
    s.add(Plan(id="mcera", name="MCERA", state="CA"))
    s.commit()
    s.add(PerformanceObservation(
        plan_id="mcera", document_id=None, source="targeted_read",
        asset_class_raw="Domestic Equity - Large Cap",
        asset_class="public_equity_us",
        horizon_key="annual", period_label="1-Year", period_end="2025Q4",
        as_of_date=date(2026, 2, 1), return_pct=12.4, benchmark_pct=12.0,
        offset=41000, model="deepseek/deepseek-v4-flash"))
    s.commit()

    got = s.query(PerformanceObservation).one()
    # The benchmark is the strongest free check available and the horizon
    # table has no column for it.
    assert got.benchmark_pct == 12.0
    # The raw label is what makes an unmapped asset class a measurable gap
    # rather than a silent loss.
    assert got.asset_class_raw == "Domestic Equity - Large Cap"
    # The offset is what makes a figure auditable against the retained PDF.
    assert got.offset == 41000
    s.close()


def test_an_unmapped_asset_class_is_stored_not_dropped(tmp_db):
    """_rows_from_payload drops these. The fact table must not."""
    s = database.get_session()
    s.add(Plan(id="mcera", name="MCERA", state="CA"))
    s.commit()
    s.add(PerformanceObservation(
        plan_id="mcera", source="board_doc",
        asset_class_raw="Transition Account", asset_class=None,
        period_label="1-Year", return_pct=3.3))
    s.commit()
    got = s.query(PerformanceObservation).one()
    assert got.asset_class is None
    assert got.asset_class_raw == "Transition Account"
    s.close()


def test_a_flag_records_what_fired_and_stays_resolvable(tmp_db):
    s = database.get_session()
    s.add(Plan(id="mcera", name="MCERA", state="CA"))
    s.commit()
    s.add(PerformanceFlag(
        check_name="peer_dispersion", plan_id="mcera",
        asset_class="total", horizon_key="5y", period_end="2023Q4",
        value=110.0, expected=8.23,
        detail="robust z=32.5 against 8 peers"))
    s.commit()
    f = s.query(PerformanceFlag).one()
    assert f.check_name == "peer_dispersion"
    assert f.resolved_at is None and f.resolution is None
    f.resolved_at = datetime.now(timezone.utc)
    f.resolution = "confirmed_bad"
    s.commit()
    assert s.query(PerformanceFlag).one().resolution == "confirmed_bad"
    s.close()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `LLM_MODE=mock python -m pytest tests/test_performance_observations.py -q`
Expected: FAIL with `ImportError: cannot import name 'PerformanceObservation' from 'database'`

- [ ] **Step 3: Add the two model classes**

Append to `database.py`, after the `PlanAssetClassHorizon` class:

```python
class PerformanceObservation(Base):
    """One extracted number, exactly as an extractor produced it.

    The durable record the derived views are built from. Everything in
    plan_asset_class_horizon is a *selection* over these rows -- one winner
    per cell -- and that table is dropped and recreated whenever its shape
    changes. This one is never dropped, so a rebuild stops destroying the
    evidence underneath it.

    Three columns exist because the view discards them and should not:

    ``benchmark_pct`` -- 17,959 of 66,041 observations (27%) carry one.
    A return beside its own benchmark is the strongest free sanity check
    available, and the derived table has nowhere to put it.

    ``asset_class_raw`` -- what the document actually said, beside the
    canonical mapping. Where ``asset_class`` is NULL the mapping failed, and
    storing the raw label turns a silent loss into a countable gap.

    ``offset`` -- the character position in the document window this came
    from, for targeted reads. With the PDF retained in R2, that makes every
    figure auditable back to the bytes it was read from.

    Not unique on anything. A document legitimately reports the same asset
    class twice (a table and a summary line), and collapsing those hides a
    disagreement worth seeing. The write path clears a document's rows and
    reinserts, which is idempotent without pretending duplicates cannot exist.
    """

    __tablename__ = "performance_observation"

    id = Column(Integer, primary_key=True)
    plan_id = Column(String, ForeignKey("plans.id"), nullable=False, index=True)
    document_id = Column(Integer, ForeignKey("documents.id"), index=True)
    source = Column(String(16), nullable=False)   # targeted_read|board_doc|cafr

    asset_class_raw = Column(String(200))
    asset_class = Column(String(64), index=True)  # canonical; NULL if unmapped

    horizon_key = Column(String(16), index=True)
    period_label = Column(String(64))
    period_end = Column(String(8), index=True)    # 'YYYYQn'
    as_of_date = Column(Date)

    return_pct = Column(Float)
    benchmark_pct = Column(Float)

    offset = Column(Integer)
    model = Column(String(64))
    extracted_at = Column(DateTime(timezone=True), default=utcnow)

    __table_args__ = (
        Index("ix_perfobs_cell", "asset_class", "horizon_key", "period_end"),
        Index("ix_perfobs_doc_source", "document_id", "source"),
    )


class PerformanceFlag(Base):
    """A check that fired on an observation, and whether anyone dealt with it.

    Findings, not test failures. A red CI run is the wrong channel for "one
    plan's private equity looks odd this quarter", so these land in a table
    and an Admin queue instead.

    ``resolved_at``/``resolution`` exist so a cell cleared once is not
    re-flagged forever. Nothing here ever edits or deletes the observation it
    points at: a silent correction is the same failure as a silent wrong
    number, minus the evidence.
    """

    __tablename__ = "performance_flag"

    id = Column(Integer, primary_key=True)
    check_name = Column(String(32), nullable=False, index=True)
    observation_id = Column(Integer, ForeignKey("performance_observation.id"),
                            index=True)
    plan_id = Column(String, ForeignKey("plans.id"), index=True)
    asset_class = Column(String(64))
    horizon_key = Column(String(16))
    period_end = Column(String(8))

    value = Column(Float)        # what we stored
    expected = Column(Float)     # peer median, benchmark, or bound
    detail = Column(String(300))

    flagged_at = Column(DateTime(timezone=True), default=utcnow)
    resolved_at = Column(DateTime(timezone=True))
    resolution = Column(String(32))   # confirmed_bad|confirmed_ok|ignored
```

No import changes needed: `Column, Integer, String, Float, Date, DateTime, ForeignKey, Index` are all already imported at `database.py:33-37`, and `utcnow` is defined in the same module.

- [ ] **Step 4: Run the test to verify it passes**

Run: `LLM_MODE=mock python -m pytest tests/test_performance_observations.py -q`
Expected: PASS (3 tests)

- [ ] **Step 5: Create the tables on the live database**

Run: `python -c "import database; database.init_db()"`
Expected: no output, exit 0. `create_all` adds the two missing tables and skips the 39 that exist.

- [ ] **Step 6: Commit**

```bash
git add database.py tests/test_performance_observations.py
git commit -m "Add performance_observation and performance_flag"
```

---

### Task 2: Parse a payload into observations, keeping what the view drops

**Files:**
- Modify: `scripts/build_performance_view.py` (add after `_rows_from_payload`, which ends at line 434)
- Test: `tests/test_performance_observations.py`

**Interfaces:**
- Consumes: `database.PerformanceObservation` (Task 1); existing helpers in the same module — `canonical(raw: str, class_map: dict) -> str | None`, `horizon_key(period_label: str | None) -> str | None`, `_as_float(v) -> float | None`, `_as_date(v) -> date | None`, `_NOT_A_RETURN_PERIOD`.
- Produces: `observations_from_payload(payload, plan_id, meeting_date, doc_id, class_map, source, offset=None, model=None) -> list[dict]`, where each dict has exactly the column names of `PerformanceObservation` minus `id` and `extracted_at`.

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_performance_observations.py
import json
from datetime import date

from scripts import build_performance_view as bpv


def _payload(items):
    return json.dumps(items)


def test_benchmark_survives_the_parse(tmp_db):
    cm = bpv.load_class_map()
    obs = bpv.observations_from_payload(
        _payload([{"asset_class": "US Equity", "return_pct": 12.4,
                   "benchmark_pct": 12.0, "period": "1-Year"}]),
        "mcera", date(2026, 2, 1), 7, cm, "targeted_read", offset=41000,
        model="deepseek/deepseek-v4-flash")
    assert len(obs) == 1
    assert obs[0]["benchmark_pct"] == 12.0
    assert obs[0]["offset"] == 41000
    assert obs[0]["model"] == "deepseek/deepseek-v4-flash"


def test_an_unmapped_class_is_kept_with_a_null_canonical(tmp_db):
    """_rows_from_payload returns [] for these. That is the loss D20 names."""
    cm = bpv.load_class_map()
    payload = _payload([{"asset_class": "Zzz Transition Account",
                         "return_pct": 3.3, "period": "1-Year"}])
    assert bpv._rows_from_payload(payload, "mcera", date(2026, 2, 1), 7,
                                  cm, "board_doc") == []
    obs = bpv.observations_from_payload(payload, "mcera", date(2026, 2, 1), 7,
                                        cm, "board_doc")
    assert len(obs) == 1
    assert obs[0]["asset_class"] is None
    assert obs[0]["asset_class_raw"] == "Zzz Transition Account"


def test_a_null_return_with_a_benchmark_is_kept(tmp_db):
    """The old parser required return_pct. An observation of "we reported a
    benchmark and no return" is still a fact about the document."""
    cm = bpv.load_class_map()
    obs = bpv.observations_from_payload(
        _payload([{"asset_class": "US Equity", "return_pct": None,
                   "benchmark_pct": 9.1, "period": "1-Year"}]),
        "mcera", date(2026, 2, 1), 7, cm, "board_doc")
    assert len(obs) == 1
    assert obs[0]["return_pct"] is None and obs[0]["benchmark_pct"] == 9.1


def test_a_period_that_is_not_a_return_is_still_skipped(tmp_db):
    """Keep the one filter that is about meaning rather than mapping."""
    cm = bpv.load_class_map()
    obs = bpv.observations_from_payload(
        _payload([{"asset_class": "US Equity", "return_pct": 4.0,
                   "period": "as of June 30, 2025 market value"}]),
        "mcera", date(2026, 2, 1), 7, cm, "board_doc")
    assert obs == []


def test_period_end_and_horizon_are_derived_once_here(tmp_db):
    cm = bpv.load_class_map()
    obs = bpv.observations_from_payload(
        _payload([{"asset_class": "US Equity", "return_pct": 4.0,
                   "period": "3-Year"}]),
        "mcera", date(2026, 2, 14), 7, cm, "board_doc")
    assert obs[0]["horizon_key"] == "3y"
    assert obs[0]["period_end"] == "2025Q4"


def test_a_malformed_payload_yields_nothing_rather_than_raising(tmp_db):
    cm = bpv.load_class_map()
    assert bpv.observations_from_payload("not json", "mcera", None, 7, cm,
                                          "board_doc") == []
    assert bpv.observations_from_payload('{"not": "a list"}', "mcera", None, 7,
                                          cm, "board_doc") == []
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `LLM_MODE=mock python -m pytest tests/test_performance_observations.py -q`
Expected: FAIL with `AttributeError: module 'scripts.build_performance_view' has no attribute 'observations_from_payload'`

- [ ] **Step 3: Write the parser**

Add to `scripts/build_performance_view.py`, immediately after `_rows_from_payload`:

```python
def observations_from_payload(payload, plan_id, meeting_date, doc_id,
                              class_map, source: str,
                              offset: int | None = None,
                              model: str | None = None) -> list[dict]:
    """Every number in a payload, as fact-table rows.

    Deliberately more permissive than _rows_from_payload, which exists to feed
    a *view* and so drops anything it cannot place: an unmapped asset class, a
    missing return. Those drops are right for a table with one column per
    horizon and wrong for a record of what was extracted -- they are how 66,041
    observations became ~8,000 cells with no way to ask what happened to the
    rest.

    Kept where _rows_from_payload drops:
      * asset_class NULL, asset_class_raw populated -- a countable mapping gap.
      * return_pct NULL where a benchmark is present -- still a fact.
      * benchmark_pct, which the view has no column for at all.

    The one filter retained is _NOT_A_RETURN_PERIOD: a market value labelled
    "as of June 30" is not a return under any horizon, and storing it as one
    would put a nine-figure number in a percentage column.
    """
    try:
        items = json.loads(payload)
    except (TypeError, ValueError):
        return []
    if not isinstance(items, list):
        return []

    out = []
    for item in items:
        if not isinstance(item, dict):
            continue
        period = item.get("period") or ""
        if _NOT_A_RETURN_PERIOD.search(period):
            continue
        ret = _as_float(item.get("return_pct"))
        bench = _as_float(item.get("benchmark_pct"))
        if ret is None and bench is None:
            continue
        raw = (item.get("asset_class") or "").strip()
        as_of = _as_date(meeting_date)
        label = (period or "")[:64] or None
        out.append({
            "plan_id": plan_id,
            "document_id": doc_id,
            "source": source,
            "asset_class_raw": raw[:200] or None,
            "asset_class": canonical(raw, class_map),
            "horizon_key": horizon_key(period),
            "period_label": label,
            "period_end": queries.period_end_quarter(label, as_of),
            "as_of_date": as_of,
            "return_pct": ret,
            "benchmark_pct": bench,
            "offset": offset,
            "model": model,
        })
    return out
```

No import changes needed: `import queries` is already at `scripts/build_performance_view.py:42`, and `json`, `date` and `canonical`/`horizon_key`/`_as_float`/`_as_date`/`_NOT_A_RETURN_PERIOD` are all in the same module.

- [ ] **Step 4: Run the test to verify it passes**

Run: `LLM_MODE=mock python -m pytest tests/test_performance_observations.py -q`
Expected: PASS (9 tests)

- [ ] **Step 5: Commit**

```bash
git add scripts/build_performance_view.py tests/test_performance_observations.py
git commit -m "Parse payloads into observations, keeping what the view drops"
```

---

### Task 3: Write observations during the build

**Files:**
- Modify: `scripts/build_performance_view.py` (add `write_observations`; call it from `main()` at line ~688, before `collect_from_cafr`)
- Test: `tests/test_performance_observations.py`

**Interfaces:**
- Consumes: `observations_from_payload(...)` (Task 2), `database.PerformanceObservation` (Task 1).
- Produces: `write_observations(session, class_map) -> dict[str, int]`, returning counts keyed by source, e.g. `{"targeted_read": 53474, "board_doc": 12567, "cafr": 2234}`.

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_performance_observations.py
from database import Document, DocumentSectionRead, Summary


def _doc(s, plan_id="mcera", doc_id=None):
    d = Document(plan_id=plan_id, url=f"https://x/{doc_id or 1}.pdf",
                 filename="p.pdf", extraction_status="done",
                 meeting_date=date(2026, 2, 14))
    s.add(d); s.commit()
    return d


def test_the_write_path_records_every_source(tmp_db):
    s = database.get_session()
    s.add(Plan(id="mcera", name="MCERA", state="CA")); s.commit()
    d = _doc(s)
    s.add(DocumentSectionRead(
        document_id=d.id, offset=41000, model="deepseek/deepseek-v4-flash",
        returns_json=_payload([{"asset_class": "US Equity", "return_pct": 12.4,
                                "benchmark_pct": 12.0, "period": "1-Year"}])))
    s.add(Summary(document_id=d.id, summary_text="x",
                  performance_data=_payload(
                      [{"asset_class": "Real Estate", "return_pct": 5.0,
                        "period": "3-Year"}])))
    s.commit()

    counts = bpv.write_observations(s, bpv.load_class_map())
    assert counts["targeted_read"] == 1
    assert counts["board_doc"] == 1

    obs = {o.source: o for o in s.query(PerformanceObservation).all()}
    assert obs["targeted_read"].benchmark_pct == 12.0
    # The offset and model come from the section read, not the payload.
    assert obs["targeted_read"].offset == 41000
    assert obs["targeted_read"].model == "deepseek/deepseek-v4-flash"
    assert obs["board_doc"].offset is None
    s.close()


def test_rerunning_the_write_path_does_not_duplicate(tmp_db):
    """Idempotent by clearing a document's rows per source and reinserting --
    a rebuild must not multiply the corpus."""
    s = database.get_session()
    s.add(Plan(id="mcera", name="MCERA", state="CA")); s.commit()
    d = _doc(s)
    s.add(DocumentSectionRead(
        document_id=d.id, offset=1,
        returns_json=_payload([{"asset_class": "US Equity", "return_pct": 1.0,
                                "period": "1-Year"}])))
    s.commit()

    bpv.write_observations(s, bpv.load_class_map())
    bpv.write_observations(s, bpv.load_class_map())
    assert s.query(PerformanceObservation).count() == 1
    s.close()


def test_a_document_reporting_the_same_class_twice_keeps_both(tmp_db):
    """Not deduped: two readings of one class is a disagreement worth seeing,
    and collapsing it silently picks a winner nobody chose."""
    s = database.get_session()
    s.add(Plan(id="mcera", name="MCERA", state="CA")); s.commit()
    d = _doc(s)
    s.add(DocumentSectionRead(
        document_id=d.id, offset=1,
        returns_json=_payload([
            {"asset_class": "US Equity", "return_pct": 12.4, "period": "1-Year"},
            {"asset_class": "US Equity", "return_pct": 12.9, "period": "1-Year"},
        ])))
    s.commit()
    bpv.write_observations(s, bpv.load_class_map())
    assert s.query(PerformanceObservation).count() == 2
    s.close()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `LLM_MODE=mock python -m pytest tests/test_performance_observations.py -q`
Expected: FAIL with `AttributeError: module 'scripts.build_performance_view' has no attribute 'write_observations'`

- [ ] **Step 3: Write the write path**

Add to `scripts/build_performance_view.py`, after `observations_from_payload`:

```python
def write_observations(session, class_map) -> dict[str, int]:
    """Record every extracted number as a row, then return counts by source.

    Runs over the whole corpus every time, which is affordable because it
    reads only the JSON columns -- never documents.extracted_text, which is
    deferred and gzipped precisely because bulk-loading it exhausted Neon's
    transfer quota on 2026-08-25.

    Idempotence is delete-then-insert per (document_id, source) rather than a
    unique constraint, because duplicates within one document are real and
    worth keeping (see the table's docstring). Deleting by source means the
    CAFR rows for a document survive a summariser rewrite and vice versa.
    """
    counts: dict[str, int] = {}
    batches: list[tuple[int | None, str, list[dict]]] = []

    reads = (session.query(DocumentSectionRead.returns_json,
                           DocumentSectionRead.offset,
                           DocumentSectionRead.model,
                           Document.plan_id, Document.meeting_date, Document.id)
             .join(Document, Document.id == DocumentSectionRead.document_id)
             .filter(DocumentSectionRead.returns_json.isnot(None),
                     DocumentSectionRead.returns_json.notin_(("[]", ""))))
    for payload, offset, model, plan_id, when, doc_id in reads:
        batches.append((doc_id, "targeted_read", observations_from_payload(
            payload, plan_id, when, doc_id, class_map, "targeted_read",
            offset=offset, model=model)))

    sums = (session.query(Summary.performance_data, Summary.model_used,
                          Document.plan_id, Document.meeting_date, Document.id)
            .join(Document, Document.id == Summary.document_id)
            .filter(Summary.performance_data.isnot(None),
                    Summary.performance_data.notin_(("[]", ""))))
    for payload, model, plan_id, when, doc_id in sums:
        batches.append((doc_id, "board_doc", observations_from_payload(
            payload, plan_id, when, doc_id, class_map, "board_doc",
            model=model)))

    touched: set[tuple[int | None, str]] = set()
    for doc_id, source, rows in batches:
        if (doc_id, source) not in touched:
            (session.query(PerformanceObservation)
             .filter(PerformanceObservation.document_id == doc_id,
                     PerformanceObservation.source == source)
             .delete(synchronize_session=False))
            touched.add((doc_id, source))
        for r in rows:
            session.add(PerformanceObservation(**r))
            counts[source] = counts.get(source, 0) + 1
    session.commit()
    return counts
```

Add `PerformanceObservation` to the `from database import ...` line at the top of the module (it currently imports `DocumentSectionRead, PlanAssetClassHorizon` among others, around line 44).

- [ ] **Step 4: Call it from `main()`**

In `main()`, immediately after `class_map = load_class_map()` and inside the `try:` block, before `cafr = collect_from_cafr(...)`:

```python
        obs_counts = write_observations(session, class_map)
        console.print(
            f"{sum(obs_counts.values()):,} observations recorded "
            f"({obs_counts.get('targeted_read', 0):,} targeted reads, "
            f"{obs_counts.get('board_doc', 0):,} summariser)")
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `LLM_MODE=mock python -m pytest tests/test_performance_observations.py -q`
Expected: PASS (12 tests)

- [ ] **Step 6: Run the real build and check the count**

Run: `python -m scripts.build_performance_view`
Expected: a line reading roughly `66,000 observations recorded (53,000 targeted reads, 12,500 summariser)`, then the existing view-build output unchanged — `~1,236 rows across 133 plans` and `~7,999 per-asset-class-horizon cells`. If the cell count moved, something in this task touched the view path and should not have.

- [ ] **Step 7: Commit**

```bash
git add scripts/build_performance_view.py tests/test_performance_observations.py
git commit -m "Record every extracted figure as a durable observation"
```

---

### Task 4: The peer-dispersion check

**Files:**
- Create: `scripts/check_performance.py`
- Test: `tests/test_check_performance.py`

**Interfaces:**
- Consumes: `database.PerformanceObservation`, `database.PerformanceFlag`.
- Produces: `peer_dispersion(session, min_peers: int = 8, z: float = 6.0) -> list[dict]`, each dict `{"check_name", "observation_id", "plan_id", "asset_class", "horizon_key", "period_end", "value", "expected", "detail"}`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_check_performance.py
"""Checks over the fact table. Arithmetic only -- no API calls anywhere.

Peer dispersion is the load-bearing one: plans holding the same asset class
over the same quarter earn similar returns, so a robust z-score against the
peer median finds parse errors with no ground truth. Prototyped against the
live table on 2026-09-03: 203 groups qualified, 82 of ~7,999 cells flagged,
and the top of the queue was unambiguous -- a 110% five-year return, cash
losing 11.6% in a year.
"""
from datetime import date

import database
from database import PerformanceObservation, Plan
from scripts import check_performance as cp


def _obs(s, plan_id, ret, asset_class="public_equity_us",
         horizon_key="annual", period_end="2025Q4", benchmark_pct=None):
    o = PerformanceObservation(
        plan_id=plan_id, source="targeted_read", asset_class=asset_class,
        asset_class_raw=asset_class, horizon_key=horizon_key,
        period_end=period_end, period_label="1-Year",
        as_of_date=date(2026, 2, 1), return_pct=ret,
        benchmark_pct=benchmark_pct)
    s.add(o)
    return o


def _plans(s, n):
    for i in range(n):
        s.add(Plan(id=f"p{i}", name=f"Plan {i}", state="CA"))
    s.commit()


def test_an_outlier_among_peers_is_flagged(tmp_db):
    s = database.get_session()
    _plans(s, 10)
    for i in range(9):
        _obs(s, f"p{i}", 8.0 + i * 0.1)
    bad = _obs(s, "p9", 110.0)
    s.commit()

    hits = cp.peer_dispersion(s)
    assert len(hits) == 1
    assert hits[0]["plan_id"] == "p9"
    assert hits[0]["value"] == 110.0
    assert 8.0 <= hits[0]["expected"] <= 8.9
    assert hits[0]["check_name"] == "peer_dispersion"
    assert hits[0]["observation_id"] == bad.id
    s.close()


def test_a_tight_group_flags_nothing(tmp_db):
    s = database.get_session()
    _plans(s, 10)
    for i in range(10):
        _obs(s, f"p{i}", 8.0 + i * 0.1)
    s.commit()
    assert cp.peer_dispersion(s) == []
    s.close()


def test_a_small_group_is_not_judged(tmp_db):
    """Dispersion needs peers. Four plans is not a distribution, and flagging
    against it manufactures work rather than finding errors."""
    s = database.get_session()
    _plans(s, 4)
    for i in range(3):
        _obs(s, f"p{i}", 8.0)
    _obs(s, "p3", 110.0)
    s.commit()
    assert cp.peer_dispersion(s) == []
    s.close()


def test_groups_are_split_by_horizon_and_quarter(tmp_db):
    """A 1-year and a 10-year return are not peers, and neither are two
    quarters. Pooling them is how a correct figure gets flagged."""
    s = database.get_session()
    _plans(s, 10)
    for i in range(9):
        _obs(s, f"p{i}", 8.0, horizon_key="annual")
    # Same value, different horizon: alone in its group, so unjudged.
    _obs(s, "p9", 40.0, horizon_key="10y")
    s.commit()
    assert cp.peer_dispersion(s) == []
    s.close()


def test_zero_dispersion_does_not_divide_by_zero(tmp_db):
    """MAD is 0 when every peer agrees exactly. A naive z blows up."""
    s = database.get_session()
    _plans(s, 10)
    for i in range(9):
        _obs(s, f"p{i}", 8.0)
    _obs(s, "p9", 8.0)
    s.commit()
    assert cp.peer_dispersion(s) == []
    s.close()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `LLM_MODE=mock python -m pytest tests/test_check_performance.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'scripts.check_performance'`

- [ ] **Step 3: Write the check module**

Create `scripts/check_performance.py`:

```python
"""Find extracted figures that are probably wrong.

    python -m scripts.check_performance              # report, write nothing
    python -m scripts.check_performance --record     # write performance_flag

Runs over performance_observation. **No API calls** -- every check here is
arithmetic over stored rows, and any version of this that needs a model has
lost the plot.

Findings, not test failures. A red CI run is the wrong channel for "one plan's
private equity looks odd this quarter", so results land in a table and an
Admin queue. Nothing here edits or deletes an observation: a silent correction
is the same failure as a silent wrong number, minus the evidence.
"""
from __future__ import annotations

import argparse
import statistics
import sys

from rich.console import Console

import database
from database import PerformanceFlag, PerformanceObservation

console = Console(legacy_windows=False)

# Below this, a "peer group" is a handful of plans and the median is noise.
MIN_PEERS = 8
# Robust z above which a figure is worth a human look. 6 was chosen against
# the live table: it flags ~1% of cells, which is a queue someone can read.
Z_THRESHOLD = 6.0


def peer_dispersion(session, min_peers: int = MIN_PEERS,
                    z: float = Z_THRESHOLD) -> list[dict]:
    """Figures far from their peers in the same asset class, horizon, quarter.

    Public plans holding the same asset class over the same quarter earn
    similar returns -- dispersion is real but bounded -- so the peer median is
    a usable expectation with no ground truth anywhere.

    Median and MAD rather than mean and standard deviation, because the thing
    being detected is exactly what would wreck a mean: one value at 110% drags
    the mean up and inflates the standard deviation, so the outlier hides
    itself. The 1.4826 factor puts MAD on the same scale as a standard
    deviation for normally distributed data.
    """
    rows = (session.query(PerformanceObservation)
            .filter(PerformanceObservation.return_pct.isnot(None),
                    PerformanceObservation.asset_class.isnot(None),
                    PerformanceObservation.horizon_key.isnot(None),
                    PerformanceObservation.period_end.isnot(None))
            .all())

    groups: dict[tuple, list] = {}
    for o in rows:
        groups.setdefault(
            (o.asset_class, o.horizon_key, o.period_end), []).append(o)

    out: list[dict] = []
    for (asset_class, horizon, quarter), members in groups.items():
        # One observation per plan: a plan reporting a class twice would
        # otherwise vote twice and skew its own peer median.
        by_plan: dict[str, PerformanceObservation] = {}
        for o in members:
            by_plan.setdefault(o.plan_id, o)
        if len(by_plan) < min_peers:
            continue

        values = [o.return_pct for o in by_plan.values()]
        median = statistics.median(values)
        mad = statistics.median([abs(v - median) for v in values])
        if mad <= 0:
            # Every peer agrees exactly. Either the group is degenerate or the
            # data is duplicated; flagging on a zero scale would flag anything
            # that differs by any amount at all.
            continue
        scale = 1.4826 * mad

        for o in by_plan.values():
            score = abs(o.return_pct - median) / scale
            if score <= z:
                continue
            out.append({
                "check_name": "peer_dispersion",
                "observation_id": o.id,
                "plan_id": o.plan_id,
                "asset_class": asset_class,
                "horizon_key": horizon,
                "period_end": quarter,
                "value": o.return_pct,
                "expected": round(median, 4),
                "detail": (f"robust z={score:.1f} against {len(by_plan)} "
                           f"peers (median {median:.2f}%)")[:300],
            })
    out.sort(key=lambda h: -abs(h["value"] - h["expected"]))
    return out


CHECKS = {"peer_dispersion": peer_dispersion}


def record(session, hits: list[dict]) -> int:
    """Store findings, skipping any already open for the same observation.

    Re-flagging a cell a human has already resolved is how a queue becomes
    noise nobody reads.
    """
    open_keys = {
        (f.check_name, f.observation_id)
        for f in session.query(PerformanceFlag)
                        .filter(PerformanceFlag.resolved_at.is_(None)).all()}
    n = 0
    for h in hits:
        if (h["check_name"], h["observation_id"]) in open_keys:
            continue
        session.add(PerformanceFlag(**h))
        n += 1
    session.commit()
    return n


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--record", action="store_true",
                    help="write findings to performance_flag")
    ap.add_argument("--check", choices=sorted(CHECKS),
                    help="run one check instead of all")
    args = ap.parse_args()

    session = database.SessionLocal()
    try:
        names = [args.check] if args.check else sorted(CHECKS)
        all_hits: list[dict] = []
        for name in names:
            hits = CHECKS[name](session)
            all_hits += hits
            console.print(f"[bold]{name}[/bold]: {len(hits)} flagged")
            for h in hits[:10]:
                console.print(
                    f"   {h['plan_id']:14s} {str(h['asset_class'])[:20]:20s} "
                    f"{str(h['horizon_key']):8s} {h['period_end']}  "
                    f"[red]{h['value']:9.2f}%[/red] vs {h['expected']:.2f}%")
        if args.record:
            console.print(f"\nrecorded {record(session, all_hits)} new flags")
        else:
            console.print("\n[yellow]Nothing written. "
                          "Re-run with --record to store findings.[/yellow]")
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `LLM_MODE=mock python -m pytest tests/test_check_performance.py -q`
Expected: PASS (5 tests)

- [ ] **Step 5: Run it against the live table**

Run: `python -m scripts.check_performance`
Expected: `peer_dispersion: ~82 flagged`, with `persi_id total 5y 2023Q4 110.00% vs 8.23%` near the top. Nothing written.

- [ ] **Step 6: Commit**

```bash
git add scripts/check_performance.py tests/test_check_performance.py
git commit -m "Peer-dispersion check over the observation table"
```

---

### Task 5: The benchmark and range checks

**Files:**
- Modify: `scripts/check_performance.py` (add two functions, register both in `CHECKS`)
- Test: `tests/test_check_performance.py`

**Interfaces:**
- Consumes: everything from Task 4.
- Produces: `benchmark_gap(session, max_gap: float = 30.0) -> list[dict]` and `range_gate(session) -> list[dict]`, both returning the same dict shape as `peer_dispersion`. `CHECKS` gains keys `"benchmark_gap"` and `"range_gate"`.

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_check_performance.py

def test_a_return_far_from_its_own_benchmark_is_flagged(tmp_db):
    """The free check the horizon table throws away: 27% of observations
    carry a benchmark, and a return beside it is the strongest signal there
    is that the number was read correctly."""
    s = database.get_session()
    _plans(s, 2)
    _obs(s, "p0", 12.4, benchmark_pct=12.0)     # fine
    _obs(s, "p1", 110.0, benchmark_pct=8.0)     # not fine
    s.commit()
    hits = cp.benchmark_gap(s)
    assert [h["plan_id"] for h in hits] == ["p1"]
    assert hits[0]["expected"] == 8.0
    assert hits[0]["check_name"] == "benchmark_gap"
    s.close()


def test_an_observation_without_a_benchmark_is_not_judged(tmp_db):
    s = database.get_session()
    _plans(s, 1)
    _obs(s, "p0", 110.0, benchmark_pct=None)
    s.commit()
    assert cp.benchmark_gap(s) == []
    s.close()


def test_impossible_values_are_flagged_by_horizon(tmp_db):
    """Bounds differ by horizon: +60% in a quarter is impossible, +60% over
    a year is merely extraordinary, and a 10-year annualised +60% is a parse
    error every time."""
    s = database.get_session()
    _plans(s, 4)
    _obs(s, "p0", 60.0, horizon_key="quarter")   # out of range
    _obs(s, "p1", 60.0, horizon_key="annual")    # in range
    _obs(s, "p2", 60.0, horizon_key="10y")       # out of range
    _obs(s, "p3", 8.0, horizon_key="10y")        # in range
    s.commit()
    flagged = {h["plan_id"] for h in cp.range_gate(s)}
    assert flagged == {"p0", "p2"}
    s.close()


def test_the_range_gate_catches_cash_losing_money(tmp_db):
    """sers_oh reported cash at -11.6% for a year. Cash does not do that."""
    s = database.get_session()
    _plans(s, 1)
    _obs(s, "p0", -11.6, asset_class="cash_short_term", horizon_key="annual")
    s.commit()
    hits = cp.range_gate(s)
    assert len(hits) == 1 and hits[0]["value"] == -11.6
    s.close()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `LLM_MODE=mock python -m pytest tests/test_check_performance.py -q`
Expected: FAIL with `AttributeError: module 'scripts.check_performance' has no attribute 'benchmark_gap'`

- [ ] **Step 3: Write the two checks**

Add to `scripts/check_performance.py`, after `peer_dispersion`:

```python
# Plausible bounds per horizon, in percent. Deliberately wide: the job is to
# catch index levels, market values and basis points misread as returns, not
# to second-guess a real bad quarter. An annualised figure narrows as the
# window lengthens because averaging does that -- a 10-year annualised return
# outside +/-25% has never happened to a US public plan.
RANGE_BY_HORIZON = {
    "month": (-25.0, 25.0),
    "quarter": (-40.0, 40.0),
    "partial": (-50.0, 50.0),
    "annual": (-80.0, 80.0),
    "3y": (-40.0, 40.0),
    "5y": (-30.0, 30.0),
    "10y": (-25.0, 25.0),
    "20y": (-20.0, 20.0),
    "30y": (-20.0, 20.0),
    "inception": (-20.0, 30.0),
}

# Cash is the one asset class with a bound tighter than its horizon's. A
# short-term portfolio tracking policy rates cannot lose double digits.
CASH_RANGE = (-2.0, 15.0)


def benchmark_gap(session, max_gap: float = 30.0) -> list[dict]:
    """Returns implausibly far from the benchmark reported beside them.

    The cheapest check available and, until the fact table existed, an
    impossible one: the extractors capture benchmark_pct on 27% of
    observations and plan_asset_class_horizon has no column for it.

    A wide threshold on purpose. Real excess return reaches several points and
    private-market benchmarks lag by a quarter, so anything under 30pp is
    plan business. Beyond that, the two numbers did not come from the same
    row of the same table.
    """
    rows = (session.query(PerformanceObservation)
            .filter(PerformanceObservation.return_pct.isnot(None),
                    PerformanceObservation.benchmark_pct.isnot(None))
            .all())
    out = []
    for o in rows:
        gap = abs(o.return_pct - o.benchmark_pct)
        if gap <= max_gap:
            continue
        out.append({
            "check_name": "benchmark_gap",
            "observation_id": o.id,
            "plan_id": o.plan_id,
            "asset_class": o.asset_class,
            "horizon_key": o.horizon_key,
            "period_end": o.period_end,
            "value": o.return_pct,
            "expected": o.benchmark_pct,
            "detail": (f"{gap:.1f}pp from its own reported benchmark "
                       f"({o.period_label or 'no period'})")[:300],
        })
    out.sort(key=lambda h: -abs(h["value"] - h["expected"]))
    return out


def range_gate(session) -> list[dict]:
    """Values outside what a return can be over that horizon.

    Catches the failure the LLM paths actually make: a market value, an index
    level or a basis-point figure read into a percentage column. Needs no
    peers, so unlike peer_dispersion it works on a thinly covered asset class.
    """
    rows = (session.query(PerformanceObservation)
            .filter(PerformanceObservation.return_pct.isnot(None),
                    PerformanceObservation.horizon_key.isnot(None))
            .all())
    out = []
    for o in rows:
        bounds = RANGE_BY_HORIZON.get(o.horizon_key)
        if bounds is None:
            continue
        if o.asset_class == "cash_short_term" and o.horizon_key != "quarter":
            bounds = CASH_RANGE
        low, high = bounds
        if low <= o.return_pct <= high:
            continue
        out.append({
            "check_name": "range_gate",
            "observation_id": o.id,
            "plan_id": o.plan_id,
            "asset_class": o.asset_class,
            "horizon_key": o.horizon_key,
            "period_end": o.period_end,
            "value": o.return_pct,
            "expected": high if o.return_pct > high else low,
            "detail": (f"outside {low:g}%..{high:g}% for horizon "
                       f"{o.horizon_key}")[:300],
        })
    out.sort(key=lambda h: -abs(h["value"]))
    return out
```

Then replace the `CHECKS` mapping:

```python
CHECKS = {
    "peer_dispersion": peer_dispersion,
    "benchmark_gap": benchmark_gap,
    "range_gate": range_gate,
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `LLM_MODE=mock python -m pytest tests/test_check_performance.py -q`
Expected: PASS (9 tests)

- [ ] **Step 5: Run all three against the live table**

Run: `python -m scripts.check_performance`
Expected: three sections. `range_gate` should independently catch `persi_id` at 110% and `sers_oh` cash at −11.6%, which peer dispersion also found — agreement between two checks on the same rows is the result to want, not duplication to remove.

- [ ] **Step 6: Commit**

```bash
git add scripts/check_performance.py tests/test_check_performance.py
git commit -m "Benchmark-gap and range-gate checks"
```

---

### Task 6: Record findings and show the queue

**Files:**
- Modify: `.github/workflows/daily-pipeline.yml` (add a step after the derived-data rebuild)
- Modify: `app.py` (add a sub-tab to the Admin page)
- Test: `tests/test_check_performance.py`

**Interfaces:**
- Consumes: `record(session, hits) -> int` and `CHECKS` from Task 4.
- Produces: `queries.open_performance_flags(session, limit: int = 200) -> list[dict]` with keys `Check, Plan, Asset class, Horizon, Period end, Value, Expected, Detail, Flagged`.

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_check_performance.py
from database import PerformanceFlag
import queries


def test_recording_is_idempotent_for_an_open_flag(tmp_db):
    s = database.get_session()
    _plans(s, 10)
    for i in range(9):
        _obs(s, f"p{i}", 8.0 + i * 0.1)
    _obs(s, "p9", 110.0)
    s.commit()

    hits = cp.peer_dispersion(s)
    assert cp.record(s, hits) == 1
    assert cp.record(s, hits) == 0, "re-flagged an already-open finding"
    assert s.query(PerformanceFlag).count() == 1
    s.close()


def test_a_resolved_flag_is_not_reopened(tmp_db):
    """A human decided. Re-flagging turns the queue into noise."""
    from datetime import datetime, timezone
    s = database.get_session()
    _plans(s, 10)
    for i in range(9):
        _obs(s, f"p{i}", 8.0 + i * 0.1)
    _obs(s, "p9", 110.0)
    s.commit()

    hits = cp.peer_dispersion(s)
    cp.record(s, hits)
    f = s.query(PerformanceFlag).one()
    f.resolved_at = datetime.now(timezone.utc)
    f.resolution = "confirmed_ok"
    s.commit()

    cp.record(s, hits)
    assert s.query(PerformanceFlag).count() == 2, (
        "a resolved flag should not block a fresh one; the queue view hides "
        "resolved rows instead")
    s.close()


def test_the_queue_shows_open_flags_newest_first(tmp_db):
    s = database.get_session()
    _plans(s, 10)
    for i in range(9):
        _obs(s, f"p{i}", 8.0 + i * 0.1)
    _obs(s, "p9", 110.0)
    s.commit()
    cp.record(s, cp.peer_dispersion(s))

    rows = queries.open_performance_flags(s)
    assert len(rows) == 1
    assert rows[0]["Plan"] == "p9"
    assert rows[0]["Check"] == "peer_dispersion"
    assert rows[0]["Value"] == 110.0
    s.close()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `LLM_MODE=mock python -m pytest tests/test_check_performance.py -q`
Expected: FAIL with `AttributeError: module 'queries' has no attribute 'open_performance_flags'`

- [ ] **Step 3: Add the query**

Add to `queries.py`:

```python
def open_performance_flags(session, limit: int = 200) -> list[dict]:
    """Unresolved data-quality findings, newest first.

    Resolved rows are hidden rather than deleted: what a check found and what
    a human concluded are both worth keeping, and re-running the checks must
    not look like the problem came back.
    """
    from database import PerformanceFlag

    rows = (session.query(PerformanceFlag)
            .filter(PerformanceFlag.resolved_at.is_(None))
            .order_by(PerformanceFlag.flagged_at.desc(),
                      PerformanceFlag.id.desc())
            .limit(limit).all())
    return [{
        "Check": f.check_name,
        "Plan": f.plan_id,
        "Asset class": f.asset_class,
        "Horizon": f.horizon_key,
        "Period end": f.period_end,
        "Value": f.value,
        "Expected": f.expected,
        "Detail": f.detail,
        "Flagged": f.flagged_at.date().isoformat() if f.flagged_at else None,
    } for f in rows]
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `LLM_MODE=mock python -m pytest tests/test_check_performance.py -q`
Expected: PASS (12 tests)

- [ ] **Step 5: Add the Admin sub-tab**

In `app.py`, inside the Admin page's sub-tab list, add a "Data quality" tab rendering:

```python
    flags = queries.open_performance_flags(get_db_session())
    st.caption(
        "Figures a check thinks are wrong. Findings, not failures — nothing "
        "here has been changed or deleted, and a flag stays until someone "
        "resolves it."
    )
    if not flags:
        st.success("No open data-quality flags.")
    else:
        st.metric("Open flags", len(flags))
        st.dataframe(pd.DataFrame(flags), width="stretch", hide_index=True)
```

- [ ] **Step 6: Wire it into the daily pipeline**

In `.github/workflows/daily-pipeline.yml`, after the derived-data rebuild step, add:

```yaml
      - name: Performance data checks
        if: ${{ !cancelled() }}
        env:
          DATABASE_URL: ${{ secrets.DATABASE_URL }}
        run: python -m scripts.check_performance --record
```

`!cancelled()` for the same reason the other post-steps use it: the extractors exit 1 when any single item fails, and one plan's quirk must not skip the checks for the other 149.

- [ ] **Step 7: Run the whole suite**

Run: `LLM_MODE=mock python -m pytest tests/ -q`
Expected: PASS, ~994 passed / 30 skipped (973 before this plan, plus 21 new).

- [ ] **Step 8: Record the real findings and look at them**

Run: `python -m scripts.check_performance --record`
Then start the app and open Admin → Data quality. Expect roughly 80–120 open flags. Read the top ten; if any is a *correct* figure, the thresholds are wrong and that is worth knowing before this runs daily.

- [ ] **Step 9: Commit**

```bash
git add queries.py app.py .github/workflows/daily-pipeline.yml tests/test_check_performance.py
git commit -m "Record data-quality findings and surface the queue"
```

---

## Self-Review

**Spec coverage (D19).** Peer dispersion — Task 4. Benchmark comparison — Task 5, enabled by Task 1 carrying the column. Range gates — Task 5. Where it lives, storage plus Admin queue plus daily wiring — Task 6. Never auto-delete — enforced by `PerformanceFlag` having no write path back to the observation, and stated in both docstrings. **Not covered, and deliberately deferred:** allocation weight sums (needs `allocation_observation`), temporal continuity, cross-source agreement, CAFR-as-ground-truth, and the sampled human audit. Each is one more function in `CHECKS` with the same signature, which is why the registry exists; they are follow-on work rather than gaps in the design.

**Spec coverage (D20).** Fact table with `benchmark_pct`, `asset_class_raw` and `offset` — Task 1. Write path covering the whole corpus — Task 3, and because `--since` defaults to `None` the build already reads every blob, so this doubles as the backfill and no separate script is needed. Checks becoming SQL over one table — Tasks 4 and 5. **Not covered, listed under Scope:** `allocation_observation`, the Parquet export, the MCP server, and rebuilding the horizon view from observations.

**Placeholder scan.** No TBDs. Every code step carries the code. Every test step carries the assertions and the exact command with its expected output.

**Type consistency.** `observations_from_payload` returns dicts whose keys are exactly `PerformanceObservation`'s columns minus `id`/`extracted_at`, which is what lets Task 3 do `PerformanceObservation(**r)`. All three checks return the dict shape `PerformanceFlag(**h)` consumes in `record`. `peer_dispersion`, `benchmark_gap` and `range_gate` share one signature so `CHECKS` can call them uniformly.

**One risk worth naming.** Task 3 adds a full-corpus write to every `build_performance_view` run — ~66,000 rows deleted and reinserted. That is fine against Neon today (the JSON columns are small; nothing touches `extracted_text`), but if the build gets slow, the fix is an incremental write keyed on `documents.id` rather than dropping the observation table. Do not solve that until it is a problem.
