# Datetime Audit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Status (2026-08-20):** COMPLETE. All seven tasks landed; the Postgres CI
job is green, so the TIMESTAMPTZ semantics are verified on a real Postgres
rather than inferred.

**Added during Task 5, not in the original plan:** `database.as_utc()`.
Reads are aware on Postgres and naive on SQLite, so any Python-level
comparison against `utcnow()` needs normalising — and step 4's dual-run
means both shapes are live simultaneously. Six such sites were found; one
(`insights/daily.py`'s reappear trigger) passed all 284 unit tests and
still raised TypeError on the first real cadence run, which is exactly what
Step 4's smoke test exists to catch.

**Goal:** Make every datetime in the codebase timezone-aware UTC — in the schema, in the code, and in the existing data — so the SQLite→Postgres migration (step 3) cannot silently shift or crash on timestamps.

**Architecture:** One source of truth for "now" (`database.utcnow()`), all 58 `DateTime` columns redeclared `DateTime(timezone=True)`, all 81 naive `datetime.utcnow()` call sites converted, and a one-shot backfill that stamps UTC onto the 45 populated columns. Enforced by a shrinking allowlist ("ratchet") test that runs on SQLite, and verified for real semantics by a new CI job against a Postgres service container.

**Tech Stack:** SQLAlchemy 2.x, SQLite (dev/test), Postgres 16 (CI service container, then Neon), pytest, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-08-16-low-maintenance-app-design.md` (§10 Risks, §11 Testing). Working doc: `nextsteps.md`.

## Global Constraints

- All stored datetimes are **UTC**. No local-time value may enter the database.
- `datetime.utcnow()` is banned in project code (deprecated in Python 3.12, and always naive).
- Never write SQL `ALTER TABLE` migrations — add/modify the SQLAlchemy model and call `init_db()`. Existing-row backfill is a one-off script (CLAUDE.md).
- Tests run with `LLM_MODE=mock` and `INSIGHTS_MODE=mock`; `tests/conftest.py` rebinds `database.engine`/`SessionLocal` per test and must not reload the module.
- The suite must stay green at every commit. Baseline at audit time: **263 passed**.

---

# Part 1 — Audit findings (2026-08-19)

Method: schema introspection via `Base.metadata`, value classification over all
rows of `db/pension.db` (post-merge, 67.8 MB), and a comment-stripped scan of
every `.py` outside `.venv/`, `.claude/`, `build/` and `tests/`.

## Finding 1 — The data is uniformly naive. There is no mixture.

| Category | Columns |
|---|---|
| Columns holding **both** naive and aware values | **0** |
| Columns holding only aware values | **0** |
| Columns holding only naive values | **45** |
| Columns with no data yet | 13 |
| **Total `DateTime` columns** | **58** |

This **contradicts the premise in spec §10 and `nextsteps.md`**, which both
describe a "naive/aware mixture" that SQLite renders harmless. There is no
mixture in the data. Every one of the 45 populated columns is 100% naive —
*including the 17 whose column default is the timezone-aware
`database._utcnow`*.

**Why:** SQLAlchemy's SQLite `DATETIME` storage format has no timezone field, so
an aware value is stripped on write. Verified empirically:

```
wrote (aware): 2026-08-20 04:52:06.015924+00:00
  DateTime()              -> 2026-08-20 04:52:06.015924
  DateTime(timezone=True) -> 2026-08-20 04:52:06.015924   <-- flag ignored
  read back, tzinfo:      -> None (both columns)
```

This is better news than the spec assumed — no data normalisation is needed,
only a uniform stamp — but it also means the *code* divergence is completely
invisible today and stays invisible right up to the migration.

## Finding 2 — SQLite ignores `timezone=True`, so the test suite cannot catch any of this.

The second line above is the important one. On SQLite, `DateTime(timezone=True)`
behaves identically to `DateTime()`. Every test in the 263-test suite runs on
SQLite. **No test can distinguish a correct fix from a broken one.**

Spec §11 calls a Postgres CI container "the single most valuable testing
addition in this design". This audit upgrades that from valuable to
**mandatory**: it is the only mechanism that can verify this work. Task 3 below
builds it, before any semantic change is made.

## Finding 3 — All existing values really are UTC, so the backfill is safe.

Searched for local-time writers: `datetime.now()` with no argument, and
`date.today()`/`datetime.today()` feeding a DB write.

- **Zero** naive `datetime.now()` calls in project code.
- Every `datetime.now(...)` call passes `timezone.utc` explicitly.
- The only `.today()` calls are 4 period selectors in `insights/{annual,monthly,quarterly,weekly}.py`, which choose a briefing period, not a stored value.

**Consequence:** the 45 populated columns can be stamped `UTC` wholesale. No
per-column offset analysis, no ambiguity, no DST correction. This is the single
biggest risk that turned out not to exist.

*(Minor, separate: those 4 `date.today()` calls read the runner's local date. On
GHA that is UTC; run locally near midnight they could select the adjacent
period. Logged in Part 2 as a non-blocking follow-up.)*

## Finding 4 — Four functions named `_utcnow`, two opposite meanings.

| Definition | Returns |
|---|---|
| `database.py:66` | `datetime.now(timezone.utc)` — **aware** |
| `refresh_recordings.py:133` | `datetime.now(timezone.utc).replace(tzinfo=None)` — **naive** |
| `notify_new_recordings.py:43` | same — **naive** |
| `download_recordings.py:81` | same — **naive** |

A reader who sees `_utcnow()` in `refresh_recordings.py` and assumes it matches
`database._utcnow` is wrong. The three recordings modules deliberately strip the
offset to stay consistent with what SQLite stored anyway. Two more sites strip
it the same way (`refresh_recordings.py:209`,
`scripts/hydrate_recording_metadata.py:76`).

These 5 strippers are the sites most likely to be "fixed" incorrectly by a
careless sweep, because the stripping looks intentional — it *was* intentional,
and it becomes wrong at migration.

## Finding 5 — Schema: 58 `DateTime` columns, none timezone-aware.

```
timezone=True:  0 of 58
defaults:       17 aware (_utcnow) | 4 naive (utcnow) | 37 none (set in code)
```

The 4 naive defaults are `approval_tokens.created_at`, `subscribers.created_at`,
`subscriber_tokens.created_at`, `weekly_runs.started_at`
(`database.py:415,468,498,516`).

Note `approval_tokens`, `subscribers` and `subscriber_tokens` are all
**unused-but-retained** tables (CLAUDE.md: the approval gate is gone; the table
stays as an audit trail). They still need fixing — `init_db()` creates them on
Postgres regardless — but they carry no behavioural risk.

## Finding 6 — Code: 81 naive call sites across 39 files.

| Kind | Sites | Risk |
|---|---|---|
| **(a) Writes to a `DateTime` column** | **31** | Wrong type reaches Postgres |
| (b) Cutoffs / arithmetic vs DB values | ~20 | `TypeError` once reads return aware |
| (c) Display strings (`strftime`) | ~30 | Cosmetic; convert for the ban |

Concentration: `insights/subscribers.py` (10), `generate_notes.py` (10),
`insights/weekly.py` (6), `insights/compose.py` (5), `twin_builder.py` (4),
`pipeline.py` (4), `insights/config.py` (4).

Category (b) is the one that bites *after* the fix rather than before: today a
naive cutoff compares fine against naive DB values. The moment columns become
`TIMESTAMPTZ` and reads return aware values, every one of those comparisons
raises `TypeError: can't subtract offset-naive and offset-aware datetimes`.
**They must be converted in the same commit as the column-type change**, which
is why Task 5 is not split by module.

## Finding 7 — The correct pattern already exists in the repo.

`extract_cafr_actuarial.py:453` and `extract_ips.py:270` (the two extractors
fixed on 2026-08-15) omit the field entirely and let the aware column default
fire, with a comment explaining why. That is the target idiom for insert paths.
Those two files mention `utcnow` only inside comments and are correctly excluded
from the offender list.

---

# Part 2 — Decisions and follow-ups

**DECIDED 2026-08-19: `TIMESTAMPTZ`.** All 58 columns become
`DateTime(timezone=True)`, per spec §10's "Postgres will not discard it".

The alternative considered and rejected — leaving all 58 columns naive on
Postgres — was less work and would have preserved current behaviour exactly, but
it bakes in a schema that cannot represent when anything actually happened, and
pushes the same migration cost onto whoever adds the first non-UTC data source.

Task 4 is unblocked. The whole plan is now executable end to end.

**Non-blocking follow-ups (not in this plan):**
- The 4 `date.today()` period selectors (Finding 3) should take an injected `now` for testability.
- `insights/config.py`'s 4 helpers already accept an optional `now` — a good pattern to spread.

**Deliberately out of scope.** Spec §10 lists three dialect risks; this plan
addresses only the first. Autoincrement id preservation and `LENGTH()` over
gzipped `BYTEA` are properties of the migration script itself, not of datetime
handling, and belong to step 3. They are called out here so a reader does not
mistake their absence for an oversight.

---

# Part 3 — The plan

## File structure

| File | Responsibility |
|---|---|
| `database.py` | Modify: public `utcnow()`; all 58 columns → `DateTime(timezone=True)`; 4 naive defaults |
| `tests/test_datetime_discipline.py` | Create: the ratchet + schema guard (runs on SQLite) |
| `tests/postgres/conftest.py` | Create: Postgres engine fixture, skips when `TEST_POSTGRES_URL` unset |
| `tests/postgres/test_tz_semantics.py` | Create: the only tests that can verify aware round-trips |
| `.github/workflows/test.yml` | Modify: add a `postgres` job with a service container |
| `scripts/backfill_utc_datetimes.py` | Create: one-shot, idempotent UTC stamp for the 45 populated columns |
| ~39 project modules | Modify: convert 81 call sites |

## Task 1: The ratchet — freeze the offender list

**Files:**
- Create: `tests/test_datetime_discipline.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `KNOWN_OFFENDERS: set[str]` — later tasks delete entries from it. `_scan_offenders() -> dict[str, list[int]]`.

- [x] **Step 1: Write the test (it passes immediately — it encodes today's state)**

```python
"""Datetime discipline: UTC-aware everywhere, enforced by a shrinking list.

SQLite cannot verify timezone semantics (it ignores DateTime(timezone=True)
entirely), so these are static checks. Real round-trip semantics are tested
in tests/postgres/, which only runs when TEST_POSTGRES_URL is set.
"""
from __future__ import annotations

import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
SKIP_DIRS = {".venv", ".claude", "build", "tests", "node_modules",
             "__pycache__", "tmp", "db"}

# Every file that still calls the banned naive constructor. This list may only
# ever SHRINK. Delete an entry when you convert that file; the ratchet test
# fails if you leave a stale entry behind.
KNOWN_OFFENDERS = {
    "app.py", "backfill_downloads.py", "cafr_year_check.py", "database.py",
    "discover_video_sources.py", "export_cafr_summaries.py",
    "extract_cafr_investments.py", "extractor.py", "fetch_cafr.py",
    "fetcher.py", "generate_notes.py", "insights/approval.py",
    "insights/compose.py", "insights/config.py", "insights/cycle_common.py",
    "insights/daily.py", "insights/subscribers.py", "insights/weekly.py",
    "pipeline.py", "publish_notes.py", "queries.py", "refresh_cafrs.py",
    "refresh_ips.py", "retry_asrs.py", "run_report.py",
    "scripts/backfill_april_monthly.py", "scripts/backfill_extraction_details.py",
    "scripts/backfill_pruned_documents.py", "scripts/build_manager_roster.py",
    "scripts/cleanup_video_sources.py", "scripts/hydrate_recording_metadata.py",
    "scripts/notify_failure.py", "scripts/probe_scrape.py",
    "scripts/prune_pre_2026_docs.py", "scripts/prune_pre_2026_failed_docs.py",
    "scripts/send_publication_notice.py", "scripts/send_test_email.py",
    "summarizer.py", "twin_builder.py",
}


def _scan_offenders() -> dict[str, list[int]]:
    """Files calling datetime.utcnow(), ignoring comment-only mentions."""
    found: dict[str, list[int]] = {}
    for path in sorted(ROOT.rglob("*.py")):
        rel = path.relative_to(ROOT).as_posix()
        if any(part in SKIP_DIRS for part in path.relative_to(ROOT).parts):
            continue
        hits = [
            i for i, line in enumerate(
                path.read_text(encoding="utf-8", errors="replace").splitlines(), 1)
            if not line.lstrip().startswith("#") and "datetime.utcnow()" in line
        ]
        if hits:
            found[rel] = hits
    return found


def test_no_new_naive_utcnow_callers():
    """datetime.utcnow() is naive and deprecated in 3.12. No NEW file may use it."""
    new = set(_scan_offenders()) - KNOWN_OFFENDERS
    assert not new, (
        "New file(s) using naive datetime.utcnow(): " + ", ".join(sorted(new))
        + "\nUse database.utcnow() instead — it returns an aware UTC datetime.")


def test_offender_list_has_no_stale_entries():
    """The ratchet only turns one way: a converted file must leave the list."""
    stale = KNOWN_OFFENDERS - set(_scan_offenders())
    assert not stale, (
        "These files no longer call datetime.utcnow() — delete them from "
        "KNOWN_OFFENDERS: " + ", ".join(sorted(stale)))
```

- [x] **Step 2: Run it — both must pass, proving the list matches reality**

Run: `LLM_MODE=mock INSIGHTS_MODE=mock python -m pytest tests/test_datetime_discipline.py -v`
Expected: 2 passed. If `test_offender_list_has_no_stale_entries` fails, the list drifted — correct it to match the scan output.

- [x] **Step 3: Commit**

```bash
git add tests/test_datetime_discipline.py
git commit -m "Freeze the naive-utcnow offender list as a shrinking ratchet"
```

## Task 2: One source of truth for "now"

**Files:**
- Modify: `database.py:66-68`
- Modify: `tests/test_datetime_discipline.py`

**Interfaces:**
- Produces: `database.utcnow() -> datetime` (aware, UTC). `database._utcnow` retained as an alias so the 17 existing column defaults need no edit.

- [x] **Step 1: Write the failing test** (append to `tests/test_datetime_discipline.py`)

```python
def test_database_utcnow_is_public_and_aware():
    import database
    assert database.utcnow().tzinfo is not None, "utcnow() must be aware"
    assert database.utcnow().utcoffset().total_seconds() == 0, "must be UTC"
    assert database._utcnow is database.utcnow, "_utcnow must alias utcnow"


def test_no_module_defines_its_own_utcnow():
    """Four functions named _utcnow with two opposite meanings caused this bug.

    refresh_recordings/notify_new_recordings/download_recordings each defined a
    _utcnow() that STRIPPED the offset, the opposite of database._utcnow.
    """
    offenders = [
        path.relative_to(ROOT).as_posix()
        for path in sorted(ROOT.rglob("*.py"))
        if not any(p in SKIP_DIRS for p in path.relative_to(ROOT).parts)
        and path.name != "database.py"
        and "def _utcnow" in path.read_text(encoding="utf-8", errors="replace")
    ]
    assert not offenders, (
        "Local _utcnow definitions shadow database.utcnow(): " + ", ".join(offenders))
```

- [x] **Step 2: Run to verify it fails**

Run: `LLM_MODE=mock INSIGHTS_MODE=mock python -m pytest tests/test_datetime_discipline.py -v`
Expected: FAIL — `AttributeError: module 'database' has no attribute 'utcnow'`, and the second test lists 3 files.

- [x] **Step 3: Add the public name in `database.py`**

Replace lines 66-68:

```python
def utcnow() -> datetime:
    """Current UTC time, timezone-aware.

    The single source of truth for "now" across the codebase. Never use
    datetime.utcnow(): it is naive (so it cannot survive a Postgres
    TIMESTAMPTZ round-trip) and deprecated since Python 3.12.
    """
    return datetime.now(timezone.utc)


# Retained so the 17 existing column defaults keep working unchanged.
_utcnow = utcnow
```

- [x] **Step 4: Delete the 3 shadowing definitions and repoint their callers**

In `refresh_recordings.py`, `notify_new_recordings.py`, `download_recordings.py`:
delete the local `def _utcnow()` and add `from database import utcnow as _utcnow`
beside the existing `database` imports.

This is safe *only* because every `_utcnow()` result in those three modules is
assigned straight to a column and never compared: `refresh_recordings.py:274,
275, 294, 296`, `download_recordings.py:284`, `notify_new_recordings.py:185,
213`. Verified by inspection — confirm again before editing.

**Do NOT touch the two `_ts_to_dt`-style strippers in this task**
(`refresh_recordings.py:209`, `scripts/hydrate_recording_metadata.py:76`), even
though they look like part of the same change. `refresh_recordings.py:297`
compares their output against a value read from the database:

```python
source.last_checked_at = now
if newest_published and (
    source.last_recording_seen_at is None
    or newest_published > source.last_recording_seen_at   # <-- naive, from the DB
):
```

Make `_ts_to_dt` aware while `last_recording_seen_at` still reads back naive and
this raises `TypeError` immediately — on SQLite, today. It is the Finding 6(b)
hazard in miniature. Both strippers are therefore converted in Task 5, in the
same commit as the column-type change.

- [x] **Step 5: Run the full suite**

Run: `LLM_MODE=mock INSIGHTS_MODE=mock python -m pytest tests/ -q`
Expected: 267 passed (263 baseline + 2 from Task 1 + 2 here). SQLite strips the offsets, so no behaviour changes yet.

- [x] **Step 6: Commit**

```bash
git add database.py refresh_recordings.py notify_new_recordings.py \
        download_recordings.py scripts/hydrate_recording_metadata.py \
        tests/test_datetime_discipline.py
git commit -m "One source of truth for UTC now; delete the three shadowing _utcnow definitions"
```

## Task 3: The Postgres test harness — build it before changing semantics

**Files:**
- Create: `tests/postgres/__init__.py`, `tests/postgres/conftest.py`, `tests/postgres/test_tz_semantics.py`
- Modify: `.github/workflows/test.yml`, `requirements.txt`

**Interfaces:**
- Consumes: `database.Base`, `database.utcnow`.
- Produces: `pg_engine` fixture (a SQLAlchemy `Engine` bound to a throwaway Postgres, schema created via `Base.metadata.create_all`).

**Why now:** Finding 2 — SQLite ignores `timezone=True`, so from Task 4 onward the
existing suite is blind. Without this harness the rest of the plan is unverifiable.
Docker is not installed locally, so these tests **skip** on a dev machine and run in CI.

- [x] **Step 1: Add the driver**

Append to `requirements.txt`:

```
psycopg[binary]>=3.1
```

- [x] **Step 2: Write the fixture**

`tests/postgres/conftest.py`:

```python
"""Postgres-only tests. Skipped unless TEST_POSTGRES_URL is set.

SQLite ignores DateTime(timezone=True), so these are the only tests that can
verify timezone semantics. CI supplies the URL via a service container.
"""
from __future__ import annotations

import os

import pytest
import sqlalchemy as sa

URL = os.environ.get("TEST_POSTGRES_URL")

pytestmark = pytest.mark.skipif(
    not URL, reason="TEST_POSTGRES_URL not set — Postgres tests skipped")


@pytest.fixture()
def pg_engine():
    import database
    engine = sa.create_engine(URL, future=True)
    database.Base.metadata.drop_all(engine)
    database.Base.metadata.create_all(engine)
    yield engine
    database.Base.metadata.drop_all(engine)
    engine.dispose()
```

- [x] **Step 3: Write the failing semantic tests**

`tests/postgres/test_tz_semantics.py`:

```python
from __future__ import annotations

import sqlalchemy as sa

import database


def test_every_datetime_column_is_timestamptz(pg_engine):
    """On Postgres the flag is real: TIMESTAMPTZ vs TIMESTAMP."""
    insp = sa.inspect(pg_engine)
    naive = []
    for table in database.Base.metadata.sorted_tables:
        for col in insp.get_columns(table.name):
            t = col["type"]
            if isinstance(t, sa.DateTime) and not t.timezone:
                naive.append(f"{table.name}.{col['name']}")
    assert not naive, "Columns still TIMESTAMP WITHOUT TIME ZONE: " + ", ".join(naive)


def test_aware_value_round_trips_with_its_offset(pg_engine):
    """The behaviour SQLite cannot reproduce: tzinfo survives the round trip."""
    written = database.utcnow()
    with pg_engine.begin() as conn:
        conn.execute(sa.insert(database.PipelineRun.__table__).values(
            started_at=written, status="test"))
    with pg_engine.connect() as conn:
        read = conn.execute(sa.select(
            database.PipelineRun.__table__.c.started_at)).scalar_one()
    assert read.tzinfo is not None, "Postgres returned a naive datetime"
    assert read == written
    # The comparison that raises TypeError against a naive value:
    assert (database.utcnow() - read).total_seconds() >= 0
```

- [x] **Step 4: Add the CI job**

Append to `.github/workflows/test.yml` (a second job alongside the existing one):

```yaml
  postgres:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_PASSWORD: postgres
          POSTGRES_DB: pension_test
        ports: ['5432:5432']
        options: >-
          --health-cmd pg_isready --health-interval 10s
          --health-timeout 5s --health-retries 5
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'
      - run: pip install -r requirements.txt pytest
      - run: python -m pytest tests/postgres -v
        env:
          LLM_MODE: mock
          INSIGHTS_MODE: mock
          TEST_POSTGRES_URL: postgresql+psycopg://postgres:postgres@localhost:5432/pension_test
```

- [x] **Step 5: Verify the skip works locally, then push for the real run**

Run: `LLM_MODE=mock INSIGHTS_MODE=mock python -m pytest tests/postgres -v`
Expected locally: 2 skipped ("TEST_POSTGRES_URL not set").
Expected in CI after push: `test_every_datetime_column_is_timestamptz` **FAILS**, listing all 58 columns. That failure is the point — it is Task 4's specification.

- [x] **Step 6: Commit**

```bash
git add tests/postgres .github/workflows/test.yml requirements.txt
git commit -m "Add a Postgres CI job — the only harness that can test timezone semantics"
```

## Task 4: Make all 58 columns timezone-aware

**Files:**
- Modify: `database.py` (all `Column(DateTime` declarations, plus 4 naive defaults at lines 415, 468, 498, 516)
- Modify: `tests/test_datetime_discipline.py`

**Unblocked:** `TIMESTAMPTZ` confirmed 2026-08-19 (Part 2).

- [x] **Step 1: Write the failing schema test** (append to `tests/test_datetime_discipline.py`)

```python
def test_all_datetime_columns_declare_timezone():
    """Every timestamp column must be TIMESTAMPTZ on Postgres.

    SQLite ignores this flag, so this metadata check is the only SQLite-side
    guard; tests/postgres asserts the real column type.
    """
    import sqlalchemy as sa
    import database
    naive = [
        f"{name}.{col.name}"
        for name, table in database.Base.metadata.tables.items()
        for col in table.columns
        if isinstance(col.type, sa.DateTime) and not col.type.timezone
    ]
    assert not naive, f"{len(naive)} naive DateTime columns: " + ", ".join(naive)


def test_no_column_default_is_naive():
    import sqlalchemy as sa
    import database
    bad = [
        f"{name}.{col.name}"
        for name, table in database.Base.metadata.tables.items()
        for col in table.columns
        if isinstance(col.type, sa.DateTime)
        and col.default is not None
        and getattr(col.default.arg, "__name__", "") == "utcnow"
        and col.default.arg is not database.utcnow
    ]
    assert not bad, "Columns defaulting to naive datetime.utcnow: " + ", ".join(bad)
```

- [x] **Step 2: Run to verify it fails**

Run: `LLM_MODE=mock INSIGHTS_MODE=mock python -m pytest tests/test_datetime_discipline.py -v`
Expected: FAIL — "58 naive DateTime columns", and 4 naive defaults.

- [x] **Step 3: Rewrite the declarations**

Mechanical and total — every `DateTime` in `database.py` becomes `DateTime(timezone=True)`:

```bash
python - <<'PY'
import pathlib, re
p = pathlib.Path("database.py"); s = p.read_text(encoding="utf-8")
s2 = re.sub(r"Column\(DateTime(?!\()", "Column(DateTime(timezone=True)", s)
s2 = s2.replace("default=datetime.utcnow", "default=utcnow")
p.write_text(s2, encoding="utf-8")
print("rewrote", s.count("Column(DateTime"), "columns")
PY
```

Read the diff before committing — confirm 58 columns changed and the 4 defaults
at `approval_tokens.created_at`, `subscribers.created_at`,
`subscriber_tokens.created_at`, `weekly_runs.started_at` now read `default=utcnow`.

- [x] **Step 4: Run the tests**

Run: `LLM_MODE=mock INSIGHTS_MODE=mock python -m pytest tests/ -q`
Expected: 269 passed. SQLite still strips offsets, so nothing else moves.

- [x] **Step 5: Commit**

```bash
git add database.py tests/test_datetime_discipline.py
git commit -m "All 58 DateTime columns become timezone-aware"
```

## Task 5: Convert the 81 call sites and empty the ratchet

**Files:**
- Modify: the 39 files in `KNOWN_OFFENDERS`
- Modify: `tests/test_datetime_discipline.py` (empty the set)

**Interfaces:**
- Consumes: `database.utcnow`.

**Do not split this task by module.** Finding 6(b): ~20 cutoff/arithmetic sites
compare against values read from the DB. Once reads return aware datetimes, a
half-converted codebase raises `TypeError` at runtime in whichever half lags.
The suite passing is not evidence here — SQLite hides it. The Postgres job and a
live smoke run are the evidence.

- [x] **Step 1: Convert every site**

For each file in `KNOWN_OFFENDERS`, replace `datetime.utcnow()` with `utcnow()`
and add `from database import utcnow` (adjust to each module's import style).
Three cases need judgement rather than substitution:

0. **The two strippers deferred from Task 2** — now safe, because the columns they are compared against return aware values from this commit onward:

```python
# refresh_recordings.py:209
return datetime.fromtimestamp(int(ts), tz=timezone.utc)

# scripts/hydrate_recording_metadata.py:76
row.published_at = datetime.fromtimestamp(ts, tz=timezone.utc)
```

Re-check `refresh_recordings.py:297` after this edit: both sides of that
comparison must be aware.

1. **Insert paths where the column default already fires** — delete the explicit argument and add the Finding 7 comment. Applies to `extract_cafr_investments.py:388`, `extractor.py:490`, `summarizer.py:340,388`, `refresh_cafrs.py:326`, `refresh_ips.py:215`.
2. **`insights/config.py:127-142`** — the 4 helpers take an optional `now`; change only the fallback, keep the parameter.
3. **`strftime` display sites** (~30, mostly `generate_notes.py` and `insights/compose.py`) — swapping to `utcnow()` is safe; the format strings already say "UTC".

- [x] **Step 2: Empty the ratchet**

```python
KNOWN_OFFENDERS: set[str] = set()
```

- [x] **Step 3: Run the full suite**

Run: `LLM_MODE=mock INSIGHTS_MODE=mock python -m pytest tests/ -q`
Expected: 269 passed, and the DeprecationWarning count drops sharply (the 485 warnings at baseline are dominated by these calls).

- [x] **Step 4: Smoke-test the two cadences that do the most datetime arithmetic**

```bash
INSIGHTS_MODE=mock python -m insights.scheduler daily --force
INSIGHTS_MODE=mock python -m insights.scheduler weekly --skip-scrape --force
```

Expected: both complete; artifacts land in `tmp/sent_emails/`. These exercise the
`daily_runs` lookback and `weekly_runs` resumability paths — the densest
naive/aware comparison sites in the codebase.

- [x] **Step 5: Commit**

```bash
git add -u
git commit -m "Convert all 81 naive utcnow call sites; the ratchet is empty"
```

## Task 6: The backfill script

**Files:**
- Create: `scripts/backfill_utc_datetimes.py`
- Create: `tests/test_backfill_utc_datetimes.py`

**Interfaces:**
- Produces: `stamp_utc(db_path: str) -> dict[str, int]` — table.column → rows rewritten.

**Why a script, not a migration:** CLAUDE.md forbids `ALTER TABLE`; existing-row
backfill is always a one-off script. This one runs against the SQLite file as
part of step 3's export, so the values arrive at Postgres already carrying
`+00:00`. Idempotent: a value that already ends in an offset is skipped, so
re-running is safe (same contract as `scripts/migrate_compress_extracted_text.py`).

- [x] **Step 1: Write the failing test**

```python
import sqlite3

from scripts.backfill_utc_datetimes import stamp_utc


def test_stamps_naive_values_and_is_idempotent(tmp_path):
    db = tmp_path / "t.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE pipeline_runs (id INTEGER PRIMARY KEY, started_at TEXT)")
    con.execute("INSERT INTO pipeline_runs VALUES (1, '2026-08-19 11:00:00.000000')")
    con.execute("INSERT INTO pipeline_runs VALUES (2, NULL)")
    con.commit(); con.close()

    first = stamp_utc(str(db))
    assert first["pipeline_runs.started_at"] == 1

    con = sqlite3.connect(db)
    assert con.execute("SELECT started_at FROM pipeline_runs WHERE id=1").fetchone()[0] \
        == "2026-08-19 11:00:00.000000+00:00"
    assert con.execute("SELECT started_at FROM pipeline_runs WHERE id=2").fetchone()[0] is None
    con.close()

    second = stamp_utc(str(db))
    assert second["pipeline_runs.started_at"] == 0, "must be idempotent"
```

- [x] **Step 2: Run to verify it fails**

Run: `LLM_MODE=mock python -m pytest tests/test_backfill_utc_datetimes.py -v`
Expected: FAIL — `ModuleNotFoundError: scripts.backfill_utc_datetimes`.

- [x] **Step 3: Write the script**

```python
"""Stamp +00:00 onto every naive datetime value in the SQLite file.

Audit finding (2026-08-19): all 45 populated DateTime columns hold naive
values, and no writer ever used local time, so every stored value is UTC.
That makes a wholesale stamp correct. Idempotent on the offset suffix.
"""
from __future__ import annotations

import re
import sqlite3
import sys

import sqlalchemy as sa

import database

HAS_OFFSET = re.compile(r"([+-]\d{2}:?\d{2}|Z)$")


def stamp_utc(db_path: str) -> dict[str, int]:
    con = sqlite3.connect(db_path)
    present = {r[0] for r in con.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    changed: dict[str, int] = {}
    for name, table in sorted(database.Base.metadata.tables.items()):
        if name not in present:
            continue
        for col in table.columns:
            if not isinstance(col.type, sa.DateTime):
                continue
            n = 0
            for rowid, value in con.execute(
                    f'SELECT rowid, "{col.name}" FROM "{name}" '
                    f'WHERE "{col.name}" IS NOT NULL').fetchall():
                if HAS_OFFSET.search(str(value)):
                    continue
                con.execute(f'UPDATE "{name}" SET "{col.name}" = ? WHERE rowid = ?',
                            (f"{value}+00:00", rowid))
                n += 1
            changed[f"{name}.{col.name}"] = n
    con.commit()
    con.close()
    return changed


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "db/pension.db"
    for key, count in sorted(stamp_utc(path).items(), key=lambda kv: -kv[1]):
        if count:
            print(f"{count:>7}  {key}")
```

- [x] **Step 4: Run the test**

Run: `LLM_MODE=mock python -m pytest tests/test_backfill_utc_datetimes.py -v`
Expected: PASS.

- [x] **Step 5: Dry-run against a COPY of the real DB — never the tracked file**

```bash
cp db/pension.db /tmp/backfill_check.db
python scripts/backfill_utc_datetimes.py /tmp/backfill_check.db
```

Expected: 45 columns listed, ~35,000 rows total, `documents.downloaded_at` (4241)
and `summaries.generated_at` (4203) at the top. Run it a second time: every count 0.

- [x] **Step 6: Commit**

```bash
git add scripts/backfill_utc_datetimes.py tests/test_backfill_utc_datetimes.py
git commit -m "Add the one-shot UTC backfill for existing rows"
```

## Task 7: Close the audit

**Files:**
- Modify: `nextsteps.md`, `docs/superpowers/specs/2026-08-16-low-maintenance-app-design.md`

- [x] **Step 1: Correct the spec's §10 premise**

Add a `CORRECTED` block in the house style, recording that there was no
naive/aware data mixture — the data was uniformly naive because SQLite strips
offsets even from `DateTime(timezone=True)` — and that this makes the Postgres CI
container from §11 a prerequisite rather than an enhancement.

- [x] **Step 2: Update `nextsteps.md`**

Mark the datetime audit done, correct "three known call sites" to 81 sites across
39 files plus 58 columns, and record the `TIMESTAMPTZ` decision.

- [x] **Step 3: Commit**

```bash
git add nextsteps.md docs/superpowers/specs/2026-08-16-low-maintenance-app-design.md
git commit -m "Correct the spec against what the datetime audit found"
```

---

## Verification summary

| Claim | Verified by |
|---|---|
| No new naive callers | `test_no_new_naive_utcnow_callers` (SQLite) |
| All 58 columns aware | `test_all_datetime_columns_declare_timezone` (SQLite metadata) |
| Columns are really `TIMESTAMPTZ` | `test_every_datetime_column_is_timestamptz` (**Postgres CI only**) |
| Offsets survive a round trip | `test_aware_value_round_trips_with_its_offset` (**Postgres CI only**) |
| Existing rows carry UTC | `test_stamps_naive_values_and_is_idempotent` + dry run on a DB copy |
| Nothing regressed | Full suite, 269 expected; two live cadence smoke runs |
