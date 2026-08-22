# Postgres Dual-Run Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the app connect to Postgres via `DATABASE_URL`, then prove — query by query, against the real corpus — that Neon returns what SQLite returns, before anything cuts over.

**Architecture:** One pure function resolves the URL from the environment, defaulting to today's SQLite file so nothing changes until the variable is set. A comparison harness then calls every function in `queries.py` against both backends and diffs the results. The harness is the evidence; a staging Streamlit on Render is the smoke test that catches what the query layer cannot see.

**Tech Stack:** SQLAlchemy 2.x, `psycopg[binary]`, Neon Postgres 18.6, Streamlit, Render.

**Spec:** `docs/superpowers/specs/2026-08-16-low-maintenance-app-design.md` (step 4 of the migration sequence), with `docs/superpowers/specs/2026-08-19-portal-readiness-design.md` for the search and gzip decisions already landed in step 3. Running notes: `nextsteps.md`.

## Global Constraints

- **Never reload `database.py` in a test.** `tests/conftest.py` rebinds `database.engine` and `database.SessionLocal` with `monkeypatch.setattr` because a module reload orphans the ORM classes and breaks SQLAlchemy's mapper registry. Anything that needs testing must be reachable without a reload.
- **Never write SQL `ALTER TABLE` migrations.** The model is the schema; `init_db()` calls `create_all` and is idempotent.
- **Do not run `git add .`** — the repo root holds dozens of intentionally untracked scratch files. Stage by name.
- **Do not run a full re-extraction.** It would add ~20.5 MB to `db/pension.db` and put it 11 MB from GitHub's hard limit. See CLAUDE.md.
- Postgres URLs must pass through `database.normalise_pg_url`, which pins them to `postgresql+psycopg://`. A bare `postgresql://` maps to psycopg2, which this project does not install.
- `LLM_MODE=mock` and `INSIGHTS_MODE=mock` are independent flags. Tests set both.
- Postgres-only tests live in `tests/postgres/` and skip unless `TEST_POSTGRES_URL` is set. The skip belongs in a fixture — `pytestmark` in a conftest does not propagate.
- Step 4 is **read-only against Postgres**. No workflow writes to Neon and no DB-commit step is removed until step 5.

## Facts established before writing this plan

- Step 3 is complete. Neon holds all 33 tables, verified: row counts, twin `_canonical_hash` values and per-document text digests all match. 36 FK constraints, GIN index on `summaries`, zero naive timestamp columns.
- `database.py:81` builds the engine at import from a hardcoded `DATABASE_URL = f"sqlite:///{DB_PATH}"`. That constant is the only thing standing between the app and Postgres.
- `queries.py` exposes 26 read functions, all taking a `session` as their first argument. This is the comparison surface, and it exists because step 2 moved every query out of `app.py`.
- `render.yaml` declares one web service with a 5 GB disk mounted at `/data` and `DB_PATH=/data/pension.db`.
- Neon closes idle connections. An engine without `pool_pre_ping` raises `OperationalError` on the first query after an idle period rather than reconnecting.

## File structure

| File | Responsibility |
|---|---|
| `database.py` (modify) | `resolve_database_url()` and `create_app_engine()`; the module-level engine calls them |
| `tests/test_database_url_wiring.py` (create) | The resolution rules, without reloading the module |
| `scripts/compare_backends.py` (create) | Call every `queries.py` function against two sessions and diff |
| `tests/test_compare_backends.py` (create) | The harness's own normalisation and diffing |
| `render.yaml` (modify) | Declare `DATABASE_URL` on a staging service |

---

## Task 1: Resolve the database URL from the environment

**Files:**
- Modify: `database.py:58-82`
- Test: `tests/test_database_url_wiring.py`

**Interfaces:**
- Consumes: `normalise_pg_url(url: str) -> str` (already present, `database.py:65`)
- Produces: `resolve_database_url(env: Mapping | None = None, db_path: str | None = None) -> str` and `create_app_engine(url: str) -> Engine`

- [ ] **Step 1: Write the failing tests**

```python
"""The app has to be able to point at Postgres without a code change.

Both functions are pure and take their environment as an argument. That is
deliberate: database.py builds its engine at import, so the only other way to
test this would be to reload the module -- which orphans the ORM classes and
breaks SQLAlchemy's mapper registry. See tests/conftest.py.
"""

from __future__ import annotations

import database


def test_sqlite_is_still_the_default():
    """Nothing changes until DATABASE_URL is set. This is what makes the
    wiring safe to merge before the cutover."""
    url = database.resolve_database_url({}, db_path="/tmp/x.db")
    assert url == "sqlite:////tmp/x.db"


def test_database_url_wins_when_set():
    url = database.resolve_database_url(
        {"DATABASE_URL": "postgresql://u:p@host/db"}, db_path="/tmp/x.db")
    assert url.startswith("postgresql+psycopg://"), url
    assert "/tmp/x.db" not in url


def test_the_driver_is_pinned():
    """A bare postgresql:// URL maps to psycopg2, which is not installed --
    and if it happens to be present it returns BYTEA as memoryview."""
    assert database.resolve_database_url(
        {"DATABASE_URL": "postgres://u:p@h/d"}).startswith("postgresql+psycopg://")


def test_an_empty_value_is_not_a_url():
    """Render and GitHub Actions both materialise an unset secret as "".
    Treating that as a URL would fail the deploy with an unparseable DSN
    instead of falling back to SQLite."""
    assert database.resolve_database_url(
        {"DATABASE_URL": ""}, db_path="/tmp/x.db") == "sqlite:////tmp/x.db"


def test_query_parameters_survive():
    """Neon's URL carries sslmode and channel_binding. Dropping either makes
    the connection fail, or silently downgrades its security."""
    url = database.resolve_database_url({
        "DATABASE_URL":
            "postgresql://u:p@h/d?sslmode=require&channel_binding=require"})
    assert "sslmode=require" in url
    assert "channel_binding=require" in url


def test_postgres_engines_pre_ping():
    """Neon closes idle connections. Without pre-ping the first query after an
    idle period raises OperationalError instead of reconnecting -- which on
    Streamlit means a user sees a stack trace on a page they left open."""
    engine = database.create_app_engine(
        "postgresql+psycopg://u:p@h/d")           # not connected to
    assert engine.pool._pre_ping is True
    engine.dispose()


def test_sqlite_engines_do_not_take_postgres_pool_settings():
    """SQLite's default pool has no pre-ping to set, and passing pool_recycle
    to it is meaningless. Keeping the branch narrow keeps the local path
    exactly as it is today."""
    engine = database.create_app_engine("sqlite://")
    assert getattr(engine.pool, "_pre_ping", False) is False
    engine.dispose()


def test_the_module_engine_uses_the_resolver():
    """A static backstop. If someone re-hardcodes the URL, every test above
    still passes while the app quietly ignores DATABASE_URL."""
    import pathlib
    src = pathlib.Path(database.__file__).read_text(encoding="utf-8")
    assert "engine = create_app_engine(DATABASE_URL)" in src
    assert 'DATABASE_URL = resolve_database_url()' in src
```

- [ ] **Step 2: Run them and watch them fail**

Run: `LLM_MODE=mock python -m pytest tests/test_database_url_wiring.py -q`
Expected: FAIL, `AttributeError: module 'database' has no attribute 'resolve_database_url'`

- [ ] **Step 3: Implement**

Two edits, in this order. The layout matters: `resolve_database_url` calls `normalise_pg_url`, and `DATABASE_URL = resolve_database_url()` runs at import, so both definitions must sit above it.

**Edit A — delete `database.py:62`**, which is the whole of the current hardcoding:

```python
DATABASE_URL = f"sqlite:///{DB_PATH}"
```

Leave `DB_PATH` (lines 58-61) and `normalise_pg_url` (lines 65-78) exactly as they are.

**Edit B — replace `database.py:81-82`**, currently:

```python
engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)
```

with:

```python
def resolve_database_url(env=None, db_path: str | None = None) -> str:
    """The URL to connect to, from the environment.

    DATABASE_URL wins when it holds a non-empty value; otherwise the
    historical SQLite file. Both Render and GitHub Actions materialise an
    unset secret as the empty string, which is why "" falls through to SQLite
    rather than being handed to create_engine as a DSN.

    Pure, and takes its environment as an argument, because database.py builds
    its engine at import: the only other way to test this would be to reload
    the module, and a reload orphans the ORM classes.
    """
    env = os.environ if env is None else env
    url = (env.get("DATABASE_URL") or "").strip()
    if url:
        return normalise_pg_url(url)
    return f"sqlite:///{db_path or DB_PATH}"


def create_app_engine(url: str):
    """An engine tuned for the backend the URL names.

    Neon drops idle connections, so a Postgres engine needs pre-ping: without
    it the first query after an idle period raises OperationalError rather
    than reconnecting, which on Streamlit surfaces as a stack trace on a page
    the user left open. SQLite gets today's settings, untouched.
    """
    if url.startswith("postgresql"):
        return create_engine(url, echo=False, pool_pre_ping=True,
                             pool_recycle=300)
    return create_engine(url, echo=False)


DATABASE_URL = resolve_database_url()
engine = create_app_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
```

Note `create_engine` is already imported at `database.py:15`; no import change is needed.

- [ ] **Step 4: Run the tests**

Run: `LLM_MODE=mock python -m pytest tests/test_database_url_wiring.py -q`
Expected: PASS (8 tests)

- [ ] **Step 5: Run the whole suite — the engine construction changed**

Run: `LLM_MODE=mock INSIGHTS_MODE=mock python -m pytest tests/ -q`
Expected: 317+ passed, 22 skipped. Any failure here is the engine change leaking into test isolation.

- [ ] **Step 6: Prove it actually connects to Neon**

```bash
DATABASE_URL="$NEON_URL" python -c "
import database, queries
s = database.get_session()
print('dialect:', database.engine.dialect.name)
print('plans:', len(queries.plans(s)))
s.close()"
```

Expected: `dialect: postgresql` and `plans: 148`. Without `DATABASE_URL` the same command must print `sqlite` and `148`.

- [ ] **Step 7: Commit**

```bash
git add database.py tests/test_database_url_wiring.py
git commit -m "Resolve the database URL from the environment"
```

---

## Task 2: The backend-comparison harness

**Files:**
- Create: `scripts/compare_backends.py`
- Test: `tests/test_compare_backends.py`

**Interfaces:**
- Consumes: `resolve_database_url` / `create_app_engine` from Task 1; every public function in `queries.py`
- Produces: `normalise(value) -> object`, `CASES: dict[str, dict]`, `compare(sqlite_path: str, pg_url: str) -> dict` returning `{"matched": [name], "mismatched": {name: (left, right)}, "errored": {name: str}}`

Comparing rendered Streamlit pages by eye is what the spec asked for; comparing the 26 functions the pages are built from is the same check, made precise and repeatable. The staging app in Task 4 then covers what the query layer cannot see.

- [ ] **Step 1: Write the failing tests**

```python
"""The harness has to be able to report a difference, not just run.

Every function in queries.py returns some mixture of ORM instances, tuples,
dicts and floats. Two backends return equal data in unequal shapes -- Postgres
hands back timezone-aware datetimes and Decimal where SQLite gives naive
datetimes and float -- so normalisation is where this either works or
produces a wall of false positives that hides the one real diff.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest

from scripts.compare_backends import CASES, compare, normalise


def test_aware_and_naive_datetimes_compare_equal():
    """SQLite strips the offset on write; Postgres keeps it. Both hold UTC --
    the 2026-08-19 audit established every stored value is UTC either way."""
    naive = dt.datetime(2026, 8, 19, 11, 0, 0)
    aware = dt.datetime(2026, 8, 19, 11, 0, 0, tzinfo=dt.timezone.utc)
    assert normalise(naive) == normalise(aware)


def test_a_real_time_difference_still_shows():
    """Normalising must not flatten everything into equality."""
    a = dt.datetime(2026, 8, 19, 11, 0, 0)
    b = dt.datetime(2026, 8, 19, 12, 0, 0)
    assert normalise(a) != normalise(b)


def test_decimal_and_float_compare_equal():
    """Postgres NUMERIC arrives as Decimal, SQLite REAL as float."""
    assert normalise(Decimal("40.00")) == normalise(40.0)


def test_float_noise_below_the_tolerance_is_ignored():
    assert normalise(7.150000000000001) == normalise(7.15)


def test_a_real_numeric_difference_still_shows():
    assert normalise(7.15) != normalise(7.16)


def test_orm_instances_reduce_to_their_columns():
    """Comparing ORM objects by identity would mark every row different, and
    comparing repr() would compare memory addresses."""
    import database
    doc = database.Document(id=1, plan_id="opers", url="http://x/a.pdf")
    out = normalise(doc)
    assert out["id"] == 1 and out["plan_id"] == "opers"
    assert "_sa_instance_state" not in out


def test_gzipped_text_is_compared_by_digest_not_by_value():
    """extracted_text runs to 2M chars. Holding two corpora of it in memory to
    diff is not an option on the machine that runs the cutover."""
    import database
    doc = database.Document(id=1, plan_id="p", extracted_text="x" * 10_000)
    out = normalise(doc)
    assert out["extracted_text"] != "x" * 10_000
    assert len(out["extracted_text"]) == 32          # an md5 hex digest


def test_every_public_query_function_has_a_case():
    """The harness is only evidence if it covers the surface. A new query
    function with no case must fail here rather than be silently unchecked."""
    import inspect
    import queries

    public = {n for n, f in vars(queries).items()
              if inspect.isfunction(f) and not n.startswith("_")
              and f.__module__ == "queries"}
    assert public - set(CASES) == set(), \
        "queries.py functions with no comparison case: %s" % (public - set(CASES))


def test_compare_reports_a_seeded_difference(tmp_path):
    """The test that makes the other tests worth having.

    Two SQLite files standing in for two backends: identical but for one plan
    name. If compare() cannot see that, it cannot see a migration defect
    either, and a clean run would mean nothing.
    """
    import database
    import sqlalchemy as sa

    paths = []
    for i, name in enumerate(["Ohio PERS", "Ohio PERS (WRONG)"]):
        p = tmp_path / ("db%d.db" % i)
        eng = sa.create_engine("sqlite:///%s" % p)
        database.Base.metadata.create_all(eng)
        with eng.begin() as c:
            c.execute(sa.text("INSERT INTO plans (id, name) VALUES ('opers', :n)"),
                      {"n": name})
        eng.dispose()
        paths.append(str(p))

    result = compare(paths[0], "sqlite:///%s" % paths[1])
    assert "plans" in result["mismatched"], result


def test_compare_is_clean_on_two_identical_databases(tmp_path):
    """The negative control. Without it, a compare() that reported everything
    as mismatched would still pass the test above."""
    import database
    import sqlalchemy as sa

    paths = []
    for i in range(2):
        p = tmp_path / ("same%d.db" % i)
        eng = sa.create_engine("sqlite:///%s" % p)
        database.Base.metadata.create_all(eng)
        with eng.begin() as c:
            c.execute(sa.text("INSERT INTO plans (id, name) VALUES ('opers', 'Ohio PERS')"))
        eng.dispose()
        paths.append(str(p))

    result = compare(paths[0], "sqlite:///%s" % paths[1])
    assert result["mismatched"] == {}, result["mismatched"]
    assert result["errored"] == {}, result["errored"]
```

- [ ] **Step 2: Run them and watch them fail**

Run: `LLM_MODE=mock python -m pytest tests/test_compare_backends.py -q`
Expected: FAIL, `ModuleNotFoundError: No module named 'scripts.compare_backends'`

- [ ] **Step 3: Implement the harness**

```python
"""Call every read in queries.py against two databases and diff the results.

Step 4 of the migration is "dual-run staging on Postgres beside prod on
SQLite; compare pages". Comparing the 26 functions the pages are built from is
the same check made precise: it covers every branch a page can render, it runs
in seconds, and it names the function that differs rather than leaving someone
to spot a changed number in a table.

Read-only on both sides.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import pathlib
import sys
from decimal import Decimal

import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import database  # noqa: E402
import queries  # noqa: E402

FLOAT_PLACES = 6

# Columns held as gzipped text: compared by digest so two corpora of it are
# never resident at once. MAX_STORED_CHARS is 2,000,000.
DIGEST_COLUMNS = {"extracted_text"}


def normalise(value):
    """Reduce a value to something two backends can be compared on.

    Postgres returns timezone-aware datetimes and Decimal where SQLite returns
    naive datetimes and float. Both hold the same data -- every stored datetime
    is UTC either way, per the 2026-08-19 audit -- so the shapes are converged
    rather than the differences reported.
    """
    if isinstance(value, dt.datetime):
        return database.as_utc(value).isoformat()
    if isinstance(value, Decimal):
        return round(float(value), FLOAT_PLACES)
    if isinstance(value, float):
        return round(value, FLOAT_PLACES)
    if isinstance(value, (list, tuple)):
        return [normalise(v) for v in value]
    if isinstance(value, dict):
        return {k: normalise(v) for k, v in sorted(value.items())}
    if hasattr(value, "__table__"):                      # an ORM instance
        out = {}
        for column in value.__table__.columns:
            item = getattr(value, column.name)
            if column.name in DIGEST_COLUMNS:
                item = (hashlib.md5(item.encode("utf-8")).hexdigest()
                        if item is not None else None)
            out[column.name] = normalise(item)
        return out
    return value
```

Then the case table. Each entry is `{"args": (...)}`, or `{"args": (...), "limit": n}` where a function is unbounded and the full corpus would be pointlessly large. Functions taking a plan id use `"opers"`, which is present in both backends and is the plan whose CAFR extraction the step-3 repair preserved:

```python
CUTOFF = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)

CASES = {
    "plans": {"args": ()},
    "recent_summaries": {"args": (None, 50)},
    "corpus_stats": {"args": ()},
    "plan_coverage_rows": {"args": ()},
    "plans_index_rows": {"args": ()},
    "cafr_coverage_rows": {"args": ()},
    "cafr_plan_detail": {"args": ("opers",)},
    "cafr_extract_fy_range": {"args": ()},
    "allocation_rows": {"args": (("equity",), (), )},
    "investment_action_docs": {"args": ("opers", CUTOFF)},
    "documents_by_ids": {"args": ([1, 2, 3, 4315],)},
    "document_with_context": {"args": (1,)},
    "documents_for_run": {"args": ([1, 2, 3],)},
    "recent_fetch_runs": {"args": (20,)},
    "failed_extraction_rows": {"args": ()},
    "skipped_document_rows": {"args": ()},
    "cafr_coverage_summary": {"args": ()},
    "recent_cafr_refresh_runs": {"args": (20,)},
    "cafr_refresh_rows": {"args": ([],)},        # filled at runtime, see below
    "plan_labels": {"args": ()},
    "plans_by_id": {"args": ()},
    "video_sources": {"args": (None,)},
    "meeting_recordings": {"args": (None,)},
    "publications_by_status": {"args": (("published",),)},
    "drafts_awaiting_approval": {"args": ()},
}
```

That is 25 entries, one per public function. `queries._aggregator_plan_ids` is the 26th function in the module and is deliberately absent: it is private, it takes no session, and calling it as `fn(session)` would raise `TypeError` straight into `errored`. The coverage test's `not n.startswith("_")` filter already excludes it, so leaving it out keeps both sides consistent.

`documents_by_ids` is passed id 4315 on purpose — that document was pruned, so the case checks that both backends handle a missing id the same way rather than only ever seeing ids that resolve.

`cafr_refresh_rows` needs run timestamps that actually exist, so resolve its argument from the source database before comparing:

```python
def _resolve_dynamic_args(session):
    """Two cases need arguments that only exist in the data."""
    runs = queries.recent_cafr_refresh_runs(session, 3)
    CASES["cafr_refresh_rows"]["args"] = ([r.run_at for r in runs],)


def compare(sqlite_path: str, pg_url: str) -> dict:
    src = database.create_app_engine("sqlite:///%s" % sqlite_path)
    dst = database.create_app_engine(database.normalise_pg_url(pg_url))
    SrcSession, DstSession = sessionmaker(bind=src), sessionmaker(bind=dst)

    matched, mismatched, errored = [], {}, {}
    with SrcSession() as ss, DstSession() as ds:
        _resolve_dynamic_args(ss)
        for name, case in CASES.items():
            fn = getattr(queries, name)
            try:
                left = normalise(fn(ss, *case["args"]))
                right = normalise(fn(ds, *case["args"]))
            except Exception as exc:                     # noqa: BLE001
                errored[name] = "%s: %s" % (type(exc).__name__, exc)
                continue
            if left == right:
                matched.append(name)
            else:
                mismatched[name] = (left, right)
    src.dispose(); dst.dispose()
    return {"matched": matched, "mismatched": mismatched, "errored": errored}
```

A `main()` prints one line per function and exits non-zero if anything mismatched or errored.

- [ ] **Step 4: Run the tests**

Run: `LLM_MODE=mock python -m pytest tests/test_compare_backends.py -q`
Expected: PASS (10 tests)

- [ ] **Step 5: Mutation-check the harness**

The seeded-difference test is the load-bearing one. Prove it can fail:

```bash
# Make normalise() collapse everything, then confirm the suite goes red.
python - <<'EOF'
import io; p="scripts/compare_backends.py"
s=io.open(p,encoding="utf-8").read()
io.open(p,"w",encoding="utf-8").write(s.replace("def normalise(value):", "def normalise(value):\n    return None  # MUTANT", 1))
EOF
LLM_MODE=mock python -m pytest tests/test_compare_backends.py -q   # must FAIL
git checkout scripts/compare_backends.py
```

Expected: `test_compare_reports_a_seeded_difference` and the shape tests fail. If they pass, the harness proves nothing and the task is not done.

- [ ] **Step 6: Commit**

```bash
git add scripts/compare_backends.py tests/test_compare_backends.py
git commit -m "Add the SQLite/Postgres read-comparison harness"
```

---

## Task 3: Run the comparison against Neon and triage

**Files:**
- Modify: whatever the run implicates (expected: none)

This is the task the previous two exist for. It has no new tests — its deliverable is a clean run against the real corpus, or a fix and then a clean run.

- [ ] **Step 1: Run it**

```bash
DATABASE_URL= python scripts/compare_backends.py db/pension.db "$NEON_URL"
```

`DATABASE_URL=` is set empty deliberately: the harness must open both engines itself, and an inherited value would make the "SQLite" side Postgres and the comparison vacuous.

- [ ] **Step 2: Triage each mismatch against this list before changing code**

Expected classes of difference, and what each means:

| Symptom | Cause | Fix |
|---|---|---|
| Ordering differs on rows with equal sort keys | Postgres has no implicit rowid order; SQLite's is stable by accident | Add a tiebreaker to the `ORDER BY` in `queries.py` — the same fix twin hashes needed in step 3 |
| `LIKE` matches differ in case | SQLite's `LIKE` is case-insensitive for ASCII; Postgres's is not | `ILIKE`, via `.ilike()` |
| A count is higher on Postgres | Rows the step-3 repair deleted from SQLite only | Re-check: `db/pension.db` was repaired on 2026-08-21, so this should not appear. If it does, the two are out of sync — re-migrate rather than paper over it |
| `NULL` sorts at the other end | `NULLS FIRST`/`LAST` defaults differ | Make it explicit |
| Division returns an integer | Integer division semantics differ | Cast in the query |

- [ ] **Step 3: Fix any real mismatch in `queries.py`, with a test in `tests/test_queries.py`**

Every fix is a behaviour change to a page, so it needs a test at the read layer, where step 2 put this logic to make it testable. Add the test first and watch it fail on SQLite.

- [ ] **Step 4: Re-run until clean, then record the result**

```bash
DATABASE_URL= python scripts/compare_backends.py db/pension.db "$NEON_URL" | tee _dual_run_result.txt
```

Expected: every function in `matched`, `mismatched` and `errored` both empty.

- [ ] **Step 5: Commit any fixes**

```bash
git add queries.py tests/test_queries.py
git commit -m "Fix reads that differ between SQLite and Postgres"
```

---

## Task 4: Staging Streamlit on Postgres

**Files:**
- Modify: `render.yaml`

The harness covers the query layer. It cannot see Streamlit's caching, its rerun model, or how a connection pool behaves across reruns — which is exactly where Neon's idle-connection drop bites. This task is the smoke test for those.

- [ ] **Step 1: Add a staging service to `render.yaml`**

```yaml
  # ---------------------------------------------------------------------------
  # Staging — the same app on Postgres, for the step-4 dual run. Read-only
  # against Neon: no cadence, cron or pipeline points here. Delete this service
  # once step 5 cuts production over.
  #
  # No disk. DATABASE_URL is what makes this Postgres rather than SQLite;
  # without it the service would start on an empty ephemeral SQLite file and
  # look like a total data loss rather than a missing variable.
  # ---------------------------------------------------------------------------
  - type: web
    name: pension-plan-intelligence-staging
    runtime: python
    plan: free
    buildCommand: pip install -r requirements.txt
    startCommand: streamlit run app.py --server.port $PORT --server.address 0.0.0.0
    envVars:
      - key: DATABASE_URL
        sync: false
      - key: APP_BASE_URL
        value: https://pension-plan-intelligence-staging.onrender.com
      - key: ADMIN_PASSWORD
        sync: false
```

`sync: false` means the value is set in the Render dashboard and never committed. Production is untouched: it has no `DATABASE_URL`, so Task 1's resolver leaves it on SQLite.

- [ ] **Step 2: Deploy and set the variable**

Requires James: set `DATABASE_URL` on the staging service in the Render dashboard to the Neon URL, and `ADMIN_PASSWORD` to reach the gated tabs.

- [ ] **Step 3: Walk both apps side by side**

Compare production against staging on: Plans index, one plan twin (use **OPERS** — it is the plan whose FY2025 CAFR extraction the step-3 repair preserved, and its `document_id` is now NULL, so it is the single most likely page to break), CAFR coverage, Insights, Search, and the three gated tabs.

Watch specifically for:
- A stack trace after leaving a tab idle for several minutes — that is `pool_pre_ping` not working.
- Search returning unranked results — that is `_pg_search_ids` not being reached.
- Any CAFR page for OPERS erroring on a missing document relationship.

- [ ] **Step 4: Commit**

```bash
git add render.yaml
git commit -m "Add the staging service for the Postgres dual run"
```

---

## Verification summary

| Claim | Evidence |
|---|---|
| Setting `DATABASE_URL` moves the app to Postgres | Task 1 Step 6 — `dialect: postgresql`, 148 plans |
| Not setting it changes nothing | Task 1 Step 6 second half, plus the full suite green |
| Neon returns what SQLite returns | Task 3 Step 4 — every `queries.py` function matched |
| The harness could have detected a difference | Task 2 Step 5 — mutation turns the suite red |
| Streamlit works against Neon | Task 4 Step 3 — both apps walked side by side |

## What this plan deliberately does not do

- **Write to Postgres.** Step 4 is read-only. No workflow, cadence or pipeline points at Neon until step 5.
- **Remove `db_sync.py` or the DB-commit steps.** Step 5, after this dual run holds.
- **Remove the persistent disk or `DB_PATH`.** Production still runs on SQLite throughout.
- **Add auth.** Step 6, deliberately last so it lands on a stable app.
- **Backfill the 444 truncated documents.** Gated on R2 — see the portal spec §2.3.
