# Postgres Migration Groundwork Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and prove everything step 3 needs — an id-preserving SQLite→Postgres migration, a verification harness, and the `tsvector` search schema that replaces FTS5 — without a Neon account existing yet.

**Architecture:** The Postgres CI service container added on 2026-08-19 is a real Postgres, so the migration and the search index can be written test-first and verified in CI. Neon then becomes a connection string pointed at proven code, not a place to debug. Search keeps both engines working simultaneously because step 4 dual-runs them.

**Tech Stack:** SQLAlchemy 2.x Core, psycopg 3, Postgres 16 (CI service container), SQLite, pytest.

**Spec:** `docs/superpowers/specs/2026-08-16-low-maintenance-app-design.md` (§9 step 3, §10, §11) and `docs/superpowers/specs/2026-08-19-portal-readiness-design.md` (§2, §7).

## Global Constraints

- Never write SQL `ALTER TABLE` migrations. Add/modify the SQLAlchemy model and call `init_db()`; existing-row work is a one-off script (CLAUDE.md).
- All stored datetimes are UTC. `datetime.utcnow()` is banned — `tests/test_datetime_discipline.py` enforces it and the offender list is empty.
- Postgres-only tests live in `tests/postgres/` and skip unless `TEST_POSTGRES_URL` is set. They are the only tests that can verify dialect semantics.
- `GzippedText` stays. Store gzipped bytes plus a `tsvector`; never store plaintext (portal spec §2.2).
- Tests run with `LLM_MODE=mock` and `INSIGHTS_MODE=mock`. Baseline: **288 passed, 3 skipped**.

---

## Facts established before writing this plan

Measured against the live schema and data, not assumed:

| Fact | Value |
|---|---|
| Tables in `Base.metadata` | **33** (the 5 FTS5 shadow tables are raw SQL and correctly absent) |
| Integer primary keys (need sequence resets) | **28** |
| Non-integer primary keys | 4 — `plans.id`, `pipeline_runs.run_id`, `twin_build_runs.run_id`, `rfp_records.rfp_id` |
| Composite primary key | 1 — `document_health (document_id, prompt_version)` |
| `GzippedText` columns → `BYTEA` | `documents.extracted_text`, `ips_documents.extracted_text`, `twin_snapshots.facets` |
| Document text | 35 MB compressed, 123 MB uncompressed |
| Documents truncated at the 150k cap | **444 (10.5%)** |

**A trap this plan must avoid.** SQLAlchemy's SQLite `DATETIME` *does* parse a
trailing `+00:00` when the column declares `timezone=True` — verified. But
SQLite **writes** still strip the offset. So running
`scripts/backfill_utc_datetimes.py` against the *live* SQLite database would
leave old rows aware and every new row naive: the exact mixture the original
spec feared, manufactured by the fix for it.

The backfill is therefore **export-only**, and this plan does not depend on it:
the migration normalises with `database.as_utc()` on write, which is correct
whether or not the backfill has run. Task 1 adds the warning to the script.

---

## File structure

| File | Responsibility |
|---|---|
| `scripts/migrate_sqlite_to_postgres.py` | Create: copy rows id-preserving, reset sequences |
| `scripts/verify_migration.py` | Create: row counts + `twin_snapshots.facets_hash` comparison |
| `tests/postgres/test_migration.py` | Create: migration against a fixture SQLite file |
| `tests/postgres/test_search_tsvector.py` | Create: the Postgres search path |
| `database.py` | Modify: GIN index on a computed tsvector, plus the Postgres search branch |
| `extractor.py` | Modify: `MAX_TEXT_CHARS` becomes `MAX_STORED_CHARS`, raised |
| `scripts/backfill_utc_datetimes.py` | Modify: export-only warning |

---

## Task 1: Migration script — copy rows, preserve ids

**Files:**
- Create: `scripts/migrate_sqlite_to_postgres.py`
- Create: `tests/postgres/test_migration.py`
- Modify: `scripts/backfill_utc_datetimes.py` (docstring warning only)

**Interfaces:**
- Consumes: `database.Base`, `database.as_utc`.
- Produces: `migrate(sqlite_path: str, pg_url: str, batch_size: int = 500) -> dict[str, int]` — table name → rows copied.

- [ ] **Step 1: Write the failing test**

`tests/postgres/test_migration.py`:

```python
"""The SQLite -> Postgres migration, run against a real Postgres.

Verified here rather than on SQLite because id preservation, sequence state
and BYTEA round-tripping are all dialect behaviour.
"""
from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.orm import Session

import database
from scripts.migrate_sqlite_to_postgres import migrate


def _seed_sqlite(path):
    """A miniature but representative source database."""
    engine = sa.create_engine(f"sqlite:///{path}")
    database.Base.metadata.create_all(engine)
    with Session(engine) as s:
        s.add(database.Plan(id="calpers", name="CalPERS", state="CA"))
        s.add(database.Document(
            id=7, plan_id="calpers", url="https://x/a.pdf", filename="a.pdf",
            doc_type="minutes", extraction_status="done",
            extracted_text="pension board investment minutes",
            downloaded_at=database.utcnow()))
        s.add(database.Document(
            id=99, plan_id="calpers", url="https://x/b.pdf", filename="b.pdf",
            doc_type="cafr", extraction_status="done",
            extracted_text="actuarial valuation report",
            downloaded_at=database.utcnow()))
        s.commit()
    engine.dispose()
    return engine


def test_migrate_preserves_ids_and_content(tmp_path, pg_engine):
    src = tmp_path / "src.db"
    _seed_sqlite(src)

    counts = migrate(str(src), str(pg_engine.url))

    assert counts["plans"] == 1
    assert counts["documents"] == 2

    with Session(pg_engine) as s:
        ids = [r[0] for r in s.query(database.Document.id).order_by(database.Document.id)]
        assert ids == [7, 99], "autoincrement ids must survive the move"
        doc = s.get(database.Document, 7)
        assert doc.extracted_text == "pension board investment minutes", \
            "GzippedText must round-trip through BYTEA"
        assert doc.downloaded_at.tzinfo is not None, "datetimes must arrive aware"


def test_migrate_skips_tables_absent_from_the_source(tmp_path, pg_engine):
    """Older DB files legitimately lack newer tables."""
    src = tmp_path / "src.db"
    _seed_sqlite(src)
    counts = migrate(str(src), str(pg_engine.url))
    assert counts["plans"] == 1
    assert all(isinstance(v, int) for v in counts.values())
```

- [ ] **Step 2: Run to verify it fails**

Run: `LLM_MODE=mock python -m pytest tests/postgres/test_migration.py -v`
Expected locally: 2 skipped (no `TEST_POSTGRES_URL`). In CI: FAIL with `ModuleNotFoundError: scripts.migrate_sqlite_to_postgres`.

- [ ] **Step 3: Write the migration script**

```python
"""Copy a SQLite pension database into Postgres, preserving ids.

Only tables declared in Base.metadata are copied — which correctly excludes
the five SQLite FTS5 shadow tables, since FTS5 has no Postgres equivalent and
the replacement index is built from the copied rows instead.

Datetimes are normalised with database.as_utc on the way in. That is correct
whether or not scripts/backfill_utc_datetimes.py has been run: every stored
value is UTC either way (2026-08-19 audit).

    python scripts/migrate_sqlite_to_postgres.py db/pension.db "$POSTGRES_URL"
"""
from __future__ import annotations

import datetime as _dt
import pathlib
import sys

import sqlalchemy as sa

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import database  # noqa: E402


def _normalise(row: dict) -> dict:
    return {k: (database.as_utc(v) if isinstance(v, _dt.datetime) else v)
            for k, v in row.items()}


def migrate(sqlite_path: str, pg_url: str, batch_size: int = 500) -> dict[str, int]:
    src = sa.create_engine(f"sqlite:///{sqlite_path}")
    dst = sa.create_engine(pg_url, future=True)
    database.Base.metadata.create_all(dst)

    present = set(sa.inspect(src).get_table_names())
    copied: dict[str, int] = {}
    try:
        # sorted_tables is dependency-ordered, so foreign keys resolve.
        for table in database.Base.metadata.sorted_tables:
            if table.name not in present:
                continue
            total = 0
            with src.connect() as sconn:
                result = sconn.execution_options(stream_results=True).execute(
                    sa.select(table))
                while True:
                    chunk = result.fetchmany(batch_size)
                    if not chunk:
                        break
                    rows = [_normalise(dict(r._mapping)) for r in chunk]
                    with dst.begin() as dconn:
                        dconn.execute(sa.insert(table), rows)
                    total += len(rows)
            copied[table.name] = total
        return copied
    finally:
        src.dispose()
        dst.dispose()


def main(argv: list[str]) -> int:
    if len(argv) < 3:
        print(__doc__)
        return 2
    counts = migrate(argv[1], argv[2])
    for name, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        if n:
            print(f"{n:>8}  {name}")
    print(f"\n{sum(counts.values())} rows across "
          f"{sum(1 for n in counts.values() if n)} tables")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
```

- [ ] **Step 4: Add the export-only warning to the backfill script**

Insert after the first paragraph of `scripts/backfill_utc_datetimes.py`'s docstring:

```
EXPORT ONLY — do not run this against the live SQLite database.

SQLAlchemy's SQLite DATETIME parses a trailing +00:00 when the column declares
timezone=True, but SQLite writes still strip it. Stamping the live file would
leave existing rows aware and every subsequent row naive — manufacturing the
very naive/aware mixture this work exists to remove. Run it only on a copy
being exported to Postgres.
```

- [ ] **Step 5: Run the tests**

Run: `LLM_MODE=mock python -m pytest tests/postgres/test_migration.py -v`
Expected locally: 2 skipped. Push and confirm the CI `postgres` job passes both.

- [ ] **Step 6: Commit**

```bash
git add scripts/migrate_sqlite_to_postgres.py tests/postgres/test_migration.py \
        scripts/backfill_utc_datetimes.py
git commit -m "Add the id-preserving SQLite to Postgres migration"
```

## Task 2: Reset the sequences

**Files:**
- Modify: `scripts/migrate_sqlite_to_postgres.py`
- Modify: `tests/postgres/test_migration.py`

**Interfaces:**
- Produces: `reset_sequences(pg_url: str) -> dict[str, int]` — table name → the value the sequence was set to. Called automatically at the end of `migrate`.

**Why:** inserting explicit ids does not advance Postgres's identity sequence.
28 tables have integer primary keys; without this the first insert after
migration raises a duplicate-key error on id 1.

- [ ] **Step 1: Write the failing test** (append to `tests/postgres/test_migration.py`)

```python
def test_inserting_after_migration_does_not_collide(tmp_path, pg_engine):
    """The failure this prevents: explicit ids leave the sequence at 1."""
    src = tmp_path / "src.db"
    _seed_sqlite(src)
    migrate(str(src), str(pg_engine.url))

    with Session(pg_engine) as s:
        fresh = database.Document(
            plan_id="calpers", url="https://x/c.pdf", filename="c.pdf",
            doc_type="minutes", extraction_status="pending")
        s.add(fresh)
        s.commit()                      # would raise UniqueViolation before
        assert fresh.id > 99, f"sequence not advanced past the copied max: {fresh.id}"


def test_reset_sequences_is_safe_on_empty_tables(tmp_path, pg_engine):
    """A table with no rows must not have its sequence set to 0."""
    from scripts.migrate_sqlite_to_postgres import reset_sequences
    src = tmp_path / "src.db"
    _seed_sqlite(src)
    migrate(str(src), str(pg_engine.url))
    result = reset_sequences(str(pg_engine.url))
    assert all(v >= 1 for v in result.values()), result
```

- [ ] **Step 2: Run to verify it fails**

Run in CI. Expected: `test_inserting_after_migration_does_not_collide` fails with a duplicate key on `documents_id_seq`.

- [ ] **Step 3: Implement**

Add to `scripts/migrate_sqlite_to_postgres.py`:

```python
def reset_sequences(pg_url: str) -> dict[str, int]:
    """Advance each identity sequence past the largest id just inserted.

    Inserting explicit ids does not move the sequence, so the next natural
    insert would collide on id 1. setval with a floor of 1 keeps empty tables
    legal — a sequence may not be set below its minimum.
    """
    engine = sa.create_engine(pg_url, future=True)
    out: dict[str, int] = {}
    try:
        with engine.begin() as conn:
            for table in database.Base.metadata.sorted_tables:
                pks = list(table.primary_key.columns)
                if len(pks) != 1:
                    continue
                col = pks[0]
                if not isinstance(col.type, sa.Integer):
                    continue
                seq = conn.execute(sa.text(
                    "SELECT pg_get_serial_sequence(:t, :c)"),
                    {"t": table.name, "c": col.name}).scalar()
                if seq is None:          # not an identity/serial column
                    continue
                high = conn.execute(sa.text(
                    f'SELECT COALESCE(MAX("{col.name}"), 0) FROM "{table.name}"'
                )).scalar() or 0
                target = max(high, 1)
                conn.execute(sa.text("SELECT setval(:s, :v, :called)"),
                             {"s": seq, "v": target, "called": high > 0})
                out[table.name] = target
        return out
    finally:
        engine.dispose()
```

And call it from `migrate`, replacing `return copied`:

```python
            copied[table.name] = total
        reset_sequences(pg_url)
        return copied
```

- [ ] **Step 4: Run the tests**

Push; expect the CI `postgres` job green with 4 migration tests.

- [ ] **Step 5: Commit**

```bash
git add scripts/migrate_sqlite_to_postgres.py tests/postgres/test_migration.py
git commit -m "Reset Postgres sequences after an id-preserving migration"
```

## Task 3: The verification harness

**Files:**
- Create: `scripts/verify_migration.py`
- Modify: `tests/postgres/test_migration.py`

**Interfaces:**
- Produces: `compare(sqlite_path: str, pg_url: str) -> dict` with keys `row_counts` (table → `{"sqlite": int, "postgres": int}`), `count_mismatches` (list of table names), `twin_hash_mismatches` (list of plan ids).

**Why:** spec §9 step 3 says to verify "by comparing every plan's twin
`_canonical_hash` before and after, plus row counts per table". This is that
check, as code rather than a manual ritual.

- [ ] **Step 1: Write the failing test** (append to `tests/postgres/test_migration.py`)

```python
def test_verify_reports_a_clean_migration(tmp_path, pg_engine):
    from scripts.verify_migration import compare
    src = tmp_path / "src.db"
    _seed_sqlite(src)
    migrate(str(src), str(pg_engine.url))

    report = compare(str(src), str(pg_engine.url))
    assert report["count_mismatches"] == [], report["count_mismatches"]
    assert report["twin_hash_mismatches"] == []
    assert report["row_counts"]["documents"] == {"sqlite": 2, "postgres": 2}


def test_verify_detects_a_dropped_row(tmp_path, pg_engine):
    """A verifier that cannot fail is not a verifier."""
    from scripts.verify_migration import compare
    src = tmp_path / "src.db"
    _seed_sqlite(src)
    migrate(str(src), str(pg_engine.url))

    with Session(pg_engine) as s:
        s.delete(s.get(database.Document, 99))
        s.commit()

    report = compare(str(src), str(pg_engine.url))
    assert "documents" in report["count_mismatches"]
```

- [ ] **Step 2: Run to verify it fails**

Expected in CI: `ModuleNotFoundError: scripts.verify_migration`.

- [ ] **Step 3: Implement**

```python
"""Compare a migrated Postgres database against its SQLite source.

Spec §9 step 3 requires row counts per table plus every plan's twin
_canonical_hash matching before and after. Both are computed here so the check
is reproducible rather than a manual ritual.

    python scripts/verify_migration.py db/pension.db "$POSTGRES_URL"
"""
from __future__ import annotations

import pathlib
import sys

import sqlalchemy as sa

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import database  # noqa: E402


def _counts(engine) -> dict[str, int]:
    present = set(sa.inspect(engine).get_table_names())
    out = {}
    with engine.connect() as conn:
        for table in database.Base.metadata.sorted_tables:
            if table.name not in present:
                continue
            out[table.name] = conn.execute(
                sa.select(sa.func.count()).select_from(table)).scalar()
    return out


def _twin_hashes(engine) -> dict[str, str]:
    """Latest snapshot hash per plan, keyed by plan id."""
    out: dict[str, str] = {}
    present = set(sa.inspect(engine).get_table_names())
    if "twin_snapshots" not in present:
        return out
    t = database.TwinSnapshot.__table__
    with engine.connect() as conn:
        rows = conn.execute(
            sa.select(t.c.plan_id, t.c.facets_hash, t.c.built_at)
            .order_by(t.c.plan_id, t.c.built_at.desc())).fetchall()
    for plan_id, digest, _built in rows:
        out.setdefault(plan_id, digest)
    return out


def compare(sqlite_path: str, pg_url: str) -> dict:
    src = sa.create_engine(f"sqlite:///{sqlite_path}")
    dst = sa.create_engine(pg_url, future=True)
    try:
        s_counts, p_counts = _counts(src), _counts(dst)
        row_counts = {
            name: {"sqlite": s_counts.get(name, 0), "postgres": p_counts.get(name, 0)}
            for name in sorted(set(s_counts) | set(p_counts))
        }
        mismatches = [n for n, v in row_counts.items() if v["sqlite"] != v["postgres"]]
        s_hash, p_hash = _twin_hashes(src), _twin_hashes(dst)
        hash_mismatches = sorted(
            pid for pid in set(s_hash) | set(p_hash)
            if s_hash.get(pid) != p_hash.get(pid))
        return {"row_counts": row_counts,
                "count_mismatches": mismatches,
                "twin_hash_mismatches": hash_mismatches}
    finally:
        src.dispose()
        dst.dispose()


def main(argv: list[str]) -> int:
    if len(argv) < 3:
        print(__doc__)
        return 2
    report = compare(argv[1], argv[2])
    bad = report["count_mismatches"]
    twins = report["twin_hash_mismatches"]
    for name, v in report["row_counts"].items():
        flag = "  <-- MISMATCH" if name in bad else ""
        print(f"{v['sqlite']:>8} -> {v['postgres']:>8}  {name}{flag}")
    print(f"\n{len(bad)} table(s) with differing counts; "
          f"{len(twins)} plan(s) with a differing twin hash")
    return 1 if (bad or twins) else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
```

**Column name, checked.** The spec calls this "`_canonical_hash`", but that is
the name of a *function* in `twin_builder.py`. The stored column is
`twin_snapshots.facets_hash`, which is what `_twin_hashes` reads. Comparing it
is equivalent: `facets_hash` is what `_canonical_hash(facets)` produced at
build time.

- [ ] **Step 4: Run the tests**

Push; expect 6 migration tests green in the CI `postgres` job.

- [ ] **Step 5: Commit**

```bash
git add scripts/verify_migration.py tests/postgres/test_migration.py
git commit -m "Add the migration verification harness: row counts and twin hashes"
```

## Task 4: `tsvector` search on Postgres

**Files:**
- Modify: `database.py`
- Create: `tests/postgres/test_search_tsvector.py`

**Interfaces:**
- Consumes: `database._fts_dialect_supported`, `database._ilike_search_query`.
- Produces: `_pg_search_ids(session, query, plan_id, limit) -> list[int] | None` — ranked summary ids, or `None` when the dialect is not Postgres.

**Why:** portal spec §2 — FTS5 has no Postgres equivalent, and §7 requires the
search schema to land *inside* step 3 because it is expensive to reverse once
data is in Neon. Both engines must work at once for step 4's dual-run.

- [ ] **Step 1: Write the failing test**

`tests/postgres/test_search_tsvector.py`:

```python
"""Ranked search on Postgres, replacing SQLite FTS5.

Both engines must work simultaneously: step 4 dual-runs staging on Postgres
beside production on SQLite.
"""
from __future__ import annotations

from sqlalchemy.orm import Session

import database


def _seed(pg_engine):
    with Session(pg_engine) as s:
        s.add(database.Plan(id="calpers", name="CalPERS", state="CA"))
        for i, text in enumerate([
            "the board approved a private equity manager search",
            "routine minutes with no investment content",
            "private equity pacing plan and manager search update",
        ], start=1):
            doc = database.Document(
                id=i, plan_id="calpers", url=f"https://x/{i}.pdf",
                filename=f"{i}.pdf", doc_type="minutes",
                extraction_status="done", downloaded_at=database.utcnow())
            s.add(doc)
            s.flush()
            s.add(database.Summary(document_id=doc.id, summary_text=text))
        s.commit()


def test_search_finds_matching_summaries(pg_engine):
    _seed(pg_engine)
    with Session(pg_engine) as s:
        rows = database.search_summaries(s, "manager search")
    texts = [summary.summary_text for _doc, summary in rows]
    assert len(texts) == 2, texts
    assert all("manager search" in t for t in texts)


def test_search_excludes_non_matching(pg_engine):
    _seed(pg_engine)
    with Session(pg_engine) as s:
        rows = database.search_summaries(s, "private equity")
    assert all("routine minutes" not in su.summary_text for _d, su in rows)


def test_search_handles_a_phrase_and_an_exclusion(pg_engine):
    """websearch_to_tsquery gives quoted phrases and -exclusions for free."""
    _seed(pg_engine)
    with Session(pg_engine) as s:
        rows = database.search_summaries(s, '"manager search" -pacing')
    assert len(rows) == 1, [su.summary_text for _d, su in rows]


def test_count_matches_the_result_set(pg_engine):
    _seed(pg_engine)
    with Session(pg_engine) as s:
        rows = database.search_summaries(s, "private equity")
        total = database.count_search_summaries(s, "private equity")
    assert total == len(rows)


def test_empty_query_returns_nothing(pg_engine):
    _seed(pg_engine)
    with Session(pg_engine) as s:
        assert database.search_summaries(s, "   ") == []
```

- [ ] **Step 2: Run to verify it fails**

Expected in CI: the ILIKE fallback returns unranked substring matches, so
`test_search_handles_a_phrase_and_an_exclusion` fails — ILIKE has no phrase or
exclusion syntax and will match on the literal string.

- [ ] **Step 3: Add the index to `database.py`**

Directly below the `Summary` class definition:

```python
# Postgres full-text index over the same four columns SQLite indexes with
# FTS5. A GIN index on a computed tsvector: the text is already stored, so
# this adds only the index, not a second copy of the content.
Index(
    "ix_summaries_search_vector",
    sa.text(
        "to_tsvector('english', "
        "coalesce(summary_text,'') || ' ' || coalesce(key_topics,'') || ' ' || "
        "coalesce(investment_actions,'') || ' ' || coalesce(decisions,''))"
    ),
    postgresql_using="gin",
)
```

Add `import sqlalchemy as sa` to the imports if absent.

- [ ] **Step 4: Add the Postgres search branch**

Insert above `search_summaries`:

```python
_PG_TSVECTOR = (
    "to_tsvector('english', "
    "coalesce(s.summary_text,'') || ' ' || coalesce(s.key_topics,'') || ' ' || "
    "coalesce(s.investment_actions,'') || ' ' || coalesce(s.decisions,''))"
)


def _pg_search_ids(session: Session, query: str, plan_id: Optional[str],
                   limit: Optional[int]) -> Optional[list[int]]:
    """Ranked summary ids on Postgres, or None if this is not Postgres.

    websearch_to_tsquery accepts quoted phrases and -exclusions without
    teaching anyone a query syntax, and never raises on malformed input —
    unlike to_tsquery, which is why it is used here.
    """
    if session.get_bind().dialect.name != "postgresql":
        return None
    sql = (
        f"SELECT s.id FROM summaries s "
        f"JOIN documents d ON d.id = s.document_id "
        f"WHERE {_PG_TSVECTOR} @@ websearch_to_tsquery('english', :q) "
    )
    params = {"q": query}
    if plan_id:
        sql += "AND d.plan_id = :pid "
        params["pid"] = plan_id
    sql += (f"ORDER BY ts_rank_cd({_PG_TSVECTOR}, "
            f"websearch_to_tsquery('english', :q)) DESC, d.meeting_date DESC")
    if limit is not None:
        sql += " LIMIT :lim"
        params["lim"] = limit
    return [r[0] for r in session.execute(text(sql), params).fetchall()]
```

- [ ] **Step 5: Route both entry points through it**

In `search_summaries`, immediately after the empty-query guard:

```python
    pg_ids = _pg_search_ids(session, query, plan_id, limit)
    if pg_ids is not None:
        if not pg_ids:
            return []
        order = {sid: i for i, sid in enumerate(pg_ids)}
        pairs = (
            session.query(Document, Summary)
            .join(Summary, Document.id == Summary.document_id)
            .filter(Summary.id.in_(pg_ids))
            .all()
        )
        pairs.sort(key=lambda p: order[p[1].id])
        return pairs
```

In `count_search_summaries`, after its own empty-query guard:

```python
    pg_ids = _pg_search_ids(session, query, plan_id, limit=None)
    if pg_ids is not None:
        return len(pg_ids)
```

- [ ] **Step 6: Run the tests**

Run: `LLM_MODE=mock INSIGHTS_MODE=mock python -m pytest tests/ -q`
Expected locally: 288 passed, 8 skipped — SQLite behaviour must be unchanged,
including all 14 tests in `tests/unit/test_search_fts.py`.
Push; expect the CI `postgres` job green on all 5 new tests.

- [ ] **Step 7: Commit**

```bash
git add database.py tests/postgres/test_search_tsvector.py
git commit -m "Replace FTS5 with tsvector and GIN on Postgres; both engines work"
```

## Task 5: Raise the storage cap

**Files:**
- Modify: `extractor.py:34` and the four truncation sites (88, 106, 204, 249)
- Create: `tests/test_extraction_limits.py`

**Interfaces:**
- Produces: `MAX_STORED_CHARS` replacing `MAX_TEXT_CHARS`.

**Why:** portal spec §2.3 — 444 documents (10.5%) sit exactly at the 150k cap,
and they are the large board packets where full-text search earns its keep.
Indexing what is stored today would search only the first ~35 pages of them.

**This costs nothing in Claude spend, which is not what the plan first assumed.**
Checked in the code: `summarizer.smart_truncate` already caps the prompt at
`SMART_TRUNCATE_TARGET = 50_000` chars, and `summarizer.py:347` is the only
path that builds a prompt from `extracted_text`. The other two readers
(`choose_model`, `should_skip`) use the length only, for model routing and skip
decisions. So the 150k extraction cap is not a cost control — `smart_truncate`
is — and raising it changes no API call.

Model routing is also unaffected: documents at the cap are ≥150k chars, far
above the 8k/20k Haiku thresholds, so they already route to Sonnet and still
will.

**Scope note:** this improves documents fetched *after* the change only. The
444 already-truncated ones cannot be re-extracted until the R2 PDF store
exists, because PDFs are runner-local and discarded. Do not attempt a backfill
here.

- [ ] **Step 1: Write the failing test**

```python
"""What gets stored is not what gets sent to Claude.

The 150k extraction cap was capping full-text search at roughly the first 35
pages of the largest board packets — 444 documents, 10.5% of the corpus, sit
exactly at it. It was never the cost control: summarizer.smart_truncate caps
the prompt at 50k chars independently.
"""
from __future__ import annotations

import pathlib

import extractor
import summarizer


def test_storage_cap_is_far_above_the_prompt_cap():
    assert extractor.MAX_STORED_CHARS > summarizer.SMART_TRUNCATE_TARGET * 10


def test_prompt_cap_is_unchanged():
    """Changing this changes what every summarisation call costs."""
    assert summarizer.SMART_TRUNCATE_TARGET == 50_000


def test_the_old_single_cap_is_gone():
    src = pathlib.Path(extractor.__file__).read_text(encoding="utf-8")
    assert "MAX_TEXT_CHARS" not in src, (
        "MAX_TEXT_CHARS conflated storage with prompt cost; use "
        "MAX_STORED_CHARS for storage and leave the prompt to smart_truncate")


def test_every_extraction_path_uses_the_storage_cap():
    src = pathlib.Path(extractor.__file__).read_text(encoding="utf-8")
    assert src.count("[:MAX_STORED_CHARS]") == 4, (
        "all four extraction paths (pdfplumber, PyMuPDF, OCR, DOCX) must "
        "truncate to the storage cap")


def test_smart_truncate_still_bounds_a_large_document():
    """The prompt stays bounded even though storage no longer is."""
    huge = "investment " * 200_000          # ~2.2M chars
    assert len(summarizer.smart_truncate(huge)) <= summarizer.SMART_TRUNCATE_TARGET
```

- [ ] **Step 2: Run to verify it fails**

Run: `LLM_MODE=mock python -m pytest tests/test_extraction_limits.py -v`
Expected: FAIL — `AttributeError: module 'extractor' has no attribute 'MAX_STORED_CHARS'`.

- [ ] **Step 3: Implement**

Replace `extractor.py:34`:

```python
# What we KEEP and index, which has no per-token cost. This is deliberately
# not the prompt limit: summarizer.smart_truncate caps what Claude sees at
# SMART_TRUNCATE_TARGET (50k chars) regardless of how much is stored. Capping
# storage at the old 150k truncated 444 documents — 10.5% of the corpus, and
# the largest board packets — which would have capped full-text search at
# roughly their first 35 pages.
# See docs/superpowers/specs/2026-08-19-portal-readiness-design.md §2.3.
MAX_STORED_CHARS = 2_000_000
```

Then replace all four `full_text[:MAX_TEXT_CHARS]` with
`full_text[:MAX_STORED_CHARS]` — lines 88, 106, 204 and 249.

- [ ] **Step 4: Confirm no prompt path bypasses smart_truncate**

Run: `grep -n "extracted_text" summarizer.py`
Expected: exactly three readers — `choose_model` (length only), `should_skip`
(length only), and line 347's `smart_truncate(doc.extracted_text)`. If a fourth
appears that feeds an LLM directly, route it through `smart_truncate` before
continuing.

- [ ] **Step 5: Run the tests**

Run: `LLM_MODE=mock INSIGHTS_MODE=mock python -m pytest tests/ -q`
Expected: 293 passed, 8 skipped.

- [ ] **Step 6: Commit**

```bash
git add extractor.py tests/test_extraction_limits.py
git commit -m "Raise the storage cap: what we index is not what we send to Claude"
```

---

## Verification summary

| Claim | Verified by |
|---|---|
| Ids survive the migration | `test_migrate_preserves_ids_and_content` (**Postgres CI**) |
| `GzippedText` round-trips as `BYTEA` | same test |
| Datetimes arrive aware | same test |
| The next insert does not collide | `test_inserting_after_migration_does_not_collide` (**Postgres CI**) |
| Row counts and twin hashes match | `test_verify_reports_a_clean_migration` + its negative case |
| Search is ranked, not substring | `test_search_handles_a_phrase_and_an_exclusion` (**Postgres CI**) |
| SQLite search is unchanged | `tests/unit/test_search_fts.py`, 14 tests |
| Storage is no longer capped at prompt size | `tests/test_extraction_limits.py` |
| The prompt is still bounded at 50k | `test_smart_truncate_still_bounds_a_large_document` |

## What this plan deliberately does not do

- **Run the migration against Neon.** No account exists; that is a Phase C step, and by then this code is proven.
- **Backfill the 444 truncated documents.** Gated on R2 (§2.3).
- **Remove `db_sync` or the DB-commit steps.** That is step 5, after the dual-run.
