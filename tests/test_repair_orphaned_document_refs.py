"""Foreign keys that point at deleted documents must be reconciled, not lost.

SQLite left these unenforced for the life of the project; Postgres rejects
them on insert, which is what stopped the migration. The repair has to make
two different calls -- keep the row and drop the pointer, or drop the row --
and getting that backwards silently destroys real data: `cafr_extract` 151
carries 158 child allocation and performance rows that cascade away with it.

See scripts/repair_orphaned_document_refs.py.
"""

from __future__ import annotations

import json
import sqlite3

import pytest

import database
from scripts.repair_orphaned_document_refs import (
    UnknownOrphan, find_orphans, repair,
)


# cafr_extract exactly as it stands in db/pension.db, i.e. before document_id
# was made nullable. create_all() would emit the *fixed* model instead, which
# would leave the whole NOT NULL rebuild untested -- the first version of this
# file made that mistake and every rebuild assertion below passed vacuously.
LEGACY_CAFR_EXTRACT = """
CREATE TABLE cafr_extract (
    id INTEGER NOT NULL,
    plan_id VARCHAR NOT NULL,
    document_id INTEGER NOT NULL,
    fiscal_year INTEGER,
    investment_policy_text TEXT,
    extracted_at DATETIME,
    model_used VARCHAR,
    pages_used VARCHAR,
    text_hash VARCHAR,
    notes TEXT,
    PRIMARY KEY (id),
    FOREIGN KEY(plan_id) REFERENCES plans (id),
    UNIQUE (document_id),
    FOREIGN KEY(document_id) REFERENCES documents (id)
)
"""


@pytest.fixture
def db(tmp_path):
    """Production's schema: models everywhere, legacy DDL for cafr_extract.

    Everything but cafr_extract comes from create_all, so the constraints
    under test are the ones production actually has. cafr_extract is then
    replaced with the on-disk version so that the NOT NULL the repair has to
    relax is really there.
    """
    from sqlalchemy import create_engine

    path = tmp_path / "orphans.db"
    engine = create_engine("sqlite:///%s" % path)
    database.Base.metadata.create_all(engine)
    engine.dispose()

    con = sqlite3.connect(path)
    con.execute("PRAGMA legacy_alter_table = ON")  # do not rewrite child FKs
    con.execute("DROP TABLE cafr_extract")
    con.execute(LEGACY_CAFR_EXTRACT)
    con.execute("CREATE INDEX ix_cafr_extract_plan_fy "
                "ON cafr_extract (plan_id, fiscal_year)")
    con.execute("INSERT INTO plans (id, name) VALUES ('opers', 'Ohio PERS')")
    con.execute("INSERT INTO documents (id, plan_id, url, doc_type) "
                "VALUES (900, 'opers', 'http://x/live.pdf', 'cafr')")
    con.commit()
    con.close()
    return str(path)


def _document_id_is_not_null(path):
    con = sqlite3.connect(path)
    try:
        return any(r[1] == "document_id" and r[3] == 1
                   for r in con.execute("PRAGMA table_info(cafr_extract)"))
    finally:
        con.close()


def _seed_orphans(path):
    """One orphan per policy, plus a healthy row of each kind to spare."""
    con = sqlite3.connect(path)
    con.executescript(
        """
        INSERT INTO cafr_extract (id, plan_id, document_id, fiscal_year)
             VALUES (1, 'opers', 4315, 2025),      -- orphan: doc deleted
                    (2, 'opers',  900, 2024);      -- healthy
        INSERT INTO cafr_allocation (id, cafr_extract_id, asset_class, target_pct)
             VALUES (10, 1, 'Public Equity', 40.0),
                    (11, 1, 'Fixed Income',  15.0);
        INSERT INTO cafr_refresh_log (id, plan_id, run_at, status, document_id)
             VALUES (1, 'opers', '2026-06-01 00:00:00', 'saved', 4315),
                    (2, 'opers', '2026-06-01 00:00:00', 'saved',  900);
        -- evaluated_at is NOT NULL with a Python-side default, so raw SQL
        -- has to supply it.
        INSERT INTO document_health
                    (document_id, prompt_version, stage1_verdict, evaluated_at)
             VALUES (749, 'rfp_v1', 'STAGE_1_SUSPECTED', '2026-04-30 05:38:30'),
                    (900, 'rfp_v1', 'STAGE_1_HEALTHY',   '2026-04-30 05:48:28');
        """
    )
    con.commit()
    con.close()


def test_find_orphans_names_the_column_not_just_the_table(db):
    """foreign_key_check reports an FK ordinal; the repair needs the column."""
    _seed_orphans(db)
    con = sqlite3.connect(db)
    try:
        found = find_orphans(con)
    finally:
        con.close()

    assert {(o["table"], o["column"]) for o in found} == {
        ("cafr_extract", "document_id"),
        ("cafr_refresh_log", "document_id"),
        ("document_health", "document_id"),
    }
    # cafr_extract also has a plan_id FK; resolving the ordinal to the wrong
    # column would name that one instead and the UPDATE would clear plan_id.
    assert all(o["column"] == "document_id" for o in found)


def test_the_fixture_really_carries_the_constraint_under_test(db):
    """Guard on the guard.

    If create_all ever supplies cafr_extract again, document_id arrives
    already nullable, the repair skips the rebuild, and every rebuild
    assertion in this file passes without executing a line of it.
    """
    assert _document_id_is_not_null(db), \
        "cafr_extract.document_id is already nullable -- the rebuild is untested"


def test_the_constraint_is_relaxed(db):
    """The rebuild has to actually happen, and outlast the run."""
    _seed_orphans(db)
    repair(db)
    assert not _document_id_is_not_null(db)


def test_the_extraction_survives_with_its_children(db):
    """The whole point: keep the row, drop the pointer.

    Deleting cafr_extract 1 would take both cafr_allocation rows with it via
    the delete-orphan cascade, so their survival is what proves the row was
    preserved rather than re-created.
    """
    _seed_orphans(db)
    repair(db)

    con = sqlite3.connect(db)
    try:
        row = con.execute("SELECT document_id, fiscal_year FROM cafr_extract "
                          "WHERE id = 1").fetchone()
        allocations = con.execute(
            "SELECT COUNT(*) FROM cafr_allocation WHERE cafr_extract_id = 1"
        ).fetchone()[0]
        healthy = con.execute("SELECT document_id FROM cafr_extract "
                              "WHERE id = 2").fetchone()[0]
    finally:
        con.close()

    assert row == (None, 2025), "the extraction was destroyed or altered"
    assert allocations == 2, "the child rows cascaded away"
    assert healthy == 900, "a valid reference was cleared"


def test_the_rebuild_keeps_child_tables_pointing_at_the_real_table(db):
    """The legacy_alter_table trap.

    Relaxing NOT NULL means renaming cafr_extract out of the way. Modern
    SQLite "helpfully" rewrites other tables' FK clauses to follow a rename,
    which would leave cafr_allocation referencing _repair_old_cafr_extract --
    a table that no longer exists. Nothing would fail until the next
    foreign_key_check, or until Postgres refused the schema.
    """
    _seed_orphans(db)
    repair(db)

    con = sqlite3.connect(db)
    try:
        ddl = con.execute("SELECT sql FROM sqlite_master "
                          "WHERE name = 'cafr_allocation'").fetchone()[0]
        tables = {r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'")}
    finally:
        con.close()

    assert "_repair_old_" not in ddl, ddl
    assert "cafr_extract" in ddl
    assert not any(t.startswith("_repair_old_") for t in tables), tables


def test_the_unique_constraint_survives_the_rebuild(db):
    """document_id is UNIQUE. Emitting the table by hand would lose that."""
    _seed_orphans(db)
    repair(db)

    con = sqlite3.connect(db)
    try:
        con.execute("INSERT INTO documents (id, plan_id, url, doc_type) "
                    "VALUES (901, 'opers', 'http://x/b.pdf', 'cafr')")
        con.execute("INSERT INTO cafr_extract (id, plan_id, document_id) "
                    "VALUES (3, 'opers', 901)")
        with pytest.raises(sqlite3.IntegrityError):
            con.execute("INSERT INTO cafr_extract (id, plan_id, document_id) "
                        "VALUES (4, 'opers', 901)")
    finally:
        con.close()


def test_multiple_cleared_references_do_not_collide(db):
    """NULL is exempt from UNIQUE -- but only if the rebuild kept it a real
    UNIQUE constraint rather than a primary key or a NOT NULL index."""
    con = sqlite3.connect(db)
    con.executescript(
        """
        INSERT INTO cafr_extract (id, plan_id, document_id, fiscal_year)
             VALUES (1, 'opers', 4315, 2025),
                    (2, 'opers', 4316, 2023);
        """
    )
    con.commit()
    con.close()

    repair(db)

    con = sqlite3.connect(db)
    try:
        cleared = con.execute("SELECT COUNT(*) FROM cafr_extract "
                              "WHERE document_id IS NULL").fetchone()[0]
    finally:
        con.close()
    assert cleared == 2


def test_identity_bearing_rows_are_deleted(db):
    """document_health.document_id is half the primary key -- it cannot be
    cleared, so the row goes. The healthy row must not."""
    _seed_orphans(db)
    repair(db)

    con = sqlite3.connect(db)
    try:
        remaining = [r[0] for r in con.execute(
            "SELECT document_id FROM document_health")]
    finally:
        con.close()
    assert remaining == [900]


def test_every_row_is_dumped_before_it_is_touched(db, tmp_path):
    """The dump is the only copy of a deleted row. It must be complete and
    hold the real field values, not just the ids."""
    _seed_orphans(db)
    dump_path = tmp_path / "dump.json"
    repair(db, dump_path=str(dump_path))

    dumped = json.loads(dump_path.read_text(encoding="utf-8"))
    assert len(dumped) == 3  # one orphan per policy seeded by _seed_orphans
    by_table = {}
    for entry in dumped:
        by_table.setdefault(entry["table"], []).append(entry["row"])

    assert by_table["cafr_extract"][0]["fiscal_year"] == 2025
    assert by_table["document_health"][0]["stage1_verdict"] == "STAGE_1_SUSPECTED"
    assert by_table["cafr_refresh_log"][0]["status"] == "saved"


def test_a_second_run_changes_nothing(db):
    """Idempotent, the same contract as the gzip and UTC migrations."""
    _seed_orphans(db)
    first = repair(db)
    assert first

    con = sqlite3.connect(db)
    before = con.execute("SELECT COUNT(*) FROM cafr_extract").fetchone()[0]
    con.close()

    assert repair(db) == {}, "the second run found work to do"

    con = sqlite3.connect(db)
    try:
        after = con.execute("SELECT COUNT(*) FROM cafr_extract").fetchone()[0]
        doc_id = con.execute("SELECT document_id FROM cafr_extract "
                             "WHERE id = 1").fetchone()[0]
    finally:
        con.close()
    assert after == before
    assert doc_id is None


def test_a_clean_database_is_left_alone(db):
    """No orphans, no rebuild, no dump."""
    con = sqlite3.connect(db)
    con.execute("INSERT INTO cafr_extract (id, plan_id, document_id) "
                "VALUES (1, 'opers', 900)")
    con.commit()
    con.close()

    assert repair(db) == {}


def test_dry_run_reports_without_writing(db):
    _seed_orphans(db)
    summary = repair(db, dry_run=True)
    assert summary["cafr_extract.document_id"] == 1
    assert summary["document_health.document_id"] == 1

    con = sqlite3.connect(db)
    try:
        still_there = con.execute(
            "SELECT document_id FROM cafr_extract WHERE id = 1").fetchone()[0]
        health = con.execute("SELECT COUNT(*) FROM document_health").fetchone()[0]
    finally:
        con.close()
    assert still_there == 4315, "dry run mutated the database"
    assert health == 2, "dry run deleted a row"


def test_an_unrecognised_orphan_is_refused(db, monkeypatch):
    """A table with no policy must stop the run.

    Guessing between "clear the pointer" and "delete the row" is exactly the
    decision that loses data, so an unfamiliar table is an error, not a
    default. Simulated by removing a known policy rather than inventing a
    table, so the refusal is exercised against a real foreign key.
    """
    _seed_orphans(db)
    import scripts.repair_orphaned_document_refs as mod
    policy = dict(mod.POLICY)
    policy.pop(("cafr_extract", "document_id"))
    monkeypatch.setattr(mod, "POLICY", policy)

    with pytest.raises(UnknownOrphan, match="cafr_extract.document_id"):
        mod.repair(db)

    con = sqlite3.connect(db)
    try:
        health = con.execute("SELECT COUNT(*) FROM document_health").fetchone()[0]
    finally:
        con.close()
    assert health == 2, "the refusal must happen before anything is written"


def test_the_model_allows_a_missing_document():
    """The schema half of the fix. Without this, create_all on Postgres
    rebuilds the NOT NULL constraint and the next prune breaks it again."""
    assert database.CafrExtract.__table__.c.document_id.nullable is True
    assert database.CafrExtract.__table__.c.document_id.unique is True
