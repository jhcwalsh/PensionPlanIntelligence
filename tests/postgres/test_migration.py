"""The SQLite -> Postgres migration, run against a real Postgres.

Verified here rather than on SQLite because id preservation, sequence state
and BYTEA round-tripping are all dialect behaviour.
"""
from __future__ import annotations

from datetime import timedelta

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


def _seed_sqlite_with_twins(path):
    """Two plans, each with a twin_snapshots history with a distinct latest hash.

    Separate from _seed_sqlite so the existing document-focused tests keep
    their exact row shape.
    """
    engine = sa.create_engine(f"sqlite:///{path}")
    database.Base.metadata.create_all(engine)
    with Session(engine) as s:
        s.add(database.Plan(id="calpers", name="CalPERS", state="CA"))
        s.add(database.Plan(id="opers", name="OPERS", state="OH"))
        t0 = database.utcnow()
        s.add(database.TwinSnapshot(
            id=1, plan_id="calpers", built_at=t0 - timedelta(days=2),
            schema_version="1", facets="{}", facets_hash="hash-calpers-old"))
        s.add(database.TwinSnapshot(
            id=2, plan_id="calpers", built_at=t0 - timedelta(days=1),
            schema_version="1", facets="{}", facets_hash="hash-calpers-new"))
        s.add(database.TwinSnapshot(
            id=3, plan_id="opers", built_at=t0 - timedelta(days=1),
            schema_version="1", facets="{}", facets_hash="hash-opers-latest"))
        s.commit()
    engine.dispose()
    return engine


def _seed_sqlite_with_tied_snapshots(path):
    """One plan, two snapshots sharing the same built_at but different hashes.

    Pins the tiebreaker: without ordering by id as well, SQLite and Postgres
    are not guaranteed to agree on which row is "latest".
    """
    engine = sa.create_engine(f"sqlite:///{path}")
    database.Base.metadata.create_all(engine)
    with Session(engine) as s:
        s.add(database.Plan(id="calpers", name="CalPERS", state="CA"))
        tied = database.utcnow()
        s.add(database.TwinSnapshot(
            id=1, plan_id="calpers", built_at=tied,
            schema_version="1", facets="{}", facets_hash="hash-lower-id"))
        s.add(database.TwinSnapshot(
            id=2, plan_id="calpers", built_at=tied,
            schema_version="1", facets="{}", facets_hash="hash-higher-id"))
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
    """Older DB files legitimately lack tables added later.

    Creates ONLY plans and documents in the source, so migrate()'s skip
    branch is genuinely exercised — with a full metadata.create_all the
    branch is unreachable and this test would pass with the guard deleted.
    """
    src = tmp_path / "partial.db"
    engine = sa.create_engine(f"sqlite:///{src}")
    database.Plan.__table__.create(engine)
    database.Document.__table__.create(engine)
    with Session(engine) as s:
        s.add(database.Plan(id="calpers", name="CalPERS", state="CA"))
        s.commit()
    engine.dispose()

    counts = migrate(str(src), str(pg_engine.url))

    assert counts["plans"] == 1
    assert "twin_snapshots" not in counts, \
        "a table absent from the source must be skipped, not reported"


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


def test_verify_matches_twin_hashes_across_multiple_plans(tmp_path, pg_engine):
    """The happy path, but with twin_snapshots rows actually present."""
    from scripts.verify_migration import compare
    src = tmp_path / "src.db"
    _seed_sqlite_with_twins(src)
    migrate(str(src), str(pg_engine.url))

    report = compare(str(src), str(pg_engine.url))
    assert report["twin_hash_mismatches"] == []


def test_verify_detects_a_changed_twin_hash(tmp_path, pg_engine):
    """The test that proves the twin-hash comparison can actually fail."""
    from scripts.verify_migration import compare
    src = tmp_path / "src.db"
    _seed_sqlite_with_twins(src)
    migrate(str(src), str(pg_engine.url))

    with Session(pg_engine) as s:
        snap = s.get(database.TwinSnapshot, 2)  # calpers' latest snapshot
        snap.facets_hash = "corrupted-hash"
        s.commit()

    report = compare(str(src), str(pg_engine.url))
    assert report["twin_hash_mismatches"] == ["calpers"]


def test_verify_breaks_built_at_ties_by_id_consistently(tmp_path, pg_engine):
    """Two snapshots sharing built_at must resolve to the same row on both sides."""
    from scripts.verify_migration import compare, _twin_hashes
    src = tmp_path / "src.db"
    _seed_sqlite_with_tied_snapshots(src)
    migrate(str(src), str(pg_engine.url))

    report = compare(str(src), str(pg_engine.url))
    assert report["twin_hash_mismatches"] == []

    src_engine = sa.create_engine(f"sqlite:///{src}")
    try:
        assert _twin_hashes(src_engine)["calpers"] == "hash-higher-id", \
            "tiebreaker must deterministically pick the higher id"
    finally:
        src_engine.dispose()
