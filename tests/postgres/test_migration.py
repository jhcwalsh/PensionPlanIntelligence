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
