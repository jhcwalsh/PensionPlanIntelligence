"""The timezone behaviour SQLite cannot reproduce.

SQLite's DATETIME storage format has no timezone field, so it strips the offset
from an aware value on write — and it ignores DateTime(timezone=True) entirely.
Both facts were verified before this file was written. That makes every
assertion here impossible to make anywhere else in the suite.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.orm import Session

import database


def test_every_datetime_column_is_timestamptz(pg_engine):
    """On Postgres the flag is real: TIMESTAMPTZ vs TIMESTAMP.

    This is the assertion the SQLite-side metadata guard in
    tests/test_datetime_discipline.py can only approximate.
    """
    insp = sa.inspect(pg_engine)
    naive = []
    for table in database.Base.metadata.sorted_tables:
        for col in insp.get_columns(table.name):
            col_type = col["type"]
            if isinstance(col_type, sa.DateTime) and not col_type.timezone:
                naive.append(f"{table.name}.{col['name']}")
    assert not naive, (
        f"{len(naive)} column(s) are still TIMESTAMP WITHOUT TIME ZONE: "
        + ", ".join(naive))


def test_aware_value_round_trips_with_its_offset(pg_engine):
    """tzinfo survives the round trip — the thing SQLite silently destroys."""
    written = database.utcnow()
    with Session(pg_engine) as session:
        session.add(database.PipelineRun(started_at=written, status="test"))
        session.commit()

    with Session(pg_engine) as session:
        read = session.query(database.PipelineRun.started_at).scalar()

    assert read.tzinfo is not None, "Postgres returned a naive datetime"
    assert read == written, f"value changed in transit: {written} -> {read}"


def test_reads_can_be_compared_against_utcnow(pg_engine):
    """The comparison that raises TypeError when either side is naive.

    Roughly 20 cutoff/arithmetic sites across the codebase do exactly this,
    which is why the call-site sweep has to land with the column-type change
    rather than after it.
    """
    with Session(pg_engine) as session:
        session.add(database.PipelineRun(started_at=database.utcnow(),
                                         status="test"))
        session.commit()

    with Session(pg_engine) as session:
        read = session.query(database.PipelineRun.started_at).scalar()

    elapsed = database.utcnow() - read          # TypeError if either is naive
    assert elapsed.total_seconds() >= 0
