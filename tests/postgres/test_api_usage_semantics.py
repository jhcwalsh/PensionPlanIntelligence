"""api_usage against a real Postgres.

Two things SQLite cannot show. It stores TIMESTAMPTZ as naive text, so an
aware round-trip is unobservable there; and it has no NUMERIC, so it cannot
demonstrate that summed money keeps its precision.

The second matters more than it looks. cost_usd is Numeric(12, 6) because
individual calls cost fractions of a cent and the useful question is always a
sum over thousands of them — the shape where float error accumulates.
"""

from __future__ import annotations

from decimal import Decimal

import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

import database


def test_occurred_at_round_trips_aware(pg_engine):
    """The column is TIMESTAMPTZ and reads come back aware."""
    with sessionmaker(bind=pg_engine)() as session:
        row = database.ApiUsage(
            model="claude-haiku-4-5-20251001", operation="test",
            input_tokens=10, output_tokens=1, cost_usd=Decimal("0.000015"))
        session.add(row)
        session.commit()
        session.refresh(row)
        assert row.occurred_at.tzinfo is not None


def test_cost_survives_summation_without_float_drift(pg_engine):
    """1,000 calls at $0.000015 is exactly $0.015 — not $0.014999999.

    A Float column would make the Spend tab's totals drift, and drift in a
    money figure destroys trust in the whole measurement.
    """
    with sessionmaker(bind=pg_engine)() as session:
        for _ in range(1000):
            session.add(database.ApiUsage(
                model="claude-haiku-4-5-20251001", operation="test",
                input_tokens=10, output_tokens=1,
                cost_usd=Decimal("0.000015")))
        session.commit()

        total = session.execute(
            sa.select(sa.func.sum(database.ApiUsage.cost_usd))).scalar()

    assert isinstance(total, Decimal), type(total)
    assert total == Decimal("0.015000"), total


def test_the_operation_index_exists(pg_engine):
    """The Spend tab groups by operation over a date window."""
    with pg_engine.connect() as conn:
        names = {r[0] for r in conn.execute(sa.text(
            "select indexname from pg_indexes where tablename = 'api_usage'"))}
    assert "ix_api_usage_operation" in names, names
    assert "ix_api_usage_occurred" in names, names
