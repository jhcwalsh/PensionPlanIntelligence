"""Schema + basic CRUD for the daily_runs tracking table."""
from __future__ import annotations

from datetime import date, datetime

from database import DailyRun, Publication, get_session


def test_daily_runs_table_exists_after_init():
    """init_db (autouse via _isolated_environment) creates daily_runs."""
    s = get_session()
    try:
        assert s.query(DailyRun).count() == 0
    finally:
        s.close()


def test_record_daily_run_inserts_row():
    s = get_session()
    try:
        pub = Publication(
            cadence="daily",
            period_start=date(2026, 5, 16),
            period_end=date(2026, 5, 16),
            status="published",
        )
        s.add(pub)
        s.flush()

        row = DailyRun(
            sent_at=datetime(2026, 5, 16, 13, 0, 0),
            publication_id=pub.id,
            docs_count=5,
            triggers=["volume:12"],
            approval_gated=True,
        )
        s.add(row)
        s.commit()

        fetched = s.query(DailyRun).one()
        assert fetched.docs_count == 5
        assert fetched.triggers == ["volume:12"]
        assert fetched.approval_gated is True
        assert fetched.publication_id == pub.id
    finally:
        s.close()


def test_last_sent_at_returns_max_sent_at():
    s = get_session()
    try:
        pub = Publication(
            cadence="daily", period_start=date(2026, 5, 16),
            period_end=date(2026, 5, 16), status="published",
        )
        s.add(pub); s.flush()

        s.add_all([
            DailyRun(sent_at=datetime(2026, 5, 14, 13, 0), publication_id=pub.id,
                     docs_count=0, triggers=[], approval_gated=False),
            DailyRun(sent_at=datetime(2026, 5, 16, 13, 0), publication_id=pub.id,
                     docs_count=3, triggers=[], approval_gated=False),
            DailyRun(sent_at=datetime(2026, 5, 15, 13, 0), publication_id=pub.id,
                     docs_count=1, triggers=[], approval_gated=False),
        ])
        s.commit()

        from sqlalchemy import func
        last = s.query(func.max(DailyRun.sent_at)).scalar()
        assert last == datetime(2026, 5, 16, 13, 0)
    finally:
        s.close()
