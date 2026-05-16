"""Selection of "new since last digest" documents."""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from database import Document, Plan, get_session
from insights import daily


@pytest.fixture()
def seeded_plan_and_docs():
    s = get_session()
    try:
        s.add(Plan(id="p1", name="Plan One", abbreviation="P1"))
        s.flush()

        # Three docs at known downloaded_at times.
        s.add_all([
            Document(plan_id="p1", url="u1", filename="f1.pdf",
                     doc_type="agenda", downloaded_at=datetime(2026, 5, 14, 10, 0),
                     meeting_date=datetime(2026, 5, 12)),
            Document(plan_id="p1", url="u2", filename="f2.pdf",
                     doc_type="minutes", downloaded_at=datetime(2026, 5, 15, 10, 0),
                     meeting_date=datetime(2026, 5, 13)),
            Document(plan_id="p1", url="u3", filename="f3.pdf",
                     doc_type="agenda", downloaded_at=datetime(2026, 5, 16, 10, 0),
                     meeting_date=datetime(2026, 5, 16)),
            # Discovered but never downloaded — excluded.
            Document(plan_id="p1", url="u4", filename=None,
                     doc_type="agenda", downloaded_at=None),
        ])
        s.commit()
    finally:
        s.close()


def test_select_new_docs_uses_strict_greater_than_boundary(seeded_plan_and_docs):
    s = get_session()
    try:
        # since = exact downloaded_at of doc u2 → u2 excluded, u3 included.
        docs = daily.select_new_docs(
            since=datetime(2026, 5, 15, 10, 0),
            now_utc=datetime(2026, 5, 17, 0, 0),
            session=s,
        )
        urls = [d.url for d in docs]
        assert urls == ["u3"]
    finally:
        s.close()


def test_select_new_docs_excludes_null_downloaded_at(seeded_plan_and_docs):
    s = get_session()
    try:
        docs = daily.select_new_docs(
            since=datetime(2026, 5, 13, 0, 0),
            now_utc=datetime(2026, 5, 17, 0, 0),
            session=s,
        )
        urls = sorted(d.url for d in docs)
        assert urls == ["u1", "u2", "u3"]
        assert "u4" not in urls
    finally:
        s.close()


def test_select_new_docs_excludes_future_dated_rows(seeded_plan_and_docs):
    s = get_session()
    try:
        # Pretend "now" is between u2 and u3 — u3's downloaded_at is in the future.
        docs = daily.select_new_docs(
            since=datetime(2026, 5, 13, 0, 0),
            now_utc=datetime(2026, 5, 15, 12, 0),
            session=s,
        )
        urls = sorted(d.url for d in docs)
        assert urls == ["u1", "u2"]
    finally:
        s.close()


def test_select_new_docs_none_since_uses_24h_fallback(seeded_plan_and_docs):
    s = get_session()
    try:
        # No prior daily_runs row → fall back to (now - 24h, now).
        docs = daily.select_new_docs(
            since=None,
            now_utc=datetime(2026, 5, 16, 10, 30),
            session=s,
        )
        urls = sorted(d.url for d in docs)
        # 24h before 2026-05-16 10:30 = 2026-05-15 10:30.
        # u1 (2026-05-14 10:00) — before cutoff, excluded.
        # u2 (2026-05-15 10:00) — before cutoff (10:00 < 10:30 same day), excluded.
        # u3 (2026-05-16 10:00) — after cutoff (next day), before now_utc, included.
        assert urls == ["u3"]
    finally:
        s.close()


def test_select_new_docs_orders_by_plan_then_meeting_date_desc():
    s = get_session()
    try:
        s.add_all([
            Plan(id="a_plan", name="A Plan", abbreviation="AP"),
            Plan(id="z_plan", name="Z Plan", abbreviation="ZP"),
        ])
        s.flush()
        s.add_all([
            Document(plan_id="z_plan", url="z1", filename="z1.pdf",
                     downloaded_at=datetime(2026, 5, 16, 10, 0),
                     meeting_date=datetime(2026, 5, 10)),
            Document(plan_id="a_plan", url="a1", filename="a1.pdf",
                     downloaded_at=datetime(2026, 5, 16, 10, 0),
                     meeting_date=datetime(2026, 5, 12)),
            Document(plan_id="a_plan", url="a2", filename="a2.pdf",
                     downloaded_at=datetime(2026, 5, 16, 10, 0),
                     meeting_date=datetime(2026, 5, 14)),
        ])
        s.commit()

        docs = daily.select_new_docs(
            since=datetime(2026, 5, 1),
            now_utc=datetime(2026, 5, 17),
            session=s,
        )
        # Ordered by plan_id then meeting_date desc.
        assert [d.url for d in docs] == ["a2", "a1", "z1"]
    finally:
        s.close()
