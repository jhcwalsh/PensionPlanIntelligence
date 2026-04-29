"""Verify ``(cadence, period_start)`` is unique and re-runs reuse rows."""

from __future__ import annotations

from datetime import date

import pytest

from database import Publication, get_session
from insights.cycle_common import find_or_create_publication


def test_find_or_create_returns_same_row_for_same_period():
    s = get_session()
    try:
        a = find_or_create_publication(
            s, cadence="weekly",
            period_start=date(2026, 4, 19), period_end=date(2026, 4, 25),
        )
        s.commit()
        b = find_or_create_publication(
            s, cadence="weekly",
            period_start=date(2026, 4, 19), period_end=date(2026, 4, 25),
        )
        s.commit()
        assert a.id == b.id
        assert s.query(Publication).count() == 1
    finally:
        s.close()


def test_different_cadence_same_period_get_distinct_rows():
    s = get_session()
    try:
        weekly = find_or_create_publication(
            s, cadence="weekly",
            period_start=date(2026, 4, 1), period_end=date(2026, 4, 30),
        )
        monthly = find_or_create_publication(
            s, cadence="monthly",
            period_start=date(2026, 4, 1), period_end=date(2026, 4, 30),
        )
        s.commit()
        assert weekly.id != monthly.id
        assert s.query(Publication).count() == 2
    finally:
        s.close()


def test_concurrent_insert_is_handled_by_unique_constraint():
    """Force two inserts of the same (cadence, period_start) and verify one wins.

    Simulates the race by inserting an ORM row directly, then asking
    ``find_or_create_publication`` to insert the same key.
    """
    s1 = get_session()
    try:
        s1.add(Publication(
            cadence="weekly",
            period_start=date(2026, 4, 19),
            period_end=date(2026, 4, 25),
        ))
        s1.commit()
    finally:
        s1.close()

    s2 = get_session()
    try:
        winner = find_or_create_publication(
            s2, cadence="weekly",
            period_start=date(2026, 4, 19), period_end=date(2026, 4, 25),
        )
        s2.commit()
        assert s2.query(Publication).count() == 1
        assert winner.cadence == "weekly"
    finally:
        s2.close()
