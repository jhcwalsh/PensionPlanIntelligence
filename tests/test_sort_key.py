"""Sorting a nullable datetime column must not depend on the backend.

`row.meeting_date or datetime.min` reads as obviously correct and is a silent
Postgres bug: datetime.min is naive, so as soon as one row in the list has a
NULL date and another does not, sorted() compares naive against aware and
raises TypeError. On SQLite reads are naive too, so it never fires.

The whole test suite ran green with five of these in place. It took rendering
the real Streamlit app against Neon to find the first -- the read layer
returned correct data and the crash was in the sort above it, which is why the
query-layer dual-run comparison could not see it either.

See docs/superpowers/plans/2026-08-21-postgres-dual-run.md, Task 4.
"""

from __future__ import annotations

import pathlib
import re
from datetime import datetime, timezone

import pytest

import database

ROOT = pathlib.Path(__file__).resolve().parents[1]


def test_a_naive_value_becomes_aware():
    out = database.sort_key(datetime(2026, 8, 20, 11, 0))
    assert out.tzinfo is not None
    assert out == datetime(2026, 8, 20, 11, 0, tzinfo=timezone.utc)


def test_an_aware_value_is_unchanged():
    value = datetime(2026, 8, 20, 11, 0, tzinfo=timezone.utc)
    assert database.sort_key(value) == value


def test_none_becomes_an_aware_floor():
    out = database.sort_key(None)
    assert out.tzinfo is not None, "the floor must be aware or it defeats the point"
    assert out == database.MIN_UTC


def test_the_floor_sorts_below_every_real_value():
    assert database.sort_key(None) < database.sort_key(datetime(1970, 1, 1))


def test_a_mixed_list_sorts_without_raising():
    """The exact shape that crashed: some rows dated, some NULL, aware values.

    This is what `or datetime.min` could not do.
    """
    values = [datetime(2026, 8, 20, tzinfo=timezone.utc),
              None,
              datetime(2026, 8, 19, tzinfo=timezone.utc),
              None]
    ordered = sorted(values, key=database.sort_key, reverse=True)
    assert ordered[0] == datetime(2026, 8, 20, tzinfo=timezone.utc)
    assert ordered[-1] is None


def test_the_old_idiom_would_have_raised():
    """Name the bug this replaces, so the test explains itself.

    If this ever stops raising, Python's comparison rules changed and the
    helper's rationale needs rewriting.
    """
    values = [datetime(2026, 8, 20, tzinfo=timezone.utc), None]
    with pytest.raises(TypeError, match="offset-naive and offset-aware"):
        sorted(values, key=lambda v: v or datetime.min)


# Files whose datetime.min comes from strptime on file *content*, where both
# sides are naive by construction and no database value is involved.
_ALLOWED = {
    "app.py": 2,          # _list_note_files and _find_latest_insights
}


def test_no_module_sorts_a_database_value_by_a_naive_floor():
    """A ratchet, in the style of tests/test_datetime_discipline.py.

    Five of these existed at once. Catching the sixth by rendering the app
    against Neon again is not a plan.
    """
    offenders = {}
    for path in ROOT.rglob("*.py"):
        rel = path.relative_to(ROOT).as_posix()
        if any(rel.startswith(p) for p in
               (".venv/", ".claude/", "tests/", "build/")):
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        # The comparison idiom, not the module reference: `datetime.min.time()`
        # and prose mentions are fine.
        hits = len(re.findall(r"or\s+datetime\.min\b(?!\.)", text))
        hits += len(re.findall(r"=\s*datetime\.min\b(?!\.)", text))
        allowed = _ALLOWED.get(path.name, 0)
        if hits > allowed:
            offenders[rel] = (hits, allowed)

    assert offenders == {}, (
        "naive datetime floor used where a database value may be aware: %s. "
        "Use database.sort_key() instead." % offenders)
