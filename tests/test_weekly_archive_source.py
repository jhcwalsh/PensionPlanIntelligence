"""The Weekly Insights archive reads the database, not notes/.

The tab globbed ``notes/7day_highlights_*.md`` and had been frozen on
2026-05-24 for three months. Nothing writes those files any more:
``insights/weekly.py:206`` composes with ``archive=False``, because weekly
exists to feed monthly rather than to be published on its own. Meanwhile 17
weekly Publications accumulated in the table with full ``draft_markdown``.

Nothing errored. The glob returned the newest of a directory that had
stopped growing, and the tab reported it faithfully -- which is why this is
worth a test rather than a comment: the failure mode is silent staleness,
and the next filename convention change would reintroduce it just as quietly.

The default status filter matches ``monthly._gather_approved_weeklies``. Those
are the weeks that actually feed the monthly, so the archive shows the same
set the cascade counts rather than implying that stranded weeks contributed.
"""

from __future__ import annotations

import pathlib
from datetime import date, datetime, timezone

import pytest

import database
import queries

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _weekly(session, start, end, status, markdown="# Week\n\nbody"):
    pub = database.Publication(
        cadence="weekly",
        period_start=start,
        period_end=end,
        status=status,
        draft_markdown=markdown,
        composed_at=datetime(2026, 8, 1, tzinfo=timezone.utc),
    )
    session.add(pub)
    return pub


@pytest.fixture()
def seeded(tmp_db):
    session = database.get_session()
    try:
        _weekly(session, date(2026, 8, 16), date(2026, 8, 22), "published")
        _weekly(session, date(2026, 8, 9), date(2026, 8, 15), "published")
        _weekly(session, date(2026, 8, 2), date(2026, 8, 8), "approved")
        _weekly(session, date(2026, 7, 26), date(2026, 8, 1), "awaiting_approval")
        _weekly(session, date(2026, 7, 19), date(2026, 7, 25), "expired")
        session.commit()
    finally:
        session.close()


def test_newest_week_comes_first(seeded):
    """The bug users saw: the tab opened on a briefing three months old."""
    session = database.get_session()
    try:
        rows = queries.weekly_briefings(session)
        assert rows, "no weekly briefings returned"
        assert rows[0].period_start == date(2026, 8, 16), (
            "archive does not open on the newest week — got %s"
            % rows[0].period_start)
    finally:
        session.close()


def test_only_the_weeks_that_feed_monthly_are_shown(seeded):
    session = database.get_session()
    try:
        rows = queries.weekly_briefings(session)
        assert {r.status for r in rows} == {"published", "approved"}
        assert len(rows) == 3
    finally:
        session.close()


def test_the_wider_filter_can_include_stranded_weeks(seeded):
    """Callers may opt into the ones the removed approval gate left behind."""
    session = database.get_session()
    try:
        rows = queries.weekly_briefings(
            session, ("approved", "published", "awaiting_approval", "expired"))
        assert len(rows) == 5
    finally:
        session.close()


def test_it_matches_what_monthly_would_gather(seeded):
    """The archive and the cascade must not disagree about a given month.

    If these two ever diverge, the tab shows a week as published that the
    monthly silently ignored — the exact confusion that made the July and
    August monthlies look like they had material when they did not.
    """
    from insights.monthly import _gather_approved_weeklies

    session = database.get_session()
    try:
        shown = {r.period_start for r in queries.weekly_briefings(session)
                 if r.period_start >= date(2026, 8, 1)
                 and r.period_end <= date(2026, 8, 31)}
        gathered = {w.period_start for w in _gather_approved_weeklies(
            session, date(2026, 8, 1), date(2026, 8, 31))}
        assert shown == gathered, (
            "archive shows %s but monthly gathers %s" % (shown, gathered))
    finally:
        session.close()


def test_empty_is_handled(tmp_db):
    session = database.get_session()
    try:
        assert queries.weekly_briefings(session) == []
    finally:
        session.close()


def test_the_tab_no_longer_globs_the_dead_filename():
    """Source guard. 7day_highlights_*.md has not been written since May.

    Kept as a source assertion because the behavioural tests above would all
    still pass if someone reintroduced the glob alongside the query.
    """
    src = (ROOT / "app.py").read_text(encoding="utf-8")
    tab = src[src.index("with tab_week:"):]
    tab = tab[:tab.index("_DEPLOYMENT_ACTIONS")]
    # Assert on the call, not the string: the tab carries a comment that
    # names the dead filename to explain why it is not used, and a bare
    # substring check flags that comment as the defect it warns about.
    assert "_find_all_highlights(" not in tab, (
        "the Weekly tab is reading notes/ again — nothing writes those files")
    assert ".glob(" not in tab, "the tab is globbing the filesystem"
    assert "weekly_briefings(" in tab, "the tab does not read the database"
