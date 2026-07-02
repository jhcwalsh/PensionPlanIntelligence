"""Quarterly cycle pulls from approved monthlies in the quarter window."""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from database import Publication, get_session
from insights import quarterly
from insights.compose import quarterly_period_for
from insights.publish import _filename_for


def _seed_monthlies(months: list[date], status: str = "published") -> list[int]:
    """Create one monthly per given first-of-month date."""
    s = get_session()
    ids = []
    try:
        for ps in months:
            next_month = (ps.replace(day=28) + timedelta(days=4)).replace(day=1)
            pe = next_month - timedelta(days=1)
            pub = Publication(
                cadence="monthly",
                period_start=ps,
                period_end=pe,
                status=status,
                draft_markdown=f"# Monthly {ps.strftime('%B %Y')}\n\nContent.\n",
                composed_at=datetime.utcnow(),
            )
            s.add(pub)
            s.commit()
            s.refresh(pub)
            ids.append(pub.id)
        return ids
    finally:
        s.close()


def test_quarterly_period_for_prior_quarter():
    # Cron fires on the 1st of Jan/Apr/Jul/Oct — always the prior quarter.
    assert quarterly_period_for(date(2026, 7, 1)) == (date(2026, 4, 1), date(2026, 6, 30))
    assert quarterly_period_for(date(2026, 10, 1)) == (date(2026, 7, 1), date(2026, 9, 30))
    assert quarterly_period_for(date(2027, 1, 1)) == (date(2026, 10, 1), date(2026, 12, 31))
    # Mid-quarter reference dates behave the same way.
    assert quarterly_period_for(date(2026, 8, 15)) == (date(2026, 4, 1), date(2026, 6, 30))


def test_quarterly_composes_from_monthlies():
    monthly_ids = _seed_monthlies(
        [date(2026, 4, 1), date(2026, 5, 1), date(2026, 6, 1)]
    )

    pub = quarterly.run_quarterly_cycle(period_start=date(2026, 4, 1))

    assert pub.cadence == "quarterly"
    assert pub.period_start == date(2026, 4, 1)
    assert pub.period_end == date(2026, 6, 30)
    assert pub.status == "awaiting_approval"
    assert set(pub.source_publication_ids) == set(monthly_ids)
    assert pub.draft_markdown


def test_quarterly_with_no_approved_monthlies_fails():
    with pytest.raises(RuntimeError, match="No approved monthlies"):
        quarterly.run_quarterly_cycle(period_start=date(2026, 4, 1))


def test_quarterly_excludes_out_of_window_monthlies():
    # A March monthly must not leak into the Q2 window.
    _seed_monthlies([date(2026, 3, 1)])
    in_window = _seed_monthlies([date(2026, 4, 1), date(2026, 5, 1)])

    pub = quarterly.run_quarterly_cycle(period_start=date(2026, 4, 1))
    assert set(pub.source_publication_ids) == set(in_window)


def test_quarterly_publish_filename():
    pub = Publication(
        cadence="quarterly",
        period_start=date(2026, 4, 1),
        period_end=date(2026, 6, 30),
        status="approved",
    )
    assert _filename_for(pub) == "quarterly_cio_insights_2026-04-01.md"
