"""Annual cycle works from a partial year of approved monthlies.

2026 monthlies start in April (the system's first month of production),
so the Jan 2027 annual run must synthesize 9 monthlies — not assume a
dense January-anchored list of 12.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from database import Publication, get_session
from insights import annual


def _seed_monthlies(months: list[date], status: str = "published") -> list[int]:
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


def test_annual_composes_from_partial_year():
    # A 2026-style year: monthlies exist only from April onward.
    monthly_ids = _seed_monthlies(
        [date(2026, m, 1) for m in (4, 5, 6, 7, 8, 9, 10, 11, 12)]
    )

    pub = annual.run_annual_cycle(year=2026)

    assert pub.cadence == "annual"
    assert pub.period_start == date(2026, 1, 1)
    assert pub.period_end == date(2026, 12, 31)
    assert pub.status == "awaiting_approval"
    assert set(pub.source_publication_ids) == set(monthly_ids)
    assert pub.draft_markdown


def test_annual_with_no_approved_monthlies_fails():
    with pytest.raises(RuntimeError, match="No approved monthlies"):
        annual.run_annual_cycle(year=2026)


def test_annual_excludes_other_years():
    _seed_monthlies([date(2025, 12, 1)])
    in_year = _seed_monthlies([date(2026, 4, 1), date(2026, 5, 1)])

    pub = annual.run_annual_cycle(year=2026)
    assert set(pub.source_publication_ids) == set(in_year)
