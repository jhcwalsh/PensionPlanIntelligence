"""Monthly cycle pulls from approved weeklies and records source ids."""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from database import Publication, get_session
from insights import monthly


def _seed_approved_weeklies(count: int, base: date) -> list[int]:
    """Create ``count`` approved weeklies starting at ``base``."""
    s = get_session()
    ids = []
    try:
        for i in range(count):
            ps = base + timedelta(weeks=i)
            pe = ps + timedelta(days=6)
            pub = Publication(
                cadence="weekly",
                period_start=ps,
                period_end=pe,
                status="approved",
                draft_markdown=f"# Weekly {i+1}\n\nSome content for week {i+1}.\n",
                composed_at=datetime.utcnow(),
                approved_at=datetime.utcnow(),
            )
            s.add(pub)
            s.commit()
            s.refresh(pub)
            ids.append(pub.id)
        return ids
    finally:
        s.close()


def test_monthly_composes_from_weeklies():
    # 4 approved weeklies covering March 2026 (period_start: Mar 1, 8, 15, 22).
    weekly_ids = _seed_approved_weeklies(4, date(2026, 3, 1))

    pub = monthly.run_monthly_cycle(period_start=date(2026, 3, 1))

    assert pub.cadence == "monthly"
    assert pub.period_start == date(2026, 3, 1)
    assert pub.period_end == date(2026, 3, 31)
    assert pub.status == "awaiting_approval"
    assert set(pub.source_publication_ids) == set(weekly_ids)
    assert pub.draft_markdown


def test_monthly_with_no_approved_weeklies_fails():
    """An empty corpus should raise rather than producing an empty briefing."""
    with pytest.raises(RuntimeError, match="No approved weeklies"):
        monthly.run_monthly_cycle(period_start=date(2026, 3, 1))


def test_monthly_excludes_rejected_weeklies():
    s = get_session()
    try:
        # Two approved, one rejected — only the two should feed the monthly.
        for i, status in enumerate(["approved", "rejected", "approved"]):
            ps = date(2026, 3, 1) + timedelta(weeks=i)
            s.add(Publication(
                cadence="weekly",
                period_start=ps,
                period_end=ps + timedelta(days=6),
                status=status,
                draft_markdown=f"# W{i}",
                composed_at=datetime.utcnow(),
            ))
        s.commit()
    finally:
        s.close()

    pub = monthly.run_monthly_cycle(period_start=date(2026, 3, 1))
    assert len(pub.source_publication_ids) == 2
