"""Status transition guard — invalid moves raise."""

from __future__ import annotations

from datetime import date

import pytest

from database import Publication
from insights.cycle_common import transition_status


def _pub(status: str) -> Publication:
    return Publication(
        cadence="weekly",
        period_start=date(2026, 4, 19),
        period_end=date(2026, 4, 25),
        status=status,
    )


def test_generating_to_awaiting_approval_ok():
    p = _pub("generating")
    transition_status(p, "awaiting_approval")
    assert p.status == "awaiting_approval"


def test_awaiting_to_approved_ok():
    p = _pub("awaiting_approval")
    transition_status(p, "approved")
    assert p.status == "approved"


def test_approved_to_published_ok():
    p = _pub("approved")
    transition_status(p, "published")
    assert p.status == "published"


def test_published_back_to_generating_blocked():
    p = _pub("published")
    with pytest.raises(ValueError, match="Invalid transition"):
        transition_status(p, "generating")


def test_rejected_is_terminal():
    p = _pub("rejected")
    with pytest.raises(ValueError):
        transition_status(p, "approved")
    with pytest.raises(ValueError):
        transition_status(p, "awaiting_approval")


def test_failed_can_restart_via_explicit_force():
    p = _pub("failed")
    transition_status(p, "generating")
    assert p.status == "generating"
