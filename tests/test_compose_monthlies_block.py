"""The briefings block must label each monthly by its own month.

The old ``compose_annual`` labelled month *i* as ``January + i``, which
captioned a partial year starting in April as "January", "February", ...
"""

from __future__ import annotations

from datetime import date

from insights.compose import _monthlies_block


def test_partial_year_months_labelled_correctly():
    block = _monthlies_block([
        (date(2026, 4, 1), "# April content"),
        (date(2026, 5, 1), "# May content"),
        (date(2026, 12, 1), "# December content"),
    ])
    assert "=== April 2026 ===" in block
    assert "=== May 2026 ===" in block
    assert "=== December 2026 ===" in block
    assert "January" not in block
