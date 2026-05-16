"""insights.config additions for the daily cadence."""
from __future__ import annotations

import importlib

from insights import config


def test_daily_cadence_display():
    prefix, product, slug = config.cadence_display("daily")
    assert prefix == "Daily"
    assert product == "Pension Digest"
    assert slug == "daily_digest"


def test_daily_thresholds_have_sane_defaults():
    # Defaults are read at import time from environment; the test fixture
    # doesn't set them, so we expect the module-level defaults.
    assert config.DAILY_APPROVAL_DOC_THRESHOLD == 10
    assert "RFP" in config.DAILY_APPROVAL_KEYWORDS
    assert "manager" in config.DAILY_APPROVAL_KEYWORDS
    assert config.DAILY_REAPPEAR_DAYS == 30


def test_daily_keywords_split_and_stripped(monkeypatch):
    monkeypatch.setenv("DAILY_APPROVAL_KEYWORDS", " foo , bar ,, baz ")
    # Re-import to pick up the new env value.
    import insights.config as ic
    importlib.reload(ic)
    assert ic.DAILY_APPROVAL_KEYWORDS == ["foo", "bar", "baz"]
    # Restore module state so later tests don't see the reload.
    importlib.reload(ic)
