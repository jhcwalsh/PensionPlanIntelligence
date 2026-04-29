"""When a cycle fails, the Slack webhook (mocked) is called."""

from __future__ import annotations

from datetime import date

import pytest

from insights import notify, weekly


def test_compose_failure_writes_slack_notification(monkeypatch):
    """Force compose to raise and assert the alerter logged it."""
    from insights import compose
    notify.clear_mock_notifications()

    def boom(*a, **kw):
        raise RuntimeError("simulated compose failure")

    monkeypatch.setattr(compose, "compose_weekly", boom)

    # We need at least one Plan so the cycle reaches the compose step.
    from database import Plan, get_session
    s = get_session()
    try:
        s.add(Plan(id="x", name="X"))
        s.commit()
    finally:
        s.close()

    with pytest.raises(RuntimeError, match="simulated compose failure"):
        weekly.run_weekly_cycle(period_start=date(2026, 4, 19))

    notifications = notify.read_mock_notifications()
    assert len(notifications) == 1
    body = notifications[0]["text"]
    assert "weekly" in body
    assert "simulated compose failure" in body


def test_alert_failure_is_safe_when_webhook_unset(monkeypatch):
    """Live mode without a webhook url should warn but never raise."""
    monkeypatch.setenv("INSIGHTS_MODE", "live")
    monkeypatch.setattr(notify.config, "SLACK_WEBHOOK_URL", "")

    # Should not raise.
    notify.alert_failure("weekly", "2026-04-19", RuntimeError("x"))
