"""Slack failure alerts for the insights pipeline.

Best-effort posts to ``SLACK_WEBHOOK_URL``. If the webhook is missing
or the post fails, we log and move on — failure-handler exceptions
must not mask the original failure.
"""

from __future__ import annotations

import json
import logging
import traceback
from pathlib import Path

import requests

from insights import config

logger = logging.getLogger(__name__)

# Mock-mode notifications go here so tests can read them.
_MOCK_NOTIFICATIONS_FILE = config.TMP_DIR / "slack_notifications.jsonl"


def alert_failure(cycle: str, period: str, error: BaseException,
                  *, context: dict | None = None) -> None:
    """Post a failure alert for a scheduled cycle.

    Parameters
    ----------
    cycle:
        ``"weekly"`` / ``"monthly"`` / ``"annual"`` / ``"reminders"``
    period:
        Human-readable period label (e.g. ``"2026-04-19"``).
    error:
        The exception that triggered the alert. The traceback is
        attached to the Slack message body.
    context:
        Optional structured fields surfaced to Slack ("publication_id",
        "plans_remaining", etc.).
    """
    payload = _format_payload(cycle, period, error, context or {})

    if config.is_mock():
        _write_mock_notification(payload)
        return

    if not config.SLACK_WEBHOOK_URL:
        logger.warning("SLACK_WEBHOOK_URL not set — failure alert dropped")
        return

    try:
        resp = requests.post(
            config.SLACK_WEBHOOK_URL, json=payload, timeout=10
        )
        if resp.status_code >= 400:
            logger.warning(
                "Slack alert returned %s: %s", resp.status_code, resp.text[:200]
            )
    except Exception as send_err:  # noqa: BLE001
        # Never let the alerter mask the real failure.
        logger.exception("Slack alert failed: %s", send_err)


def _format_payload(cycle: str, period: str, error: BaseException,
                    context: dict) -> dict:
    tb = "".join(traceback.format_exception(type(error), error, error.__traceback__))
    context_lines = "\n".join(f"• *{k}:* `{v}`" for k, v in context.items())
    text = (
        f":rotating_light: *CIO Insights `{cycle}` cycle failed* "
        f"(period `{period}`)\n"
        f"*Error:* `{type(error).__name__}: {error}`\n"
        + (context_lines + "\n" if context_lines else "")
        + f"```{tb[-2500:]}```"  # Slack messages cap around 4kB
    )
    return {"text": text}


def _write_mock_notification(payload: dict) -> None:
    config.TMP_DIR.mkdir(parents=True, exist_ok=True)
    with _MOCK_NOTIFICATIONS_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload) + "\n")


def read_mock_notifications() -> list[dict]:
    """Test helper: read every mock notification posted this session."""
    if not _MOCK_NOTIFICATIONS_FILE.exists():
        return []
    return [
        json.loads(line)
        for line in _MOCK_NOTIFICATIONS_FILE.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def clear_mock_notifications() -> None:
    """Test helper: reset the mock notification log."""
    if _MOCK_NOTIFICATIONS_FILE.exists():
        _MOCK_NOTIFICATIONS_FILE.unlink()
