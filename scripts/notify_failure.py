"""Send a failure-alert email when a scheduled local task fails.

Invoked from the .bat wrappers in this directory whenever a step
returns non-zero. Reads the last few lines of the log so the email
is self-contained (no need to log into the box to triage).

Usage (from a .bat, after a failing command):
    python -m scripts.notify_failure <task_name> <step_name> <log_path> [exit_code]

Reuses the same Resend account that sends CIO Insights approval mails.
Silently no-ops if RESEND_API_KEY isn't set so dev environments don't
crash on cron runs.
"""

from __future__ import annotations

import os
import socket
import sys
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv


REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env")

LOG_TAIL_LINES = 60


def _tail(log_path: Path, n: int = LOG_TAIL_LINES) -> str:
    if not log_path.exists():
        return f"(log file {log_path} not found)"
    try:
        text = log_path.read_text(encoding="utf-8", errors="replace")
    except Exception as exc:  # noqa: BLE001
        return f"(could not read log: {exc})"
    lines = text.splitlines()
    return "\n".join(lines[-n:])


def main(argv: list[str]) -> int:
    if len(argv) < 4:
        print("usage: notify_failure.py <task_name> <step_name> <log_path> [exit_code]")
        return 2

    task_name = argv[1]
    step_name = argv[2]
    log_path = Path(argv[3])
    exit_code = argv[4] if len(argv) > 4 else "?"

    recipient = os.environ.get("APPROVAL_EMAIL_RECIPIENT", "")
    sender = os.environ.get("APPROVAL_EMAIL_FROM", "onboarding@resend.dev")
    api_key = os.environ.get("RESEND_API_KEY", "")

    if not (recipient and api_key):
        print("[notify_failure] Email not configured — skipping alert.")
        return 0

    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    host = socket.gethostname()
    log_tail = _tail(log_path)

    subject = f"[FAIL] {task_name}/{step_name} on {host} ({timestamp})"
    text_body = (
        f"Scheduled task '{task_name}' step '{step_name}' failed.\n"
        f"Host: {host}\n"
        f"Exit code: {exit_code}\n"
        f"Time (UTC): {timestamp}\n"
        f"Log: {log_path}\n\n"
        f"--- Last {LOG_TAIL_LINES} lines ---\n{log_tail}\n"
    )
    html_body = (
        f"<p>Scheduled task <code>{task_name}</code> step "
        f"<code>{step_name}</code> failed.</p>"
        f"<ul><li>Host: <code>{host}</code></li>"
        f"<li>Exit code: <code>{exit_code}</code></li>"
        f"<li>Time (UTC): {timestamp}</li>"
        f"<li>Log: <code>{log_path}</code></li></ul>"
        f"<p>Last {LOG_TAIL_LINES} lines:</p>"
        f"<pre style='background:#f5f5f5;padding:12px;font-size:12px;"
        f"white-space:pre-wrap;'>{log_tail}</pre>"
    )

    try:
        resp = requests.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "from": sender,
                "to": [recipient],
                "subject": subject,
                "text": text_body,
                "html": html_body,
            },
            timeout=15,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[notify_failure] Resend request raised: {exc}")
        return 1

    if resp.status_code >= 400:
        print(f"[notify_failure] Resend returned {resp.status_code}: {resp.text[:200]}")
        return 1

    delivery_id = resp.json().get("id", "?")
    print(f"[notify_failure] Sent ({delivery_id}) to {recipient}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
