"""Configuration and mock-mode plumbing for the insights package.

Reads environment variables (loaded from ``.env`` by the modules that
import this) and exposes them as typed constants. The mock-mode
sentinel (``INSIGHTS_MODE=mock``) is checked once at module import
time but can be re-checked dynamically via ``is_mock()`` for tests
that flip the env var inside a single process.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

_ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(_ENV_PATH, override=False)


# ---------------------------------------------------------------------------
# Approval flow
# ---------------------------------------------------------------------------

APPROVAL_BASE_URL = os.environ.get(
    "APPROVAL_BASE_URL", "https://pensionplanintelligence.onrender.com"
).rstrip("/")
APPROVAL_TOKEN_TTL_DAYS = int(os.environ.get("APPROVAL_TOKEN_TTL_DAYS", "7"))
APPROVAL_REMINDER_HOURS = int(os.environ.get("APPROVAL_REMINDER_HOURS", "72"))
APPROVAL_EMAIL_RECIPIENT = os.environ.get(
    "APPROVAL_EMAIL_RECIPIENT", "founder@pensionintel.com"
)
# Comma-separated recipient list parsed once at import. Every approval /
# notice / reminder email sent through ``insights.approval.send_email``
# fans out to all entries. The single-string ``APPROVAL_EMAIL_RECIPIENT``
# is preserved for any caller that still references it directly.
APPROVAL_EMAIL_RECIPIENTS: list[str] = [
    addr.strip() for addr in APPROVAL_EMAIL_RECIPIENT.split(",")
    if addr.strip()
]
APPROVAL_EMAIL_FROM = os.environ.get(
    "APPROVAL_EMAIL_FROM", "insights@pensionintel.com"
)

# ---------------------------------------------------------------------------
# Email + Slack
# ---------------------------------------------------------------------------

RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")

# Zapier/Make catch-hook URL that fans the LinkedIn post out to LinkedIn.
# Empty string disables the third "post to LinkedIn" button in the
# approval email; tokens are still issued so old emails keep working.
LINKEDIN_POST_WEBHOOK_URL = os.environ.get("LINKEDIN_POST_WEBHOOK_URL", "")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent
TMP_DIR = REPO_ROOT / "tmp"
SENT_EMAILS_DIR = TMP_DIR / "sent_emails"
SENT_SOCIAL_DIR = TMP_DIR / "sent_social"
PDF_OUTPUT_DIR = REPO_ROOT / "notes" / "pdfs"


def is_mock() -> bool:
    """Return True if ``INSIGHTS_MODE=mock``.

    Re-evaluated on each call so tests can flip the env var.
    """
    return os.environ.get("INSIGHTS_MODE", "live").lower() == "mock"


def expires_at_default(now: datetime | None = None) -> datetime:
    """Default token expiry — ``APPROVAL_TOKEN_TTL_DAYS`` from now."""
    return (now or datetime.utcnow()) + timedelta(days=APPROVAL_TOKEN_TTL_DAYS)


def reminder_threshold(now: datetime | None = None) -> datetime:
    """Cutoff for the 72h reminder — pubs older than this need nudging."""
    return (now or datetime.utcnow()) - timedelta(hours=APPROVAL_REMINDER_HOURS)


def expiry_threshold(now: datetime | None = None) -> datetime:
    """Cutoff for stale-draft expiry — pubs older than this auto-expire."""
    return (now or datetime.utcnow()) - timedelta(days=APPROVAL_TOKEN_TTL_DAYS)
