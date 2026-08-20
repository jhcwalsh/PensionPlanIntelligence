"""Approval token lifecycle and email content for the insights pipeline.

A publication has two tokens — one approve, one reject — both single-
use, both expiring after ``APPROVAL_TOKEN_TTL_DAYS``. The raw token
appears only in the email body and the URL clicked; only its SHA-256
hash is persisted.

The approval email is sent from this module via Resend, with the
draft Markdown inline and the rendered PDF as an attachment. In
``INSIGHTS_MODE=mock`` it writes a `.eml`-style file to
``tmp/sent_emails/`` instead of calling Resend.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import secrets
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests

from database import utcnow, ApprovalToken, Publication, get_session
from insights import config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Token primitives
# ---------------------------------------------------------------------------

def generate_raw_token() -> str:
    """32-byte URL-safe random token (~43 chars)."""
    if config.is_mock():
        # Deterministic per-process tokens make integration tests
        # readable without breaking single-use semantics across runs.
        global _MOCK_COUNTER
        _MOCK_COUNTER += 1
        return f"mock-token-{_MOCK_COUNTER:06d}"
    return secrets.token_urlsafe(32)


_MOCK_COUNTER = 0


def hash_token(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


@dataclass
class ApprovalEmail:
    subject: str
    html: str
    text: str
    pdf_attachment: Optional[bytes]
    pdf_filename: Optional[str]


def send_email(email: ApprovalEmail,
               to: Optional[str | list[str]] = None) -> str:
    """Deliver ``email``. Returns a delivery id (Resend id, or filename in mock).

    Live mode posts to https://api.resend.com/emails. Mock mode writes
    the rendered email to ``tmp/sent_emails/<timestamp>.eml`` so tests
    can assert on what would have been sent.

    ``to`` accepts a single address, a list, a comma-separated string,
    or ``None`` (falls back to ``config.APPROVAL_EMAIL_RECIPIENTS``).
    All recipients receive the same magic-link tokens — first click on
    approve / reject wins; the rest see the "already actioned" page.
    """
    if to is None:
        recipients = list(config.APPROVAL_EMAIL_RECIPIENTS)
    elif isinstance(to, str):
        recipients = [a.strip() for a in to.split(",") if a.strip()]
    else:
        recipients = [a.strip() for a in to if a and a.strip()]

    if not recipients:
        raise RuntimeError(
            "No approval-email recipients configured. Set APPROVAL_EMAIL_RECIPIENT "
            "(comma-separate to add more than one)."
        )

    if config.is_mock():
        return _write_mock_email(email, recipients)

    if not config.RESEND_API_KEY:
        raise RuntimeError(
            "RESEND_API_KEY not set — can't send approval email in live mode. "
            "Set INSIGHTS_MODE=mock for local dev."
        )

    payload = {
        "from": config.APPROVAL_EMAIL_FROM,
        "to": recipients,
        "subject": email.subject,
        "html": email.html,
        "text": email.text,
    }
    if email.pdf_attachment:
        payload["attachments"] = [{
            "filename": email.pdf_filename,
            "content": base64.b64encode(email.pdf_attachment).decode("ascii"),
        }]

    resp = requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {config.RESEND_API_KEY}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=30,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"Resend returned {resp.status_code}: {resp.text[:300]}")
    return resp.json().get("id", "")


def _write_mock_email(email: ApprovalEmail, recipients: list[str]) -> str:
    config.SENT_EMAILS_DIR.mkdir(parents=True, exist_ok=True)
    ts = utcnow().strftime("%Y%m%dT%H%M%S%f")
    base = config.SENT_EMAILS_DIR / f"{ts}"

    metadata = {
        "to": recipients,
        "from": config.APPROVAL_EMAIL_FROM,
        "subject": email.subject,
        "has_attachment": bool(email.pdf_attachment),
        "pdf_filename": email.pdf_filename if email.pdf_attachment else None,
    }
    meta_path = base.with_suffix(".json")
    meta_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    eml_path = base.with_suffix(".eml")
    eml_path.write_text(
        f"To: {', '.join(recipients)}\n"
        f"From: {config.APPROVAL_EMAIL_FROM}\n"
        f"Subject: {email.subject}\n"
        f"Content-Type: text/html\n\n"
        f"{email.html}",
        encoding="utf-8",
    )
    if email.pdf_attachment:
        base.with_suffix(".pdf").write_bytes(email.pdf_attachment)

    return str(eml_path)


def list_mock_emails() -> list[Path]:
    """Test helper: return the sorted list of mock email metadata files."""
    if not config.SENT_EMAILS_DIR.exists():
        return []
    return sorted(config.SENT_EMAILS_DIR.glob("*.json"))


def clear_mock_emails() -> None:
    """Test helper: reset the mock-email outbox."""
    if config.SENT_EMAILS_DIR.exists():
        for p in config.SENT_EMAILS_DIR.iterdir():
            p.unlink()
