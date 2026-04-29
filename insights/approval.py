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

from database import ApprovalToken, Publication, get_session
from insights import config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Token primitives
# ---------------------------------------------------------------------------

@dataclass
class IssuedToken:
    """The plaintext token to embed in URLs (returned once, never stored)."""
    raw: str
    action: str

    @property
    def hash(self) -> str:
        return hash_token(self.raw)


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


def issue_tokens(session, publication: Publication) -> tuple[IssuedToken, IssuedToken]:
    """Create approve + reject token rows for ``publication``.

    Returns the plaintext tokens — caller is responsible for embedding
    them in the outgoing email and never persisting them again.
    """
    expires = config.expires_at_default()

    approve_raw = generate_raw_token()
    reject_raw = generate_raw_token()

    session.add_all([
        ApprovalToken(
            publication_id=publication.id,
            token_hash=hash_token(approve_raw),
            action="approve",
            expires_at=expires,
        ),
        ApprovalToken(
            publication_id=publication.id,
            token_hash=hash_token(reject_raw),
            action="reject",
            expires_at=expires,
        ),
    ])
    session.flush()
    return (
        IssuedToken(raw=approve_raw, action="approve"),
        IssuedToken(raw=reject_raw, action="reject"),
    )


# ---------------------------------------------------------------------------
# Token consumption (called from the Streamlit query-param handler)
# ---------------------------------------------------------------------------

class TokenError(Exception):
    """Raised for any non-success path in ``consume_token``."""


def consume_token(raw_token: str, expected_action: str) -> Publication:
    """Mark a token consumed and apply its action to the publication.

    Atomic: the token row, the publication row, and the action timestamp
    are written in a single transaction. Re-clicking the same link is a
    no-op error rather than a double-action.

    Raises ``TokenError`` for not-found / expired / consumed / wrong-action.
    Returns the affected ``Publication`` on success.
    """
    session = get_session()
    try:
        token = (
            session.query(ApprovalToken)
            .filter_by(token_hash=hash_token(raw_token))
            .one_or_none()
        )
        if token is None:
            raise TokenError("Token not found")
        if token.action != expected_action:
            raise TokenError(f"Token is for '{token.action}', not '{expected_action}'")
        if token.consumed_at is not None:
            raise TokenError("Token already used")
        if token.expires_at < datetime.utcnow():
            raise TokenError("Token expired")

        pub = session.get(Publication, token.publication_id)
        if pub is None:
            raise TokenError("Publication missing")
        if pub.status not in ("awaiting_approval",):
            raise TokenError(f"Publication is in status '{pub.status}', cannot {expected_action}")

        now = datetime.utcnow()
        token.consumed_at = now
        if expected_action == "approve":
            pub.status = "approved"
            pub.approved_at = now
        else:
            pub.status = "rejected"
            pub.rejected_at = now

        session.commit()
        session.refresh(pub)
        session.expunge(pub)
        return pub
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Email content
# ---------------------------------------------------------------------------

@dataclass
class ApprovalEmail:
    subject: str
    html: str
    text: str
    pdf_attachment: Optional[bytes]
    pdf_filename: Optional[str]


def _approval_url(token: IssuedToken) -> str:
    return f"{config.APPROVAL_BASE_URL}/?{token.action}={token.raw}"


def render_approval_email(publication: Publication,
                          approve: IssuedToken, reject: IssuedToken,
                          pdf_bytes: Optional[bytes],
                          *, is_reminder: bool = False,
                          is_expiry: bool = False) -> ApprovalEmail:
    """Render the email content (subject + html + text + attachment).

    ``is_reminder`` switches the subject line to a more urgent variant
    (sent at 72h unapproved). ``is_expiry`` switches it to the "draft
    has expired" notice (sent at 7 days).
    """
    cadence = publication.cadence.title()
    period = f"{publication.period_start.isoformat()} – {publication.period_end.isoformat()}"

    if is_expiry:
        subject = f"[Expired] {cadence} CIO Insights ({period})"
    elif is_reminder:
        subject = f"[Reminder — please review] {cadence} CIO Insights ({period})"
    else:
        subject = f"[Action required] {cadence} CIO Insights ready to publish ({period})"

    approve_url = _approval_url(approve)
    reject_url = _approval_url(reject)

    headline = (publication.draft_markdown or "")[:1500]
    if len(publication.draft_markdown or "") > 1500:
        headline += "\n\n[... full draft attached as PDF ...]"

    if is_expiry:
        body_intro = (
            "<p>This draft has expired without action and will not be "
            "published. Re-run the cycle to generate a fresh draft.</p>"
        )
        text_intro = (
            "This draft has expired without action and will not be published.\n"
            "Re-run the cycle to generate a fresh draft.\n\n"
        )
        action_buttons_html = ""
        action_buttons_text = ""
    else:
        body_intro = (
            "<p>The latest CIO Insights draft is ready for your review. "
            "Click <strong>Approve and publish</strong> to push it live, "
            "or <strong>Reject</strong> to discard it.</p>"
        )
        text_intro = (
            "The latest CIO Insights draft is ready for your review.\n"
            "Click Approve to push it live, or Reject to discard.\n\n"
        )
        action_buttons_html = f"""\
<p style="margin: 20px 0;">
  <a href="{approve_url}"
     style="display:inline-block;padding:12px 24px;background:#0066cc;color:#fff;
            text-decoration:none;border-radius:4px;margin-right:10px;">
     Approve and publish
  </a>
  <a href="{reject_url}"
     style="display:inline-block;padding:12px 24px;background:#cc0033;color:#fff;
            text-decoration:none;border-radius:4px;">
     Reject
  </a>
</p>
<p style="font-size:0.85em;color:#666;">
  Plain-text fallback links:<br>
  Approve: <a href="{approve_url}">{approve_url}</a><br>
  Reject: <a href="{reject_url}">{reject_url}</a>
</p>"""
        action_buttons_text = (
            f"Approve and publish: {approve_url}\n"
            f"Reject: {reject_url}\n\n"
        )

    html = f"""\
<!DOCTYPE html>
<html><body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;
                  max-width:720px;margin:auto;padding:24px;color:#222;">
<h2 style="color:#003366;">{subject}</h2>
{body_intro}
{action_buttons_html}
<hr style="border:none;border-top:1px solid #ccc;margin:24px 0;">
<h3 style="color:#003366;">Draft preview</h3>
<pre style="white-space:pre-wrap;font-family:Georgia,serif;font-size:14px;line-height:1.5;
            background:#f8f9fa;padding:16px;border-left:4px solid #0066cc;border-radius:4px;">
{headline}
</pre>
<p style="font-size:0.85em;color:#666;">Full draft attached as PDF.</p>
</body></html>"""

    text = (
        f"{subject}\n"
        f"{'=' * len(subject)}\n\n"
        f"{text_intro}"
        f"{action_buttons_text}"
        f"--- Draft preview ---\n\n"
        f"{headline}\n\n"
        f"Full draft attached as PDF."
    )

    pdf_filename = (
        f"{publication.cadence}_cio_insights_{publication.period_start.isoformat()}.pdf"
    )

    return ApprovalEmail(
        subject=subject,
        html=html,
        text=text,
        pdf_attachment=pdf_bytes,
        pdf_filename=pdf_filename,
    )


# ---------------------------------------------------------------------------
# Email delivery (Resend in live mode, file in mock mode)
# ---------------------------------------------------------------------------

def send_email(email: ApprovalEmail, to: Optional[str] = None) -> str:
    """Deliver ``email``. Returns a delivery id (Resend id, or filename in mock).

    Live mode posts to https://api.resend.com/emails. Mock mode writes
    the rendered email to ``tmp/sent_emails/<timestamp>.eml`` so tests
    can assert on what would have been sent.
    """
    recipient = to or config.APPROVAL_EMAIL_RECIPIENT

    if config.is_mock():
        return _write_mock_email(email, recipient)

    if not config.RESEND_API_KEY:
        raise RuntimeError(
            "RESEND_API_KEY not set — can't send approval email in live mode. "
            "Set INSIGHTS_MODE=mock for local dev."
        )

    payload = {
        "from": config.APPROVAL_EMAIL_FROM,
        "to": [recipient],
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


def _write_mock_email(email: ApprovalEmail, recipient: str) -> str:
    config.SENT_EMAILS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S%f")
    base = config.SENT_EMAILS_DIR / f"{ts}"

    metadata = {
        "to": recipient,
        "from": config.APPROVAL_EMAIL_FROM,
        "subject": email.subject,
        "has_attachment": bool(email.pdf_attachment),
        "pdf_filename": email.pdf_filename if email.pdf_attachment else None,
    }
    meta_path = base.with_suffix(".json")
    meta_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    eml_path = base.with_suffix(".eml")
    eml_path.write_text(
        f"To: {recipient}\n"
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
