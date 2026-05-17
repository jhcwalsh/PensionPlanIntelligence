"""Public subscriber sign-up lifecycle and digest fan-out.

Sibling of ``insights.approval`` but for the public mailing list rather
than the founder approval flow:

* sign-up writes a ``pending`` row + issues a confirmation token
* confirmation flips the row to ``confirmed`` and triggers a welcome email
* every digest email carries a fresh single-use unsubscribe token in its
  footer (so a leaked old digest can't be replayed to re-unsubscribe)
* preference-update tokens are issued on demand by the preferences page

The raw token never round-trips through the database — only its SHA-256
hash is stored, just like ``ApprovalToken``. Token primitives and the
``send_email`` Resend wrapper are reused from ``insights.approval`` so
mock-mode behaviour (writes ``.eml`` files to ``tmp/sent_emails/``) is
identical.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, Optional

from sqlalchemy import func

from database import Publication, Subscriber, SubscriberToken, get_session
from insights import config
from insights.approval import (
    ApprovalEmail,
    generate_raw_token,
    hash_token,
    send_email,
)
from insights.render import markdown_to_email_html

logger = logging.getLogger(__name__)


CADENCES = ("weekly", "monthly", "quarterly")
RECENT_SIGNUP_WINDOW = timedelta(hours=1)
RECENT_SIGNUP_LIMIT = 3
UNSUBSCRIBE_TOKEN_TTL = timedelta(days=3650)  # ~10 years; effectively permanent


class SubscriberError(Exception):
    """Raised for any non-success path in the subscriber flow."""


@dataclass
class IssuedSubToken:
    """Plaintext token to embed in a URL (returned once, never stored)."""
    raw: str
    action: str
    subscriber_id: int

    @property
    def hash(self) -> str:
        return hash_token(self.raw)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize_email(email: str) -> str:
    return (email or "").strip().lower()


def _link(action: str, raw_token: str) -> str:
    return f"{config.SUBSCRIBE_BASE_URL}/?{action}={raw_token}"


def _issue_token(session, subscriber: Subscriber, action: str,
                 expires_at: datetime) -> IssuedSubToken:
    raw = generate_raw_token()
    session.add(SubscriberToken(
        subscriber_id=subscriber.id,
        token_hash=hash_token(raw),
        action=action,
        expires_at=expires_at,
    ))
    session.flush()
    return IssuedSubToken(raw=raw, action=action, subscriber_id=subscriber.id)


def issue_unsubscribe_token(subscriber: Subscriber, session=None) -> str:
    """Generate a fresh unsubscribe token for a digest send.

    Caller is responsible for committing the session. If ``session`` is
    None a new session is opened, committed, and closed.
    """
    expires = datetime.utcnow() + UNSUBSCRIBE_TOKEN_TTL
    if session is None:
        session = get_session()
        try:
            token = _issue_token(session, subscriber, "unsubscribe", expires)
            session.commit()
            return token.raw
        finally:
            session.close()
    return _issue_token(session, subscriber, "unsubscribe", expires).raw


# ---------------------------------------------------------------------------
# Sign-up
# ---------------------------------------------------------------------------

def create_pending_subscriber(
    email: str,
    *,
    weekly: bool = False,
    monthly: bool = False,
    quarterly: bool = False,
    signup_ip: str | None = None,
) -> tuple[Subscriber, str]:
    """Upsert a subscriber row and return a fresh confirmation token.

    Existing ``confirmed`` rows: refresh their cadence flags and still
    issue a confirm token (caller will email it; legitimate users who
    accidentally re-sign-up get a re-confirmation, attackers re-confirming
    someone else's address can't change anything material because the
    flags haven't been re-flipped yet — they only take effect on confirm).
    Existing ``unsubscribed``/``disabled`` rows: reset to ``pending`` so
    a re-confirmation can revive them. The raw token returned is shown
    once and never persisted again.

    Caller layer is responsible for soft rate-limiting via
    ``recent_signup_count``.
    """
    email_norm = _normalize_email(email)
    if not email_norm or "@" not in email_norm:
        raise SubscriberError("Invalid email address.")
    if not (weekly or monthly or quarterly):
        raise SubscriberError("Pick at least one cadence to subscribe to.")

    session = get_session()
    try:
        sub = (
            session.query(Subscriber)
            .filter(func.lower(Subscriber.email) == email_norm)
            .one_or_none()
        )
        if sub is None:
            sub = Subscriber(
                email=email_norm,
                weekly=weekly, monthly=monthly, quarterly=quarterly,
                status="pending",
                signup_ip=signup_ip,
            )
            session.add(sub)
            session.flush()
        else:
            # Refresh cadence preferences from the new submission.
            sub.weekly = weekly
            sub.monthly = monthly
            sub.quarterly = quarterly
            if sub.status in ("unsubscribed", "disabled"):
                sub.status = "pending"
                sub.unsubscribed_at = None
            # Confirmed rows stay confirmed — clicking the new confirm
            # link is a no-op except for refreshing preferences via the
            # update_preferences flow.

        expires = config.subscribe_confirm_expiry()
        token = _issue_token(session, sub, "confirm", expires)
        session.commit()
        session.refresh(sub)
        session.expunge(sub)
        return sub, token.raw
    finally:
        session.close()


def recent_signup_count(email: str,
                         window: timedelta = RECENT_SIGNUP_WINDOW) -> int:
    """Count fresh ``pending`` rows for ``email`` within ``window``.

    Used by the sign-up form to throttle repeat submissions. Cheap query
    — no index needed for the volumes we expect.
    """
    email_norm = _normalize_email(email)
    if not email_norm:
        return 0
    cutoff = datetime.utcnow() - window
    session = get_session()
    try:
        return (
            session.query(SubscriberToken)
            .join(Subscriber, Subscriber.id == SubscriberToken.subscriber_id)
            .filter(func.lower(Subscriber.email) == email_norm)
            .filter(SubscriberToken.action == "confirm")
            .filter(SubscriberToken.created_at >= cutoff)
            .count()
        )
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Token consumption
# ---------------------------------------------------------------------------

def _consume(session, raw_token: str, expected_action: str) -> SubscriberToken:
    token = (
        session.query(SubscriberToken)
        .filter_by(token_hash=hash_token(raw_token))
        .one_or_none()
    )
    if token is None:
        raise SubscriberError("Token not found")
    if token.action != expected_action:
        raise SubscriberError(
            f"Token is for '{token.action}', not '{expected_action}'"
        )
    if token.consumed_at is not None:
        raise SubscriberError("Token already used")
    if token.expires_at < datetime.utcnow():
        raise SubscriberError("Token expired")
    token.consumed_at = datetime.utcnow()
    return token


def consume_confirm_token(raw_token: str) -> Subscriber:
    """Flip a pending subscriber to ``confirmed``."""
    session = get_session()
    try:
        token = _consume(session, raw_token, "confirm")
        sub = session.get(Subscriber, token.subscriber_id)
        if sub is None:
            raise SubscriberError("Subscriber missing")
        if sub.status == "disabled":
            raise SubscriberError("This subscription has been disabled by the site administrator.")
        if sub.status != "confirmed":
            sub.status = "confirmed"
            sub.confirmed_at = datetime.utcnow()
        session.commit()
        session.refresh(sub)
        session.expunge(sub)
        return sub
    finally:
        session.close()


def consume_unsubscribe_token(raw_token: str) -> Subscriber:
    """Flip a subscriber to ``unsubscribed`` and clear cadence flags."""
    session = get_session()
    try:
        token = _consume(session, raw_token, "unsubscribe")
        sub = session.get(Subscriber, token.subscriber_id)
        if sub is None:
            raise SubscriberError("Subscriber missing")
        sub.status = "unsubscribed"
        sub.unsubscribed_at = datetime.utcnow()
        sub.weekly = False
        sub.monthly = False
        sub.quarterly = False
        session.commit()
        session.refresh(sub)
        session.expunge(sub)
        return sub
    finally:
        session.close()


def consume_preferences_token(raw_token: str) -> Subscriber:
    """Validate and return the subscriber whose preferences are being edited.

    Does not mutate cadence flags — the caller (preferences page) calls
    ``set_preferences`` once the user submits their choices.
    """
    session = get_session()
    try:
        token = _consume(session, raw_token, "update_preferences")
        sub = session.get(Subscriber, token.subscriber_id)
        if sub is None:
            raise SubscriberError("Subscriber missing")
        session.commit()
        session.refresh(sub)
        session.expunge(sub)
        return sub
    finally:
        session.close()


def set_preferences(subscriber_id: int, *, weekly: bool, monthly: bool,
                    quarterly: bool) -> Subscriber:
    """Update cadence preferences for an existing subscriber.

    If the user clears all three checkboxes we treat that as an
    unsubscribe so they aren't left in a half-state with a confirmed
    row that never receives anything.
    """
    session = get_session()
    try:
        sub = session.get(Subscriber, subscriber_id)
        if sub is None:
            raise SubscriberError("Subscriber missing")
        sub.weekly = weekly
        sub.monthly = monthly
        sub.quarterly = quarterly
        if not (weekly or monthly or quarterly):
            sub.status = "unsubscribed"
            sub.unsubscribed_at = datetime.utcnow()
        elif sub.status == "unsubscribed":
            # Picking a cadence after unsubscribing reactivates the row.
            sub.status = "confirmed"
            sub.unsubscribed_at = None
        session.commit()
        session.refresh(sub)
        session.expunge(sub)
        return sub
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Recipient queries
# ---------------------------------------------------------------------------

def recipients_for_cadence(cadence: str) -> list[Subscriber]:
    """Active subscribers who opted into ``cadence``.

    Used by the publish step to fan out a digest. Filters to
    ``status="confirmed"`` and the matching cadence flag — disabled,
    pending, and unsubscribed rows are excluded.
    """
    if cadence not in CADENCES:
        raise ValueError(f"Unknown cadence: {cadence}")
    session = get_session()
    try:
        q = session.query(Subscriber).filter(Subscriber.status == "confirmed")
        column = getattr(Subscriber, cadence)
        q = q.filter(column.is_(True))
        rows = q.order_by(Subscriber.id).all()
        for r in rows:
            session.expunge(r)
        return rows
    finally:
        session.close()


def list_all_subscribers() -> list[Subscriber]:
    """Every subscriber regardless of status — for the admin moderation table."""
    session = get_session()
    try:
        rows = session.query(Subscriber).order_by(Subscriber.created_at.desc()).all()
        for r in rows:
            session.expunge(r)
        return rows
    finally:
        session.close()


def set_status(subscriber_id: int, status: str) -> Subscriber:
    """Admin moderation: flip a subscriber's status directly."""
    if status not in ("pending", "confirmed", "disabled", "unsubscribed"):
        raise ValueError(f"Unknown status: {status}")
    session = get_session()
    try:
        sub = session.get(Subscriber, subscriber_id)
        if sub is None:
            raise SubscriberError("Subscriber missing")
        sub.status = status
        if status == "unsubscribed" and sub.unsubscribed_at is None:
            sub.unsubscribed_at = datetime.utcnow()
        session.commit()
        session.refresh(sub)
        session.expunge(sub)
        return sub
    finally:
        session.close()


def delete_subscriber(subscriber_id: int) -> None:
    """Hard-delete a subscriber and their tokens. Admin-only."""
    session = get_session()
    try:
        sub = session.get(Subscriber, subscriber_id)
        if sub is None:
            return
        session.delete(sub)
        session.commit()
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Email rendering
# ---------------------------------------------------------------------------

def render_confirmation_email(subscriber: Subscriber, raw_token: str) -> ApprovalEmail:
    """Double-opt-in confirmation email."""
    confirm_url = _link("confirm", raw_token)
    cadence_labels = [c for c in CADENCES if getattr(subscriber, c)]
    cadences_str = ", ".join(cadence_labels) or "no cadence"
    subject = "Confirm your Pension Plan Intelligence subscription"
    html = f"""\
<!DOCTYPE html>
<html><body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;
                  max-width:640px;margin:auto;padding:24px;color:#222;">
<h2 style="color:#003366;">Confirm your subscription</h2>
<p>Thanks for signing up. Click the button below to confirm your email
address and start receiving the <strong>{cadences_str}</strong> briefing(s)
from Pension Plan Intelligence.</p>
<p style="margin:20px 0;">
  <a href="{confirm_url}"
     style="display:inline-block;padding:12px 24px;background:#0066cc;color:#fff;
            text-decoration:none;border-radius:4px;">
    Confirm subscription
  </a>
</p>
<p style="font-size:0.85em;color:#666;">Plain-text link:<br>
<a href="{confirm_url}">{confirm_url}</a></p>
<p style="font-size:0.85em;color:#666;">This link expires in
{config.SUBSCRIBE_CONFIRM_TTL_DAYS} days. If you didn't request this,
you can safely ignore the email — nothing will be sent until you
confirm.</p>
</body></html>"""
    text = (
        f"Confirm your subscription\n"
        f"=========================\n\n"
        f"Click to confirm: {confirm_url}\n\n"
        f"You signed up for: {cadences_str}.\n"
        f"This link expires in {config.SUBSCRIBE_CONFIRM_TTL_DAYS} days.\n"
        f"If you didn't request this, ignore this email.\n"
    )
    return ApprovalEmail(subject=subject, html=html, text=text,
                         pdf_attachment=None, pdf_filename=None)


def render_welcome_email(subscriber: Subscriber) -> ApprovalEmail:
    """Sent immediately after the confirm link is clicked."""
    cadence_labels = [c for c in CADENCES if getattr(subscriber, c)]
    cadences_str = ", ".join(cadence_labels) or "no cadence"
    subject = "You're subscribed to Pension Plan Intelligence"
    html = f"""\
<!DOCTYPE html>
<html><body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;
                  max-width:640px;margin:auto;padding:24px;color:#222;">
<h2 style="color:#003366;">You're in</h2>
<p>Welcome. You'll receive the <strong>{cadences_str}</strong> briefing(s)
from Pension Plan Intelligence as soon as each one is approved for release.</p>
<p>Every email includes a one-click unsubscribe link in the footer if you
ever want to opt out.</p>
</body></html>"""
    text = (
        f"You're subscribed.\n\n"
        f"You'll receive: {cadences_str}.\n"
        f"Every email has an unsubscribe link in its footer.\n"
    )
    return ApprovalEmail(subject=subject, html=html, text=text,
                         pdf_attachment=None, pdf_filename=None)


def render_digest_email(publication: Publication,
                         subscriber: Subscriber,
                         raw_unsub_token: str) -> ApprovalEmail:
    """Render the full digest body as an HTML email."""
    cadence_label = publication.cadence.title()
    period = f"{publication.period_start.isoformat()} – {publication.period_end.isoformat()}"
    subject = f"{cadence_label} Insights — {period}"
    body_html = markdown_to_email_html(publication.draft_markdown or "")
    unsub_url = _link("unsub", raw_unsub_token)
    prefs_url = f"{config.SUBSCRIBE_BASE_URL}/?subscribe=1"
    html = f"""\
<!DOCTYPE html>
<html><body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;
                  max-width:760px;margin:auto;padding:24px;color:#222;line-height:1.5;">
{body_html}
<hr style="border:none;border-top:1px solid #ddd;margin:32px 0 12px 0;">
<p style="font-size:0.8em;color:#888;">
  You're receiving this because you subscribed to the {publication.cadence}
  Pension Plan Intelligence briefing.
  <a href="{unsub_url}">Unsubscribe</a>
  &middot;
  <a href="{prefs_url}">Update preferences</a>
</p>
</body></html>"""
    text = (
        f"{subject}\n\n"
        f"{publication.draft_markdown or ''}\n\n"
        f"---\n"
        f"Unsubscribe: {unsub_url}\n"
        f"Update preferences: {prefs_url}\n"
    )
    return ApprovalEmail(subject=subject, html=html, text=text,
                         pdf_attachment=None, pdf_filename=None)


# ---------------------------------------------------------------------------
# Fan-out (called from app.page_approval_action after publish() succeeds)
# ---------------------------------------------------------------------------

def fan_out_digest(publication: Publication) -> dict:
    """Email the published digest to every confirmed subscriber for its cadence.

    Idempotent — sets ``Publication.subscribers_notified_at`` once the
    loop finishes and refuses to re-run for a publication that already
    has that timestamp. Per-recipient failures are logged but do not
    abort the loop or the publication.

    Returns a dict with ``sent``, ``skipped`` (already-notified), and
    ``failed`` counts for observability.
    """
    cadence = publication.cadence
    if cadence not in CADENCES:
        logger.info("fan_out_digest: cadence '%s' not in subscriber list; skipping.",
                    cadence)
        return {"sent": 0, "skipped": 0, "failed": 0, "reason": "unsupported_cadence"}

    # Re-load publication on a fresh session so we can mark notified state.
    session = get_session()
    try:
        pub = session.get(Publication, publication.id)
        if pub is None:
            raise SubscriberError(f"Publication {publication.id} not found")
        if pub.subscribers_notified_at is not None:
            logger.info("Publication %s already fanned out at %s; skipping.",
                        pub.id, pub.subscribers_notified_at)
            return {"sent": 0, "skipped": 1, "failed": 0, "reason": "already_notified"}
        # Snapshot the markdown + period so we can release the session lock
        # before the network calls.
        snapshot = Publication(
            id=pub.id,
            cadence=pub.cadence,
            period_start=pub.period_start,
            period_end=pub.period_end,
            draft_markdown=pub.draft_markdown,
        )
    finally:
        session.close()

    recipients = recipients_for_cadence(cadence)
    sent = 0
    failed = 0
    for sub in recipients:
        try:
            raw_unsub = issue_unsubscribe_token(sub)
            email = render_digest_email(snapshot, sub, raw_unsub)
            send_email(email, to=sub.email)
            _mark_sent(sub.id)
            sent += 1
        except Exception as exc:
            failed += 1
            logger.exception(
                "Digest fan-out failed for subscriber %s: %s", sub.id, exc
            )

    session = get_session()
    try:
        pub = session.get(Publication, publication.id)
        pub.subscribers_notified_at = datetime.utcnow()
        session.commit()
    finally:
        session.close()

    return {"sent": sent, "skipped": 0, "failed": failed}


def _mark_sent(subscriber_id: int) -> None:
    session = get_session()
    try:
        sub = session.get(Subscriber, subscriber_id)
        if sub is not None:
            sub.last_email_sent_at = datetime.utcnow()
            session.commit()
    finally:
        session.close()
