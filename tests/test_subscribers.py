"""Subscriber sign-up lifecycle: tokens, status transitions, and digest fan-out.

Mirrors the conventions in test_token_lifecycle.py — DB isolation comes
from conftest's autouse fixture; INSIGHTS_MODE=mock means send_email()
writes to tmp/sent_emails/ instead of calling Resend.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta

import pytest

from database import (
    Publication,
    Subscriber,
    SubscriberToken,
    get_session,
)
from insights import approval as _approval
from insights import config as ic
from insights import subscribers as subs


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _signup(email="alice@example.com", *, weekly=True, monthly=False,
            quarterly=False) -> tuple[Subscriber, str]:
    return subs.create_pending_subscriber(
        email, weekly=weekly, monthly=monthly, quarterly=quarterly,
    )


def _seed_published_pub(cadence="weekly", markdown="# Test\n\n## TL;DR\n\nBody.") -> Publication:
    s = get_session()
    try:
        pub = Publication(
            cadence=cadence,
            period_start=date(2026, 4, 19),
            period_end=date(2026, 4, 25),
            status="published",
            draft_markdown=markdown,
            composed_at=datetime.utcnow(),
            approved_at=datetime.utcnow(),
            published_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(days=7),
        )
        s.add(pub)
        s.commit()
        s.refresh(pub)
        s.expunge(pub)
        return pub
    finally:
        s.close()


# ---------------------------------------------------------------------------
# Sign-up + confirm
# ---------------------------------------------------------------------------

def test_signup_writes_pending_row_with_confirm_token():
    sub, raw = _signup()
    assert sub.status == "pending"
    assert sub.weekly is True
    assert sub.confirmed_at is None
    assert raw  # plaintext returned once

    s = get_session()
    try:
        tokens = s.query(SubscriberToken).filter_by(subscriber_id=sub.id).all()
        assert len(tokens) == 1
        assert tokens[0].action == "confirm"
        assert tokens[0].consumed_at is None
        # Stored as hash, not plaintext.
        assert tokens[0].token_hash == _approval.hash_token(raw)
    finally:
        s.close()


def test_signup_email_is_normalized_lowercased():
    sub, _ = _signup(email="  ALICE@Example.COM  ")
    assert sub.email == "alice@example.com"


def test_signup_requires_at_least_one_cadence():
    with pytest.raises(subs.SubscriberError, match="at least one"):
        _signup(weekly=False, monthly=False, quarterly=False)


def test_signup_rejects_invalid_email():
    with pytest.raises(subs.SubscriberError, match="Invalid email"):
        _signup(email="not-an-email")


def test_signup_rolls_back_when_send_callback_raises():
    """A failing email send must not leave a stale pending row + token.

    The whole point: if Render is misconfigured (no RESEND_API_KEY) or
    Resend hits a transient 5xx, the subscriber form errors out and the
    user can re-try without burning a rate-limit slot.
    """
    def boom(sub, raw):
        raise RuntimeError("Simulated send failure")

    with pytest.raises(RuntimeError, match="Simulated send failure"):
        subs.create_pending_subscriber(
            "alice@example.com", weekly=True, send_callback=boom,
        )

    s = get_session()
    try:
        # No subscriber row was committed.
        assert s.query(Subscriber).count() == 0
        # No confirm token either — the rate limiter sees a clean slate.
        assert s.query(SubscriberToken).count() == 0
    finally:
        s.close()
    assert subs.recent_signup_count("alice@example.com") == 0


def test_signup_rollback_preserves_existing_row():
    """Re-signup with a failing send must not mutate the existing subscriber.

    If a confirmed user re-submits the form (different cadences) and the
    confirmation email fails, their original cadence flags should be
    intact — the failed retry shouldn't silently change their prefs.
    """
    sub, raw = _signup(email="alice@example.com", weekly=True, monthly=False)
    subs.consume_confirm_token(raw)  # alice is confirmed, weekly=True

    def boom(sub, raw):
        raise RuntimeError("Simulated send failure")

    with pytest.raises(RuntimeError):
        subs.create_pending_subscriber(
            "alice@example.com",
            weekly=False, monthly=True,  # would-be new prefs
            send_callback=boom,
        )

    s = get_session()
    try:
        reloaded = s.query(Subscriber).filter_by(email="alice@example.com").one()
        assert reloaded.status == "confirmed"
        assert reloaded.weekly is True   # original pref preserved
        assert reloaded.monthly is False
        # The original confirm token is still there (consumed); no second one.
        tokens = s.query(SubscriberToken).filter_by(subscriber_id=reloaded.id).all()
        assert len(tokens) == 1
        assert tokens[0].consumed_at is not None
    finally:
        s.close()


def test_signup_callback_success_persists_normally():
    """Sanity check: a successful callback commits as before."""
    calls = []

    def ok(sub, raw):
        calls.append((sub.email, raw))

    sub, raw = subs.create_pending_subscriber(
        "alice@example.com", weekly=True, send_callback=ok,
    )
    assert calls == [("alice@example.com", raw)]
    assert sub.status == "pending"
    s = get_session()
    try:
        assert s.query(Subscriber).count() == 1
        assert s.query(SubscriberToken).count() == 1
    finally:
        s.close()


def test_signup_repeat_for_same_email_refreshes_prefs():
    sub1, raw1 = _signup(weekly=True, monthly=False)
    sub2, raw2 = _signup(weekly=False, monthly=True)
    assert sub1.id == sub2.id
    assert sub2.weekly is False
    assert sub2.monthly is True
    assert raw1 != raw2  # fresh token each call

    s = get_session()
    try:
        tokens = s.query(SubscriberToken).filter_by(
            subscriber_id=sub1.id, action="confirm",
        ).all()
        assert len(tokens) == 2
    finally:
        s.close()


def test_confirm_flips_subscriber_to_confirmed():
    sub, raw = _signup()
    confirmed = subs.consume_confirm_token(raw)
    assert confirmed.status == "confirmed"
    assert confirmed.confirmed_at is not None


def test_confirm_is_single_use():
    _, raw = _signup()
    subs.consume_confirm_token(raw)
    with pytest.raises(subs.SubscriberError, match="already used"):
        subs.consume_confirm_token(raw)


def test_confirm_wrong_action_rejected():
    _, raw = _signup()
    with pytest.raises(subs.SubscriberError, match="confirm"):
        subs.consume_unsubscribe_token(raw)


def test_confirm_expired_rejected():
    sub, raw = _signup()
    s = get_session()
    try:
        s.query(SubscriberToken).filter_by(
            subscriber_id=sub.id, action="confirm",
        ).update({"expires_at": datetime.utcnow() - timedelta(seconds=1)})
        s.commit()
    finally:
        s.close()
    with pytest.raises(subs.SubscriberError, match="expired"):
        subs.consume_confirm_token(raw)


def test_confirm_blocked_for_disabled_row():
    sub, raw = _signup()
    subs.set_status(sub.id, "disabled")
    with pytest.raises(subs.SubscriberError, match="disabled"):
        subs.consume_confirm_token(raw)


# ---------------------------------------------------------------------------
# Unsubscribe + preferences
# ---------------------------------------------------------------------------

def test_unsubscribe_clears_cadences_and_sets_timestamp():
    sub, raw = _signup(weekly=True, monthly=True)
    subs.consume_confirm_token(raw)
    raw_unsub = subs.issue_unsubscribe_token(_reload(sub))
    result = subs.consume_unsubscribe_token(raw_unsub)
    assert result.status == "unsubscribed"
    assert result.weekly is False
    assert result.monthly is False
    assert result.unsubscribed_at is not None


def test_resubscribe_after_unsubscribe_resets_to_pending():
    sub, raw = _signup(weekly=True)
    subs.consume_confirm_token(raw)
    raw_unsub = subs.issue_unsubscribe_token(_reload(sub))
    subs.consume_unsubscribe_token(raw_unsub)

    sub2, raw2 = _signup(weekly=True)
    assert sub.id == sub2.id
    assert sub2.status == "pending"
    confirmed = subs.consume_confirm_token(raw2)
    assert confirmed.status == "confirmed"


def test_set_preferences_clears_all_treated_as_unsubscribe():
    sub, raw = _signup(weekly=True, monthly=True)
    subs.consume_confirm_token(raw)
    updated = subs.set_preferences(sub.id, weekly=False, monthly=False, quarterly=False)
    assert updated.status == "unsubscribed"


def test_set_preferences_reactivates_unsubscribed_row():
    sub, raw = _signup(weekly=True)
    subs.consume_confirm_token(raw)
    raw_unsub = subs.issue_unsubscribe_token(_reload(sub))
    subs.consume_unsubscribe_token(raw_unsub)

    revived = subs.set_preferences(sub.id, weekly=False, monthly=True, quarterly=False)
    assert revived.status == "confirmed"
    assert revived.monthly is True


# ---------------------------------------------------------------------------
# Recipient queries
# ---------------------------------------------------------------------------

def test_recipients_for_cadence_only_returns_confirmed_matching():
    a, raw_a = _signup(email="a@example.com", weekly=True)
    b, _ = _signup(email="b@example.com", weekly=True)  # stays pending
    c, raw_c = _signup(email="c@example.com", weekly=False, monthly=True)
    subs.consume_confirm_token(raw_a)
    subs.consume_confirm_token(raw_c)

    weekly_recipients = subs.recipients_for_cadence("weekly")
    monthly_recipients = subs.recipients_for_cadence("monthly")

    weekly_emails = {r.email for r in weekly_recipients}
    monthly_emails = {r.email for r in monthly_recipients}
    assert weekly_emails == {"a@example.com"}
    assert monthly_emails == {"c@example.com"}


def test_recipients_excludes_disabled():
    a, raw_a = _signup(email="a@example.com", weekly=True)
    subs.consume_confirm_token(raw_a)
    subs.set_status(a.id, "disabled")
    assert subs.recipients_for_cadence("weekly") == []


def test_recipients_excludes_unsubscribed():
    a, raw_a = _signup(email="a@example.com", weekly=True)
    subs.consume_confirm_token(raw_a)
    raw_unsub = subs.issue_unsubscribe_token(_reload(a))
    subs.consume_unsubscribe_token(raw_unsub)
    assert subs.recipients_for_cadence("weekly") == []


# ---------------------------------------------------------------------------
# Digest fan-out
# ---------------------------------------------------------------------------

def test_fan_out_digest_sends_to_confirmed_subscribers(monkeypatch):
    _approval.clear_mock_emails()
    a, raw_a = _signup(email="a@example.com", weekly=True)
    b, raw_b = _signup(email="b@example.com", weekly=True)
    c, _ = _signup(email="c@example.com", weekly=True)  # pending — excluded
    subs.consume_confirm_token(raw_a)
    subs.consume_confirm_token(raw_b)

    _approval.clear_mock_emails()  # drop confirm emails
    pub = _seed_published_pub(cadence="weekly")
    result = subs.fan_out_digest(pub)

    assert result == {"sent": 2, "skipped": 0, "failed": 0}

    metas = _approval.list_mock_emails()
    recipients = []
    for m in metas:
        data = json.loads(m.read_text())
        recipients.extend(data["to"])
    assert sorted(recipients) == ["a@example.com", "b@example.com"]


def test_fan_out_digest_is_idempotent():
    a, raw_a = _signup(email="a@example.com", weekly=True)
    subs.consume_confirm_token(raw_a)
    _approval.clear_mock_emails()

    pub = _seed_published_pub(cadence="weekly")
    first = subs.fan_out_digest(pub)
    assert first["sent"] == 1

    s = get_session()
    try:
        reloaded = s.get(Publication, pub.id)
        assert reloaded.subscribers_notified_at is not None
    finally:
        s.close()

    second = subs.fan_out_digest(pub)
    assert second == {"sent": 0, "skipped": 1, "failed": 0,
                       "reason": "already_notified"}


def test_fan_out_for_unknown_cadence_is_a_noop():
    pub = _seed_published_pub(cadence="annual")
    result = subs.fan_out_digest(pub)
    assert result["sent"] == 0
    assert result.get("reason") == "unsupported_cadence"


def test_fan_out_continues_past_per_subscriber_failures(monkeypatch):
    """One bad recipient must not abort the loop or block the publication."""
    a, raw_a = _signup(email="a@example.com", weekly=True)
    b, raw_b = _signup(email="b@example.com", weekly=True)
    subs.consume_confirm_token(raw_a)
    subs.consume_confirm_token(raw_b)
    _approval.clear_mock_emails()

    real_send = subs.send_email
    call_count = {"n": 0}

    def flaky_send(email, to=None):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise RuntimeError("Simulated Resend 5xx")
        return real_send(email, to=to)

    monkeypatch.setattr(subs, "send_email", flaky_send)

    pub = _seed_published_pub(cadence="weekly")
    result = subs.fan_out_digest(pub)
    assert result["sent"] + result["failed"] == 2
    assert result["failed"] == 1
    # Publication still marked notified so re-running won't double-send.
    s = get_session()
    try:
        reloaded = s.get(Publication, pub.id)
        assert reloaded.subscribers_notified_at is not None
    finally:
        s.close()


def test_digest_email_includes_unsubscribe_link():
    a, raw_a = _signup(email="a@example.com", weekly=True)
    subs.consume_confirm_token(raw_a)
    sub = _reload(a)

    raw_unsub = subs.issue_unsubscribe_token(sub)
    pub = _seed_published_pub()
    email = subs.render_digest_email(pub, sub, raw_unsub)
    assert "unsub=" + raw_unsub in email.html
    assert "Unsubscribe" in email.html


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _reload(sub: Subscriber) -> Subscriber:
    """Re-attach a Subscriber from a closed session."""
    s = get_session()
    try:
        out = s.get(Subscriber, sub.id)
        s.expunge(out)
        return out
    finally:
        s.close()
