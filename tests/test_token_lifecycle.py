"""Token primitives — generation, hashing, validation, single-use, expiry."""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from database import ApprovalToken, Publication, get_session
from insights import approval


def _seed_pub(status="awaiting_approval") -> Publication:
    s = get_session()
    try:
        pub = Publication(
            cadence="weekly",
            period_start=date(2026, 4, 19),
            period_end=date(2026, 4, 25),
            status=status,
            draft_markdown="# test",
            composed_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(days=7),
        )
        s.add(pub)
        s.commit()
        s.refresh(pub)
        return pub
    finally:
        s.close()


def test_hash_token_is_stable():
    h1 = approval.hash_token("hello")
    h2 = approval.hash_token("hello")
    assert h1 == h2
    assert len(h1) == 64  # sha256 hex digest


def test_issue_tokens_creates_two_rows():
    pub = _seed_pub()
    s = get_session()
    try:
        approve, reject = approval.issue_tokens(s, pub)
        s.commit()
        assert approve.action == "approve"
        assert reject.action == "reject"
        assert approve.raw != reject.raw

        rows = s.query(ApprovalToken).filter_by(publication_id=pub.id).all()
        assert {r.action for r in rows} == {"approve", "reject"}
        assert all(r.consumed_at is None for r in rows)
        # Stored as hash, not plaintext.
        assert all(len(r.token_hash) == 64 for r in rows)
        assert {r.token_hash for r in rows} == {approve.hash, reject.hash}
    finally:
        s.close()


def test_consume_token_marks_publication_approved():
    pub = _seed_pub()
    s = get_session()
    try:
        approve, _ = approval.issue_tokens(s, pub)
        s.commit()
    finally:
        s.close()

    consumed = approval.consume_token(approve.raw, expected_action="approve")
    assert consumed.status == "approved"
    assert consumed.approved_at is not None


def test_consume_token_is_single_use():
    pub = _seed_pub()
    s = get_session()
    try:
        approve, _ = approval.issue_tokens(s, pub)
        s.commit()
    finally:
        s.close()

    approval.consume_token(approve.raw, expected_action="approve")

    with pytest.raises(approval.TokenError, match="already used"):
        approval.consume_token(approve.raw, expected_action="approve")


def test_consume_token_wrong_action_rejected():
    pub = _seed_pub()
    s = get_session()
    try:
        approve, _ = approval.issue_tokens(s, pub)
        s.commit()
    finally:
        s.close()

    with pytest.raises(approval.TokenError, match="approve"):
        # Approve token used as reject — invalid.
        approval.consume_token(approve.raw, expected_action="reject")


def test_consume_token_expired_rejected():
    pub = _seed_pub()
    s = get_session()
    try:
        approve, _ = approval.issue_tokens(s, pub)
        # Backdate the expiry into the past.
        s.query(ApprovalToken).update(
            {"expires_at": datetime.utcnow() - timedelta(seconds=1)}
        )
        s.commit()
    finally:
        s.close()

    with pytest.raises(approval.TokenError, match="expired"):
        approval.consume_token(approve.raw, expected_action="approve")

    # Publication state unchanged.
    s = get_session()
    try:
        pub_after = s.get(Publication, pub.id)
        assert pub_after.status == "awaiting_approval"
    finally:
        s.close()


def test_consume_unknown_token_rejected():
    with pytest.raises(approval.TokenError, match="not found"):
        approval.consume_token("does-not-exist", expected_action="approve")


def test_consume_token_wrong_status_rejected():
    pub = _seed_pub(status="published")  # already published
    s = get_session()
    try:
        approve, _ = approval.issue_tokens(s, pub)
        s.commit()
    finally:
        s.close()

    with pytest.raises(approval.TokenError, match="cannot approve"):
        approval.consume_token(approve.raw, expected_action="approve")
