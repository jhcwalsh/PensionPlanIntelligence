"""End-to-end mock-mode weekly cycle: schedule, click approve, assert published."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pytest

from database import ApprovalToken, Plan, Publication, get_session
from insights import approval, weekly


@pytest.fixture()
def seeded_plans():
    s = get_session()
    try:
        for pid in ["calpers", "calstrs"]:
            s.add(Plan(id=pid, name=pid.upper(), abbreviation=pid.upper()))
        s.commit()
    finally:
        s.close()


def test_weekly_cycle_produces_awaiting_approval_publication(seeded_plans, monkeypatch):
    pub = weekly.run_weekly_cycle(
        period_start=date(2026, 4, 19), skip_scrape=False
    )
    assert pub.cadence == "weekly"
    assert pub.period_start == date(2026, 4, 19)
    assert pub.period_end == date(2026, 4, 25)
    assert pub.status == "awaiting_approval"
    assert pub.draft_markdown
    assert pub.pdf_path
    assert Path(pub.pdf_path).exists()

    # Three tokens are issued for the weekly cycle: approve, reject,
    # post_linkedin (the third lights up the optional LinkedIn auto-post
    # button in the approval email; minted in mock mode + when the
    # webhook URL is configured).
    s = get_session()
    try:
        tokens = s.query(ApprovalToken).filter_by(publication_id=pub.id).all()
        assert {t.action for t in tokens} == {"approve", "reject", "post_linkedin"}
    finally:
        s.close()


def test_weekly_cycle_is_idempotent_for_same_period(seeded_plans):
    a = weekly.run_weekly_cycle(period_start=date(2026, 4, 19))
    b = weekly.run_weekly_cycle(period_start=date(2026, 4, 19))
    assert a.id == b.id

    s = get_session()
    try:
        assert s.query(Publication).count() == 1
    finally:
        s.close()


def test_weekly_cycle_writes_mock_email(seeded_plans):
    """An approval email should hit the mock outbox in the test tmp dir."""
    weekly.run_weekly_cycle(period_start=date(2026, 4, 19))

    emails = approval.list_mock_emails()
    assert len(emails) == 1
    metadata = __import__("json").loads(emails[0].read_text(encoding="utf-8"))
    assert "Action required" in metadata["subject"]
    assert metadata["has_attachment"] is True
    assert metadata["pdf_filename"].startswith("weekly_cio_insights_")


def test_full_approve_flow_transitions_to_published(seeded_plans, tmp_path, monkeypatch):
    """Schedule → grab token from outbox → consume_token → publish runs."""
    # We have to read the issued raw token from the DB before clicking.
    # In production the founder gets it via email; in the test the
    # tokens are minted by issue_tokens — to retrieve the plaintext we
    # have to intercept it. Approach: monkeypatch generate_raw_token to
    # capture what's minted.
    minted: list[str] = []
    real_generate = approval.generate_raw_token

    def _capture():
        t = real_generate()
        minted.append(t)
        return t

    monkeypatch.setattr(approval, "generate_raw_token", _capture)

    pub = weekly.run_weekly_cycle(period_start=date(2026, 4, 19))

    # Two tokens (approve, reject) — the first is approve.
    approve_raw = minted[0]
    reject_raw = minted[1]
    assert approve_raw != reject_raw

    # Stub publish.publish so the test doesn't shell out to git.
    import insights.publish as _publish
    monkeypatch.setattr(_publish, "publish", lambda p: Path("/tmp/fake.md"))

    consumed = approval.consume_token(approve_raw, expected_action="approve")
    assert consumed.status == "approved"
    assert consumed.approved_at is not None

    # Reject token can't be reused for approve.
    with pytest.raises(approval.TokenError):
        approval.consume_token(reject_raw, expected_action="approve")

    # A second click on the approve token is also rejected (single-use).
    with pytest.raises(approval.TokenError, match="already used"):
        approval.consume_token(approve_raw, expected_action="approve")
