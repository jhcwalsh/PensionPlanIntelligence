"""LinkedIn auto-post: token issuance, mock-mode webhook, post-body shape."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta

import pytest

from database import ApprovalToken, Publication, get_session
from insights import approval, social


DRAFT = """\
# Weekly briefing

CalPERS approved a new private credit allocation this week, lifting the
target weight by 200bps. Three other plans in the cohort flagged similar
direction in their packets.

A handful of boards published refreshed IPS documents — the standout was
NYSCRF's update to its private equity benchmark.

* CalPERS: +200bps private credit
* CalSTRS: refreshed IPS benchmark
* NYSCRF: PE benchmark switch
"""


def _seed_pub(*, status="awaiting_approval", draft=DRAFT) -> Publication:
    s = get_session()
    try:
        pub = Publication(
            cadence="weekly",
            period_start=date(2026, 4, 19),
            period_end=date(2026, 4, 25),
            status=status,
            draft_markdown=draft,
            composed_at=datetime.utcnow(),
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
# Token primitives
# ---------------------------------------------------------------------------

def test_issue_linkedin_token_creates_third_row():
    pub = _seed_pub()
    s = get_session()
    try:
        approve, reject = approval.issue_tokens(s, pub)
        linkedin = approval.issue_linkedin_token(s, pub)
        s.commit()

        rows = s.query(ApprovalToken).filter_by(publication_id=pub.id).all()
        assert {r.action for r in rows} == {"approve", "reject", "post_linkedin"}
        assert linkedin.action == "post_linkedin"
        assert linkedin.raw not in (approve.raw, reject.raw)
    finally:
        s.close()


def test_consume_linkedin_token_does_not_change_status():
    pub = _seed_pub()
    s = get_session()
    try:
        linkedin = approval.issue_linkedin_token(s, pub)
        s.commit()
    finally:
        s.close()

    consumed = approval.consume_token(linkedin.raw, expected_action="post_linkedin")
    assert consumed.status == "awaiting_approval"  # unchanged
    assert consumed.approved_at is None
    assert consumed.rejected_at is None


def test_linkedin_token_works_after_email_approval():
    """Founder might click Approve first, then click Post-to-LinkedIn later."""
    pub = _seed_pub(status="approved")
    s = get_session()
    try:
        linkedin = approval.issue_linkedin_token(s, pub)
        s.commit()
    finally:
        s.close()

    # Should not raise — post_linkedin doesn't require awaiting_approval.
    consumed = approval.consume_token(linkedin.raw, expected_action="post_linkedin")
    assert consumed.status == "approved"


def test_linkedin_token_blocked_for_rejected_pub():
    pub = _seed_pub(status="rejected")
    s = get_session()
    try:
        linkedin = approval.issue_linkedin_token(s, pub)
        s.commit()
    finally:
        s.close()

    with pytest.raises(approval.TokenError, match="rejected"):
        approval.consume_token(linkedin.raw, expected_action="post_linkedin")


def test_linkedin_token_single_use():
    pub = _seed_pub()
    s = get_session()
    try:
        linkedin = approval.issue_linkedin_token(s, pub)
        s.commit()
    finally:
        s.close()

    approval.consume_token(linkedin.raw, expected_action="post_linkedin")
    with pytest.raises(approval.TokenError, match="already used"):
        approval.consume_token(linkedin.raw, expected_action="post_linkedin")


# ---------------------------------------------------------------------------
# Post body shape
# ---------------------------------------------------------------------------

def test_build_linkedin_post_includes_lede_and_link():
    from insights import config as ic
    pub = _seed_pub()
    body = social.build_linkedin_post(pub)

    assert "CalPERS approved" in body
    assert "Read the full briefing" in body
    assert ic.APPROVAL_BASE_URL in body
    assert "Weekly pension briefing" in body
    # Heading row "# Weekly briefing" must be stripped, not posted verbatim.
    assert "# Weekly briefing" not in body


def test_build_linkedin_post_strips_markdown():
    pub = _seed_pub(draft="**Bold lede.** With *italic* and `code` and a [link](https://x.test).")
    body = social.build_linkedin_post(pub)
    assert "**" not in body
    assert "`" not in body
    assert "Bold lede." in body
    # Inline link is unwrapped to "label (url)".
    assert "link (https://x.test)" in body


def test_build_linkedin_post_caps_at_3000_chars():
    long = "A long paragraph. " * 500  # ~9000 chars
    pub = _seed_pub(draft=long)
    body = social.build_linkedin_post(pub)
    assert len(body) <= social.LINKEDIN_MAX_CHARS


def test_build_linkedin_post_raises_on_empty_draft():
    pub = _seed_pub(draft="")
    with pytest.raises(ValueError, match="no draft_markdown"):
        social.build_linkedin_post(pub)


# ---------------------------------------------------------------------------
# Mock-mode webhook
# ---------------------------------------------------------------------------

def test_post_to_linkedin_mock_writes_file():
    pub = _seed_pub()
    social.clear_mock_posts()

    path = social.post_to_linkedin(pub)

    posts = social.list_mock_posts()
    assert len(posts) == 1
    payload = json.loads(posts[0].read_text())
    assert payload["publication_id"] == pub.id
    assert payload["cadence"] == "weekly"
    assert payload["period_start"] == "2026-04-19"
    assert "CalPERS approved" in payload["text"]
    assert path.endswith("_linkedin.json")


# ---------------------------------------------------------------------------
# Email integration: third button only when LinkedIn token supplied
# ---------------------------------------------------------------------------

def test_render_email_without_linkedin_token_has_no_third_button():
    pub = _seed_pub()
    s = get_session()
    try:
        approve, reject = approval.issue_tokens(s, pub)
        s.commit()
    finally:
        s.close()

    email = approval.render_approval_email(pub, approve, reject, pdf_bytes=None)
    assert "post_linkedin" not in email.html
    assert "post to LinkedIn" not in email.html.lower()


def test_render_email_with_linkedin_token_has_third_button():
    pub = _seed_pub()
    s = get_session()
    try:
        approve, reject = approval.issue_tokens(s, pub)
        linkedin = approval.issue_linkedin_token(s, pub)
        s.commit()
    finally:
        s.close()

    email = approval.render_approval_email(
        pub, approve, reject, pdf_bytes=None, post_linkedin=linkedin,
    )
    assert "Approve &amp; post to LinkedIn" in email.html or "Approve & post to LinkedIn" in email.html
    assert f"post_linkedin={linkedin.raw}" in email.html
    assert f"post_linkedin={linkedin.raw}" in email.text
