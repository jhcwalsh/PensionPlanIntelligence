"""Auto-send path that bypasses approval — used by daily digests on calm days."""
from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

import pytest

from database import ApprovalToken, Publication, get_session
from insights import approval, config, cycle_common


@pytest.fixture()
def fresh_publication():
    s = get_session()
    try:
        pub = Publication(
            cadence="daily",
            period_start=date(2026, 5, 16),
            period_end=date(2026, 5, 16),
            status="generating",
        )
        s.add(pub)
        s.commit()
        s.refresh(pub)
        yield pub.id
    finally:
        s.close()


def test_finalize_and_send_transitions_to_published(fresh_publication):
    s = get_session()
    try:
        pub = s.get(Publication, fresh_publication)
        cycle_common.finalize_and_send(
            s, pub,
            draft_markdown="# Daily Digest 2026-05-16\n\nNo new documents.\n",
            title_for_pdf="Daily Pension Digest — 2026-05-16",
        )
        s.refresh(pub)
        assert pub.status == "published"
        assert pub.draft_markdown.startswith("# Daily Digest")
        assert pub.pdf_path is not None
        assert Path(pub.pdf_path).exists()
        assert pub.published_at is not None
    finally:
        s.close()


def test_finalize_and_send_does_not_create_approval_tokens(fresh_publication):
    s = get_session()
    try:
        pub = s.get(Publication, fresh_publication)
        cycle_common.finalize_and_send(
            s, pub, draft_markdown="body", title_for_pdf="Title",
        )
        assert s.query(ApprovalToken).count() == 0
    finally:
        s.close()


def test_finalize_and_send_writes_mock_email(fresh_publication):
    s = get_session()
    try:
        pub = s.get(Publication, fresh_publication)
        cycle_common.finalize_and_send(
            s, pub, draft_markdown="body", title_for_pdf="Title",
        )
    finally:
        s.close()

    emails = approval.list_mock_emails()
    assert len(emails) == 1
    meta_path = emails[0].with_suffix(".json")
    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    # Subject should use Daily Pension Digest, no "Action required".
    assert "Daily Pension Digest" in metadata["subject"]
    assert "Action required" not in metadata["subject"]
    assert metadata["has_attachment"] is True
    assert metadata["pdf_filename"].startswith("daily_digest_")


def test_finalize_and_send_rejects_non_generating_status(fresh_publication):
    s = get_session()
    try:
        pub = s.get(Publication, fresh_publication)
        pub.status = "published"
        s.commit()
        with pytest.raises(ValueError, match="generating"):
            cycle_common.finalize_and_send(
                s, pub, draft_markdown="body", title_for_pdf="Title",
            )
    finally:
        s.close()
