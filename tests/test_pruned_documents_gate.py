"""Pruned-document URL gate.

The fetcher must skip URLs in pruned_documents so that an intentionally
deleted document doesn't get re-downloaded the next time the source listing
exposes the same URL. The fetcher's gate is a single ``document_pruned()``
check (fetcher.py — adjacent to ``document_exists()``); the substantive
behaviour to test is the helper itself.
"""

from datetime import datetime

from database import PrunedDocument, document_pruned


def test_document_pruned_returns_true_for_listed_url(session):
    session.add(PrunedDocument(
        url="https://example.com/agenda-2024.pdf",
        plan_id="testplan",
        doc_type="agenda",
        meeting_date=datetime(2024, 5, 1),
        reason="pre-2026-agenda-prune",
    ))
    session.commit()

    assert document_pruned(session, "https://example.com/agenda-2024.pdf") is True
    assert document_pruned(session, "https://example.com/other.pdf") is False


def test_pruned_documents_url_is_unique(session):
    """The same URL can only be pruned once; second insert should fail."""
    import pytest
    from sqlalchemy.exc import IntegrityError

    session.add(PrunedDocument(
        url="https://example.com/dup.pdf", reason="pre-2026-agenda-prune",
    ))
    session.commit()

    session.add(PrunedDocument(
        url="https://example.com/dup.pdf", reason="pre-2026-agenda-prune",
    ))
    with pytest.raises(IntegrityError):
        session.commit()
    session.rollback()
