"""Backfill: local files first, then re-fetch, resumable throughout.

Ordering is not cosmetic. Local-file uploads are free and risk-free; the
re-fetch path costs bandwidth and races link rot (a 20-URL sample on
2026-08-29 found 19 still live, so ~5% is already unrecoverable and rising).
Doing the free half first means an interrupted run has still made progress.
"""
from __future__ import annotations

import pytest

import pdf_store
from database import Document, Plan, get_session
from scripts import backfill_pdf_store


def _seed(session, url, local_path=None, sha=None, plan_id="p1"):
    if session.get(Plan, plan_id) is None:
        session.add(Plan(id=plan_id, name=plan_id, abbreviation=plan_id,
                         state="CA"))
        session.flush()
    doc = Document(plan_id=plan_id, url=url, filename="d.pdf",
                   doc_type="board_pack", local_path=local_path,
                   content_sha256=sha)
    session.add(doc)
    session.commit()
    return doc


def test_uploads_local_file(r2, tmp_db, tmp_path, monkeypatch):
    pdf = tmp_path / "have.pdf"
    pdf.write_bytes(b"%PDF-1.4 have")
    session = get_session()
    _seed(session, "https://x/have.pdf", local_path=str(pdf))
    session.close()

    counts = backfill_pdf_store.run(cfg=r2, refetch=False)
    assert counts["stored_local"] == 1

    session = get_session()
    try:
        doc = session.query(Document).one()
        assert doc.content_sha256 == pdf_store.sha256_bytes(b"%PDF-1.4 have")
    finally:
        session.close()


def test_skips_already_stored(r2, tmp_db, tmp_path):
    """Resume must cost nothing: a document already carrying a sha is not
    re-read, re-hashed, or re-uploaded."""
    session = get_session()
    _seed(session, "https://x/done.pdf", sha="a" * 64)
    session.close()

    counts = backfill_pdf_store.run(cfg=r2, refetch=False)
    assert counts["already"] == 1
    assert counts["stored_local"] == 0


def test_refetches_when_local_missing(r2, tmp_db, tmp_path, monkeypatch):
    monkeypatch.setattr(backfill_pdf_store, "_fetch_bytes",
                        lambda url: b"%PDF-1.4 refetched")
    session = get_session()
    _seed(session, "https://x/gone.pdf", local_path=str(tmp_path / "nope.pdf"))
    session.close()

    counts = backfill_pdf_store.run(cfg=r2, refetch=True)
    assert counts["stored_refetch"] == 1


def test_dead_url_recorded_as_unrecoverable_and_continues(r2, tmp_db, tmp_path,
                                                          monkeypatch):
    """A 404 is a permanent fact about the corpus, not a crash. The run must
    continue and the count must be visible -- that number is the floor of
    what can never be recovered."""
    def dead(url):
        return None

    monkeypatch.setattr(backfill_pdf_store, "_fetch_bytes", dead)
    session = get_session()
    _seed(session, "https://x/dead.pdf", local_path=None)
    _seed(session, "https://x/dead2.pdf", local_path=None)
    session.close()

    counts = backfill_pdf_store.run(cfg=r2, refetch=True)
    assert counts["unrecoverable"] == 2


def test_waf_blocked_plans_are_not_refetched(r2, tmp_db, monkeypatch):
    """No runner can reach these; attempting is guaranteed waste."""
    monkeypatch.setattr(backfill_pdf_store, "_waf_blocked_plan_ids",
                        lambda: {"blocked"})
    called = []
    monkeypatch.setattr(backfill_pdf_store, "_fetch_bytes",
                        lambda url: called.append(url) or b"x")
    session = get_session()
    _seed(session, "https://x/w.pdf", plan_id="blocked")
    session.close()

    counts = backfill_pdf_store.run(cfg=r2, refetch=True)
    assert counts["skipped_waf"] == 1
    assert called == []
