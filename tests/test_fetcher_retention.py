"""The fetcher stores each PDF as it lands.

Storing at fetch time is the whole point: the window between "file exists on
the runner" and "runner is destroyed" is the only moment the bytes are
freely available. Anything later is a re-download that may 404.
"""
from __future__ import annotations

import pathlib

import fetcher
import pdf_store
from database import Document, Plan, get_session


def test_fetcher_stores_downloaded_pdf(tmp_db, tmp_path, monkeypatch):
    pdf = tmp_path / "board.pdf"
    pdf.write_bytes(b"%PDF-1.4 fetched")

    calls = []

    def fake_store(session, document, path, cfg=None):
        calls.append((document.url, pathlib.Path(path).name))
        document.content_sha256 = "f" * 64
        session.commit()
        return "f" * 64

    monkeypatch.setattr(pdf_store, "store_document", fake_store)
    monkeypatch.setattr(fetcher, "download_document",
                        lambda url, d, f: (pdf, pdf.stat().st_size))
    monkeypatch.setattr(fetcher, "discover_document_links", lambda p: [
        {"url": "https://x/board.pdf", "filename": "board.pdf",
         "doc_type": "board_pack", "meeting_date": None},
    ])
    monkeypatch.setattr(fetcher, "load_plans", lambda: [
        {"id": "p1", "abbreviation": "P1", "name": "Plan One"},
    ])

    session = get_session()
    session.add(Plan(id="p1", name="Plan One", abbreviation="P1", state="CA"))
    session.commit()
    session.close()

    fetcher.run_fetcher()

    assert calls == [("https://x/board.pdf", "board.pdf")]
    session = get_session()
    try:
        doc = session.query(Document).filter_by(url="https://x/board.pdf").one()
        assert doc.content_sha256 == "f" * 64
    finally:
        session.close()


def test_fetcher_continues_when_retention_fails(tmp_db, tmp_path, monkeypatch):
    """Retention is additive. A failed upload must still leave a usable
    document row -- otherwise an R2 outage silently costs a day's fetch."""
    pdf = tmp_path / "board.pdf"
    pdf.write_bytes(b"%PDF-1.4 fetched")

    monkeypatch.setattr(pdf_store, "store_document",
                        lambda *a, **k: None)     # simulates failure
    monkeypatch.setattr(fetcher, "download_document",
                        lambda url, d, f: (pdf, pdf.stat().st_size))
    monkeypatch.setattr(fetcher, "discover_document_links", lambda p: [
        {"url": "https://x/board.pdf", "filename": "board.pdf",
         "doc_type": "board_pack", "meeting_date": None},
    ])
    monkeypatch.setattr(fetcher, "load_plans", lambda: [
        {"id": "p1", "abbreviation": "P1", "name": "Plan One"},
    ])

    session = get_session()
    session.add(Plan(id="p1", name="Plan One", abbreviation="P1", state="CA"))
    session.commit()
    session.close()

    fetcher.run_fetcher()

    session = get_session()
    try:
        doc = session.query(Document).filter_by(url="https://x/board.pdf").one()
        assert doc.content_sha256 is None
        assert doc.extraction_status == "pending"   # still extractable
    finally:
        session.close()
