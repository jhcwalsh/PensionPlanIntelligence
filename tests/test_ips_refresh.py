"""Tests for IPS discovery, hash-dedup, and verification gate.

Uses IPS_MODE=mock (set in conftest) so verify_is_ips() runs the offline
heuristic instead of calling Anthropic. Network calls (download_document,
fetch_page_requests) are monkey-patched per-test so nothing leaves the
process.
"""
from __future__ import annotations

import hashlib
from datetime import datetime
from pathlib import Path

import pytest

from database import IpsDocument, IpsRefreshLog, Plan, get_session


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _seed_plan(session, plan_id="testplan",
               website="https://test.local",
               name="Test Plan", abbreviation="TEST"):
    p = Plan(id=plan_id, name=name, abbreviation=abbreviation,
             state="CA", aum_billions=100.0, website=website)
    session.add(p)
    session.commit()
    return p


def _fake_pdf_bytes(title: str = "Investment Policy Statement") -> bytes:
    """Minimal bytes that pass the %PDF- magic header check + size threshold."""
    body = (b"%PDF-1.4\n%fake-test-pdf\n"
            + title.encode("utf-8") + b"\n"
            + b"x" * 60_000)
    return body


def _write_fake_pdf(tmp_path: Path, body: bytes, name: str = "ips.pdf") -> Path:
    p = tmp_path / name
    p.write_bytes(body)
    return p


# ---------------------------------------------------------------------------
# fetch_ips.verify_is_ips (mock-mode heuristic)
# ---------------------------------------------------------------------------

def test_mock_verifier_accepts_strong_title_match():
    from fetch_ips import verify_is_ips
    text = "[Page 1]\n  Statement of Investment Policy  \nApproved by the Board..."
    v = verify_is_ips("Test Plan", text)
    assert v["is_ips"] is True
    assert v["confidence"] == "high"
    assert v["doc_type"] == "ips"


def test_mock_verifier_rejects_unrelated_text():
    from fetch_ips import verify_is_ips
    text = "[Page 1]\n  Proxy Voting Policy  \nThis policy governs proxy voting..."
    v = verify_is_ips("Test Plan", text)
    assert v["is_ips"] is False


# ---------------------------------------------------------------------------
# fetch_ips.mine_existing_for_ips_urls
# ---------------------------------------------------------------------------

def test_mine_existing_finds_ips_pdf_in_extracted_text(session):
    from database import Document
    _seed_plan(session)
    # Drop a few documents into the plan with IPS URLs hidden in text.
    docs = [
        Document(plan_id="testplan",
                 url="https://test.local/board/2024-01.pdf",
                 doc_type="board_pack",
                 extraction_status="done",
                 extracted_text=(
                    "Board agenda for January.\n"
                    "Latest IPS: https://test.local/policies/investment-policy-statement-2024.pdf\n"
                    "End of agenda."
                 )),
        Document(plan_id="testplan",
                 url="https://test.local/board/2024-02.pdf",
                 doc_type="board_pack",
                 extraction_status="done",
                 extracted_text=(
                    "Some other PDF: https://test.local/forms/election.pdf\n"
                    "Investment policy: https://test.local/ips.pdf\n"
                 )),
    ]
    session.add_all(docs); session.commit()

    from fetch_ips import mine_existing_for_ips_urls
    urls = mine_existing_for_ips_urls("testplan", session)
    assert "https://test.local/policies/investment-policy-statement-2024.pdf" in urls
    assert "https://test.local/ips.pdf" in urls
    # The election PDF must NOT come through — its URL doesn't hint IPS.
    assert "https://test.local/forms/election.pdf" not in urls


# ---------------------------------------------------------------------------
# fetch_ips.discover_ips_urls priority order
# ---------------------------------------------------------------------------

def test_override_url_wins_first(session, monkeypatch):
    plan = {"id": "testplan", "name": "Test", "abbreviation": "TST",
            "ips_url": "https://test.local/manual.pdf",
            "website": "https://test.local"}
    _seed_plan(session)

    monkeypatch.setattr("fetch_ips.site_crawl_for_ips",
                        lambda p: ["https://test.local/scraped.pdf"])
    monkeypatch.setattr("fetch_ips.mine_existing_for_ips_urls",
                        lambda pid, s: ["https://test.local/mined.pdf"])

    from fetch_ips import discover_ips_urls
    out = discover_ips_urls(plan, session)
    assert out[0] == ("https://test.local/manual.pdf", "override")
    sources = [s for _, s in out]
    assert sources == ["override", "mine_existing", "site_crawl"]


# ---------------------------------------------------------------------------
# refresh_ips: end-to-end with all I/O monkey-patched
# ---------------------------------------------------------------------------

def test_refresh_saves_new_ips_then_dedupes_on_rerun(
    session, monkeypatch, tmp_path
):
    _seed_plan(session)
    plan = {"id": "testplan", "name": "Test Plan", "abbreviation": "TST",
            "ips_url": "https://test.local/policies/ips.pdf",
            "website": "https://test.local"}

    # Fake download_document: write a deterministic PDF and return its size.
    pdf_body = _fake_pdf_bytes("Investment Policy Statement\nAdopted 2024.")

    def fake_download(url, dest_dir, filename):
        dest_dir.mkdir(parents=True, exist_ok=True)
        path = dest_dir / filename
        path.write_bytes(pdf_body)
        return path, len(pdf_body)

    monkeypatch.setattr("refresh_ips.download_document", fake_download)
    # Avoid running the real pdfplumber/pymupdf extractor on a fake PDF.
    monkeypatch.setattr(
        "refresh_ips._extract_first_pages",
        lambda p: ("[Page 1] Investment Policy Statement of Test Plan", 1),
    )
    # Block actual site crawls + mining: only the override URL should be tried.
    monkeypatch.setattr("fetch_ips.site_crawl_for_ips", lambda p: [])
    monkeypatch.setattr("fetch_ips.mine_existing_for_ips_urls",
                        lambda pid, s: [])

    # First run — save new IPS.
    from refresh_ips import refresh_plan
    run_at = datetime.utcnow()
    status = refresh_plan(session, plan, run_at)
    assert status == "saved"
    rows = session.query(IpsDocument).filter_by(plan_id="testplan").all()
    assert len(rows) == 1
    saved = rows[0]
    assert saved.content_hash == hashlib.sha256(pdf_body).hexdigest()
    assert saved.verification_verdict == "yes"

    # Second run with identical content — should NOT create a new row.
    status2 = refresh_plan(session, plan, datetime.utcnow())
    assert status2 == "already_have"
    rows2 = session.query(IpsDocument).filter_by(plan_id="testplan").all()
    assert len(rows2) == 1


def test_refresh_creates_new_version_when_content_changes(
    session, monkeypatch
):
    _seed_plan(session)
    plan = {"id": "testplan", "name": "Test Plan", "abbreviation": "TST",
            "ips_url": "https://test.local/policies/ips.pdf",
            "website": "https://test.local"}

    monkeypatch.setattr("fetch_ips.site_crawl_for_ips", lambda p: [])
    monkeypatch.setattr("fetch_ips.mine_existing_for_ips_urls",
                        lambda pid, s: [])
    monkeypatch.setattr(
        "refresh_ips._extract_first_pages",
        lambda p: ("[Page 1] Investment Policy Statement of Test Plan", 1),
    )

    body_v1 = _fake_pdf_bytes("Investment Policy Statement v1")
    body_v2 = _fake_pdf_bytes("Investment Policy Statement v2 (revised)")
    state = {"body": body_v1}

    def fake_download(url, dest_dir, filename):
        dest_dir.mkdir(parents=True, exist_ok=True)
        path = dest_dir / filename
        path.write_bytes(state["body"])
        return path, len(state["body"])

    monkeypatch.setattr("refresh_ips.download_document", fake_download)

    from refresh_ips import refresh_plan
    assert refresh_plan(session, plan, datetime.utcnow()) == "saved"

    # Plan publishes a revised IPS; same URL, different content.
    state["body"] = body_v2
    assert refresh_plan(session, plan, datetime.utcnow()) == "saved"

    rows = (session.query(IpsDocument)
            .filter_by(plan_id="testplan")
            .order_by(IpsDocument.id).all())
    assert len(rows) == 2
    assert rows[0].content_hash != rows[1].content_hash


def test_refresh_rejects_when_verifier_says_not_ips(session, monkeypatch):
    _seed_plan(session)
    plan = {"id": "testplan", "name": "Test Plan", "abbreviation": "TST",
            "ips_url": "https://test.local/policies/proxy-voting.pdf",
            "website": "https://test.local"}

    monkeypatch.setattr("fetch_ips.site_crawl_for_ips", lambda p: [])
    monkeypatch.setattr("fetch_ips.mine_existing_for_ips_urls",
                        lambda pid, s: [])
    body = _fake_pdf_bytes("Proxy Voting Policy")

    def fake_download(url, dest, fn):
        dest.mkdir(parents=True, exist_ok=True)
        path = dest / fn
        path.write_bytes(body)
        return path, len(body)

    monkeypatch.setattr("refresh_ips.download_document", fake_download)
    # The mock verifier rejects since "Proxy Voting Policy" isn't an IPS title.
    monkeypatch.setattr(
        "refresh_ips._extract_first_pages",
        lambda p: ("Proxy Voting Policy. This policy governs voting.", 1),
    )

    from refresh_ips import refresh_plan
    status = refresh_plan(session, plan, datetime.utcnow())
    assert status == "verification_failed"
    assert session.query(IpsDocument).filter_by(plan_id="testplan").count() == 0
    log_row = (session.query(IpsRefreshLog).filter_by(plan_id="testplan").one())
    assert log_row.status == "verification_failed"
    assert "verifier said no" in (log_row.notes or "")


def test_refresh_logs_no_candidates_when_discovery_returns_nothing(
    session, monkeypatch
):
    _seed_plan(session)
    plan = {"id": "testplan", "name": "Test Plan", "abbreviation": "TST",
            "website": "https://test.local"}

    monkeypatch.setattr("fetch_ips.site_crawl_for_ips", lambda p: [])
    monkeypatch.setattr("fetch_ips.mine_existing_for_ips_urls",
                        lambda pid, s: [])

    from refresh_ips import refresh_plan
    status = refresh_plan(session, plan, datetime.utcnow())
    assert status == "no_candidates"
    log_row = session.query(IpsRefreshLog).filter_by(plan_id="testplan").one()
    assert log_row.status == "no_candidates"
