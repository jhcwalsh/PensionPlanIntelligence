"""refresh_cafrs.refresh_plan: the prior-FY fallback.

A June-30 plan's newest closed FY flips on 1 July, roughly six months before
its ACFR is published. A URL template happily renders that year's URL, the
download 404s, and the refresh must still go on to the year before — on
2026-09-01 it returned url_failed instead, for 42 plans.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

import database
import refresh_cafrs

PLAN = {
    "id": "tmplplan",
    "abbreviation": "TMPL",
    "fiscal_year_end": "06-30",
    "cafr_url_template": "https://example.org/acfr-{year}.pdf",
}
RUN_AT = datetime(2026, 9, 1, 15, 0, tzinfo=timezone.utc)


@pytest.fixture
def fake_net(tmp_path, monkeypatch):
    """Only the FY2025 URL downloads; its cover says 2025."""
    monkeypatch.setattr(refresh_cafrs, "DOWNLOADS_DIR", str(tmp_path / "dl"))
    monkeypatch.setattr(refresh_cafrs.time, "sleep", lambda s: None)
    tried = []

    def download(url, plan_dir, filename):
        tried.append(url)
        if "2025" not in url:
            return None, 0
        path = plan_dir / filename
        path.write_bytes(b"%PDF-1.7\n" + b"x" * (refresh_cafrs.MIN_PDF_BYTES + 10))
        return path, path.stat().st_size

    monkeypatch.setattr(refresh_cafrs, "download_document", download)
    monkeypatch.setattr(refresh_cafrs, "fiscal_year_from_pdf", lambda path, max_year=None: 2025)
    return tried


def test_a_failed_download_of_the_newest_year_falls_back_to_the_prior_year(fake_net):
    session = database.get_session()
    try:
        status = refresh_cafrs.refresh_plan(session, PLAN, RUN_AT)

        assert status == "saved"
        assert fake_net == ["https://example.org/acfr-2026.pdf",
                            "https://example.org/acfr-2025.pdf"]
        doc = session.query(database.Document).filter_by(plan_id="tmplplan").one()
        assert doc.fiscal_year == 2025
        log = {(r.expected_year, r.status)
               for r in session.query(database.CafrRefreshLog).filter_by(plan_id="tmplplan")}
        assert log == {(2026, "url_failed"), (2025, "saved")}
    finally:
        session.close()


def test_a_forced_year_still_stops_at_that_year(fake_net):
    session = database.get_session()
    try:
        status = refresh_cafrs.refresh_plan(session, PLAN, RUN_AT, force_year=2026)
        assert status == "url_failed"
        assert fake_net == ["https://example.org/acfr-2026.pdf"]
    finally:
        session.close()
