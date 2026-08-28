"""Periodic-performance extraction: mock round-trip, plan allowlist, fund scoping.

extract_performance_reports.py exists because the 48 doc_type='performance'
documents were assumed to be quarterly fund-return reports (see
queries.performance_report_rows's docstring) but turn out to be that for
only one plan -- see the module docstring there for the other four. These
tests pin down the two hazards that would matter if that finding were ever
wrong or forgotten: a document from an unlisted plan must never be
processed, and a fund-scope mismatch must never be silently swallowed.
"""

from __future__ import annotations

from datetime import datetime, timezone

import extract_performance_reports as epr
from database import (
    Document,
    Plan,
    PerformanceReportExtract,
    get_session,
)


def _seed_plan(session, plan_id, abbreviation=None):
    if session.get(Plan, plan_id) is None:
        session.add(Plan(id=plan_id, name=plan_id.upper(),
                         abbreviation=abbreviation or plan_id.upper(),
                         state="NY", aum_billions=1.0))
        session.flush()


def _seed_doc(session, plan_id, url, text):
    doc = Document(
        plan_id=plan_id, url=url, filename=url.rsplit("/", 1)[-1],
        doc_type="performance", extraction_status="done",
        downloaded_at=datetime(2026, 4, 5, tzinfo=timezone.utc),
        extracted_text=text,
    )
    session.add(doc)
    session.flush()
    return doc


def test_fund_scope_from_url():
    assert epr.fund_scope_from_url(
        "https://x/Monthly-Performance-Review-Material_01-2026-NYCERS.pdf") == "NYCERS"
    assert epr.fund_scope_from_url(
        "https://x/Monthly-Performance-Review-Material_01-2026-police.pdf") == "POLICE"
    assert epr.fund_scope_from_url("https://x/something-else.pdf") is None


def test_mock_roundtrip_and_skip(tmp_db):
    session = get_session()
    _seed_plan(session, "nycrs_comptroller")
    doc = _seed_doc(session, "nycrs_comptroller",
                    "https://x/Monthly-Performance-Review-Material_01-2026-NYCERS.pdf",
                    "Performance Overview as of November 30, 2025\n" + "filler " * 100)
    session.commit()
    doc_id = doc.id
    session.close()

    counts = epr.run_extraction()
    assert counts["saved"] == 1

    session = get_session()
    row = session.query(PerformanceReportExtract).one()
    assert row.document_id == doc_id
    assert row.fund_scope == "NYCERS"
    assert row.as_of_date == "2026-01-31"  # from MOCK_PAYLOAD
    assert len(row.returns) == 2
    session.close()

    # Not forced: already-extracted documents are pre-filtered out of the
    # query entirely, so there's nothing left to process.
    counts = epr.run_extraction()
    assert counts == {}

    # extract_one's own hash check (reached when a caller processes a
    # specific document directly rather than through run_extraction's bulk
    # filter) also recognises unchanged text without calling Claude again.
    session = get_session()
    doc = session.get(Document, doc_id)
    plan = session.get(Plan, "nycrs_comptroller")
    status = epr.extract_one(session, doc, plan, force=False)
    assert status == "already_extracted"
    session.close()


def test_disallowed_plan_is_never_processed(tmp_db):
    """A doc_type='performance' document outside ALLOWED_PLAN_IDS is skipped.

    This is the guard against re-treating mn_msrs / pera_colorado / calpers /
    dcrb documents as fund performance reports -- see the module docstring.
    """
    session = get_session()
    _seed_plan(session, "mn_msrs")
    _seed_doc(session, "mn_msrs", "https://x/MNDCP-IOAG-Mar-2026.pdf",
             "Core Investment Option Performance\n" + "filler " * 100)
    session.commit()
    session.close()

    counts = epr.run_extraction()
    assert counts == {}

    session = get_session()
    assert session.query(PerformanceReportExtract).count() == 0
    session.close()


def test_too_short_text_is_skipped(tmp_db):
    session = get_session()
    _seed_plan(session, "nycrs_comptroller")
    _seed_doc(session, "nycrs_comptroller",
             "https://x/Monthly-Performance-Review-Material_01-2026-TRS.pdf",
             "too short")
    session.commit()
    session.close()

    counts = epr.run_extraction()
    assert counts["too_short"] == 1
    assert counts.get("saved", 0) == 0
