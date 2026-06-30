"""Bucketing, outcome, and period-filter logic for compose_rfp_weekly.

These tests exercise pure-Python helpers and the SQL period filter
without invoking the LLM. The e2e mock test in
``tests/test_rfp_weekly_e2e_mock.py`` covers the full cycle end-to-end.
"""

from __future__ import annotations

import json
from datetime import date, datetime

import pytest

from database import Document, Plan, RFPRecord, get_session
from insights.compose import (
    _RFP_STAGE_BUCKETS,
    _gather_consultant_rfps,
    _rfp_outcome,
)


# ---------------------------------------------------------------------------
# Bucket mapping
# ---------------------------------------------------------------------------

def test_stage_buckets_cover_every_schema_status():
    """Every status enum value in lib/rfp_schema.json must map somewhere."""
    schema_statuses = {
        "Planned", "Issued", "ResponsesReceived",
        "FinalistsNamed", "Awarded", "Withdrawn",
    }
    bucketed = {s for _, statuses in _RFP_STAGE_BUCKETS for s in statuses}
    assert bucketed == schema_statuses


def test_stage_bucket_order_is_forward_lifecycle():
    labels = [label for label, _ in _RFP_STAGE_BUCKETS]
    assert labels == ["Initial plans", "Launch", "Review", "Decisions"]


def test_review_bucket_contains_both_response_stages():
    review = dict(_RFP_STAGE_BUCKETS)["Review"]
    assert set(review) == {"ResponsesReceived", "FinalistsNamed"}


def test_decisions_bucket_includes_withdrawn():
    decisions = dict(_RFP_STAGE_BUCKETS)["Decisions"]
    assert set(decisions) == {"Awarded", "Withdrawn"}


# ---------------------------------------------------------------------------
# Outcome column logic
# ---------------------------------------------------------------------------

def test_outcome_retained_when_awarded_matches_incumbent():
    payload = {"awarded_manager": "StepStone Group LP",
               "incumbent_manager": "StepStone Group LP"}
    assert _rfp_outcome(payload) == "Retained"


def test_outcome_retained_is_case_insensitive():
    payload = {"awarded_manager": "stepstone group lp",
               "incumbent_manager": "StepStone Group LP"}
    assert _rfp_outcome(payload) == "Retained"


def test_outcome_switched_when_different():
    payload = {"awarded_manager": "Meketa",
               "incumbent_manager": "Wilshire"}
    assert _rfp_outcome(payload) == "Switched"


def test_outcome_dash_when_either_missing():
    assert _rfp_outcome({"awarded_manager": "Meketa", "incumbent_manager": ""}) == "—"
    assert _rfp_outcome({"awarded_manager": "", "incumbent_manager": "Wilshire"}) == "—"
    assert _rfp_outcome({}) == "—"


def test_outcome_dash_when_none_values():
    payload = {"awarded_manager": None, "incumbent_manager": None}
    assert _rfp_outcome(payload) == "—"


# ---------------------------------------------------------------------------
# DB query — period filter + consultant-only filter + bucket assignment
# ---------------------------------------------------------------------------

def _seed(session, plan_id="testplan", abbrev="TEST"):
    session.add(Plan(id=plan_id, name=plan_id.upper(), abbreviation=abbrev))
    session.add(Document(id=1, plan_id=plan_id, url="https://example.com/d1.pdf",
                          filename="d1.pdf"))
    session.add(Document(id=2, plan_id=plan_id, url="https://example.com/d2.pdf",
                          filename="d2.pdf"))
    session.commit()


def _add_rfp(session, *, rfp_id, doc_id, rfp_type, status,
             extracted_at, plan_id="testplan",
             awarded=None, incumbent=None, title="A consultant search"):
    payload = {
        "rfp_id": rfp_id,
        "plan_id": plan_id,
        "rfp_type": rfp_type,
        "title": title,
        "status": status,
        "awarded_manager": awarded,
        "incumbent_manager": incumbent,
        "shortlisted_managers": [],
        "source_document": {"url": "x", "page_number": 1, "document_id": doc_id},
        "source_quote": "verbatim source quote here",
        "extraction_confidence": 0.9,
    }
    rec = RFPRecord(
        rfp_id=rfp_id,
        document_id=doc_id,
        plan_id=plan_id,
        record=json.dumps(payload),
        extraction_confidence=0.9,
        needs_review=False,
    )
    rec.extracted_at = extracted_at
    session.add(rec)
    session.commit()


def test_query_filters_by_period_window():
    s = get_session()
    try:
        _seed(s)
        # In-window: Sun Apr 19 – Sat Apr 25 2026
        _add_rfp(s, rfp_id="a" * 16, doc_id=1, rfp_type="Consultant",
                 status="Issued", extracted_at=datetime(2026, 4, 22, 12, 0))
        # Out-of-window (week before)
        _add_rfp(s, rfp_id="b" * 16, doc_id=2, rfp_type="Consultant",
                 status="Issued", extracted_at=datetime(2026, 4, 12, 12, 0))

        records = _gather_consultant_rfps(s, date(2026, 4, 19), date(2026, 4, 25))
    finally:
        s.close()
    assert len(records) == 1
    assert records[0]["title"] == "A consultant search"


def test_query_filters_out_non_consultant_types():
    s = get_session()
    try:
        _seed(s)
        _add_rfp(s, rfp_id="a" * 16, doc_id=1, rfp_type="Consultant",
                 status="Planned", extracted_at=datetime(2026, 4, 22))
        _add_rfp(s, rfp_id="b" * 16, doc_id=2, rfp_type="Manager",
                 status="Awarded", extracted_at=datetime(2026, 4, 22))

        records = _gather_consultant_rfps(s, date(2026, 4, 19), date(2026, 4, 25))
    finally:
        s.close()
    assert len(records) == 1
    assert records[0]["status"] == "Planned"


def test_query_assigns_bucket_per_status():
    s = get_session()
    try:
        _seed(s)
        cases = [
            ("a" * 16, "Planned",           "Initial plans"),
            ("b" * 16, "Issued",            "Launch"),
            ("c" * 16, "ResponsesReceived", "Review"),
            ("d" * 16, "FinalistsNamed",    "Review"),
            ("e" * 16, "Awarded",           "Decisions"),
            ("f" * 16, "Withdrawn",         "Decisions"),
        ]
        for rfp_id, status, _expected in cases:
            _add_rfp(s, rfp_id=rfp_id, doc_id=1, rfp_type="Consultant",
                     status=status, extracted_at=datetime(2026, 4, 22))

        records = _gather_consultant_rfps(s, date(2026, 4, 19), date(2026, 4, 25))
    finally:
        s.close()

    status_to_bucket = {r["status"]: r["bucket"] for r in records}
    for _rfp_id, status, expected in cases:
        assert status_to_bucket[status] == expected


def test_query_returns_empty_list_when_no_records():
    s = get_session()
    try:
        _seed(s)
        records = _gather_consultant_rfps(s, date(2026, 4, 19), date(2026, 4, 25))
    finally:
        s.close()
    assert records == []


def test_query_period_boundary_is_inclusive():
    """A record extracted at 23:59:59 on period_end (Saturday) must be included."""
    s = get_session()
    try:
        _seed(s)
        # Saturday, last second of the window
        _add_rfp(s, rfp_id="a" * 16, doc_id=1, rfp_type="Consultant",
                 status="Awarded",
                 extracted_at=datetime(2026, 4, 25, 23, 59, 59))
        # Sunday next, just outside
        _add_rfp(s, rfp_id="b" * 16, doc_id=2, rfp_type="Consultant",
                 status="Awarded",
                 extracted_at=datetime(2026, 4, 26, 0, 0, 0))

        records = _gather_consultant_rfps(s, date(2026, 4, 19), date(2026, 4, 25))
    finally:
        s.close()
    assert len(records) == 1
