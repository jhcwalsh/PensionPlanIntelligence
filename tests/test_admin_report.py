"""Tests for admin_report.build_report — the data layer behind the Admin
'Report Card' sub-tab.

Each test seeds a fresh DB (via the autouse `_isolated_environment`
conftest fixture) and asserts on the structured payload. We don't import
app.py / Streamlit — the renderer is a thin wrapper over this payload.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from admin_report import (
    DAILY_PIPELINE_STALE_HOURS,
    PLAN_STALE_DAYS,
    build_report,
    weekly_period,
)
from database import (
    Document,
    FetchRun,
    IpsDocument,
    Plan,
    Publication,
    Summary,
    WeeklyRun,
    get_session,
)


# Pick a frozen "now" that's a Wednesday so the latest reportable week is the
# preceding Sun→Sat. This avoids edge cases where today itself is Sunday.
NOW = datetime(2026, 5, 6, 12, 0, 0)             # Wed 6 May 2026
LATEST_WEEK_START = date(2026, 4, 26)            # Sunday
LATEST_WEEK_END = date(2026, 5, 2)               # Saturday


# ---------------------------------------------------------------------------
# Helpers — keep test setup readable
# ---------------------------------------------------------------------------

def _add_plan(s, plan_id: str, name: str = None) -> Plan:
    p = Plan(id=plan_id, name=name or plan_id.upper())
    s.add(p)
    s.flush()
    return p


def _add_doc(s, plan_id: str, downloaded_at: datetime, *,
             doc_type: str = "agenda", url: str = None,
             extraction_status: str = "done", with_summary: bool = False,
             summary_at: datetime = None) -> Document:
    d = Document(
        plan_id=plan_id,
        url=url or f"https://example/{plan_id}/{downloaded_at.isoformat()}",
        filename=f"{plan_id}-{downloaded_at:%Y%m%d}.pdf",
        doc_type=doc_type,
        downloaded_at=downloaded_at,
        extraction_status=extraction_status,
    )
    s.add(d)
    s.flush()
    if with_summary:
        s.add(Summary(
            document_id=d.id,
            summary_text="x",
            generated_at=summary_at or downloaded_at + timedelta(hours=1),
        ))
        s.flush()
    return d


def _add_fetch_run(s, source: str, started_at: datetime,
                   status: str = "success", error: str = None) -> FetchRun:
    r = FetchRun(
        source=source,
        started_at=started_at,
        completed_at=started_at + timedelta(minutes=5),
        status=status,
        error_message=error,
    )
    s.add(r)
    s.flush()
    return r


# ---------------------------------------------------------------------------
# Period helper
# ---------------------------------------------------------------------------

def test_weekly_period_returns_prior_sun_sat_for_midweek():
    assert weekly_period(date(2026, 5, 6)) == (LATEST_WEEK_START, LATEST_WEEK_END)


def test_weekly_period_on_sunday_returns_prior_week():
    # Sunday 3 May → prior fully-completed week is Sun 26 Apr → Sat 2 May.
    assert weekly_period(date(2026, 5, 3)) == (LATEST_WEEK_START, LATEST_WEEK_END)


# ---------------------------------------------------------------------------
# Cumulative + weekly counts
# ---------------------------------------------------------------------------

def test_empty_db_yields_zero_metrics_and_no_unique_plan_counts():
    report = build_report(weeks_back=4, now=NOW)
    assert report["cumulative"] == {
        "total_plans": 0,
        "plans_with_document": 0,
        "plans_with_summary": 0,
        "plans_with_cafr": 0,
        "plans_with_ips": 0,
    }
    assert len(report["weeks"]) == 4
    assert all(w["unique_plans"] == 0 for w in report["weeks"])
    assert all(w["new_documents"] == 0 for w in report["weeks"])
    assert report["weeks"][0]["week_start"] == LATEST_WEEK_START.isoformat()


def test_unique_plans_per_week_dedupes_within_week():
    s = get_session()
    try:
        _add_plan(s, "alpha")
        _add_plan(s, "beta")
        # Three docs from alpha + one from beta in latest week → 2 unique plans
        for offset in (0, 2, 5):
            _add_doc(s, "alpha",
                     datetime.combine(LATEST_WEEK_START, datetime.min.time())
                     + timedelta(days=offset, hours=10),
                     with_summary=True)
        _add_doc(s, "beta",
                 datetime.combine(LATEST_WEEK_END, datetime.min.time())
                 + timedelta(hours=9),
                 with_summary=True)
        s.commit()
    finally:
        s.close()

    report = build_report(weeks_back=4, now=NOW)
    latest = report["weeks"][0]
    assert latest["unique_plans"] == 2
    assert latest["new_documents"] == 4
    assert latest["new_summaries"] == 4
    assert latest["cumulative_unique_plans"] == 2
    assert report["cumulative"]["plans_with_document"] == 2
    assert report["cumulative"]["plans_with_summary"] == 2


def test_cumulative_unique_plans_grows_monotonically():
    s = get_session()
    try:
        _add_plan(s, "alpha")
        _add_plan(s, "beta")
        _add_plan(s, "gamma")
        # alpha: 3 weeks back. beta: 2 weeks back. gamma: latest week.
        weeks_back_to_when = {
            3: datetime(2026, 4, 8, 10),     # in week starting Sun 5 Apr
            2: datetime(2026, 4, 15, 10),    # in week starting Sun 12 Apr
            0: datetime(2026, 4, 28, 10),    # in latest week
        }
        _add_doc(s, "alpha", weeks_back_to_when[3])
        _add_doc(s, "beta", weeks_back_to_when[2])
        _add_doc(s, "gamma", weeks_back_to_when[0])
        s.commit()
    finally:
        s.close()

    report = build_report(weeks_back=5, now=NOW)
    # weeks list is latest-first; index 0 = latest week (3 cumulative),
    # index 1 = 1 week back (2 cumulative — gamma not yet seen),
    # index 2 = 2 weeks back (2 cumulative), index 3 = 3 weeks back (1 cumulative)
    cum = [w["cumulative_unique_plans"] for w in report["weeks"]]
    assert cum[0] == 3
    assert cum[1] == 2
    assert cum[2] == 2
    assert cum[3] == 1
    # Monotonic going from oldest to newest
    assert cum == sorted(cum, reverse=True)


def test_cafr_and_ips_counts_segregate_doc_types():
    s = get_session()
    try:
        _add_plan(s, "alpha")
        _add_plan(s, "beta")
        _add_doc(s, "alpha", NOW - timedelta(days=3), doc_type="cafr")
        _add_doc(s, "beta", NOW - timedelta(days=3), doc_type="agenda")
        s.add(IpsDocument(
            plan_id="alpha",
            content_hash="a" * 64,
            url="https://example/ips.pdf",
            fetched_at=NOW - timedelta(days=10),
        ))
        s.commit()
    finally:
        s.close()

    report = build_report(weeks_back=2, now=NOW)
    cum = report["cumulative"]
    assert cum["plans_with_document"] == 2
    assert cum["plans_with_cafr"] == 1
    assert cum["plans_with_ips"] == 1


# ---------------------------------------------------------------------------
# Fetch-run bucketing
# ---------------------------------------------------------------------------

def test_fetch_runs_bucket_into_correct_week_and_source():
    s = get_session()
    try:
        # 2 GHA successes + 1 GHA failure in latest week; 1 local success.
        ws = datetime.combine(LATEST_WEEK_START, datetime.min.time())
        _add_fetch_run(s, "gha", ws + timedelta(days=1, hours=11))
        _add_fetch_run(s, "gha", ws + timedelta(days=2, hours=11))
        _add_fetch_run(s, "gha", ws + timedelta(days=3, hours=11),
                       status="failed", error="boom")
        _add_fetch_run(s, "local", ws + timedelta(days=4, hours=4))
        s.commit()
    finally:
        s.close()

    report = build_report(weeks_back=3, now=NOW)
    latest = report["weeks"][0]
    assert latest["gha_success"] == 2
    assert latest["gha_failed"] == 1
    assert latest["local_success"] == 1
    assert latest["local_failed"] == 0


# ---------------------------------------------------------------------------
# Issue detection
# ---------------------------------------------------------------------------

def _issue_categories(report, severity=None):
    return [
        i["category"] for i in report["issues"]
        if severity is None or i["severity"] == severity
    ]


def test_issue_no_gha_run_ever_is_an_error():
    report = build_report(weeks_back=2, now=NOW)
    cats = _issue_categories(report, severity="error")
    assert "fetch_pipeline" in cats
    # Hint should reference the workflow file so an AI knows where to look.
    msg = next(i for i in report["issues"]
               if i["severity"] == "error" and i["category"] == "fetch_pipeline")
    assert "daily-pipeline.yml" in msg["fix_hint"]


def test_issue_recent_gha_success_clears_freshness_error():
    s = get_session()
    try:
        _add_plan(s, "alpha")
        _add_fetch_run(s, "gha", NOW - timedelta(hours=3))
        _add_fetch_run(s, "local", NOW - timedelta(hours=4))
        s.commit()
    finally:
        s.close()

    report = build_report(weeks_back=2, now=NOW)
    fetch_issues = [i for i in report["issues"]
                    if i["category"] == "fetch_pipeline"]
    assert fetch_issues == []


def test_issue_stale_gha_run_flags_error_with_age():
    s = get_session()
    try:
        _add_fetch_run(s, "gha",
                       NOW - timedelta(hours=DAILY_PIPELINE_STALE_HOURS + 12))
        _add_fetch_run(s, "local", NOW - timedelta(hours=2))
        s.commit()
    finally:
        s.close()

    report = build_report(weeks_back=2, now=NOW)
    errors = [i for i in report["issues"] if i["severity"] == "error"
              and i["category"] == "fetch_pipeline"]
    assert any("ago" in i["message"] for i in errors)


def test_issue_failed_publication_carries_error_in_fix_hint():
    s = get_session()
    try:
        _add_fetch_run(s, "gha", NOW - timedelta(hours=2))
        _add_fetch_run(s, "local", NOW - timedelta(hours=2))
        s.add(Publication(
            cadence="weekly",
            period_start=LATEST_WEEK_START,
            period_end=LATEST_WEEK_END,
            status="failed",
            error_message="anthropic 429 rate limit",
            composed_at=NOW - timedelta(hours=10),
        ))
        s.commit()
    finally:
        s.close()

    report = build_report(weeks_back=2, now=NOW)
    pub_issues = [i for i in report["issues"]
                  if i["category"] == "weekly_insights"]
    assert any(i["severity"] == "error"
               and "anthropic 429 rate limit" in i["fix_hint"]
               for i in pub_issues)


def test_issue_missing_publication_grace_period():
    """If today is the Sunday right after the period ends, don't flag yet."""
    s = get_session()
    try:
        _add_fetch_run(s, "gha", NOW - timedelta(hours=2))
        _add_fetch_run(s, "local", NOW - timedelta(hours=2))
        s.commit()
    finally:
        s.close()

    just_after_window = datetime(2026, 5, 3, 10, 0)   # Sunday, < 2 days after Sat 2 May
    report = build_report(weeks_back=2, now=just_after_window)
    cats = [i["category"] for i in report["issues"]]
    assert "weekly_insights" not in cats


def test_stale_plans_listed_with_last_seen_date():
    s = get_session()
    try:
        _add_plan(s, "alpha", "Alpha Plan")
        _add_plan(s, "beta", "Beta Plan")
        _add_fetch_run(s, "gha", NOW - timedelta(hours=2))
        _add_fetch_run(s, "local", NOW - timedelta(hours=2))
        # alpha last seen 60 days ago → stale; beta seen yesterday → fresh.
        _add_doc(s, "alpha", NOW - timedelta(days=PLAN_STALE_DAYS + 30))
        _add_doc(s, "beta", NOW - timedelta(days=1))
        s.commit()
    finally:
        s.close()

    report = build_report(weeks_back=2, now=NOW)
    stale = [i for i in report["issues"] if i["category"] == "stale_plans"]
    assert len(stale) == 1
    details = stale[0]["details"]
    assert any("Alpha Plan" in d for d in details)
    assert not any("Beta Plan" in d for d in details)


def test_never_fetched_plans_listed_as_info():
    s = get_session()
    try:
        _add_plan(s, "alpha", "Alpha Plan")
        _add_plan(s, "beta", "Beta Plan")
        _add_fetch_run(s, "gha", NOW - timedelta(hours=2))
        _add_fetch_run(s, "local", NOW - timedelta(hours=2))
        _add_doc(s, "alpha", NOW - timedelta(days=1))
        # beta has no documents
        s.commit()
    finally:
        s.close()

    report = build_report(weeks_back=2, now=NOW)
    info = [i for i in report["issues"]
            if i["category"] == "no_documents" and i["severity"] == "info"]
    assert len(info) == 1
    assert info[0]["details"] == ["Beta Plan"]


def test_payload_is_json_serializable():
    """Sanity: the dict must round-trip through JSON. Tests the renderer's
    `st.code(json.dumps(report, ...))` path."""
    import json
    report = build_report(weeks_back=2, now=NOW)
    s = json.dumps(report, default=str)
    assert "cumulative" in s
    assert "weeks" in s
    assert "issues" in s
