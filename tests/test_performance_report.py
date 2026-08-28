"""Headline returns by asset class, one row per plan.

Sourced from ``cafr_performance``, which already held 2,690 rows covering
exactly the classes asked for. The important thing this suite pins down is
what the numbers *are*: CAFRs are annual, so these are fiscal-year returns,
not the calendar quarters the request named. The 48 ``doc_type='performance'``
documents hold true quarterly reports and have no structured extraction yet.

Two normalisation hazards are covered because both silently corrupt a
comparison rather than raising:

* A plan reporting both "Private Equity" and "Private Equity Composite"
  would overwrite one with the other in whatever order rows come back.
* Substituting a 3-year annualised return when no fiscal-year figure exists
  would put a number in a column that claims to be the latest period.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

import database
import queries

MAP = {
    "Private Equity": {"canonical": "private_equity"},
    "Private Equity Composite": {"canonical": "private_equity"},
    "Private Credit": {"canonical": "private_credit"},
    "Real Assets": {"canonical": "real_assets_infrastructure"},
    "Real Estate": {"canonical": "real_estate"},
    "Mystery Bucket": {"canonical": "unmapped"},
}


def _plan_with_cafr(session, plan_id, fy, perf, abbreviation=None):
    # Only once per plan: the multi-CAFR test calls this twice for one plan,
    # and re-adding the Plan row would collide on the primary key.
    if session.get(database.Plan, plan_id) is None:
        session.add(database.Plan(id=plan_id, name=plan_id.upper(),
                                  abbreviation=abbreviation or plan_id.upper(),
                                  state="CA", aum_billions=1.0))
        session.flush()
    doc = database.Document(
        plan_id=plan_id, url="https://example.invalid/%s-%s.pdf" % (plan_id, fy),
        filename="%s-%s.pdf" % (plan_id, fy), doc_type="cafr", fiscal_year=fy,
        downloaded_at=datetime(2026, 4, 26, tzinfo=timezone.utc),
        extraction_status="done")
    session.add(doc)
    session.flush()
    extract = database.CafrExtract(document_id=doc.id, plan_id=plan_id,
                                   fiscal_year=fy)
    session.add(extract)
    session.flush()
    for scope, period, pct in perf:
        session.add(database.CafrPerformance(
            cafr_extract_id=extract.id, scope=scope, period=period,
            return_pct=pct))
    return doc, extract


@pytest.fixture()
def seeded(tmp_db):
    session = database.get_session()
    try:
        _plan_with_cafr(session, "alpha", 2025, [
            ("total_fund", "fy", 10.6),
            ("Private Equity", "fy", 5.5),
            ("Private Credit", "fy", 7.1),
            ("Real Estate", "fy", 1.6),
            ("Mystery Bucket", "fy", 99.9),      # unmapped — must not appear
            ("total_fund", "3y", 8.0),           # wrong period — must not leak
        ])
        _plan_with_cafr(session, "beta", 2024, [
            ("total_fund", "1y", 11.4),          # no fy figure; 1y is the fallback
            ("Real Assets", "1y", 4.2),
        ])
        session.commit()
    finally:
        session.close()


def test_one_row_per_plan_with_the_named_classes(seeded):
    session = database.get_session()
    try:
        rows = {r["Plan"]: r for r in
                queries.performance_report_rows(session, MAP)}
        assert set(rows) == {"ALPHA", "BETA"}

        a = rows["ALPHA"]
        assert a["Total plan"] == 10.6
        assert a["Private equity"] == 5.5
        assert a["Private credit"] == 7.1
        assert a["Real estate"] == 1.6
        assert a["Real assets"] is None          # not reported, not invented
    finally:
        session.close()


def test_unmapped_scopes_are_dropped_not_guessed(seeded):
    session = database.get_session()
    try:
        row = next(r for r in queries.performance_report_rows(session, MAP)
                   if r["Plan"] == "ALPHA")
        assert 99.9 not in row.values(), (
            "an 'unmapped' scope leaked into a named asset-class column")
    finally:
        session.close()


def test_a_longer_window_is_never_substituted(seeded):
    """3y exists for ALPHA. It must not fill a column claiming the latest period."""
    session = database.get_session()
    try:
        row = next(r for r in queries.performance_report_rows(session, MAP)
                   if r["Plan"] == "ALPHA")
        assert row["Period"] == "fy"
        assert row["Total plan"] == 10.6, "picked up the 3-year annualised return"
    finally:
        session.close()


def test_one_year_is_the_fallback_when_no_fiscal_year_figure(seeded):
    session = database.get_session()
    try:
        row = next(r for r in queries.performance_report_rows(session, MAP)
                   if r["Plan"] == "BETA")
        assert row["Period"] == "1y"
        assert row["Total plan"] == 11.4
    finally:
        session.close()


def test_duplicate_synonyms_do_not_overwrite_each_other(tmp_db):
    """"Private Equity" and "Private Equity Composite" map to one column."""
    session = database.get_session()
    try:
        _plan_with_cafr(session, "gamma", 2025, [
            ("total_fund", "fy", 9.0),
            ("Private Equity", "fy", 6.0),
            ("Private Equity Composite", "fy", 77.7),
        ])
        session.commit()
        row = queries.performance_report_rows(session, MAP)[0]
        assert row["Private equity"] == 6.0, (
            "a synonym overwrote the primary scope — order-dependent result")
    finally:
        session.close()


def test_the_latest_fiscal_year_wins(tmp_db):
    """A plan with several CAFRs contributes only its newest."""
    session = database.get_session()
    try:
        _plan_with_cafr(session, "delta", 2023, [("total_fund", "fy", 1.0)])
        _plan_with_cafr(session, "delta", 2025, [("total_fund", "fy", 2.0)])
        session.commit()
        rows = queries.performance_report_rows(session, MAP)
        assert len(rows) == 1
        assert rows[0]["Fiscal year"] == 2025
        assert rows[0]["Total plan"] == 2.0
    finally:
        session.close()


def test_plain_string_mappings_work_too(seeded):
    """twin_builder.load_asset_class_mappings normalises to strings."""
    flat = {k: v["canonical"] for k, v in MAP.items()}
    session = database.get_session()
    try:
        row = next(r for r in queries.performance_report_rows(session, flat)
                   if r["Plan"] == "ALPHA")
        assert row["Private equity"] == 5.5
    finally:
        session.close()


def test_a_plan_with_no_performance_rows_is_omitted(tmp_db):
    session = database.get_session()
    try:
        _plan_with_cafr(session, "epsilon", 2025, [])
        session.commit()
        assert queries.performance_report_rows(session, MAP) == []
    finally:
        session.close()


def test_the_source_link_and_date_come_from_the_document(seeded):
    session = database.get_session()
    try:
        row = next(r for r in queries.performance_report_rows(session, MAP)
                   if r["Plan"] == "ALPHA")
        assert row["Source"].startswith("https://")
        assert row["Source date"] is not None
    finally:
        session.close()


# ---------------------------------------------------------------------------
# CAFR coverage by reporting year (D3)
#
# The headline CAFR metrics say how many plans have a CAFR. This says how
# *current* those CAFRs are — a corpus whose newest CAFRs are mostly FY2023
# is a different dataset from one that is mostly FY2025, and the plan counts
# cannot tell the two apart.
# ---------------------------------------------------------------------------

def test_counts_group_plans_by_their_latest_fiscal_year(tmp_db):
    session = database.get_session()
    try:
        _plan_with_cafr(session, "aa", 2025, [])
        _plan_with_cafr(session, "bb", 2025, [])
        _plan_with_cafr(session, "cc", 2024, [])
        session.commit()

        rows = {r["Fiscal year"]: r["Plans"]
                for r in queries.cafr_fiscal_year_counts(session)}
        assert rows == {2025: 2, 2024: 1}
    finally:
        session.close()


def test_a_plan_is_counted_once_under_its_newest_year(tmp_db):
    """Two CAFRs for one plan must not double-count it."""
    session = database.get_session()
    try:
        _plan_with_cafr(session, "aa", 2023, [])
        _plan_with_cafr(session, "aa", 2025, [])
        session.commit()

        rows = {r["Fiscal year"]: r["Plans"]
                for r in queries.cafr_fiscal_year_counts(session)}
        assert rows == {2025: 1}, "plan counted under a superseded year too"
    finally:
        session.close()


def test_newest_year_leads(tmp_db):
    session = database.get_session()
    try:
        _plan_with_cafr(session, "aa", 2021, [])
        _plan_with_cafr(session, "bb", 2025, [])
        session.commit()
        years = [r["Fiscal year"] for r in queries.cafr_fiscal_year_counts(session)]
        assert years == sorted(years, reverse=True)
    finally:
        session.close()


def test_a_recent_arrival_shows_as_a_change(tmp_db):
    """The delta is derived from downloaded_at, not a stored snapshot."""
    from datetime import timedelta

    session = database.get_session()
    try:
        _plan_with_cafr(session, "aa", 2025, [])
        session.commit()
        # Seeded downloads are dated 2026-04-26, comfortably older than the
        # window, so a 30-day view sees no movement...
        assert all(r["Change (30d)"] == 0
                   for r in queries.cafr_fiscal_year_counts(session))
        # ...but a window wide enough to predate them attributes the arrival.
        wide = queries.cafr_fiscal_year_counts(session, prior_days=100000)
        assert wide[0]["Change (30d)"] == 1
    finally:
        session.close()
