"""queries.quarterly_performance_rows: latest-per-fund selection, scope filter.

One row per (plan, fund_scope) rather than per plan -- a plan whose
comptroller publishes separate reports per constituent system (NYC's
NYCERS/TRS/POLICE/FIRE/BERS) has no single "total fund" return to show.
"""

from __future__ import annotations

from datetime import datetime, timezone

import database
import queries


def _seed_extract(session, plan_id, document_id_seed, *, fund_scope, as_of_date,
                  returns, downloaded_at=None):
    if session.get(database.Plan, plan_id) is None:
        session.add(database.Plan(id=plan_id, name=plan_id.upper(),
                                  abbreviation=plan_id.upper(), state="NY",
                                  aum_billions=1.0))
        session.flush()
    doc = database.Document(
        plan_id=plan_id,
        url=f"https://example.invalid/{plan_id}-{document_id_seed}.pdf",
        filename=f"{document_id_seed}.pdf", doc_type="performance",
        extraction_status="done",
        downloaded_at=downloaded_at or datetime(2026, 4, 5, tzinfo=timezone.utc),
    )
    session.add(doc)
    session.flush()
    extract = database.PerformanceReportExtract(
        plan_id=plan_id, document_id=doc.id, fund_scope=fund_scope,
        as_of_date=as_of_date)
    session.add(extract)
    session.flush()
    for scope, period, pct in returns:
        session.add(database.PerformanceReportReturn(
            extract_id=extract.id, scope=scope, period=period, return_pct=pct))
    return doc, extract


def test_picks_latest_as_of_date_per_fund(tmp_db):
    session = database.get_session()
    try:
        _seed_extract(session, "nycrs_comptroller", "nycers-jan", fund_scope="NYCERS",
                     as_of_date="2026-01-31",
                     returns=[("total_fund", "3mo", 3.2), ("total_fund", "1mo", 1.1)])
        _seed_extract(session, "nycrs_comptroller", "nycers-feb", fund_scope="NYCERS",
                     as_of_date="2026-02-28",
                     returns=[("total_fund", "3mo", 4.0), ("total_fund", "1mo", 0.9)])
        _seed_extract(session, "nycrs_comptroller", "trs-jan", fund_scope="TRS",
                     as_of_date="2026-01-31",
                     returns=[("total_fund", "3mo", 3.0)])
        session.commit()

        rows = queries.quarterly_performance_rows(session)
    finally:
        session.close()

    by_fund = {r["Fund"]: r for r in rows}
    assert set(by_fund) == {"NYCERS", "TRS"}
    assert by_fund["NYCERS"]["As of"] == "2026-02-28"
    assert by_fund["NYCERS"]["3 months"] == 4.0
    assert by_fund["TRS"]["3 months"] == 3.0


def test_non_total_fund_scope_is_excluded(tmp_db):
    session = database.get_session()
    try:
        _seed_extract(session, "nycrs_comptroller", "nycers-jan", fund_scope="NYCERS",
                     as_of_date="2026-01-31",
                     returns=[("Domestic Equity", "3mo", 5.5)])
        session.commit()

        rows = queries.quarterly_performance_rows(session)
    finally:
        session.close()

    # No total_fund return -> the fund has nothing to show, so it's dropped
    # rather than surfaced as a row full of blanks.
    assert rows == []


def test_single_fund_plan_has_no_fund_label_collision(tmp_db):
    session = database.get_session()
    try:
        _seed_extract(session, "solo_plan", "solo-q1", fund_scope=None,
                     as_of_date="2026-03-31",
                     returns=[("total_fund", "3mo", 2.1)])
        session.commit()

        rows = queries.quarterly_performance_rows(session)
    finally:
        session.close()

    assert len(rows) == 1
    assert rows[0]["Fund"] == "—"
    assert rows[0]["3 months"] == 2.1
