"""Coverage over time and gap detection (coverage_health.py)."""
from datetime import date, datetime, timezone

import database
from database import CafrExtract, Document, Plan, Summary
import coverage_health as ch

TODAY = date(2026, 10, 24)  # a Saturday; its week starts Monday 19 Oct


def _dt(s):
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)


_ids = iter(range(1, 100000))


def _doc(s, plan, downloaded, meeting=None, doc_type="minutes", summarised=True):
    i = next(_ids)
    s.add(Document(id=i, plan_id=plan, url=f"https://x.test/{i}", doc_type=doc_type,
                   downloaded_at=_dt(downloaded), meeting_date=_dt(meeting) if meeting else None))
    if summarised:
        s.add(Summary(document_id=i, generated_at=_dt(downloaded)))


def _plans(s, *specs):
    for pid, aum in specs:
        s.add(Plan(id=pid, name=pid.upper(), aum_billions=aum))
    s.commit()


def test_weekly_intake_counts_documents_and_plans_by_monday_week():
    s = database.get_session()
    _plans(s, ("a", 10.0), ("b", 5.0))
    _doc(s, "a", "2026-10-19T10:00:00")
    _doc(s, "b", "2026-10-21T10:00:00")
    _doc(s, "a", "2026-10-13T10:00:00", doc_type="cafr")  # annual reports are not board material
    s.commit()
    weeks = {w["week"]: w for w in ch.weekly_intake(s, weeks=2, today=TODAY)}
    assert weeks[date(2026, 10, 19)]["downloads"] == 2
    assert weeks[date(2026, 10, 19)]["plans"] == 2
    assert weeks[date(2026, 10, 19)]["partial"] is True
    assert weeks[date(2026, 10, 12)]["downloads"] == 0
    assert weeks[date(2026, 10, 12)]["partial"] is False


def test_pipeline_gaps_merge_low_complete_weeks_and_skip_the_current_one():
    def wk(d, n, partial=False):
        return {"week": d, "downloads": n, "plans": 0, "summaries": 0, "partial": partial}
    weekly = ([wk(date(2026, 8, 3 + 7 * i), 100) for i in range(4)]
              + [wk(date(2026, 8, 31), 2), wk(date(2026, 9, 7), 0)]
              + [wk(date(2026, 9, 14), 120), wk(date(2026, 9, 21), 1, partial=True)])
    gaps = ch.pipeline_gaps(weekly)
    assert gaps == [{"start": date(2026, 8, 31), "end": date(2026, 9, 13), "weeks": 2,
                     "downloads": 2, "typical": 100}]


def test_plan_status_classifies_structural_overdue_and_ok(monkeypatch):
    monkeypatch.setattr(ch, "_blocked_reasons",
                        lambda: {"blocked": "listing page 403s from every IP"})
    s = database.get_session()
    _plans(s, ("monthly_ok", 50.0), ("monthly_late", 40.0), ("blocked", 30.0), ("silent", 20.0))
    for m in range(1, 11):  # Jan-Oct meetings
        _doc(s, "monthly_ok", f"2026-{m:02d}-20T00:00:00", meeting=f"2026-{m:02d}-15T00:00:00")
    # Nine meeting months, Nov 2025 - Jul 2026, then nothing for three months.
    for y, m in [(2025, 11), (2025, 12)] + [(2026, m) for m in range(1, 8)]:
        _doc(s, "monthly_late", f"{y}-{m:02d}-20T00:00:00", meeting=f"{y}-{m:02d}-15T00:00:00")
    _doc(s, "silent", "2025-03-01T00:00:00", meeting="2025-02-20T00:00:00")
    s.commit()
    rows = {r["plan_id"]: r for r in ch.plan_status(s, today=TODAY)}
    assert rows["monthly_ok"]["status"] == "ok" and rows["monthly_ok"]["cadence"] == "monthly"
    assert rows["monthly_late"]["status"] == "overdue"
    assert rows["monthly_late"]["months_since"] == 3
    assert rows["blocked"]["status"] == "structural"
    assert rows["blocked"]["reason"] == "listing page 403s from every IP"
    assert rows["silent"]["status"] == "structural"
    assert rows["silent"]["last_month"] == "2025-02"
    assert rows["silent"]["reason"].startswith("Nothing since 2025-02")


def test_plan_status_carries_the_other_datasets(monkeypatch):
    monkeypatch.setattr(ch, "_blocked_reasons", lambda: {})
    s = database.get_session()
    _plans(s, ("a", 10.0))
    _doc(s, "a", "2026-10-01T00:00:00", meeting="2026-09-20T00:00:00")
    _doc(s, "a", "2026-10-02T00:00:00", meeting="2026-09-21T00:00:00", summarised=False)
    s.add(CafrExtract(plan_id="a", fiscal_year=2024))
    s.add(CafrExtract(plan_id="a", fiscal_year=2025))
    s.commit()
    r = ch.plan_status(s, today=TODAY)[0]
    assert r["docs_12m"] == 2 and r["unsummarised"] == 1
    assert r["latest_cafr_fy"] == 2025
    assert r["recordings_12m"] == 0 and r["has_ips"] is False


def test_plan_months_uses_meeting_month_else_download_month():
    s = database.get_session()
    _plans(s, ("a", 10.0))
    _doc(s, "a", "2026-10-02T00:00:00", meeting="2026-08-20T00:00:00")
    _doc(s, "a", "2026-10-03T00:00:00")
    s.commit()
    grid = ch.plan_months(s, months=3, today=TODAY)
    assert grid["months"] == ["2026-08", "2026-09", "2026-10"]
    assert grid["rows"]["a"] == {"2026-08": 1, "2026-10": 1}


def test_balance_reports_concentration_and_aum_coverage():
    rows = [{"plan_id": f"p{i}", "aum": aum, "docs_12m": n, "docs_90d": n90}
            for i, (aum, n, n90) in enumerate([(100.0, 80, 5), (50.0, 10, 0), (25.0, 10, 1),
                                               (25.0, 0, 0)])]
    b = ch.balance(rows, top=1)
    assert b["top_share"] == 0.8
    assert b["aum_share_12m"] == 0.875
    assert b["aum_share_90d"] == 0.625
    assert b["plans_12m"] == 3 and b["plans_total"] == 4
