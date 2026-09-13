"""Plan references by month, and AUM-weighted coverage.

Built on 2026-09-12 after a one-off table showed 41 of 150 plans with no
summarised document in six months, five of the ten largest funds among
them. Two measures per plan per month: documents summarised (placed in
the month of the meeting date, else the download date) and mentions in
published briefings. Plus an observed cadence so a plan that should have
produced something and did not can be told from a board that did not
meet, and one AUM-weighted coverage number to watch.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

import queries
from database import Document, Plan, Publication, Summary, get_session

TODAY = date(2026, 9, 12)


def _dt(year, month, day=10):
    return datetime(year, month, day, tzinfo=timezone.utc)


@pytest.fixture()
def seeded(tmp_db):
    s = get_session()
    s.add(Plan(id="alpha", name="Alpha Plan", abbreviation="ALPHA", aum_billions=100.0))
    s.add(Plan(id="bravo", name="Bravo Plan", abbreviation="BRV", aum_billions=50.0))
    s.add(Plan(id="charlie", name="Charlie Plan", abbreviation="CHRL", aum_billions=None))
    i = 0

    def doc(pid, meeting, downloaded, summarised=True, doc_type="minutes"):
        nonlocal i
        i += 1
        d = Document(plan_id=pid, url=f"https://x/{i}.pdf", filename=f"{i}.pdf",
                     doc_type=doc_type, extraction_status="done",
                     meeting_date=meeting, downloaded_at=downloaded)
        s.add(d); s.flush()
        if summarised:
            s.add(Summary(document_id=d.id, summary_text=f"summary {i}"))
        return d

    # Alpha: one board document every month for the last twelve months.
    for k in range(12):
        m = (TODAY.month - k - 1) % 12 + 1
        y = TODAY.year if TODAY.month - k > 0 else TODAY.year - 1
        doc("alpha", _dt(y, m), _dt(y, m, 12))
    # Alpha also has an unsummarised document this month: not counted.
    doc("alpha", _dt(2026, 9, 11), _dt(2026, 9, 11), summarised=False)
    # Alpha's undated document: falls in the download month.
    doc("alpha", None, _dt(2026, 7, 20))
    # Bravo: one document five months ago, and a CAFR downloaded yesterday
    # (a CAFR is not board material, so it does not count as coverage).
    doc("bravo", _dt(2026, 4), _dt(2026, 4, 15))
    doc("bravo", _dt(2025, 6, 30), _dt(2026, 9, 11), doc_type="cafr")
    # A briefing this month naming Alpha twice and Bravo once by abbreviation.
    s.add(Publication(cadence="weekly", period_start=date(2026, 9, 6),
                      period_end=date(2026, 9, 12), status="published",
                      draft_markdown="Alpha Plan did X. Later Alpha Plan did Y. BRV met."))
    # A draft is not a publication.
    s.add(Publication(cadence="weekly", period_start=date(2026, 8, 30),
                      period_end=date(2026, 9, 5), status="generating",
                      draft_markdown="Charlie Plan everywhere. Charlie Plan."))
    s.commit()
    yield s
    s.close()


def _row(table, pid):
    return next(r for r in table["rows"] if r["plan_id"] == pid)


def test_months_are_the_last_n_ending_this_month(seeded):
    t = queries.plan_reference_rows(seeded, months=6, today=TODAY)
    assert t["months"] == ["2026-04", "2026-05", "2026-06", "2026-07", "2026-08", "2026-09"]


def test_documents_are_counted_by_meeting_month_and_only_when_summarised(seeded):
    t = queries.plan_reference_rows(seeded, months=6, today=TODAY)
    a = _row(t, "alpha")
    assert a["docs"] == {"2026-04": 1, "2026-05": 1, "2026-06": 1,
                         "2026-07": 2, "2026-08": 1, "2026-09": 1}
    assert a["docs_total"] == 7
    b = _row(t, "bravo")
    assert b["docs"] == {"2026-04": 1, "2026-05": 0, "2026-06": 0,
                         "2026-07": 0, "2026-08": 0, "2026-09": 0}


def test_briefing_mentions_count_published_names_and_abbreviations(seeded):
    t = queries.plan_reference_rows(seeded, months=6, today=TODAY)
    assert _row(t, "alpha")["mentions"]["2026-09"] == 2
    assert _row(t, "bravo")["mentions"]["2026-09"] == 1
    assert _row(t, "charlie")["mentions_total"] == 0, "drafts do not count"


def test_observed_cadence_and_overdue(seeded):
    t = queries.plan_reference_rows(seeded, months=6, today=TODAY)
    a, b, c = (_row(t, p) for p in ("alpha", "bravo", "charlie"))
    assert a["cadence"] == "monthly" and a["overdue"] is False
    assert b["cadence"] == "sparse" and b["overdue"] is True
    assert b["last_doc"] == "2026-04"
    assert c["cadence"] == "none" and c["last_doc"] is None and c["overdue"] is False


def test_rows_are_ordered_by_aum_with_unknown_last(seeded):
    t = queries.plan_reference_rows(seeded, months=6, today=TODAY)
    assert [r["plan_id"] for r in t["rows"]] == ["alpha", "bravo", "charlie"]


def test_aum_coverage_counts_board_material_downloaded_in_the_window(seeded):
    c = queries.aum_coverage(seeded, days=60, today=TODAY)
    # Alpha has a download inside 60 days; Bravo's only recent download is a
    # CAFR; Charlie has no AUM and no documents.
    assert c["plans_covered"] == 1
    assert c["plans_total"] == 3
    assert c["covered_aum"] == 100.0
    assert c["total_aum"] == 150.0
    assert c["share"] == pytest.approx(100 / 150)
