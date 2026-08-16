"""The read layer is exercised directly, with no Streamlit runtime.

That is half the point of extracting it: app.py's own data functions could
only be called through Streamlit's caching machinery, so none of this logic
was reachable from a test before.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

import queries
from database import Document, Plan, Summary, get_session


@pytest.fixture()
def seeded(tmp_db):
    s = get_session()
    s.add(Plan(id="bbb", name="Bravo Plan", abbreviation="BRAVO",
               state="CA", aum_billions=10.0))
    s.add(Plan(id="aaa", name="Alpha Plan", abbreviation=None,
               state=None, aum_billions=None))
    for i, (pid, dtype, days) in enumerate([
        ("bbb", "minutes", 1), ("bbb", "cafr", 2), ("aaa", "minutes", 3),
        ("bbb", "performance", 4), ("aaa", "minutes", 400),
    ]):
        d = Document(plan_id=pid, url=f"https://x/{i}.pdf", filename=f"{i}.pdf",
                     doc_type=dtype, extraction_status="done",
                     meeting_date=datetime.utcnow() - timedelta(days=days),
                     downloaded_at=datetime.utcnow() - timedelta(days=days))
        s.add(d); s.flush()
        s.add(Summary(document_id=d.id, summary_text=f"summary {i}"))
    s.commit()
    yield s
    s.close()


def test_corpus_stats_counts(seeded):
    plans, docs, downloaded, summarized = queries.corpus_stats(seeded)
    assert (plans, docs, downloaded, summarized) == (2, 5, 5, 5)


def test_plans_are_alphabetical_by_name(seeded):
    assert [p.id for p in queries.plans(seeded)] == ["aaa", "bbb"]


def test_recent_summaries_excludes_cafr_and_performance(seeded):
    rows = queries.recent_summaries(seeded)
    kinds = {d.doc_type for d, _ in rows}
    assert kinds == {"minutes"}, kinds


def test_recent_summaries_filters_by_plan(seeded):
    rows = queries.recent_summaries(seeded, "aaa")
    assert {d.plan_id for d, _ in rows} == {"aaa"}
    # "All" is the sentinel the UI passes for no filter.
    assert len(queries.recent_summaries(seeded, "All")) == 3


def test_recent_summaries_respects_limit(seeded):
    assert len(queries.recent_summaries(seeded, limit=1)) == 1


def test_plan_coverage_rows_shape_and_nulls(seeded):
    rows = {r["Plan"]: r for r in queries.plan_coverage_rows(seeded)}
    assert list(rows) == ["Alpha Plan", "Bravo Plan"]   # ordered by name
    alpha = rows["Alpha Plan"]
    # Null abbreviation/state render as empty strings, not "None".
    assert alpha["Abbrev"] == "" and alpha["State"] == ""
    assert alpha["Downloaded"] == 2 and alpha["Summarized"] == 2
    assert alpha["Last download"] != "—"


def test_plans_index_includes_plans_without_a_twin(seeded):
    """get_twin_index inner-joins twin_snapshots; the index must still list
    every plan, with an em-dash where no snapshot exists."""
    rows = queries.plans_index_rows(seeded)
    assert len(rows) == 2
    assert {r["Twin built"] for r in rows} == {"—"}
    assert {r["Completeness"] for r in rows} == {"—"}
    # Falls back to plan id when abbreviation is null.
    assert {r["Plan"] for r in rows} == {"aaa", "BRAVO"}
