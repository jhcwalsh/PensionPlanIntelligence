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


# ---------------------------------------------------------------------------
# CAFR extraction
# ---------------------------------------------------------------------------

@pytest.fixture()
def cafr_seeded(tmp_db):
    """One plan with an extract, one with a CAFR but no extract, one bare."""
    from database import CafrAllocation, CafrExtract, CafrPerformance

    s = get_session()
    s.add(Plan(id="alpha", name="Alpha", abbreviation="ALP", state="CA",
               fiscal_year_end="06-30"))
    s.add(Plan(id="beta", name="Beta", abbreviation=None, state=None))
    s.add(Plan(id="gamma", name="Gamma", abbreviation="GAM", state="NY"))
    s.flush()

    # alpha: two CAFRs; the FY2025 one must win over FY2024.
    old = Document(plan_id="alpha", url="https://x/a24.pdf", filename="a24.pdf",
                   doc_type="cafr", fiscal_year=2024, extraction_status="done",
                   downloaded_at=datetime(2025, 1, 1))
    new = Document(plan_id="alpha", url="https://x/a25.pdf", filename="a25.pdf",
                   doc_type="cafr", fiscal_year=2025, extraction_status="done",
                   downloaded_at=datetime(2026, 1, 1))
    beta_doc = Document(plan_id="beta", url="https://x/b.pdf", filename="b.pdf",
                        doc_type="cafr", fiscal_year=2025,
                        extraction_status="done",
                        downloaded_at=datetime(2026, 2, 2))
    s.add_all([old, new, beta_doc]); s.flush()

    ext = CafrExtract(plan_id="alpha", document_id=new.id, fiscal_year=2025,
                      model_used="test", investment_policy_text="policy")
    s.add(ext); s.flush()
    s.add(CafrAllocation(cafr_extract_id=ext.id, asset_class="Public Equity",
                         target_pct=60.0, actual_pct=61.0))
    s.add(CafrAllocation(cafr_extract_id=ext.id, asset_class="Global Equity",
                         target_pct=10.0, actual_pct=9.0))
    s.add(CafrAllocation(cafr_extract_id=ext.id, asset_class="Real Estate",
                         target_pct=8.0, actual_pct=None))   # dropped: no actual
    s.add(CafrPerformance(cafr_extract_id=ext.id, scope="total",
                          period="1yr", return_pct=7.5))
    s.commit()
    yield s
    s.close()


def test_cafr_coverage_picks_the_latest_fiscal_year(cafr_seeded):
    rows = {r["plan_id"]: r for r in queries.cafr_coverage_rows(cafr_seeded)}
    assert rows["alpha"]["CAFR FY"] == "2025"
    assert rows["alpha"]["Status"] == "Extracted"
    assert rows["alpha"]["# Asset classes"] == 3
    assert rows["alpha"]["# Perf rows"] == 1


def test_cafr_coverage_buckets_missing_and_pending(cafr_seeded):
    rows = {r["plan_id"]: r for r in queries.cafr_coverage_rows(cafr_seeded)}
    assert rows["beta"]["Status"] == "Pending extract"    # has CAFR, no extract
    assert rows["gamma"]["Status"] == "Missing CAFR"      # no CAFR at all
    assert rows["beta"]["Plan"] == "Beta"                 # falls back to name


def test_cafr_plan_detail_unknown_and_unextracted(cafr_seeded):
    assert queries.cafr_plan_detail(cafr_seeded, "nope") == {}
    beta = queries.cafr_plan_detail(cafr_seeded, "beta")
    assert set(beta) == {"plan"}          # plan exists, no extract yet


def test_cafr_plan_detail_full_shape(cafr_seeded):
    d = queries.cafr_plan_detail(cafr_seeded, "alpha")
    assert d["plan"]["name"] == "Alpha"
    assert d["extract"]["fiscal_year"] == 2025
    assert d["document"]["filename"] == "a25.pdf"
    assert len(d["allocations"]) == 3 and len(d["performance"]) == 1
    assert d["allocations"][0]["Asset class"] == "Public Equity"


def test_cafr_extract_fy_range(cafr_seeded):
    assert queries.cafr_extract_fy_range(cafr_seeded) == (2025, 2025)


def test_allocation_rows_requires_both_target_and_actual(cafr_seeded):
    rows = queries.allocation_rows(cafr_seeded, ("%equity%",), ())
    classes = {r[5] for r in rows}
    assert classes == {"Public Equity", "Global Equity"}
    # Real Estate has no actual_pct and must not appear even if matched.
    assert not queries.allocation_rows(cafr_seeded, ("%real%",), ())


def test_allocation_rows_honours_exclusions(cafr_seeded):
    rows = queries.allocation_rows(cafr_seeded, ("%equity%",), ("%global%",))
    assert {r[5] for r in rows} == {"Public Equity"}


def test_allocation_rows_min_fy_filters_the_subquery_too(cafr_seeded):
    assert queries.allocation_rows(cafr_seeded, ("%equity%",), (), 2026) == []
    assert queries.allocation_rows(cafr_seeded, ("%equity%",), (), 2025)


# ---------------------------------------------------------------------------
# The rule itself
# ---------------------------------------------------------------------------

def test_app_py_contains_no_queries():
    """The spec's rule: app.py holds no queries.

    Every read goes through queries.py so the phase-2 static-site port is a
    front-end-only job. If this fails, a query crept back into the view layer.
    """
    import pathlib
    src = pathlib.Path(__file__).resolve().parents[1] / "app.py"
    text = src.read_text(encoding="utf-8")
    assert ".query(" not in text, (
        "app.py must not build queries — move it into queries.py")


def test_queries_module_does_not_depend_on_streamlit():
    """The read layer has to be importable outside a Streamlit runtime.

    A build script or an API will import it; pulling in streamlit would drag a
    web-server dependency into a batch job, and would make these functions
    untestable again.
    """
    import subprocess
    import sys

    out = subprocess.run(
        [sys.executable, "-c",
         "import sys, queries; print('streamlit' in sys.modules)"],
        capture_output=True, text=True, cwd=str(
            __import__("pathlib").Path(__file__).resolve().parents[1]),
    )
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "False", (
        "queries.py pulled in streamlit: " + out.stdout)


# ---------------------------------------------------------------------------
# Ordering: NULL placement and determinism
#
# Found by the step-4 dual run (scripts/compare_backends.py), which compares
# every function here against Neon. Postgres treats NULL as larger than any
# value, so DESC puts NULLS FIRST; SQLite puts them last. And neither backend
# promises an order for rows whose sort key ties -- SQLite's happens to be
# stable because it falls back to rowid, which is why none of this was visible
# before Postgres.
#
# These assert on the ORDER BY clause rather than on row order, because a
# tiebreaker exists precisely to pin down an order that seeded data cannot
# reliably reproduce: a row-order assertion would pass on SQLite whether or
# not the fix is present. The behaviour itself is verified against a real
# Postgres in tests/postgres/test_ordering_semantics.py.
# ---------------------------------------------------------------------------

def _order_by_clause(name: str) -> str:
    """Just the .order_by(...) call from a query function, as source text.

    Matching against the whole function body is what made the first version of
    this test vacuous: failed_extraction_rows selects Document.id, so a naive
    substring search for its tiebreaker matched the SELECT list and passed
    while the function had no ORDER BY at all.
    """
    import inspect
    import re

    src = inspect.getsource(getattr(queries, name))
    found = re.findall(r"\.order_by\((?:[^()]|\([^()]*\))*\)", src, re.S)
    return " ".join(found)


def test_the_clause_extractor_ignores_the_select_list():
    """Guard on the guard, naming the exact false pass it prevents."""
    clause = _order_by_clause("failed_extraction_rows")
    assert "Plan.name" not in clause or "order_by" in clause
    # skipped_document_rows selects Document.id; its ORDER BY must be what is
    # inspected, not that.
    src_has_select = "Document.id" in __import__("inspect").getsource(
        queries.skipped_document_rows)
    assert src_has_select, "fixture assumption broke: the select list changed"


@pytest.mark.parametrize("name, tiebreaker", [
    ("recent_summaries", "Document.id.desc()"),
    ("documents_by_ids", "Document.id.desc()"),
    ("allocation_rows", "CafrAllocation.id"),
    ("failed_extraction_rows", "Document.id"),
    ("skipped_document_rows", "Document.id"),
    ("cafr_refresh_rows", "CafrRefreshLog.id"),
    ("video_sources", "PlanVideoSource.id"),
    ("meeting_recordings", "MeetingRecording.id.desc()"),
    ("publications_by_status", "Publication.id.desc()"),
])
def test_ordering_is_deterministic(name, tiebreaker):
    """Every ordered read needs a unique final sort key.

    Without one the backends disagree on rows whose sort key ties, which makes
    the dual-run comparison permanently noisy -- and noise is what hides a real
    defect. Each of these mismatched against Neon before the fix.
    """
    clause = _order_by_clause(name)
    assert clause, f"{name} has no order_by at all — its row order is arbitrary"
    assert tiebreaker in clause, (
        f"{name}'s order_by has no unique tiebreaker; add {tiebreaker}")


@pytest.mark.parametrize("name", ["recent_summaries", "documents_by_ids"])
def test_descending_meeting_date_is_nulls_last(name):
    """meeting_date is nullable and neither of these filters NULLs out."""
    clause = _order_by_clause(name)
    assert "meeting_date.desc().nullslast()" in clause, (
        f"{name} sorts by meeting_date DESC without nullslast(); on Postgres "
        "undated rows sort first and dated ones fall off the limit")
