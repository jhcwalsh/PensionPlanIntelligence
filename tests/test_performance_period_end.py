"""The Period end column on the collated performance table, and the
Performance page's sub-tabs.

The collated table already showed a verbatim Period ("FY2025", "1 Yr.") and
an As of date. Neither sorts, and neither answers "is this 2026Q1?" -- which
is the question that decides whether two rows can be read against each other.
"""
from datetime import date

import pytest

import database
import queries
from database import Document, Plan, PlanAssetClassPerformance


@pytest.fixture
def session(tmp_db):
    s = database.get_session()
    s.add(Plan(id="mcera", name="MCERA", state="CA"))
    s.commit()
    yield s
    s.close()


def _seed(session, asset_class, return_pct, period_label, as_of,
          horizon="annual", document_id=None):
    session.add(PlanAssetClassPerformance(
        plan_id="mcera", asset_class=asset_class, return_pct=return_pct,
        period_label=period_label, horizon=horizon, as_of_date=as_of,
        source="board_doc", document_id=document_id))


def test_collated_rows_carry_the_quarter_the_period_ends_in(session):
    _seed(session, "real_estate", 6.0, "FY2025", date(2025, 6, 30))
    session.commit()

    row = queries.collated_performance_rows(session)[0]

    assert row["Period"] == "FY2025"          # verbatim, unchanged
    assert row["Period end"] == "2025Q2"      # sortable, and filterable


def test_the_quarter_comes_from_the_period_not_the_meeting(session):
    """A board pack presented on 14 May reports through 31 March. Bucketing
    on the document's own date files a 2026Q1 return under 2026Q2 and stands
    it beside the next quarter's numbers -- the exact mixing this column
    exists to prevent."""
    _seed(session, "real_estate", 6.0, "1 Year", date(2026, 5, 14))
    session.commit()

    row = queries.collated_performance_rows(session)[0]
    assert row["As of"] == "2026-05-14"
    assert row["Period end"] == "2026Q1"


def test_two_documents_from_different_quarters_stay_separate(session):
    """They are already separate rows (the key is per-document); this checks
    they are separable, which is what the filter needs."""
    d1 = Document(plan_id="mcera", url="https://x/a.pdf", filename="a.pdf",
                  meeting_date=date(2026, 5, 14), extraction_status="done")
    d2 = Document(plan_id="mcera", url="https://x/b.pdf", filename="b.pdf",
                  meeting_date=date(2026, 2, 10), extraction_status="done")
    session.add_all([d1, d2])
    session.commit()

    _seed(session, "real_estate", 6.0, "1 Year", date(2026, 5, 14),
          document_id=d1.id)
    _seed(session, "real_estate", 4.0, "1 Year", date(2026, 2, 10),
          document_id=d2.id)
    session.commit()

    quarters = {r["Period end"] for r in queries.collated_performance_rows(session)}
    assert quarters == {"2026Q1", "2025Q4"}


def test_period_end_is_none_when_nothing_says_when(session):
    _seed(session, "real_estate", 6.0, None, None)
    session.commit()

    row = queries.collated_performance_rows(session)[0]
    assert row["Period end"] is None


# --------------------------------------------------------------------------
# The Performance page's sub-tabs
# --------------------------------------------------------------------------

def test_the_performance_page_renders_one_tab_per_table(monkeypatch, tmp_db):
    """Four tables stacked on one page read as one long table with three
    interruptions, and each table's filters sat screens away from its rows.
    The last tab is conditional: an empty tab reads as a broken page."""
    pytest.importorskip("streamlit")
    import app as app_module

    labels = []
    monkeypatch.setattr(app_module.st, "title", lambda *a, **k: None)
    # Never touch the real cached session: get_db_session is
    # @st.cache_resource, a process-level singleton, and binding it here
    # would leave the next test file holding a session onto this test's
    # torn-down tmp_path.
    monkeypatch.setattr(app_module, "get_db_session", lambda: None)
    monkeypatch.setattr(app_module.queries, "quarterly_performance_rows",
                        lambda session: [])

    class _Tab:
        def __enter__(self): return self
        def __exit__(self, *a): return False

    def _tabs(names):
        labels.extend(names)
        return [_Tab() for _ in names]

    monkeypatch.setattr(app_module.st, "tabs", _tabs)
    for name in ("_render_collated_performance", "_render_asset_class_horizons",
                 "_render_cafr_fiscal_year"):
        monkeypatch.setattr(app_module, name, lambda: None)

    app_module.page_performance()

    assert labels == ["By plan", "By asset class", "CAFR fiscal years"]


def test_the_quarterly_tab_appears_only_when_it_has_rows(monkeypatch, tmp_db):
    pytest.importorskip("streamlit")
    import app as app_module

    labels = []
    monkeypatch.setattr(app_module.st, "title", lambda *a, **k: None)
    monkeypatch.setattr(app_module, "get_db_session", lambda: None)
    monkeypatch.setattr(app_module.queries, "quarterly_performance_rows",
                        lambda session: [{"Plan": "NYC", "Fund": "NYCERS"}])

    class _Tab:
        def __enter__(self): return self
        def __exit__(self, *a): return False

    monkeypatch.setattr(app_module.st, "tabs",
                        lambda names: (labels.extend(names),
                                       [_Tab() for _ in names])[1])
    for name in ("_render_collated_performance", "_render_asset_class_horizons",
                 "_render_cafr_fiscal_year"):
        monkeypatch.setattr(app_module, name, lambda: None)
    monkeypatch.setattr(app_module, "_render_quarterly_reports",
                        lambda rows: None)

    app_module.page_performance()

    assert labels[-1] == "Quarterly reports"
