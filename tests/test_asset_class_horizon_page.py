"""The per-asset-class Performance page loader (app._asset_class_horizon_rows).

Mirrors tests/test_twin_page_data.py's pattern: seed via tmp_db, call the
module-level function directly rather than exercising Streamlit rendering.
The underlying query (queries.asset_class_horizon_rows) already has its own
coverage in tests/test_asset_class_horizon.py -- this file only checks the
thin app-level wrapper that the new UI section calls.
"""
from datetime import date

import pytest

pytest.importorskip("streamlit")

from database import Plan, PlanAssetClassHorizon, get_session


@pytest.fixture(autouse=True)
def _reset_app_caches(tmp_db):
    """``app.get_db_session`` is ``@st.cache_resource`` -- a process-level
    singleton, not scoped to ``tmp_db``'s per-test engine. Clearing it before
    a test binds it to *this* test's engine; clearing it again after leaves
    no stale session pointing at a torn-down tmp_path for the next test file
    (alphabetically, tests/test_twin_page_data.py's first test runs right
    after this file and does not clear the cache itself, on the assumption
    that it is the first thing to touch it)."""
    import app as app_module
    app_module.get_db_session.clear()
    app_module._asset_class_horizon_rows.clear()
    yield
    app_module.get_db_session.clear()
    app_module._asset_class_horizon_rows.clear()


def test_asset_class_horizon_rows_returns_data_for_a_populated_class(tmp_db):
    import app as app_module

    session = get_session()
    session.add(Plan(id="mcera", name="MCERA", state="CA"))
    session.commit()
    session.add(PlanAssetClassHorizon(
        plan_id="mcera", asset_class="real_estate", horizon_key="annual",
        return_pct=6.0, as_of_date=date(2026, 1, 1), source="board_doc"))
    session.commit()
    session.close()

    rows = app_module._asset_class_horizon_rows("real_estate")

    assert len(rows) == 1
    assert rows[0]["Plan"] == "MCERA"
    assert rows[0]["1 year"] == 6.0


def test_asset_class_horizon_rows_empty_class_yields_empty_list(tmp_db):
    import app as app_module

    session = get_session()
    session.add(Plan(id="mcera", name="MCERA", state="CA"))
    session.commit()
    session.add(PlanAssetClassHorizon(
        plan_id="mcera", asset_class="real_estate", horizon_key="annual",
        return_pct=6.0, as_of_date=date(2026, 1, 1), source="board_doc"))
    session.commit()
    session.close()

    rows = app_module._asset_class_horizon_rows("private_credit")

    assert rows == []
