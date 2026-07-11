"""The twin page loader returns the parsed snapshot (or {} when absent)."""
import pytest

pytest.importorskip("streamlit")

from database import Plan, get_session
import twin_builder


def test_twin_page_data_roundtrip(tmp_db):
    import app as app_module
    session = get_session()
    session.add(Plan(id="testplan", name="Test Plan", abbreviation="TP",
                     state="CA", aum_billions=10.0))
    session.commit()
    plan = session.get(Plan, "testplan")
    twin_builder.save_snapshot(session, "testplan",
                               twin_builder.build_twin(session, plan))
    session.close()
    data = app_module._twin_page_data("testplan")
    assert data["twin"]["plan_id"] == "testplan"
    assert "identity" in data["twin"]["facets"]
    assert app_module._twin_page_data("nosuch") == {}
