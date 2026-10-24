"""The admin-only Coverage tab (app.page_coverage). Logic is in test_coverage_health.py."""
import pathlib
import re
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("streamlit")

from database import Document, Plan, get_session

SRC = (pathlib.Path(__file__).resolve().parents[1] / "app.py").read_text(encoding="utf-8")


@pytest.fixture(autouse=True)
def _reset_app_caches(tmp_db):
    import app as app_module
    for c in (app_module.get_db_session, app_module._coverage_report):
        c.clear()
    yield
    for c in (app_module.get_db_session, app_module._coverage_report):
        c.clear()


def test_coverage_tab_is_admin_only():
    start = SRC.index("tab_specs: list[tuple[str, callable]] = [")
    block = SRC[start:SRC.index("tabs = st.tabs(", start)]
    gate = block.index("if _admin_unlocked():")
    assert '"Coverage"' not in block[:gate]
    assert re.search(r'tab_specs\.append\(\("Coverage"', block[gate:])


def _coverage_script():
    import app
    app.page_coverage()


def test_page_renders_alerts_and_charts():
    s = get_session()
    s.add_all([Plan(id="busy", name="Busy Plan", aum_billions=100.0),
               Plan(id="quiet", name="Quiet Plan", aum_billions=40.0)])
    now = datetime.now(timezone.utc)
    for i in range(30):
        d = now - timedelta(days=12 * i)
        s.add(Document(id=i + 1, plan_id="busy", url=f"https://x.test/{i}", doc_type="minutes",
                       downloaded_at=d, meeting_date=d))
    s.commit()
    s.close()
    from streamlit.testing.v1 import AppTest
    at = AppTest.from_function(_coverage_script, default_timeout=60).run()
    assert not at.exception
    text = " ".join(m.value for m in at.markdown)
    assert "Quiet Plan" in text and "No board documents ever" in text
    assert len(at.get("vega_lite_chart")) == 3
    at.segmented_control(key="cov_show").set_value("With gaps").run()
    assert not at.exception
