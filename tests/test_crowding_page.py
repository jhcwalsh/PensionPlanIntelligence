"""The admin-only Fund Crowding tab (app.page_crowding and its loaders).

Follows tests/test_asset_class_horizon_page.py: seed via tmp_db and call the
module-level loaders directly. The ranking itself is covered in
tests/test_fund_query.py; this file checks the gate, the wiring and the HTML.
"""
import json
import pathlib
import re

import pytest

pytest.importorskip("streamlit")

from database import Fund, FundCommitment, Plan, get_session

SRC = (pathlib.Path(__file__).resolve().parents[1] / "app.py").read_text(encoding="utf-8")


@pytest.fixture(autouse=True)
def _reset_app_caches(tmp_db):
    import app as app_module
    caches = (app_module.get_db_session, app_module._crowding_overview,
              app_module._crowding_leaderboard, app_module._crowding_detail)
    for c in caches:
        c.clear()
    yield
    for c in caches:
        c.clear()


def _seed():
    s = get_session()
    s.add_all([Plan(id="a", name="Alpha Retirement", aum_billions=100.0),
               Plan(id="b", name="Beta Pension", aum_billions=50.0)])
    f = Fund(fund_key="gtcr||15", canonical_name="GTCR Fund XV", manager_canonical="GTCR",
             series="", number=15, asset_class="private_equity")
    s.add(f)
    s.flush()
    for pid, amt in (("a", 100.0), ("b", None)):
        s.add(FundCommitment(
            plan_id=pid, fund_id=f.id, manager_canonical="GTCR", fund_name_raw="GTCR XV",
            status="approved", amount_millions=amt, first_seen="2026-03-01", last_seen="2026-03-01",
            match_confidence="exact",
            evidence=json.dumps([{"document_id": 7, "meeting_date": "2026-03-01",
                                  "status": "approved", "quote": "Commit <$100m> to GTCR XV"}])))
    s.commit()
    s.close()


def test_crowding_tab_is_admin_only():
    start = SRC.index("tab_specs: list[tuple[str, callable]] = [")
    block = SRC[start:SRC.index("tabs = st.tabs(", start)]
    gate = block.index("if _admin_unlocked():")
    assert '"Crowding"' not in block[:gate]
    assert re.search(r'tab_specs\.append\(\("Crowding"', block[gate:])


def test_loaders_read_the_fund_tables():
    import app as app_module
    _seed()
    ov = app_module._crowding_overview()
    assert ov["funds"] == 1 and ov["crowded"] == 1
    assert ov["fund_index"] == {"gtcr||15": "GTCR Fund XV"}
    board = app_module._crowding_leaderboard("private_equity")
    assert [(r["fund"]["key"], r["holders"], r["total_millions"]) for r in board] == [
        ("gtcr||15", 2, 100.0)]
    assert app_module._crowding_leaderboard("real_estate") == []
    who, alongside = app_module._crowding_detail("gtcr||15")
    assert {h["plan_name"] for h in who["holders"]} == {"Alpha Retirement", "Beta Pension"}
    assert alongside["holders_considered"] == 2


def test_holder_html_escapes_quotes_and_links_the_source():
    import app as app_module
    _seed()
    who, _ = app_module._crowding_detail("gtcr||15")
    by_plan = {h["plan_id"]: app_module._holder_html(h) for h in who["holders"]}
    assert "&lt;$100m&gt;" in by_plan["a"] and "<$100m>" not in by_plan["a"]
    assert 'href="?doc=7"' in by_plan["a"]
    assert "$100m" in by_plan["a"]
    assert "amount n/a" in by_plan["b"]


def test_fmt_m():
    import app as app_module
    assert app_module._fmt_m(None) == "—"
    assert app_module._fmt_m(125.0) == "$125m"
    assert app_module._fmt_m(1500.0) == "$1.5bn"


def test_fmt_day():
    import app as app_module
    assert app_module._fmt_day(None) == "—"
    assert app_module._fmt_day("2026-10-03") == "3 Oct 2026"
