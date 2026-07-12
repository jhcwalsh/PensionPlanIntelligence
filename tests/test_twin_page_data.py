"""The twin page loader returns the parsed snapshot (or {} when absent)."""
import pytest

pytest.importorskip("streamlit")

from database import (
    CafrActuarial, CafrAllocation, CafrExtract, Document, IpsAllocation,
    IpsDocument, IpsExtract, Plan, get_session,
)
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


def test_twin_page_data_v1_loads_and_page_is_callable(tmp_db):
    """v1 facets (IPS + actuarial) round-trip through the page loader, and
    the rendering function itself is importable/callable (smoke check —
    full Streamlit rendering isn't exercised outside a live app session)."""
    import app as app_module

    session = get_session()
    plan = Plan(id="testplan", name="Test Plan", abbreviation="TP",
                state="CA", aum_billions=10.0, fiscal_year_end="06-30")
    session.add(plan)
    cafr_doc = Document(plan_id="testplan", url="https://x/cafr.pdf", filename="cafr.pdf",
                        doc_type="cafr", extraction_status="done", fiscal_year=2025)
    session.add(cafr_doc)
    session.commit()

    ext = CafrExtract(plan_id="testplan", document_id=cafr_doc.id, fiscal_year=2025,
                      investment_policy_text="Prudent person rule.")
    session.add(ext)
    session.commit()
    session.add(CafrAllocation(cafr_extract_id=ext.id, asset_class="Global Equity",
                               target_pct=40.0, actual_pct=45.0,
                               target_range_low=35.0, target_range_high=44.0))
    session.add(CafrActuarial(plan_id="testplan", document_id=cafr_doc.id, fiscal_year=2025,
                              valuation_date="2025-06-30", funded_ratio_pct=75.0,
                              discount_rate_pct=6.8, actuary_firm="Cavanaugh Macdonald"))

    ips_doc = IpsDocument(plan_id="testplan", url="https://x/i.pdf", filename="i.pdf",
                          extracted_text="policy", extraction_status="done",
                          verification_verdict="yes", content_hash="h")
    session.add(ips_doc)
    session.commit()
    ips_ext = IpsExtract(plan_id="testplan", ips_document_id=ips_doc.id,
                         target_return_pct=7.0, effective_date="2026-01-01",
                         governance='{"consultant_name": "Wilshire"}',
                         rebalancing_policy='{"frequency": "quarterly"}',
                         permitted_prohibited='{"permitted": ["Equity"], "prohibited": ["Tobacco"]}')
    session.add(ips_ext)
    session.commit()
    session.add(IpsAllocation(ips_extract_id=ips_ext.id, asset_class="Global Equity",
                              target_pct=42.0, range_low=37.0, range_high=47.0))
    session.commit()

    twin_builder.save_snapshot(session, "testplan", twin_builder.build_twin(session, plan))
    session.close()

    # get_db_session() is @st.cache_resource'd process-wide; clear it so this
    # test doesn't read a session cached against a previous test's DB engine.
    app_module.get_db_session.clear()
    data = app_module._twin_page_data("testplan")
    twin = data["twin"]
    assert twin["schema_version"] == "twin_v1"
    facets = twin["facets"]
    assert facets["policy"]["ips"]["target_return_pct"] == 7.0
    assert facets["allocation"]["ips_targets"]["rows"][0]["target_pct"] == 42.0
    assert facets["funding_actuarial"]["status"] == "captured"
    assert facets["funding_actuarial"]["metrics"]["funded_ratio_pct"] == 75.0

    assert callable(app_module.page_plan_twin)
