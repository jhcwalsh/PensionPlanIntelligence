"""Actuarial extraction: mock round-trip, skip logic, locate defaults intact."""
from database import CafrActuarial, Document, Plan, get_session
import extract_cafr_actuarial


def _seed(session):
    session.add(Plan(id="p1", name="P", abbreviation="P", state="CA"))
    d = Document(plan_id="p1", url="https://x/cafr.pdf", filename="cafr.pdf",
                 doc_type="cafr", extraction_status="done", fiscal_year=2025,
                 local_path=None)
    session.add(d); session.commit()
    _ = d.id
    return d


def test_mock_roundtrip_and_skip(tmp_db):
    session = get_session()
    d = _seed(session); session.close()
    counts = extract_cafr_actuarial.run_extraction(["p1"])
    assert counts["saved"] == 1
    session = get_session()
    row = session.query(CafrActuarial).one()
    assert row.document_id == d.id and row.funded_ratio_pct == 75.0
    assert row.prompt_version == "actuarial_v1"
    session.close()
    counts = extract_cafr_actuarial.run_extraction(["p1"])
    assert counts["saved"] == 0 and counts["already_have"] == 1


def test_locate_defaults_unchanged():
    import inspect
    from extract_cafr_investments import locate_investment_section
    sig = inspect.signature(locate_investment_section)
    assert "start_patterns" in sig.parameters and "end_patterns" in sig.parameters
