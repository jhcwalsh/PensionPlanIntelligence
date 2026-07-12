"""IPS extraction: mock-mode end-to-end, gating, and skip logic."""
import json
from database import IpsAllocation, IpsDocument, IpsExtract, Plan, get_session
import extract_ips


def _seed(session, verdict="yes"):
    session.add(Plan(id="p1", name="P", abbreviation="P", state="CA"))
    d = IpsDocument(plan_id="p1", url="https://x/ips.pdf", filename="ips.pdf",
                    extracted_text="INVESTMENT POLICY STATEMENT ... target return 7%",
                    extraction_status="done", verification_verdict=verdict,
                    content_hash="h1")
    session.add(d); session.commit()
    _ = d.id  # force-load PK while session is still open, so it's usable post-close
    return d


def test_mock_extraction_roundtrip(tmp_db):
    session = get_session()
    d = _seed(session); session.close()
    counts = extract_ips.run_extraction(["p1"])
    assert counts["saved"] == 1
    session = get_session()
    ext = session.query(IpsExtract).one()
    assert ext.ips_document_id == d.id
    assert ext.target_return_pct == 7.0
    assert json.loads(ext.governance)["consultant_name"] == "Meketa"
    assert session.query(IpsAllocation).one().asset_class == "Global Equity"
    session.close()


def test_unverified_ips_skipped(tmp_db):
    session = get_session()
    _seed(session, verdict="no"); session.close()
    counts = extract_ips.run_extraction(["p1"])
    assert counts["saved"] == 0


def test_hash_skip_on_second_run(tmp_db):
    session = get_session()
    _seed(session); session.close()
    extract_ips.run_extraction(["p1"])
    counts = extract_ips.run_extraction(["p1"])
    assert counts["saved"] == 0 and counts["already_have"] == 1
