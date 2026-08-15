"""IPS extraction: mock-mode end-to-end, gating, and skip logic."""
import json
import warnings

from sqlalchemy import event

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


def test_revision_past_truncation_cap_is_detected(tmp_db, monkeypatch):
    """A republished IPS whose leading MAX_INPUT_CHARS chars are unchanged.

    Hashing the *truncated* slice makes the two revisions indistinguishable,
    so extract_one returns "already_have" forever and the new policy is never
    extracted. The hash must be taken before truncation.
    """
    monkeypatch.setattr(extract_ips, "MAX_INPUT_CHARS", 20)
    session = get_session()
    _seed(session)  # seeded text is 47 chars, so it is truncated at 20
    session.close()

    assert extract_ips.run_extraction(["p1"])["saved"] == 1

    session = get_session()
    doc = session.query(IpsDocument).one()
    head = doc.extracted_text[:extract_ips.MAX_INPUT_CHARS]
    doc.extracted_text = doc.extracted_text + " ... REVISED 2026 APPENDIX"
    session.commit()
    # The revision is entirely past the cap: what Claude sees is unchanged.
    assert doc.extracted_text[:extract_ips.MAX_INPUT_CHARS] == head
    session.close()

    counts = extract_ips.run_extraction(["p1"])
    assert counts["saved"] == 1 and counts["already_have"] == 0


def test_extracted_at_is_timezone_aware(tmp_db):
    """`IpsExtract.extracted_at` must come from database._utcnow (aware).

    SQLite drops the offset on write, so the value has to be captured at
    insert time; a naive datetime.utcnow() override would also raise a
    DeprecationWarning on 3.12+.
    """
    captured = []

    def _capture(mapper, connection, target):
        captured.append(target.extracted_at)

    event.listen(IpsExtract, "after_insert", _capture)
    try:
        session = get_session()
        _seed(session); session.close()
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            assert extract_ips.run_extraction(["p1"])["saved"] == 1
    finally:
        event.remove(IpsExtract, "after_insert", _capture)

    assert len(captured) == 1
    assert captured[0].tzinfo is not None
    assert captured[0].utcoffset().total_seconds() == 0
