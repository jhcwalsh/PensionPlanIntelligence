"""Investment Section location: the plan-own-name TOC collision, and the
'standalone' cafr_format bypass.

Several plans put the word "investment" in their own name (Washington State
*Investment* Board, West Virginia *Investment* Management Board, Illinois
Police Officers' Pension *Investment* Fund...). The TOC locator's lenient
"contains investment" pattern then matches the document's own title-page
entry, and because the level-preference rule only ever swaps a candidate
for one at a *strictly* lower level, that false match — almost always
level 1, the same level the real "Investment Section" entry also uses —
wins permanently and the real section is never found. Diagnosed by
comparing four plans whose own names contain "Investment": wv_imb and
ipopif_il reproduce the bug directly (both confirmed against their real
CAFR PDFs before this fix); wsib and mboi_mt could not be reproduced
locally (their extracted CAFRs only exist on a GitHub Actions runner path)
but show the same symptom -- a 1-7 page range with zero allocation/
performance rows saved.
"""

from __future__ import annotations

from datetime import datetime, timezone

import fitz
import pytest

import extract_cafr_investments as eci
from database import CafrExtract, Document, Plan, get_session


def _make_pdf(path, pages, toc=None):
    doc = fitz.open()
    for text in pages:
        doc.new_page().insert_text((72, 72), text)
    if toc is not None:
        doc.set_toc(toc)
    doc.save(str(path))
    doc.close()
    return str(path)


def test_toc_excludes_entries_containing_the_plans_own_name(tmp_path):
    """Reproduces the wv_imb shape: title-page TOC entry IS the plan name."""
    path = tmp_path / "wv_imb.pdf"
    _make_pdf(path, ["Cover\n"] * 50, toc=[
        [1, "West Virginia Investment Management Board", 1],   # false match
        [1, "Table of Contents", 4],
        [1, "Investment Objectives & Financial Highlights", 25],  # real
        [1, "Participant Plans", 47],
    ])
    rng = eci.locate_investment_section(
        str(path), plan_name="West Virginia Investment Management Board")
    assert rng == (25, 46)


def test_toc_excludes_own_name_even_as_a_substring(tmp_path):
    """Reproduces the ipopif_il shape: false match embeds the plan's name
    inside a longer title ("... Members of the Board of Trustees"), and a
    corrupted level-2 bookmark duplicate of the real title sits even
    earlier in the TOC. The real, level-1, later entry must still win."""
    path = tmp_path / "ipopif.pdf"
    _make_pdf(path, ["Cover\n"] * 65, toc=[
        [1, "_Hlk118722755", 31],       # stray Word bookmark, doesn't match
        [1, "Table of Contents", 2],
        [2, "Investment Section", 2],   # corrupted duplicate bookmark, nested
        [1, "Illinois Police Officers' Pension Investment Fund "
            "Members of the Board of Trustees", 4],             # false match
        [1, "Investment Section", 56],                            # real
        [1, "Investment Policy", 63],
    ])
    rng = eci.locate_investment_section(
        str(path), plan_name="Illinois Police Officers' Pension Investment Fund")
    assert rng == (56, 62)


def test_without_plan_name_the_collision_reproduces(tmp_path):
    """Confirms the fix is opt-in via plan_name, not a change to defaults --
    omitting it reproduces the original bug on the same fixture."""
    path = tmp_path / "wv_imb.pdf"
    _make_pdf(path, ["Cover\n"] * 30, toc=[
        [1, "West Virginia Investment Management Board", 1],
        [1, "Table of Contents", 4],
        [1, "Investment Objectives & Financial Highlights", 25],
        [1, "Participant Plans", 47],
    ])
    rng = eci.locate_investment_section(str(path))
    assert rng == (1, 3)


def test_normal_case_unaffected_by_plan_name_filter(tmp_path):
    """A plan whose name has no 'investment' collision behaves identically
    with or without plan_name."""
    path = tmp_path / "normal.pdf"
    _make_pdf(path, ["Cover\n"] * 50, toc=[
        [1, "Example Retirement System", 1],
        [1, "Table of Contents", 4],
        [1, "Investment Section", 25],
        [1, "Actuarial Section", 47],
    ])
    with_name = eci.locate_investment_section(str(path), plan_name="Example Retirement System")
    without_name = eci.locate_investment_section(str(path))
    assert with_name == without_name == (25, 46)


def test_standalone_format_feeds_the_whole_document(tmp_db, tmp_path, monkeypatch):
    """cafr_format='standalone' skips section-location entirely (Nebraska
    Investment Council's real annual report has no TOC and no section
    headers at all -- there is nothing for the locator to find)."""
    filler = "Some investment content, discussed at length for this page.\n"
    path = _make_pdf(tmp_path / "nic.pdf", ["Cover\n"] + [filler * 5] * 20)

    captured = {}

    def fake_call_claude(plan_name, fiscal_year, section_text):
        captured["section_text"] = section_text
        return {"asset_allocation": [], "performance": [], "notes": None}

    monkeypatch.setattr(eci, "call_claude", fake_call_claude)

    session = get_session()
    session.add(Plan(id="nic_ne", name="Nebraska Investment Council",
                     abbreviation="NIC", state="NE", aum_billions=1.0))
    doc = Document(plan_id="nic_ne", url="https://x/nic.pdf", filename="nic.pdf",
                   doc_type="cafr", fiscal_year=2024,
                   downloaded_at=datetime(2026, 4, 5, tzinfo=timezone.utc),
                   extraction_status="done", local_path=path)
    session.add(doc)
    session.flush()
    plan = session.get(Plan, "nic_ne")

    status = eci.extract_one(session, doc, plan,
                             plan_meta={"cafr_format": "standalone"})
    assert status == "saved"
    assert "Some investment content," in captured["section_text"]

    extract = session.query(CafrExtract).filter_by(document_id=doc.id).one()
    assert extract.pages_used == "1-21"
    session.close()


# --- Malformed tool input, and a crash leaking into the next commit --------
# On 2026-10-12 DPERS and ACERA failed with "'str' object has no attribute
# 'get'": the tool call now and then carries a nested array as a JSON string,
# so save_extract iterated its characters. The shared session was never
# rolled back, so the next document's commit saved their half-written,
# row-less extracts, whose matching hash then blocked any retry.

def _cafr_doc(session, plan_id, aum, local_path=None):
    session.add(Plan(id=plan_id, name=plan_id.upper(), abbreviation=plan_id.upper(),
                     state="DE", aum_billions=aum))
    doc = Document(plan_id=plan_id, url=f"https://x/{plan_id}.pdf",
                   filename=f"{plan_id}.pdf", doc_type="cafr", fiscal_year=2025,
                   downloaded_at=datetime(2026, 10, 12, tzinfo=timezone.utc),
                   extraction_status="done", local_path=local_path)
    session.add(doc)
    session.flush()
    return doc


def test_a_stringified_array_is_decoded_and_non_object_rows_are_skipped(tmp_db):
    from database import CafrAllocation, CafrPerformance
    session = get_session()
    doc = _cafr_doc(session, "de_pers", 14.7)
    payload = {
        "asset_allocation": '[{"asset_class": "Domestic Equity", "actual_pct": 43.3}]',
        "performance": ["total fund 11.3%", {"scope": "total_fund", "period": "fy",
                                             "return_pct": 11.3}],
        "notes": None,
    }
    extract = eci.save_extract(session, doc, payload, pages_used="1-2", text_hash="h")

    alloc = session.query(CafrAllocation).filter_by(cafr_extract_id=extract.id).all()
    perf = session.query(CafrPerformance).filter_by(cafr_extract_id=extract.id).all()
    assert [(a.asset_class, a.actual_pct) for a in alloc] == [("Domestic Equity", 43.3)]
    assert [(p.scope, p.return_pct) for p in perf] == [("total_fund", 11.3)]
    session.close()


def test_a_crashed_document_leaves_no_extract_behind(tmp_db, monkeypatch):
    session = get_session()
    bad = _cafr_doc(session, "bad_plan", 20.0)     # larger AUM: processed first
    good = _cafr_doc(session, "good_plan", 10.0)
    session.commit()
    bad_id, good_id = bad.id, good.id
    session.close()

    def fake_extract_one(session, doc, plan, **kw):
        session.add(CafrExtract(plan_id=doc.plan_id, document_id=doc.id,
                                fiscal_year=doc.fiscal_year, text_hash="h"))
        session.flush()
        if doc.plan_id == "bad_plan":
            raise AttributeError("'str' object has no attribute 'get'")
        session.commit()
        return "saved"

    monkeypatch.setattr(eci, "extract_one", fake_extract_one)
    counts = eci.run_extraction()

    assert counts == {"error": 1, "saved": 1}
    session = get_session()
    assert session.query(CafrExtract).filter_by(document_id=bad_id).count() == 0
    assert session.query(CafrExtract).filter_by(document_id=good_id).count() == 1
    session.close()


def test_a_whole_result_serialised_into_one_field_is_unpacked():
    # DPERS FY2025 on 2026-10-13: everything inside the asset_allocation
    # string, and no top-level performance key at all.
    payload = {
        "asset_allocation": '{"asset_allocation": [{"asset_class": "Equity", '
                            '"target_range_low": 50.0}], "performance": '
                            '[{"scope": "total_fund", "period": "fy", "return_pct": 11.3}]}',
        "investment_policy_text": "Prudent person rule.",
        "notes": "Policy ranges only.",
    }
    out = eci._normalize_payload(payload)
    assert out["asset_allocation"] == [{"asset_class": "Equity", "target_range_low": 50.0}]
    assert out["performance"] == [{"scope": "total_fund", "period": "fy", "return_pct": 11.3}]
    assert out["notes"] == "Policy ranges only."


def test_undecodable_or_wrongly_typed_arrays_become_empty():
    out = eci._normalize_payload({"asset_allocation": "not json", "performance": 7})
    assert out["asset_allocation"] == [] and out["performance"] == []
