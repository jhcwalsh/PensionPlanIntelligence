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
