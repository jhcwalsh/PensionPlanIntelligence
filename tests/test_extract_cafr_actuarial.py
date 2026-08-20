"""Actuarial extraction: mock round-trip, skip logic, section location."""
import warnings

import fitz
from sqlalchemy import event

from database import CafrActuarial, Document, Plan, get_session
import extract_cafr_actuarial
from extract_cafr_actuarial import (
    MAX_SECTION_PAGES,
    MIN_START_PAGE,
    locate_actuarial_section,
)

# A contents page lists every top-level section, so the anchored
# "Actuarial Section" start pattern matches it just like the real divider.
_CONTENTS = (
    "Table of Contents\n"
    "Introductory Section\n"
    "Financial Section\n"
    "Investment Section\n"
    "Actuarial Section\n"
    "Statistical Section\n"
)

# A dense Financial Section page whose stacked table column header matches
# the "Actuarial Valuation" start pattern (`\s+` spans newlines). Modelled on
# ACERA page 37, which anchored the old range 96 pages early.
_DENSE_TABLE_HEADER = (
    "Actuarial\nValuation\nDate\n"
    + "\n".join(["Notes to the basic financial statements re COLA caps."] * 25)
)

_DIVIDER = 30      # a realistic Actuarial Section start, past MIN_START_PAGE
_STATISTICAL = 40


def _make_pdf(path, pages, toc=None):
    doc = fitz.open()
    for text in pages:
        doc.new_page().insert_text((72, 72), text)
    if toc is not None:
        doc.set_toc(toc)
    doc.save(str(path))
    doc.close()
    return str(path)


def _pad(n, text="Filler content\n"):
    return [text] * n


def _front_matter():
    """Pages 1-29: cover, contents page, then filler up to the divider."""
    return ["Cover\n", _CONTENTS] + _pad(_DIVIDER - 3)


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


def test_revision_past_truncation_cap_is_detected(tmp_db, tmp_path, monkeypatch):
    """A restated CAFR whose leading MAX_SECTION_CHARS chars are unchanged.

    Hashing the *truncated* slice makes the two revisions indistinguishable,
    so extract_one returns "already_have" forever. The hash must be taken
    before truncation. Runs off the mock path (which short-circuits to a
    per-document hash) with PDF handling stubbed out.
    """
    monkeypatch.setenv("LLM_MODE", "live")     # leave extract_one's mock branch
    monkeypatch.setattr(extract_cafr_actuarial, "call_claude",
                        lambda *a, **k: extract_cafr_actuarial.MOCK_PAYLOAD)
    monkeypatch.setattr(extract_cafr_actuarial, "MAX_SECTION_CHARS", 500)
    monkeypatch.setattr(extract_cafr_actuarial, "locate_actuarial_section",
                        lambda path: (1, 1))

    # Identical for the first 500 chars; the restatement differs only after.
    section = ["A" * 1000 + " ORIGINAL"]
    monkeypatch.setattr(extract_cafr_actuarial, "extract_section_text",
                        lambda path, start, end: section[0])

    pdf = tmp_path / "cafr.pdf"
    pdf.write_bytes(b"%PDF-1.4 stub")  # only its existence is checked

    session = get_session()
    d = _seed(session)
    d.local_path = str(pdf)
    session.commit(); session.close()

    assert extract_cafr_actuarial.run_extraction(["p1"])["saved"] == 1

    section[0] = "A" * 1000 + " RESTATED"
    counts = extract_cafr_actuarial.run_extraction(["p1"])
    assert counts["saved"] == 1 and counts["already_have"] == 0


def test_extracted_at_is_timezone_aware(tmp_db):
    """`CafrActuarial.extracted_at` must come from database._utcnow (aware).

    SQLite drops the offset on write, so the value has to be captured at
    insert time; a naive datetime.utcnow() override would also raise a
    DeprecationWarning on 3.12+.
    """
    captured = []

    def _capture(mapper, connection, target):
        captured.append(target.extracted_at)

    event.listen(CafrActuarial, "after_insert", _capture)
    try:
        session = get_session()
        _seed(session); session.close()
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            assert extract_cafr_actuarial.run_extraction(["p1"])["saved"] == 1
    finally:
        event.remove(CafrActuarial, "after_insert", _capture)

    assert len(captured) == 1
    assert captured[0].tzinfo is not None
    assert captured[0].utcoffset().total_seconds() == 0


def test_locate_defaults_unchanged():
    import inspect
    from extract_cafr_investments import locate_investment_section
    sig = inspect.signature(locate_investment_section)
    assert "start_patterns" in sig.parameters and "end_patterns" in sig.parameters


def test_locate_skips_front_matter_contents(tmp_path):
    """The contents page must not anchor the range; the divider page must.

    PyMuPDF splits a TOC's titles and page numbers onto separate lines, so
    "Actuarial Section" on page 2 matches the anchored start pattern exactly
    as the real divider does. Regression: OR-PERS located 5-106 (the Actuarial
    Section actually starts on page 121).
    """
    pages = (_front_matter()
             + ["ACTUARIAL SECTION\n"]                 # 30 — the real divider
             + ["Summary of Actuarial Assumptions\n",  # 31
                "Schedule of Funding Progress\n"]      # 32
             + _pad(_STATISTICAL - _DIVIDER - 3)       # 33-39
             + ["STATISTICAL SECTION\n"]               # 40
             + _pad(3))
    assert locate_actuarial_section(_make_pdf(tmp_path / "c.pdf", pages)) == (
        _DIVIDER, _STATISTICAL - 1)


def test_investment_section_does_not_end_the_range():
    """Investment precedes Actuarial in CAFR ordering, so it can only ever
    terminate a range whose start was matched too early."""
    assert not any(p.search("Investment Section")
                   for p in extract_cafr_actuarial.ACTUARIAL_END)


def test_locate_clamps_when_end_unknown(tmp_path):
    """No Statistical Section => end runs to EOF, so the span is a guess."""
    pages = _front_matter() + ["ACTUARIAL SECTION\n"] + _pad(100)
    pdf = _make_pdf(tmp_path / "long.pdf", pages)
    assert locate_actuarial_section(pdf) == (_DIVIDER,
                                             _DIVIDER + MAX_SECTION_PAGES - 1)


def test_locate_keeps_wide_span_with_known_end(tmp_path):
    """A wide span is not suspicious on its own — VRS's Actuarial Section
    really does run 90 pages (pension + OPEB). Only an unknown end clamps."""
    pages = (_front_matter() + ["ACTUARIAL SECTION\n"] + _pad(89)
             + ["STATISTICAL SECTION\n"] + _pad(3))
    pdf = _make_pdf(tmp_path / "wide.pdf", pages)
    assert locate_actuarial_section(pdf) == (_DIVIDER, _DIVIDER + 89)


def _toc_pdf(tmp_path, name, toc, divider_page=None):
    """A PDF whose only page text is a Statistical Section marker at page 40,
    plus an optional Actuarial divider, so the end always resolves to 39."""
    pages = _pad(45)
    pages[_STATISTICAL - 1] = "STATISTICAL SECTION\n"
    if divider_page is not None:
        pages[divider_page - 1] = "ACTUARIAL SECTION\n"
    return _make_pdf(tmp_path / name, pages, toc=toc)


def test_locate_via_toc_matches_decorated_title(tmp_path):
    """TOC titles carry numbering and parentheticals that the anchored
    page-text patterns cannot match; the TOC pattern must."""
    pdf = _toc_pdf(tmp_path, "toc.pdf",
                   [[1, "Introductory Section", 1],
                    [1, "Financial Section", 3],
                    [1, "Investment Section", 15],
                    [1, "III. Actuarial Section (Unaudited)", _DIVIDER],
                    [1, "Statistical Section", _STATISTICAL]])
    assert locate_actuarial_section(pdf) == (_DIVIDER, _STATISTICAL - 1)


def test_toc_end_ignored_on_flat_outline(tmp_path):
    """KPERS ships 187 level-1 entries, so the next same-level TOC entry is
    the next *heading*, not the next section — the TOC end collapses the
    range to one page. The end must come from the page text instead."""
    pdf = _toc_pdf(tmp_path, "flat.pdf",
                   [[1, "Actuarial Section Cover", _DIVIDER],
                    [1, "Actuarial Certification Letter", _DIVIDER + 1],
                    [1, "Summary of Actuarial Employer", _DIVIDER + 6]])
    assert locate_actuarial_section(pdf) == (_DIVIDER, _STATISTICAL - 1)


def test_toc_requires_the_word_section(tmp_path):
    """A bare `\\bactuarial\\b` TOC pattern matches "Actuarial Assumptions" in
    the Financial Section notes and anchors the range 28 pages early."""
    pdf = _toc_pdf(tmp_path, "assump.pdf",
                   [[1, "Actuarial Assumptions", 22],
                    [1, "Changes in Benefit Terms & Actuarial Assumptions", 25],
                    [1, "Actuarial Section Cover", _DIVIDER]])
    assert locate_actuarial_section(pdf) == (_DIVIDER, _STATISTICAL - 1)


def test_toc_start_in_front_matter_rejected(tmp_path):
    """NMPERA's outline points its "Actuarial Section" entry at page 1 and
    PERS-MS's at page -1; fall through to the text scan."""
    pdf = _toc_pdf(tmp_path, "badtoc.pdf",
                   [[1, "Actuarial Section", 1]], divider_page=_DIVIDER)
    assert locate_actuarial_section(pdf) == (_DIVIDER, _STATISTICAL - 1)


def test_weak_pattern_ignored_on_dense_page(tmp_path):
    """"Actuarial Valuation" on a dense page is a table header, a glossary
    entry, or a mid-narrative subheading — never a section divider."""
    pages = _front_matter() + [_DENSE_TABLE_HEADER] + _pad(19)
    pages.append("ACTUARIAL VALUATION\n")          # 50 — a sparse divider
    pages += _pad(9) + ["STATISTICAL SECTION\n"] + _pad(2)
    assert len(_DENSE_TABLE_HEADER) > extract_cafr_actuarial._DIVIDER_MAX_CHARS
    assert locate_actuarial_section(_make_pdf(tmp_path / "weak.pdf", pages)) == (50, 59)


def test_weak_pattern_alone_on_dense_page_finds_nothing(tmp_path):
    pages = _front_matter() + [_DENSE_TABLE_HEADER] + _pad(10)
    assert locate_actuarial_section(_make_pdf(tmp_path / "dense.pdf", pages)) is None


def test_has_usable_text_rejects_header_only_pages():
    """PERS-OR FY2024's Actuarial Section is scanned images: 46 pages whose
    text layer is 4,088 characters of headers, extracting to all nulls."""
    assert not extract_cafr_actuarial._has_usable_text("x" * 4088, 121, 166)
    assert extract_cafr_actuarial._has_usable_text("x" * 61073, 109, 139)


def test_locate_returns_none_when_absent(tmp_path):
    pdf = _make_pdf(tmp_path / "none.pdf", _pad(MIN_START_PAGE + 5,
                                                "Nothing relevant here\n"))
    assert locate_actuarial_section(pdf) is None
