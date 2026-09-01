"""The performance view builder, and how it ranks its three sources.

The builder is derived data: the build *is* the definition, so a source it
does not read is a source that does not exist as far as the app is concerned.
document_section_read was added without a consumer, which would have made the
whole targeted read invisible.
"""
import json
from datetime import date

import pytest

import database
from database import (Document, DocumentSectionRead,
                      PlanAssetClassPerformance, Plan, Summary)
from scripts import build_performance_view as bpv


@pytest.fixture
def session(tmp_db):
    s = database.get_session()
    s.add(Plan(id="mcera", name="MCERA", state="CA"))
    s.commit()
    yield s
    s.close()


def _doc(session, when=date(2026, 8, 26), name="pack.pdf"):
    d = Document(plan_id="mcera", url=f"https://x/{name}", filename=name,
                 meeting_date=when, extraction_status="done")
    session.add(d)
    session.commit()
    return d


def _payload(cls, pct, period="1 Year"):
    return json.dumps([{"asset_class": cls, "return_pct": pct,
                        "period": period}])


def test_a_targeted_read_reaches_the_view(session, monkeypatch):
    """Without a collector for it, the whole targeted read is invisible."""
    d = _doc(session)
    session.add(DocumentSectionRead(document_id=d.id, offset=200_000,
                                    returns_json=_payload("Domestic Equity", 12.4)))
    session.commit()

    monkeypatch.setattr("sys.argv", ["build_performance_view"])
    bpv.main()

    rows = session.query(PlanAssetClassPerformance).all()
    assert [r.source for r in rows] == ["targeted_read"]
    assert float(rows[0].return_pct) == 12.4


def test_a_targeted_read_supersedes_the_summariser_for_the_same_document(
        session, monkeypatch):
    """Same document, same table, two readings — one of which saw it.

    Left in together they land in one pick_latest group, so the winner is
    decided by list order rather than by anyone's decision.
    """
    d = _doc(session)
    session.add(Summary(document_id=d.id,
                        performance_data=_payload("Domestic Equity", 9.9)))
    session.add(DocumentSectionRead(document_id=d.id, offset=200_000,
                                    returns_json=_payload("Domestic Equity", 12.4)))
    session.commit()

    monkeypatch.setattr("sys.argv", ["build_performance_view"])
    bpv.main()

    rows = session.query(PlanAssetClassPerformance).all()
    assert len(rows) == 1, "the same class was recorded twice for one document"
    assert rows[0].source == "targeted_read"
    assert float(rows[0].return_pct) == 12.4


def test_the_summariser_still_wins_where_there_was_no_targeted_read(
        session, monkeypatch):
    """Superseding is per document, not global. 300 long documents have no
    candidate section and 4,116 shorter ones were never in scope — their
    summariser figures must survive untouched.

    Two plans, not two documents: pick_latest deliberately keeps one document
    per plan per horizon, so same-plan documents compete rather than coexist.
    """
    session.add(Plan(id="other", name="Other", state="TX"))
    session.commit()

    read = _doc(session, name="read.pdf")
    unread = Document(plan_id="other", url="https://x/unread.pdf",
                      filename="unread.pdf", meeting_date=date(2026, 8, 25),
                      extraction_status="done")
    session.add(unread)
    session.commit()

    session.add(DocumentSectionRead(document_id=read.id, offset=1,
                                    returns_json=_payload("Domestic Equity", 12.4)))
    session.add(Summary(document_id=unread.id,
                        performance_data=_payload("Real Estate", 3.3)))
    session.commit()

    monkeypatch.setattr("sys.argv", ["build_performance_view"])
    bpv.main()

    got = {r.source for r in session.query(PlanAssetClassPerformance).all()}
    assert got == {"targeted_read", "board_doc"}


def test_a_shouted_heading_maps_like_a_title_cased_one():
    """Board tables shout: PUBLIC EQUITY, REAL ESTATE, PRIVATE EQUITY.

    The map was built from title-case labels, so an exact lookup missed all
    of them — 6,360 of 8,373 extracted rows discarded on formatting.
    """
    m = bpv.load_class_map()
    for shouted, titled in (("PUBLIC EQUITY", "Public Equity"),
                            ("REAL ESTATE", "Real Estate"),
                            ("DOMESTIC EQUITY", "Domestic Equity")):
        assert bpv.canonical(shouted, m) == bpv.canonical(titled, m) is not None


def test_case_folding_stops_where_case_actually_carries_meaning():
    """Three labels mean different things in different casings. Folding those
    would silently pick one; they keep today's behaviour of not matching."""
    m = bpv.ClassMap({
        "Total Fixed Income": {"canonical": "fixed_income_core"},
        "TOTAL FIXED INCOME": {"canonical": "total"},
        "Real Assets": {"canonical": "real_assets"},
    })
    assert "total fixed income" not in m.folded
    assert "real assets" in m.folded
    assert bpv.canonical("total fixed income", m) is None
    assert bpv.canonical("REAL ASSETS", m) == "real_assets"


def test_an_empty_read_contributes_nothing(session, monkeypatch):
    """A window with no returns in it is a real and common outcome — 300 of
    810 documents. It must not create a row, and must not crash the build."""
    d = _doc(session)
    session.add(DocumentSectionRead(document_id=d.id, offset=1,
                                    returns_json="[]"))
    session.commit()

    monkeypatch.setattr("sys.argv", ["build_performance_view"])
    bpv.main()

    assert session.query(PlanAssetClassPerformance).count() == 0


# horizon_of -- labels measured in the live corpus (see build_performance_view.py
# module docstring / the row counts that motivated this fix). Before this change
# every one of these except "Since Inception", "3-Year", "FY2025", "Q1 2026" and
# "1 Year" fell through to "unclear", dumping thousands of usable points where
# the app cannot use them.
@pytest.mark.parametrize("label, expected", [
    # CYTD is calendar-year-to-date; the old pattern only allowed an optional
    # leading "f" (FYTD), so "C" broke the word boundary and it never matched.
    # 643 rows in the corpus.
    ("CYTD", "partial"),
    # Bare YTD and FYTD must keep working now that the prefix is unrestricted.
    ("YTD", "partial"),
    ("FYTD", "partial"),
    # "Qtr" is a real-world abbreviation the old _QUARTER regex never spelled
    # out (it only had "quarter" in full). 640 rows.
    ("Last Qtr", "quarter"),
    ("QTD", "quarter"),
    ("3 Mo", "quarter"),
    # "Mo" is the corpus's abbreviation for "Month"; only the unabbreviated
    # forms were recognised before. 285 + 187 rows.
    ("Last Month", "month"),
    ("1 Mo", "month"),
    # FYE / CYE = fiscal/calendar year *end* followed by the end date, not a
    # bare "FYyyyy" the old _FISCAL regex expected. 396 + 381 + 146 rows.
    ("FYE 6/30/25", "annual"),
    ("FYE 6/30/24", "annual"),
    ("CYE 12/31/24", "annual"),
    # A bare four-digit year is how CAFR-derived and some board-pack rows
    # label an annual figure with no other qualifier. 368+328+317+316+299 rows.
    ("2023", "annual"),
    ("2024", "annual"),
    ("2022", "annual"),
    ("2021", "annual"),
    ("2020", "annual"),
    # A bare M/D/YY date states only when a period *ended*, not how long it
    # was -- it could just as easily be a quarter-end as a year-end (pension
    # tables commonly report both from the same 3/31, 6/30, 9/30, 12/31
    # dates). Guessing "annual" would silently mislabel every quarterly row
    # among these 284 + 161 as annual; "unclear" is the honest answer.
    ("12/31/24", "unclear"),
    ("12/31/25", "unclear"),
    # Regression: behaviours the function already had right must not move.
    ("Since Inception", "inception"),
    ("3-Year", "multi_year"),
    ("FY2025", "annual"),
    ("Q1 2026", "quarter"),
    ("1 Year", "annual"),
])
def test_horizon_of_classifies_the_measured_corpus_labels(label, expected):
    assert bpv.horizon_of(label) == expected


# The "1 Yr" bug: _MULTI's negative lookahead only spelled out "1[- ]?year",
# never the "yr" abbreviation, so a bare 1-year figure written as "1 Yr" (572
# rows in the corpus) satisfied the multi-year pattern (digit "1" + "yr") and
# was filed as multi_year -- indistinguishable from an actual 3- or 10-year
# annualised figure. Fixed at the source (_MULTI itself) rather than patched
# in horizon_key, because the mislabelling was already wrong in the existing
# plan_asset_class_performance view, which reads horizon_of directly.
def test_one_yr_abbreviation_is_not_mistaken_for_multi_year():
    assert bpv.horizon_of("1 Yr") == "annual"
    assert bpv.horizon_of("1yr") == "annual"
    assert bpv.horizon_of("1-Yr") == "annual"


# horizon_key -- the finer key the per-asset-class view needs. horizon_of's
# "multi_year" bucket lumps 3/5/10/20/30-year together, which would make a
# 3-year return and a 10-year return look comparable in the same column --
# exactly the mistake horizon_of exists to prevent one level up. horizon_key
# does not replace horizon_of (the existing collated view depends on it
# unchanged); it is a second, finer read of the same label.
@pytest.mark.parametrize("label, expected", [
    # Digit forms, both the loose and hyphenated spelling.
    ("3-Year", "3y"),
    ("5 Year", "5y"),
    ("10-Year", "10y"),
    ("20 Year", "20y"),
    ("30-Year", "30y"),
    ("7 Year", "7y"),
    ("2-Year", "2y"),
    # Word forms attested in the corpus alongside the digit forms.
    ("Three-Year", "3y"),
    ("Five Year", "5y"),
    ("Ten-Year", "10y"),
    ("Twenty Year", "20y"),
    # Everything else in horizon_of's non-multi-year vocabulary passes
    # through unchanged.
    ("Q1 2026", "quarter"),
    ("Last Qtr", "quarter"),
    ("FY2025", "annual"),
    ("1 Year", "annual"),
    ("1 Yr", "annual"),
    ("1 Mo", "month"),
    ("Last Month", "month"),
    ("YTD", "partial"),
    ("CYTD", "partial"),
    ("Since Inception", "inception"),
    # A bare period-end date is 'unclear' in horizon_of -- nothing keyed on
    # an unknown horizon is usable, so horizon_key says so with None rather
    # than inventing a bucket.
    ("12/31/24", None),
    (None, None),
])
def test_horizon_key_classifies_the_measured_corpus_labels(label, expected):
    assert bpv.horizon_key(label) == expected


def test_horizon_key_does_not_mutate_horizon_of():
    """The existing view's horizon column must not move under this change."""
    assert bpv.horizon_of("3-Year") == "multi_year"
    assert bpv.horizon_of("Since Inception") == "inception"


def test_an_actuarial_assumption_is_dropped_not_reclassified():
    """The extractor sometimes puts a forward-looking actuarial assumption --
    'Long-Term Expected Real Rate of Return' -- in the period field of a
    performance_data row: 240 of them in the live corpus. That is not a
    return for any historical period; classifying its period as 'unclear'
    would still let the figure into the performance view sitting next to
    real returns. It has to be dropped as a row in _rows_from_payload, where
    a row is built, not merely mis-horizoned.
    """
    m = bpv.load_class_map()
    payload = json.dumps([
        {"asset_class": "Total Fund", "return_pct": 7.0,
         "period": "Long-Term Expected Real Rate of Return"},
        {"asset_class": "Total Fund", "return_pct": 8.5, "period": "1 Year"},
    ])
    rows = bpv._rows_from_payload(payload, "mcera", date(2026, 8, 26), 1,
                                  m, "board_doc")
    assert len(rows) == 1
    assert rows[0]["period_label"] == "1 Year"
