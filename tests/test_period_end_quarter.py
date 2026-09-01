"""Which quarter does a return actually refer to?

The performance tables carried an "As of" date and a verbatim period label,
and neither answers the question a reader asks first: is this 2026Q1 or
2025Q4? The label is verbatim and inconsistent ("FY2025", "1 Yr.",
"12 months ended March 31, 2026"), and the date is the *document's* date --
a board pack presented on 14 May 2026 reports figures through 31 March.
Sorting or filtering on either one mixes quarters.

queries.period_end resolves both into one period-end date, and
queries.quarter_label buckets it. The precedence is deliberate: what the
label *states* always beats what the document's date *implies*.
"""
from datetime import date

import pytest

from queries import period_end, period_end_quarter, quarter_label


MEETING = date(2026, 5, 14)          # a real mid-quarter board meeting


# --------------------------------------------------------------------------
# quarter_label -- the bucket
# --------------------------------------------------------------------------

@pytest.mark.parametrize("d, expected", [
    (date(2026, 1, 1), "2026Q1"),
    (date(2026, 3, 31), "2026Q1"),
    (date(2026, 4, 1), "2026Q2"),
    (date(2026, 6, 30), "2026Q2"),
    (date(2026, 9, 30), "2026Q3"),
    (date(2025, 12, 31), "2025Q4"),
])
def test_quarter_label_buckets_by_the_quarter_the_period_ends_in(d, expected):
    assert quarter_label(d) == expected


def test_quarter_label_of_nothing_is_nothing():
    assert quarter_label(None) is None


# --------------------------------------------------------------------------
# A date stated in the label wins outright
# --------------------------------------------------------------------------

@pytest.mark.parametrize("label, expected", [
    ("1-Year (12/31/25)", date(2025, 12, 31)),
    ("1-year (as of 9/30/2025)", date(2025, 9, 30)),
    ("3-Year as of 6/30/2025", date(2025, 6, 30)),
    ("YTD through 11/20/2025", date(2025, 11, 20)),
    ("10-Year Return (09/30/2025)", date(2025, 9, 30)),
    # A range states both ends; the period ends at the later one.
    ("4/1/2024 - 6/30/2024", date(2024, 6, 30)),
    ("7/1/2024 - 6/30/2025", date(2025, 6, 30)),
    # Month names are dates too, and the corpus writes them out.
    ("12 months ended March 31, 2026", date(2026, 3, 31)),
    ("Fiscal year ended June 30, 2025", date(2025, 6, 30)),
    # A month and a year with no day: the period ends when the month does.
    ("1-Year ending May 2026", date(2026, 5, 31)),
    ("1-Year ending February 2024", date(2024, 2, 29)),   # leap year
])
def test_a_date_in_the_label_is_the_period_end(label, expected):
    assert period_end(label, MEETING) == expected


def test_two_digit_years_are_this_century():
    assert period_end("12/31/24", MEETING) == date(2024, 12, 31)


def test_an_impossible_date_falls_through_but_its_year_survives():
    """Free text from a PDF text layer: it must not raise. Falling on to the
    bare-year rule keeps the one part of "31/31/2024" that is legible, which
    beats reaching for the meeting date and filing a 2024 figure under 2026."""
    assert period_end("31/31/2024", MEETING) == date(2024, 12, 31)


# --------------------------------------------------------------------------
# Named periods, when no date is spelled out
# --------------------------------------------------------------------------

@pytest.mark.parametrize("label, expected", [
    ("Q1 2026", date(2026, 3, 31)),
    ("Q4 2025", date(2025, 12, 31)),
    ("2026 Q2", date(2026, 6, 30)),
    ("30 Year (Q3 2022)", date(2022, 9, 30)),
    # Fiscal years take the June 30 end that collect_from_cafr already
    # assumes -- the dominant US public-pension convention, and the two
    # tables must not disagree about what FY2025 means.
    ("FY2025", date(2025, 6, 30)),
    ("FY 2023", date(2023, 6, 30)),
    ("fiscal year 2024", date(2024, 6, 30)),
    # Calendar years end in December, which is the whole reason the corpus
    # bothers to say "calendar".
    ("Calendar Year 2025", date(2025, 12, 31)),
    ("CY2023 (1-Year)", date(2023, 12, 31)),
    ("2024", date(2024, 12, 31)),
])
def test_a_named_period_gives_its_own_end(label, expected):
    assert period_end(label, MEETING) == expected


def test_the_label_beats_the_document_date():
    """The point of the precedence. A 2025 CAFR discussed at a 2026 meeting
    is a 2025 figure."""
    assert period_end("FY2025", date(2026, 5, 14)) == date(2025, 6, 30)


# --------------------------------------------------------------------------
# Relative labels: 54% of the corpus, and no date anywhere in them
# --------------------------------------------------------------------------

@pytest.mark.parametrize("label", ["1 Year", "1 Yr.", "3-Year", "10 Year",
                                   "1 Quarter", "3 Mo", "ITD IRR%", "Month",
                                   "", None])
def test_a_relative_label_falls_back_to_the_last_completed_quarter(label):
    """A pack presented on 14 May reports through 31 March: the quarter that
    had actually closed when it was written. Rounding *back* rather than to
    the nearest quarter is the conservative direction -- it never claims a
    figure is fresher than the document carrying it."""
    assert period_end(label, date(2026, 5, 14)) == date(2026, 3, 31)


@pytest.mark.parametrize("meeting, expected", [
    (date(2026, 3, 31), date(2026, 3, 31)),   # already a quarter end
    (date(2026, 4, 1), date(2026, 3, 31)),
    (date(2026, 1, 2), date(2025, 12, 31)),   # back across the year boundary
    (date(2026, 12, 31), date(2026, 12, 31)),
])
def test_the_fallback_rounds_back_never_forward(meeting, expected):
    assert period_end("1 Year", meeting) == expected


def test_no_label_and_no_date_is_unknown():
    assert period_end(None, None) is None
    assert period_end_quarter("1 Year", None) is None


# --------------------------------------------------------------------------
# The composed helper the tables actually call
# --------------------------------------------------------------------------

def test_period_end_quarter_composes_the_two():
    assert period_end_quarter("Q1 2026", None) == "2026Q1"
    assert period_end_quarter("FY2025", None) == "2025Q2"
    assert period_end_quarter("1 Year", date(2026, 5, 14)) == "2026Q1"


# --------------------------------------------------------------------------
# Shapes the live corpus produced that a first pass got wrong. Each of these
# landed in a quarter that had not happened yet.
# --------------------------------------------------------------------------

@pytest.mark.parametrize("label, expected", [
    # Abbreviated months. Falling through to the bare-year rule dated five
    # pera_colorado rows to 2026Q4.
    ("1-Year ending Mar 31, 2026", date(2026, 3, 31)),
    ("1-Year (through Feb 2026)", date(2026, 2, 28)),
    ("Sept 30, 2025", date(2025, 9, 30)),
    # The quarter written after its number rather than before.
    ("1Q 2026", date(2026, 3, 31)),
    ("4Q 2025", date(2025, 12, 31)),
])
def test_shapes_the_corpus_uses_that_a_first_pass_missed(label, expected):
    assert period_end(label, date(2026, 6, 30)) == expected


def test_a_month_name_must_be_a_whole_word():
    """The month alternation is bounded on both sides. "mar" inside a longer
    word is not March, and matching it would date a row from a stray noun."""
    assert period_end("marketing 2026 review", date(2026, 6, 30)) == date(2026, 12, 31)


@pytest.mark.parametrize("label", ["YTD 2026", "Year to Date 2026",
                                   "CYTD 2026", "FYTD 2026"])
def test_year_to_date_does_not_end_in_december(label):
    """A YTD label names the year its period runs *in*. Reading it as a whole
    year dated thirteen live rows to 2026Q4 -- a quarter that had not
    happened. The document's own date is the honest answer."""
    assert period_end(label, date(2026, 7, 31)) == date(2026, 6, 30)


def test_year_to_date_still_believes_a_date_it_states():
    assert period_end("YTD through 11/20/2025", date(2026, 7, 31)) == date(2025, 11, 20)
