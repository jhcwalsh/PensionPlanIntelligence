"""Period labels that state a range, an abbreviation, or a bare noun.

These are the last classifiable shapes left in the `unclear` bucket after the
2026-08-31 repair. A range is different in kind from the bare end dates that
deliberately stay unclear: "12/31/24" says only when a period stopped, while
"7/1/2024 - 6/30/2025" gives both ends, so the length is arithmetic rather
than a guess.
"""
import pytest

from scripts.build_performance_view import horizon_of, horizon_key


@pytest.mark.parametrize("label, expected", [
    # A fiscal year written as its two endpoints — 36 rows across three
    # consecutive years in the corpus.
    ("7/1/2024 - 6/30/2025", "annual"),
    ("7/1/2023 - 6/30/2024", "annual"),
    # Two-digit years are the same period stated shorter.
    ("7/1/24 - 6/30/25", "annual"),
    # A quarter, by the same arithmetic.
    ("4/1/2022 - 6/30/2022", "quarter"),
    # An en dash mangled to U+FFFD by the PDF text layer. The corpus contains
    # this exact byte sequence; a separator list of only "-" misses it.
    ("4/1/2024 � 6/30/2024", "quarter"),
    ("4/1/2024 – 6/30/2024", "quarter"),
    ("4/1/2024 to 6/30/2024", "quarter"),
    # ITD = inception to date, the abbreviation for what "Since Inception"
    # spells out. 131 rows.
    ("ITD IRR%", "inception"),
    ("ITD", "inception"),
    # A column headed with the bare noun. 113 rows.
    ("Month", "month"),
])
def test_previously_unclear_labels_are_classified(label, expected):
    assert horizon_of(label) == expected


@pytest.mark.parametrize("label", [
    # The distinction this whole rule rests on: an end date with no start
    # states when a period stopped, never how long it ran. Pension tables
    # report quarter-end and year-end figures from the same four dates a year.
    "12/31/24",
    "12/31/2025",
    # A real six-month period. This vocabulary has no bucket for it, and
    # rounding it to "annual" would be a fabrication rather than an
    # approximation — the narrow spans are deliberate.
    "1/1/2024 - 6/30/2024",
    # Reversed, and zero-length: arithmetic that makes no sense as a period.
    "6/30/2024 - 1/1/2024",
    "6/30/2024 - 6/30/2024",
    # Not a date at all.
    "31/31/2024 - 41/41/2024",
    "?",
])
def test_what_stays_unclear_stays_unclear(label):
    assert horizon_of(label) == "unclear"
    assert horizon_key(label) is None


def test_the_bare_month_rule_does_not_swallow_a_quarter():
    """"3 Month" must stay a quarter. Anchoring the bare-noun rule to the
    whole label is what keeps 180 "3 Mo"/"3 Month" rows out of the month
    column."""
    assert horizon_of("3 Month") == "quarter"
    assert horizon_of("3 Mo") == "quarter"
    assert horizon_of("3 Months Ending 03/31/2026") == "quarter"


def test_itd_does_not_fire_inside_a_word():
    """Word-bounded, so a label that merely contains those letters is safe."""
    assert horizon_of("Bandwidth") != "inception"


def test_a_range_does_not_override_a_label_that_says_what_it_is():
    """The span rule runs last, on purpose. Every other rule reads what the
    label claims; this one measures. A label doing both should be believed."""
    assert horizon_of("3-Year (7/1/2022 - 6/30/2025)") == "multi_year"
    assert horizon_key("3-Year (7/1/2022 - 6/30/2025)") == "3y"
