"""Ranking candidate sections in a long document.

The premise of the targeted read is that the right 30,000 characters can be
found for free. These tests pin the ranking, because a wrong window is
indistinguishable from a wrong model: both produce plausible-looking figures
from the wrong part of the document.
"""
from section_finder import find_candidates, Candidate


def test_finds_a_heading_on_its_own_line():
    text = "preamble\n" * 100 + "\nASSET ALLOCATION\n" + "table rows\n" * 50
    got = find_candidates(text)
    assert got and "ASSET ALLOCATION" in got[0].heading


def test_prose_mentions_rank_below_headings():
    """The measured failure mode: 99 hits on a real pack, nearly all prose.

    'we provide holistic asset allocation advice' is a sentence; 'Asset
    Allocation' alone on a line is a heading. Taking the first match reads
    the wrong slice of a 1.1 MB document.
    """
    text = ("The consultant will provide holistic asset allocation advice "
            "to the board in due course.\n") * 20 + "\nAsset Allocation\n" + "x\n" * 40
    got = find_candidates(text)
    assert "Asset Allocation" == got[0].heading.strip()


def test_numeric_density_lifts_a_real_table():
    """A heading followed by numbers beats one followed by prose."""
    prose = "\nRates of Return\n" + "discussion of philosophy\n" * 30
    table = "\nRates of Return\n" + "Domestic Equity 12.4 11.8 9.2\n" * 30
    got = find_candidates(prose + table)
    assert got[0].offset > len(prose) - 1


def test_returns_nothing_when_there_is_nothing():
    assert find_candidates("minutes of a routine meeting\n" * 200) == []


def test_caps_the_number_of_candidates():
    """Spread the blocks past the dedup radius, or one block absorbs them all.

    The cap and the near-duplicate filter both bound the list, and only the
    filter fires when every hit sits within WINDOW//2 of the last. Padding
    each block past that radius is what makes this a test of the cap.
    """
    from section_finder import WINDOW
    block = "\nAsset Allocation\n" + "1.0 2.0 3.0\n" * 5
    text = (block + "filler\n" * (WINDOW // 10)) * 40
    got = find_candidates(text, max_candidates=6)
    assert len(got) == 6, "expected the cap to bind, got %d" % len(got)


def test_a_candidate_is_hashable_and_comparable():
    """Frozen, because the CLI puts candidates in sets to skip re-reads."""
    c = Candidate(offset=1, heading="Asset Allocation", score=2.0)
    assert c == Candidate(offset=1, heading="Asset Allocation", score=2.0)
    assert len({c, c}) == 1
