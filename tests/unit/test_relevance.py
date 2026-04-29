"""Page splitter, relevance filter, and chunker."""

from __future__ import annotations

from lib.pipeline_diagnostic import TASK_PROFILES
from rfp.relevance import (
    Chunk,
    PageText,
    chunk_relevant_pages,
    is_relevant_page,
    split_pages,
)


RFP = TASK_PROFILES["rfp"]


def test_split_pages_recovers_page_numbers():
    text = "[Page 1]\nfirst\n\n[Page 2]\nsecond\n\n[Page 3]\nthird"
    pages = split_pages(text)
    assert [p.page_number for p in pages] == [1, 2, 3]
    assert pages[1].text == "second"


def test_split_pages_handles_no_markers():
    pages = split_pages("just one blob with no markers")
    assert len(pages) == 1
    assert pages[0].page_number == 1


def test_split_pages_empty_input():
    assert split_pages("") == []


def test_is_relevant_page_keyword_match():
    assert is_relevant_page("the board issued an RFP for consulting", RFP)
    assert is_relevant_page("Finalists named: Wilshire, Verus", RFP)


def test_is_relevant_page_no_match():
    assert not is_relevant_page("Quarterly performance returns by asset class", RFP)


def test_is_relevant_page_word_boundary():
    # 'rfpcorp' should not match 'rfp'
    assert not is_relevant_page("rfpcorp filed paperwork", RFP)


def test_chunk_relevant_pages_empty_input():
    assert chunk_relevant_pages([], RFP) == []


def test_chunk_relevant_pages_no_relevant_pages():
    pages = [
        PageText(1, "performance returns"),
        PageText(2, "public comment"),
    ]
    assert chunk_relevant_pages(pages, RFP) == []


def test_chunk_groups_consecutive_relevant_pages_with_context():
    pages = [
        PageText(1, "agenda overview"),
        PageText(2, "RFP issued for consulting"),
        PageText(3, "finalists named: Wilshire"),
        PageText(4, "next agenda item"),
        PageText(5, "performance returns"),
    ]
    chunks = chunk_relevant_pages(pages, RFP)
    assert len(chunks) == 1
    # Run is pages 2-3; expanded by one neighbour each side → 1-4
    assert chunks[0].first_page == 1
    assert chunks[0].last_page == 4


def test_chunk_does_not_straddle_long_gaps():
    pages = [
        PageText(1, "RFP for consultant"),
        PageText(2, "unrelated"),
        PageText(3, "unrelated 2"),
        PageText(4, "unrelated 3"),
        PageText(5, "RFP for actuary"),
    ]
    chunks = chunk_relevant_pages(pages, RFP)
    # Two runs separated by a 3-page gap → two chunks
    assert len(chunks) == 2


def test_chunk_one_page_gap_kept_in_same_run():
    pages = [
        PageText(1, "RFP for consultant issued"),
        PageText(2, "transition slide"),
        PageText(3, "RFP finalists named"),
    ]
    chunks = chunk_relevant_pages(pages, RFP)
    assert len(chunks) == 1
    assert {p.page_number for p in chunks[0].pages} == {1, 2, 3}


def test_chunk_caps_at_max_pages():
    # 8 consecutive relevant pages → chunked into pieces of <= 6
    pages = [PageText(i, "RFP issued for search committee finalists")
             for i in range(1, 9)]
    chunks = chunk_relevant_pages(pages, RFP)
    for c in chunks:
        assert len(c.pages) <= 6


def test_chunk_text_includes_page_markers():
    pages = [
        PageText(2, "RFP for consultant"),
        PageText(3, "finalists"),
    ]
    chunks = chunk_relevant_pages(pages, RFP)
    assert "[Page 2]" in chunks[0].text
    assert "[Page 3]" in chunks[0].text
