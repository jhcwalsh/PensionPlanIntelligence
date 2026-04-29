"""
Page-level relevance filter and chunker.

The keyword regex from the task profile cuts extraction cost ~10x for
typical board packets — most pages are non-investment content (governance,
HR, public comment) that the LLM doesn't need to see.

Chunks group consecutive relevant pages so the LLM gets enough context to
fill in dates / mandate sizes that may sit a page away from the RFP
keyword. Chunks never straddle non-relevant gaps.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from lib.pipeline_diagnostic import TaskProfile

CHUNK_TARGET_PAGES = 5
CHUNK_MAX_PAGES = 6
CHUNK_MIN_PAGES = 3


# Page splitter for the [Page N] markers that extractor.py inserts.
PAGE_MARKER_RE = re.compile(r"\[Page (\d+)\]\n", re.MULTILINE)


@dataclass(frozen=True)
class PageText:
    page_number: int   # 1-indexed
    text: str


@dataclass(frozen=True)
class Chunk:
    pages: tuple[PageText, ...]

    @property
    def first_page(self) -> int:
        return self.pages[0].page_number

    @property
    def last_page(self) -> int:
        return self.pages[-1].page_number

    @property
    def text(self) -> str:
        return "\n\n".join(f"[Page {p.page_number}]\n{p.text}" for p in self.pages)


def split_pages(extracted_text: str) -> list[PageText]:
    """
    Recover per-page text from the [Page N] markers extractor.py inserts.

    extractor.py emits "[Page 1]\\n...\\n\\n[Page 2]\\n..." — split on the
    marker, keep the page number captured by the regex.
    """
    if not extracted_text:
        return []
    parts = PAGE_MARKER_RE.split(extracted_text)
    # parts[0] is whatever came before [Page 1] (usually empty); after that
    # parts come in pairs of (page_number, page_text).
    pages: list[PageText] = []
    if len(parts) < 3:
        # No markers found — treat the whole thing as page 1.
        return [PageText(page_number=1, text=extracted_text.strip())]
    for i in range(1, len(parts) - 1, 2):
        try:
            n = int(parts[i])
        except ValueError:
            continue
        pages.append(PageText(page_number=n, text=parts[i + 1].strip()))
    return pages


def is_relevant_page(text: str, profile: TaskProfile) -> bool:
    """True if the page text matches any of the profile's relevance keywords."""
    if not text:
        return False
    return bool(profile.relevance_regex.search(text))


def chunk_relevant_pages(pages: list[PageText], profile: TaskProfile) -> list[Chunk]:
    """
    Group consecutive relevant pages into chunks of 3–6 pages.

    A run of relevant pages is grouped whole. To give the LLM a little
    surrounding context, we expand each run by one neighbour on each side
    when those neighbours exist; we never cross a non-relevant gap longer
    than 1 page (so chunks don't straddle unrelated sections).
    """
    if not pages:
        return []

    relevant_idx = [i for i, p in enumerate(pages) if is_relevant_page(p.text, profile)]
    if not relevant_idx:
        return []

    # Group contiguous (or 1-page-apart) relevant indices.
    runs: list[list[int]] = []
    current: list[int] = [relevant_idx[0]]
    for idx in relevant_idx[1:]:
        if idx - current[-1] <= 2:   # allow at most 1-page gap inside a run
            current.append(idx)
        else:
            runs.append(current)
            current = [idx]
    runs.append(current)

    chunks: list[Chunk] = []
    for run in runs:
        start = max(0, run[0] - 1)
        end = min(len(pages) - 1, run[-1] + 1)
        # Cap at CHUNK_MAX_PAGES, splitting if necessary.
        run_pages = pages[start:end + 1]
        for i in range(0, len(run_pages), CHUNK_MAX_PAGES):
            sliced = run_pages[i:i + CHUNK_MAX_PAGES]
            if sliced:
                chunks.append(Chunk(pages=tuple(sliced)))
    return chunks
