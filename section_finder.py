"""Locate the parts of a long document that might hold a returns table.

Free: pure text, no API, no I/O. That matters because the alternative --
sending a 1.3 MB document to a model to be told where its tables are --
costs more than reading the tables.

Scoring is the whole content of this module. A naive keyword search over a
real board pack returns 99 hits of which nearly all are prose ("the
consultant will provide holistic asset allocation advice"). Reading the
first match reads the wrong slice.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

WINDOW = 30_000          # chars handed to the extractor around a hit
_HEADINGS = re.compile(
    r"(?im)^(.{0,80}?)(asset allocation|total fund performance"
    r"|performance summary|investment performance|rates? of return"
    r"|manager performance|portfolio performance)(.{0,60})$")
_NUMBER = re.compile(r"-?\d+\.\d")


@dataclass(frozen=True)
class Candidate:
    offset: int
    heading: str
    score: float


def _numeric_density(text: str) -> float:
    """Fraction of lines that look like table rows."""
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        return 0.0
    return sum(1 for ln in lines if len(_NUMBER.findall(ln)) >= 2) / len(lines)


def find_candidates(text: str, max_candidates: int = 12) -> list[Candidate]:
    if not text:
        return []
    scored: list[Candidate] = []
    for m in _HEADINGS.finditer(text):
        before, term, after = m.group(1), m.group(2), m.group(3)
        line = m.group(0).strip()

        # A heading is short and mostly the term itself. A sentence that
        # happens to contain the term is long and has words either side.
        clutter = len(before.strip()) + len(after.strip())
        score = 1.0 if clutter <= 4 else 1.0 / (1 + clutter / 10)

        # Numbers just below it are the strongest signal that this is a table.
        following = text[m.end():m.end() + 4_000]
        score += 2.0 * _numeric_density(following)

        scored.append(Candidate(offset=m.start(), heading=line, score=score))

    scored.sort(key=lambda c: (-c.score, c.offset))
    # Drop near-duplicates: one table produces several adjacent headings.
    kept: list[Candidate] = []
    for c in scored:
        if any(abs(c.offset - k.offset) < WINDOW // 2 for k in kept):
            continue
        kept.append(c)
        if len(kept) >= max_candidates:
            break
    return [c for c in kept if c.score >= 0.5]


def window_for(text: str, candidate: Candidate) -> str:
    """The slice to hand a model: the heading and what follows it."""
    start = max(0, candidate.offset - 500)
    return text[start:start + WINDOW]
