"""
Post-generation validator for the CIO Insights note.

Extracts specific claims (doc_id citations, $ amounts, percentages, bps,
vote tallies, bolded entity names) from ``notes/2026_cio_insights.md`` and
checks whether each appears verbatim in the MEETING DATA corpus that was
sent to Claude. Flags unmatched tokens for human review.

This is a "grep sanity check" — it does not understand semantics. Its goal
is to catch the common failure mode where the model invents a plausible-
looking number or manager name that has no basis in the source data.

Usage:
    python validate_insights.py
    python validate_insights.py --note notes/2026_cio_insights.md
    python validate_insights.py --strict        # non-zero exit on any flag
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

from rich.console import Console
from rich.table import Table

from database import SessionLocal
from generate_notes import (
    format_meetings_for_prompt, gather_recent_insights_data, gather_trends_data,
)

console = Console(legacy_windows=False)
ROOT = Path(__file__).parent


# ---------------------------------------------------------------------------
# Token extraction
# ---------------------------------------------------------------------------

# Bold-formatted entities with their inferred kind. We inspect these in order;
# the first match wins. Plan-name + AUM bolds (e.g. "**CalPERS ($502B)**") are
# excluded from the "manager name" check because plan identity is obvious and
# already covered by the doc_id citation.
# Plan-with-AUM bolds — permissive enough to match any of:
#   **CalPERS ($502B)**
#   **Colorado PERA ($60B, CO)**
#   **CalPERS (~$502B, CA)**
#   **Dallas Police & Fire Pension System (~$2.2B AUM)**
#   **SIB-ND (~$27.7B total AUM, ND)**
# Requires a bolded span containing "(~?$N[unit] ...)" — content between
# the unit and the closing paren is free-form (state code, " AUM" etc.).
PLAN_AUM_RE = re.compile(
    r"\*\*[^*]+?\(\s*~?\$[\d,.]+\s*[BMTKbmtk][^)]*\)\*\*"
)
# Dollar amounts — a word boundary after the optional unit stops the
# single-letter units (T/B/M/K) from devouring the first letter of the
# following word (e.g. "$920,000 mainframe" → "$920,000 m").
DOLLAR_RE = re.compile(
    r"\$[\d,]+(?:\.\d+)?(?:\s*(?:trillion|billion|million|thousand|T|B|M|K)\b)?",
    re.IGNORECASE,
)
PERCENT_RE = re.compile(r"-?\d+(?:\.\d+)?%")
BPS_RE = re.compile(r"\b\d+(?:\.\d+)?\s*(?:basis\s+points|bps)\b", re.IGNORECASE)
VOTE_RE = re.compile(
    r"\b(?:approved|voted?|passed)\s+\**(\d+-\d+)\**\b", re.IGNORECASE
)
# Find doc_id references in any form the note may contain:
#   bare           "(doc_id=42)"
#   legacy link    "[doc_id=42](?doc=42)"
#   current link   "[source](?doc=42)"
# All three expose the digit via either ``doc_id=N`` or ``?doc=N``.
DOC_ID_CITATION_RE = re.compile(r"(?:doc_id=|\?doc=)(\d+)")
BOLD_RE = re.compile(r"\*\*([^*]+)\*\*")

# Stop phrases to ignore when checking bolded entities: format markers, not
# factual claims. Colons are stripped before comparison.
BOLD_STOP_PHRASES = {
    "bottom line",
    "practical implication",
    "sources",
    "emerging signal — limited data",
    "emerging signal",
}


def _strip_md_links(text: str) -> str:
    """Remove markdown-link targets so citations don't leak into matches."""
    return re.sub(r"\]\([^)]+\)", "]", text)


def _normalise_dollar(token: str) -> list[str]:
    """Generate plausible variants of a $ amount for substring matching."""
    raw = token.strip()
    variants = {raw, raw.replace(",", "")}
    for v in list(variants):
        # Compact ↔ long unit forms in both directions
        variants.add(v.replace(" billion", "B").replace(" Billion", "B"))
        variants.add(v.replace(" million", "M").replace(" Million", "M"))
        variants.add(v.replace(" trillion", "T").replace(" Trillion", "T"))
        variants.add(re.sub(r"(\d)B\b", r"\1 billion", v))
        variants.add(re.sub(r"(\d)M\b", r"\1 million", v))
        variants.add(re.sub(r"(\d)T\b", r"\1 trillion", v))
        variants.add(v.lstrip("$"))
    return [v for v in variants if v]


def _normalise_percent(token: str) -> list[str]:
    raw = token.strip()
    variants = {raw, raw.replace("%", " percent"), raw.replace("%", "pct")}
    return list(variants)


def _normalise_bps(token: str) -> list[str]:
    raw = token.strip()
    num = re.match(r"(\d+(?:\.\d+)?)", raw).group(1)
    return [raw, f"{num} basis points", f"{num} bps", f"{num}bps"]


def extract_claims(note_text: str) -> dict:
    """Pull the set of checkable tokens out of the briefing."""
    body = _strip_md_links(note_text)

    # Remove the Sources: lines — they carry no factual claim beyond doc_id
    # existence, which we check separately.
    body_no_sources = re.sub(r"^\*Sources:.*$", "", body, flags=re.MULTILINE)

    # Build bolded-entity list, filtering out plan+AUM bolds, stop phrases,
    # and compound numeric claims (individual numbers are checked separately).
    plan_aum_bolds = set(PLAN_AUM_RE.findall(body_no_sources))
    all_bolds = BOLD_RE.findall(body_no_sources)
    entity_bolds = []
    for b in all_bolds:
        t = b.strip()
        stripped = t.lower().rstrip(":").rstrip()
        if stripped in BOLD_STOP_PHRASES:
            continue
        if f"**{t}**" in plan_aum_bolds:
            continue
        # Skip compound numeric claims — any bold that starts with $ or a digit
        # is a quantitative claim whose individual numbers are already checked
        # by the dollar/percent/bps extractors.
        if t and (t[0].isdigit() or t[0] in "$-+"):
            continue
        entity_bolds.append(t)

    return {
        # Scan the raw note text (not the md-link-stripped body) so we
        # catch doc_ids exposed only via ``?doc=N`` in markdown link targets
        # (e.g. ``[source](?doc=42)``).
        "doc_ids": sorted({int(m) for m in DOC_ID_CITATION_RE.findall(note_text)}),
        "dollars": sorted(set(DOLLAR_RE.findall(body_no_sources))),
        "percents": sorted(set(PERCENT_RE.findall(body_no_sources))),
        "bps": sorted(set(BPS_RE.findall(body_no_sources))),
        "votes": sorted({m for m in VOTE_RE.findall(body_no_sources)}),
        "entities": sorted(set(entity_bolds)),
    }


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

def _corpus_contains(corpus: str, token: str) -> bool:
    """Case-insensitive substring match."""
    return token.lower() in corpus.lower()


def _check_any_variant(corpus: str, variants: list[str]) -> bool:
    return any(_corpus_contains(corpus, v) for v in variants)


def verify(claims: dict, corpus: str, corpus_doc_ids: set[int]) -> dict:
    results: dict[str, list[tuple[str, bool]]] = {}

    results["doc_ids"] = [
        (str(d), d in corpus_doc_ids) for d in claims["doc_ids"]
    ]
    results["dollars"] = [
        (t, _check_any_variant(corpus, _normalise_dollar(t))) for t in claims["dollars"]
    ]
    results["percents"] = [
        (t, _check_any_variant(corpus, _normalise_percent(t))) for t in claims["percents"]
    ]
    results["bps"] = [
        (t, _check_any_variant(corpus, _normalise_bps(t))) for t in claims["bps"]
    ]
    results["votes"] = [
        (t, _corpus_contains(corpus, t)) for t in claims["votes"]
    ]
    # For bolded entities, do a simple substring check
    results["entities"] = [
        (t, _corpus_contains(corpus, t)) for t in claims["entities"]
    ]
    return results


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

CATEGORY_LABELS = {
    "doc_ids": "doc_id citations",
    "dollars": "Dollar amounts",
    "percents": "Percentages",
    "bps": "Basis points",
    "votes": "Vote tallies",
    "entities": "Bolded entities (managers/funds/plans)",
}


def print_report(results: dict) -> int:
    """Print a rich table; return count of unmatched tokens."""
    summary = Table(title="CIO Insights Fact-Check Summary",
                    show_lines=False)
    summary.add_column("Category", style="bold")
    summary.add_column("Checked", justify="right")
    summary.add_column("Matched", justify="right", style="green")
    summary.add_column("Unmatched", justify="right", style="red")

    total_unmatched = 0
    for key, label in CATEGORY_LABELS.items():
        items = results.get(key, [])
        matched = sum(1 for _, ok in items if ok)
        unmatched = len(items) - matched
        total_unmatched += unmatched
        summary.add_row(label, str(len(items)), str(matched), str(unmatched))
    console.print(summary)

    # Detail tables for any category with unmatched items
    for key, label in CATEGORY_LABELS.items():
        bad = [t for t, ok in results.get(key, []) if not ok]
        if not bad:
            continue
        detail = Table(title=f"Unmatched — {label}")
        detail.add_column("Token", style="yellow")
        for t in bad:
            detail.add_row(t)
        console.print(detail)

    return total_unmatched


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--note", type=Path,
                        default=ROOT / "notes" / "2026_cio_insights.md",
                        help="Path to the generated CIO Insights markdown file")
    parser.add_argument("--strict", action="store_true",
                        help="Exit non-zero if any unmatched tokens are found")
    args = parser.parse_args()

    if not args.note.exists():
        console.print(f"[red]Note not found: {args.note}[/red]")
        sys.exit(2)

    note_text = args.note.read_text(encoding="utf-8")
    console.print(f"[bold]Loaded:[/bold] {args.note} ({len(note_text):,} chars)")

    # Pick the corpus used when the note was generated. Rolling-window
    # notes are named like "cio_insights_30day.md"; everything else uses
    # the YTD trends corpus.
    window_match = re.match(r"cio_insights_(\d+)day\.md", args.note.name)
    session = SessionLocal()
    if window_match:
        days = int(window_match.group(1))
        console.print(
            f"[bold]Rebuilding {days}-day MEETING DATA corpus from DB...[/bold]"
        )
        data = gather_recent_insights_data(session, days=days)
    else:
        console.print("[bold]Rebuilding YTD MEETING DATA corpus from DB...[/bold]")
        data = gather_trends_data(session)
    corpus = format_meetings_for_prompt(data["meetings"])
    corpus_doc_ids = {
        int(m) for m in re.findall(r"doc_id=(\d+)", corpus)
    }
    console.print(
        f"  corpus: {len(corpus):,} chars, "
        f"{len(corpus_doc_ids)} doc_ids, "
        f"{data['plans_with_activity']} plans, "
        f"{len(data['meetings'])} meetings"
    )

    claims = extract_claims(note_text)
    console.print(
        f"[bold]Extracted claims:[/bold] "
        f"{len(claims['doc_ids'])} doc_ids, "
        f"{len(claims['dollars'])} $ figures, "
        f"{len(claims['percents'])} percents, "
        f"{len(claims['bps'])} bps values, "
        f"{len(claims['votes'])} vote tallies, "
        f"{len(claims['entities'])} bolded entities"
    )

    results = verify(claims, corpus, corpus_doc_ids)
    unmatched = print_report(results)

    if unmatched == 0:
        console.print("[bold green]All checked tokens found in source corpus.[/bold green]")
        sys.exit(0)

    console.print(
        f"[bold yellow]{unmatched} unmatched token(s) — review above for possible hallucinations.[/bold yellow]\n"
        "[dim]Note: this is a substring check. Unmatched does not always mean wrong "
        "(e.g. tokens may be paraphrased or synthesised across summaries), but every "
        "unmatched token deserves human confirmation.[/dim]"
    )
    sys.exit(1 if args.strict else 0)


if __name__ == "__main__":
    main()
