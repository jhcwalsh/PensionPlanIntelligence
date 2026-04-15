"""
Generate analyst notes from summarized pension plan data.

Checks for latest documents across all plans, then uses Claude to produce:
  - 7-Day Highlights: a weekly briefing of recent board/committee activity
  - 2026 Agenda Trends: a cumulative thematic analysis of the year's meetings

Usage:
    python generate_notes.py                    # run pipeline first, then generate notes
    python generate_notes.py --skip-pipeline    # only generate notes (assume DB is current)
    python generate_notes.py --highlights-only  # only generate 7-day highlights
    python generate_notes.py --trends-only      # only generate trends document
    python generate_notes.py --days 14          # use 14-day window for highlights
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
from rich.console import Console
from tenacity import retry, stop_after_attempt, wait_exponential

from database import (
    Document, Plan, Summary, get_new_meetings, get_session, init_db,
)

# Reuse the summarizer's client setup (handles API key + OAuth fallback)
from summarizer import _get_client, MODEL_SONNET

_ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(_ENV_PATH, override=True)

console = Console(legacy_windows=False)
NOTES_DIR = Path(__file__).parent / "notes"

MAX_TOKENS_HIGHLIGHTS = 4096
MAX_TOKENS_TRENDS = 8192

# ---------------------------------------------------------------------------
# Notes-specific Claude wrapper
# ---------------------------------------------------------------------------

NOTES_SYSTEM_PROMPT = """\
You are a senior investment analyst at a pension fund research firm.
You write detailed, analytical markdown briefings for institutional investors
tracking U.S. public pension fund board activity. Your writing style is:
- Precise: include dollar amounts, manager names, vote tallies, return percentages
- Analytical: connect themes across plans and explain significance
- Concise: no filler, no disclaimers, no preamble. Start directly with the content.
- Well-structured: use ## headings organized by theme, not by plan
- Faithful: every figure, manager name, vote tally, and plan position must come
  from the source data provided. When data is absent, say so or stay qualitative.
  Never fabricate numbers, managers, or positions to make a point sound sharper.
- When synthesizing, name the plans that support each theme. A theme with no
  named supporting plans from the data is not a theme — drop it.
Output clean markdown only — no code fences, no JSON, no commentary."""


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=4, max=30))
def generate_note(prompt: str, max_tokens: int) -> str:
    """Call Claude Sonnet to generate an analytical markdown note."""
    message = _get_client().messages.create(
        model=MODEL_SONNET,
        max_tokens=max_tokens,
        temperature=0.2,
        system=NOTES_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text


# ---------------------------------------------------------------------------
# Data gathering
# ---------------------------------------------------------------------------

def _enrich_meeting_summaries(session, meeting: dict) -> list[dict]:
    """Gather all document summaries for a meeting (not just the agenda)."""
    summaries = []
    for doc in meeting["all_docs"]:
        summary = session.query(Summary).filter_by(document_id=doc.id).first()
        if summary:
            summaries.append({
                "doc_id": doc.id,
                "doc_type": doc.doc_type or "document",
                "filename": doc.filename,
                "summary_text": summary.summary_text or "",
                "key_topics": json.loads(summary.key_topics or "[]"),
                "investment_actions": json.loads(summary.investment_actions or "[]"),
                "decisions": json.loads(summary.decisions or "[]"),
                "performance_data": json.loads(summary.performance_data or "[]"),
            })
    return summaries


def gather_highlights_data(session, days: int = 7) -> dict:
    """Collect recent meeting data for the 7-day highlights note."""
    meetings = get_new_meetings(session, days=days)

    if not meetings:
        return {"meetings": [], "date_range": None, "plans_with_activity": 0,
                "total_aum": 0}

    # Enrich with all summaries
    for m in meetings:
        m["all_summaries"] = _enrich_meeting_summaries(session, m)

    # Compute metadata
    dates = [m["meeting_date"] for m in meetings if m["meeting_date"]]
    if dates:
        today = datetime.utcnow()
        latest = min(max(dates), today)
        date_range = (latest - timedelta(days=days), latest)
    else:
        date_range = None
    plan_ids = {m["plan"].id for m in meetings if m["plan"]}
    plans = session.query(Plan).filter(Plan.id.in_(plan_ids)).all()
    total_aum = sum(p.aum_billions or 0 for p in plans)

    return {
        "meetings": meetings,
        "date_range": date_range,
        "plans_with_activity": len(plan_ids),
        "total_aum": total_aum,
    }


def gather_trends_data(session) -> dict:
    """Collect all 2026 meeting data for the agenda trends note."""
    days_since_jan1 = (datetime.utcnow() - datetime(2026, 1, 1)).days + 1
    meetings = get_new_meetings(session, days=days_since_jan1)

    if not meetings:
        return {"meetings": [], "plans_with_activity": 0, "total_aum": 0,
                "date_range_str": "2026"}

    for m in meetings:
        m["all_summaries"] = _enrich_meeting_summaries(session, m)

    plan_ids = {m["plan"].id for m in meetings if m["plan"]}
    plans = session.query(Plan).filter(Plan.id.in_(plan_ids)).all()
    total_aum = sum(p.aum_billions or 0 for p in plans)

    dates = [m["meeting_date"] for m in meetings if m["meeting_date"]]
    if dates:
        earliest = min(dates)
        latest = max(dates)
        date_range_str = f"{earliest.strftime('%B')}–{latest.strftime('%B %Y')}"
    else:
        date_range_str = "2026"

    return {
        "meetings": meetings,
        "plans_with_activity": len(plan_ids),
        "total_aum": total_aum,
        "date_range_str": date_range_str,
    }


# ---------------------------------------------------------------------------
# Prompt construction
# ---------------------------------------------------------------------------

MAX_PROMPT_CHARS = 150_000  # ~37k tokens — keeps input well within context window


def format_meetings_for_prompt(meetings: list[dict]) -> str:
    """Convert enriched meeting data into structured text for Claude.

    Prioritises meetings with investment actions and summaries.
    Truncates at MAX_PROMPT_CHARS to stay within token budget.
    """
    # Sort: meetings with investment actions first, then by date descending
    def _sort_key(m):
        has_actions = any(s.get("investment_actions") for s in m.get("all_summaries", []))
        return (not has_actions, -(m["meeting_date"].timestamp() if m["meeting_date"] else 0))

    sorted_meetings = sorted(meetings, key=_sort_key)

    parts = []
    total_chars = 0
    for m in sorted_meetings:
        plan = m["plan"]
        if not plan:
            continue
        plan_name = plan.abbreviation or plan.name
        aum = f"${plan.aum_billions:.0f}B" if plan.aum_billions else "AUM unknown"
        state = plan.state or ""
        date_str = (m["meeting_date"].strftime("%B %d, %Y")
                    if m["meeting_date"] else "Date unknown")
        doc_types = ", ".join(d.doc_type or "document" for d in m["all_docs"])

        section = f"=== {plan_name} ({aum}, {state}) | {date_str} ===\n"
        section += f"Documents: {len(m['all_docs'])} ({doc_types})\n"

        for i, s in enumerate(m.get("all_summaries", []), 1):
            section += f"\n[Summary {i} — {s['doc_type']} | doc_id={s['doc_id']}]\n"
            section += f"Summary: {s['summary_text']}\n"
            if s["key_topics"]:
                section += f"Topics: {', '.join(s['key_topics'])}\n"
            if s["investment_actions"]:
                section += "Investment Actions:\n"
                for act in s["investment_actions"]:
                    desc = act.get("description", "")
                    amt = act.get("amount_millions")
                    amt_str = f" (${amt}M)" if amt else ""
                    section += f"  - {act.get('action', 'other')}: {desc}{amt_str}\n"
            if s["decisions"]:
                section += "Decisions:\n"
                for dec in s["decisions"]:
                    vote = f" [{dec.get('vote')}]" if dec.get("vote") else ""
                    section += f"  - {dec.get('description', '')}{vote}\n"
            if s["performance_data"]:
                section += "Performance:\n"
                for perf in s["performance_data"]:
                    ret = perf.get("return_pct", "")
                    bench = perf.get("benchmark_pct", "")
                    section += (f"  - {perf.get('asset_class', '')}: "
                                f"{ret}% vs {bench}% benchmark "
                                f"({perf.get('period', '')})\n")

        if total_chars + len(section) > MAX_PROMPT_CHARS:
            remaining = len(sorted_meetings) - len(parts)
            parts.append(f"\n[... {remaining} additional meetings omitted for space ...]\n")
            break

        parts.append(section)
        total_chars += len(section)

    return "\n\n".join(parts)


def build_highlights_prompt(data: dict, days: int) -> str:
    """Build the Claude prompt for 7-day highlights generation."""
    today = datetime.utcnow()
    today_str = today.strftime("%B %d, %Y")

    if data["date_range"]:
        start_str = data["date_range"][0].strftime("%B %#d")
        end_str = data["date_range"][1].strftime("%#d, %Y")
        date_range_title = f"{start_str}–{end_str}"
    else:
        start = today - timedelta(days=days)
        date_range_title = f"{start.strftime('%B %#d')}–{today.strftime('%#d, %Y')}"

    meetings_text = format_meetings_for_prompt(data["meetings"])

    return f"""\
Write a 7-Day Highlights briefing covering U.S. public pension plan board and
investment committee activity for the period: {date_range_title}.

Below is structured data from {data['plans_with_activity']} pension plans that had meetings
or published materials in this period. Synthesize this into an analytical markdown document.

FORMAT REQUIREMENTS:
- Start with exactly: # 7-Day Highlights: {date_range_title}
- Second line must be exactly: *Generated: {today_str}*
- Then a --- horizontal rule
- Use ## headings organized by THEME (not by plan). Good themes include:
  private equity commitments, manager hires/mandate changes, portfolio strategy,
  governance actions, performance data — but choose themes that fit the data
- Bold (**) plan names, dollar amounts, and manager names on first mention
- Include plan AUM in parentheses on first mention of each plan
- End with ## Upcoming Meetings to Watch (bullet list of what's on deck next)
- Target 700–900 words total

SOURCE LINKS:
Each summary in the data below includes a doc_id (e.g. doc_id=42). At the end of
each ## section, add a *Sources:* line listing the documents referenced in that
section as markdown links. Use this exact format for each link:
  [Plan Abbreviation — DocType — Date](?doc=ID)
Example: *Sources: [CalPERS — Agenda — April 02, 2026](?doc=42), [LACERA — Board Pack — March 11, 2026](?doc=58)*
Only cite documents whose content you actually used in that section.

MEETING DATA:
{meetings_text}"""


def build_trends_prompt(data: dict) -> str:
    """Build the Claude prompt for 2026 agenda trends generation."""
    today_str = datetime.utcnow().strftime("%B %d, %Y")
    month_range = data["date_range_str"]
    aum_trillions = data["total_aum"] / 1000
    meetings_text = format_meetings_for_prompt(data["meetings"])

    return f"""\
Write a comprehensive 2026 Meeting Agenda Trends analysis covering all U.S.
public pension plan board and investment committee activity from {month_range}.

Below is structured data from {data['plans_with_activity']} pension plans with documented
meeting activity in 2026, representing approximately ${aum_trillions:.1f} trillion in
combined AUM. Synthesize this into a deep analytical document.

FORMAT REQUIREMENTS:
- Start with exactly: # 2026 Meeting Agenda Trends: Key Topics & Announcements
- Second line: *Covering {month_range} across {data['plans_with_activity']} major U.S. pension plans (~${aum_trillions:.1f} trillion AUM)*
- Third line must be exactly: *Generated: {today_str}*
- Then a --- horizontal rule
- Use ## headings organized by theme. Standard sections:
  1. Private Markets Deployment (PE, credit, real assets, secondaries commitments)
  2. Manager Hires and Mandate Changes
  3. Portfolio Strategy Shifts (allocation reviews, TPA adoption, benchmarks)
  4. Performance Data (fund returns vs benchmarks)
  5. Governance, ESG, and Leadership Transitions
- For each theme, synthesize ACROSS plans — do not list plan-by-plan
- Bold (**) plan names with AUM in parentheses on first mention, dollar amounts,
  and manager names
- Include specific numbers: commitment sizes, return percentages, vote tallies
- Target 1500–1800 words total

SOURCE LINKS:
Each summary in the data below includes a doc_id (e.g. doc_id=42). At the end of
each ## section, add a *Sources:* line listing the documents referenced in that
section as markdown links. Use this exact format for each link:
  [Plan Abbreviation — DocType — Date](?doc=ID)
Example: *Sources: [CalPERS — Agenda — April 02, 2026](?doc=42), [LACERA — Board Pack — March 11, 2026](?doc=58)*
Only cite documents whose content you actually used in that section.

MEETING DATA:
{meetings_text}"""


def build_insights_prompt(data: dict) -> str:
    """Build the Claude prompt for the CIO Insights note."""
    today_str = datetime.utcnow().strftime("%B %d, %Y")
    aum_trillions = data["total_aum"] / 1000
    meetings_text = format_meetings_for_prompt(data["meetings"])

    return f"""\
Write a CIO Insights briefing synthesizing the most important strategic themes and \
implications from U.S. public pension plan board and investment committee activity in 2026.

Below is structured data from {data['plans_with_activity']} pension plans representing \
approximately ${aum_trillions:.1f} trillion in combined AUM.

Think like a Chief Investment Officer advising a large institutional investor. Go beyond \
describing what happened — extract the signals and identify what the pattern of decisions \
means. Where the data contradicts or complicates common assumptions about pension \
allocation trends, say so — but only when you can point to 2+ specific plans in the \
MEETING DATA that demonstrate it. This is strategic analysis, not a summary.

GROUNDING RULES (non-negotiable):
- Every specific figure (%, $, vote tally, fee bps, manager name, asset class \
allocation) MUST appear verbatim in the MEETING DATA below. If a number is not in \
the data, do not state one — use qualitative language instead ("increased materially", \
"a meaningful allocation") or omit the point.
- Every manager, fund, or plan name must appear in the MEETING DATA. Do not introduce \
names from general knowledge.
- Every claim must be traceable to at least one doc_id in the MEETING DATA. If you \
cannot cite a doc_id, do not make the claim.
- If a theme is supported by fewer than 3 plans in the data, either drop it or \
explicitly flag it as "*Emerging signal — limited data*".
- Prefer "no observation" over speculation. It is better to write 1,200 well-sourced \
words than 1,800 with invented detail.

FORMAT REQUIREMENTS:
- Start with exactly: # CIO Insights: 2026 Institutional Trends
- Second line: *Synthesized from board and investment committee activity across \
{data['plans_with_activity']} U.S. public pension plans (~${aum_trillions:.1f} trillion AUM)*
- Third line must be exactly: *Generated: {today_str}*
- Then a --- horizontal rule
- Use numbered ## headings (## 1. Theme Name)
- Each section should end with a bold **Practical implication:** or **Bottom line:** sentence
- Bold plan names with AUM in parentheses on first mention, dollar amounts, manager names
- Include specific data (return percentages, commitment sizes, vote tallies, fee rates) \
ONLY when it appears in MEETING DATA — see GROUNDING RULES above
- Every sentence containing a $ figure, %, bps, vote tally, or manager name must end \
with an inline citation in the form (doc_id=42). The section-level *Sources:* line \
(see below) remains as a summary.
- Do NOT produce a section-by-section recap of each plan — synthesize across plans
- Target 1,200–1,800 words total. Err on the short side if data is thin.

SOURCE LINKS:
Each summary in the data below includes a doc_id (e.g. doc_id=42). Immediately before
the **Practical implication:** / **Bottom line:** sentence at the end of each ## section,
add a *Sources:* line listing the documents referenced in that section as markdown links.
Use this exact format for each link:
  [Plan Abbreviation — DocType — Date](?doc=ID)
Example: *Sources: [CalPERS — Agenda — April 02, 2026](?doc=42), [LACERA — Board Pack — March 11, 2026](?doc=58)*
Only cite documents whose content you actually used in that section.

BEFORE FINALISING:
- Re-read your draft. For every number, manager name, and vote tally, confirm it \
appears in MEETING DATA. Remove or soften any that don't.
- For every theme (##), confirm you named at least 2 plans with supporting evidence \
from the data. If not, drop or soften the theme.
- Confirm every inline (doc_id=N) matches a doc_id that actually appears in MEETING \
DATA.

MEETING DATA:
{meetings_text}"""


# ---------------------------------------------------------------------------
# File output
# ---------------------------------------------------------------------------

# Match a bare citation like:
#   (doc_id=42)
#   (doc_id=1, 5)                  — comma-separated digit list
#   (doc_id=1, doc_id=5)           — repeating key
# ...but not one already wrapped in markdown link syntax
#   ([source](?doc=42))
_BARE_DOC_ID_CITATION_RE = re.compile(
    r"\(doc_id=\d+(?:\s*,\s*(?:doc_id=)?\d+)*\)"
)
_DOC_ID_DIGITS_RE = re.compile(r"\d+")

# Back-compat: rewrite earlier "[doc_id=N](?doc=N)" link text to "[source](?doc=N)"
# so previously-published notes get the nicer visual on the next write.
_OLD_LINKED_CITATION_RE = re.compile(r"\[doc_id=\d+\]\(\?doc=(\d+)\)")


def _linkify_doc_id_citations(text: str) -> str:
    """Rewrite inline ``(doc_id=N)`` citations as clickable markdown links.

    The visible link text is always the word ``source`` — the underlying
    ``?doc=N`` target opens the source document in the Streamlit app.

    Accepts any of these inputs:
      ``(doc_id=42)``                       → ``([source](?doc=42))``
      ``(doc_id=1, 5)``                     → ``([source](?doc=1), [source](?doc=5))``
      ``(doc_id=1, doc_id=5)``              → ``([source](?doc=1), [source](?doc=5))``
      ``([doc_id=42](?doc=42))`` (legacy)   → ``([source](?doc=42))``

    The outer parentheses are preserved so the rendered text still reads
    the same. Each citation becomes a clickable ``(source)`` link
    matching the format used by the existing ``*Sources:*`` line. The
    function is idempotent — re-running on already-linkified text is a
    no-op.
    """
    # Pass 1: convert previously-linkified citations that used the old
    # "doc_id=N" link text to the new "source" link text.
    text = _OLD_LINKED_CITATION_RE.sub(r"[source](?doc=\1)", text)

    # Pass 2: convert bare (doc_id=N) / (doc_id=1, 5) / (doc_id=1, doc_id=5)
    # forms into parenthesised source link(s).
    def _replace(m: "re.Match[str]") -> str:
        ids = _DOC_ID_DIGITS_RE.findall(m.group(0))
        parts = [f"[source](?doc={i})" for i in ids]
        return "(" + ", ".join(parts) + ")"

    return _BARE_DOC_ID_CITATION_RE.sub(_replace, text)


def write_note(content: str, filename: str) -> Path:
    """Write a markdown note to the notes directory."""
    NOTES_DIR.mkdir(exist_ok=True)
    path = NOTES_DIR / filename
    # Strip any leading/trailing code fences Claude might add
    text = content.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
    if text.endswith("```"):
        text = text[:-3].rstrip()
    # Turn inline (doc_id=N) citations into clickable links
    text = _linkify_doc_id_citations(text)
    path.write_text(text, encoding="utf-8")
    console.print(f"[bold green]Wrote {path}[/bold green] ({len(text):,} chars)")
    return path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate analyst notes from pension plan data")
    parser.add_argument("--skip-pipeline", action="store_true",
                        help="Skip fetch/extract/summarize, only generate notes")
    parser.add_argument("--highlights-only", action="store_true",
                        help="Only generate 7-day highlights")
    parser.add_argument("--trends-only", action="store_true",
                        help="Only generate trends document")
    parser.add_argument("--insights-only", action="store_true",
                        help="Only generate CIO insights document")
    parser.add_argument("--days", type=int, default=7,
                        help="Lookback window for highlights (default: 7)")
    parser.add_argument("--no-validate", action="store_true",
                        help="Skip post-generation fact-check of CIO insights")
    parser.add_argument("--strict-validate", action="store_true",
                        help="Exit non-zero if the CIO insights fact-check finds "
                             "any unmatched tokens")
    args = parser.parse_args()

    # Step 1: Optionally run the full pipeline
    if not args.skip_pipeline:
        console.rule("[bold blue]Step 1: Run Pipeline[/bold blue]")
        try:
            from pipeline import run_pipeline
            run_pipeline()
        except Exception as exc:
            console.print(f"[red]Pipeline error: {exc}[/red]")
            console.print("[yellow]Continuing with existing DB data...[/yellow]")

    init_db()
    session = get_session()

    try:
        only_one = args.highlights_only or args.trends_only or args.insights_only
        do_highlights = args.highlights_only or not only_one
        do_trends = args.trends_only or not only_one
        do_insights = args.insights_only or not only_one

        # Step 2: Generate 7-day highlights
        if do_highlights:
            console.rule("[bold blue]Generate 7-Day Highlights[/bold blue]")
            data = gather_highlights_data(session, days=args.days)
            if not data["meetings"]:
                console.print(
                    f"[yellow]No meetings in the last {args.days} days. "
                    f"Skipping highlights.[/yellow]")
            else:
                prompt = build_highlights_prompt(data, days=args.days)
                console.print(
                    f"Calling Claude Sonnet ({len(prompt):,} char prompt, "
                    f"{data['plans_with_activity']} plans, "
                    f"{len(data['meetings'])} meetings)...")
                content = generate_note(prompt, MAX_TOKENS_HIGHLIGHTS)
                today = datetime.utcnow().strftime("%Y-%m-%d")
                write_note(content, f"7day_highlights_{today}.md")

        # Step 3: Generate 2026 agenda trends
        if do_trends:
            console.rule("[bold blue]Generate 2026 Agenda Trends[/bold blue]")
            data = gather_trends_data(session)
            if not data["meetings"]:
                console.print("[yellow]No 2026 meetings found. Skipping trends.[/yellow]")
            else:
                prompt = build_trends_prompt(data)
                console.print(
                    f"Calling Claude Sonnet ({len(prompt):,} char prompt, "
                    f"{data['plans_with_activity']} plans, "
                    f"{len(data['meetings'])} meetings)...")
                content = generate_note(prompt, MAX_TOKENS_TRENDS)
                write_note(content, "2026_meeting_trends_summary.md")

        # Step 4: Generate CIO insights
        if do_insights:
            console.rule("[bold blue]Generate CIO Insights[/bold blue]")
            data = gather_trends_data(session)
            if not data["meetings"]:
                console.print("[yellow]No 2026 meetings found. Skipping insights.[/yellow]")
            else:
                prompt = build_insights_prompt(data)
                console.print(
                    f"Calling Claude Sonnet ({len(prompt):,} char prompt, "
                    f"{data['plans_with_activity']} plans, "
                    f"{len(data['meetings'])} meetings)...")
                content = generate_note(prompt, MAX_TOKENS_TRENDS)
                note_path = write_note(content, "2026_cio_insights.md")

                # Step 4b: Fact-check the generated note against the corpus
                if not args.no_validate:
                    console.rule("[bold blue]Validate CIO Insights[/bold blue]")
                    from validate_insights import (
                        extract_claims, verify, print_report,
                    )
                    corpus = format_meetings_for_prompt(data["meetings"])
                    corpus_doc_ids = {
                        int(m) for m in re.findall(r"doc_id=(\d+)", corpus)
                    }
                    claims = extract_claims(note_path.read_text(encoding="utf-8"))
                    results = verify(claims, corpus, corpus_doc_ids)
                    unmatched = print_report(results)
                    if unmatched == 0:
                        console.print(
                            "[bold green]All checked tokens found in source corpus.[/bold green]"
                        )
                    else:
                        console.print(
                            f"[bold yellow]{unmatched} unmatched token(s) — "
                            f"review above for possible hallucinations.[/bold yellow]"
                        )
                        if args.strict_validate:
                            console.print(
                                "[red]--strict-validate set; treating unmatched tokens as failure.[/red]"
                            )
                            raise SystemExit(1)

        console.rule("[bold green]Notes generation complete[/bold green]")

    finally:
        session.close()


if __name__ == "__main__":
    main()
