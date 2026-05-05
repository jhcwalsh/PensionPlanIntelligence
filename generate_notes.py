"""
Generate analyst notes from summarized pension plan data.

Checks for latest documents across all plans, then uses Claude to produce:
  - 7-Day Highlights: a weekly briefing of recent board/committee activity
  - CIO Insights: a strategic thematic analysis of 2026 meeting activity

Usage:
    python generate_notes.py                    # run pipeline first, then generate notes
    python generate_notes.py --skip-pipeline    # only generate notes (assume DB is current)
    python generate_notes.py --highlights-only  # only generate 7-day highlights
    python generate_notes.py --insights-only    # only generate CIO insights document
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

MODEL_OPUS = "claude-opus-4-6"

_ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(_ENV_PATH, override=True)

console = Console(legacy_windows=False)
NOTES_DIR = Path(__file__).parent / "notes"

MAX_TOKENS_HIGHLIGHTS = 4096
MAX_TOKENS_INSIGHTS = 8192

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
def generate_note(prompt: str, max_tokens: int, model: str = MODEL_SONNET) -> str:
    """Call Claude to generate an analytical markdown note.

    The system prompt and MEETING DATA corpus are sent as prompt-cached content
    blocks.  Cache hits occur on retries and same-session re-runs where the
    corpus is unchanged, cutting input-token costs ~90% on those calls.

    Note: meetings are sorted most-recent-first so cross-day cache hits on the
    data block are unlikely (new meetings prepend to the text).  The system
    prompt cache always hits after the first call.
    """
    # Split at the MEETING DATA boundary so the large corpus lives in its own
    # content block and can be cached independently of the variable header.
    split_marker = "\nMEETING DATA:\n"
    if split_marker in prompt:
        instructions, meeting_data = prompt.split(split_marker, 1)
        content = [
            {"type": "text", "text": instructions + "\nMEETING DATA:\n"},
            {
                "type": "text",
                "text": meeting_data,
                "cache_control": {"type": "ephemeral"},
            },
        ]
    else:
        content = [{"type": "text", "text": prompt}]

    message = _get_client().messages.create(
        model=model,
        max_tokens=max_tokens,
        temperature=0.2,
        system=[
            {
                "type": "text",
                "text": NOTES_SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=[{"role": "user", "content": content}],
    )

    # Surface cache activity so we can confirm hits.
    usage = message.usage
    cache_read = getattr(usage, "cache_read_input_tokens", 0) or 0
    cache_write = getattr(usage, "cache_creation_input_tokens", 0) or 0
    if cache_read:
        rate = 15 if model == MODEL_OPUS else 3
        saved = cache_read * rate * 0.9 / 1_000_000
        console.print(
            f"  [dim green][cache hit] {cache_read:,} tokens read "
            f"(~${saved:.3f} saved)[/dim green]"
        )
    elif cache_write:
        console.print(
            f"  [dim][cache write] {cache_write:,} tokens stored[/dim]"
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
                "total_aum": 0, "plans": []}

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
        "plans": plans,
    }


def gather_trends_data(session) -> dict:
    """Collect all 2026 meeting data for the CIO Insights note."""
    days_since_jan1 = (datetime.utcnow() - datetime(2026, 1, 1)).days + 1
    meetings = get_new_meetings(session, days=days_since_jan1)

    if not meetings:
        return {"meetings": [], "plans_with_activity": 0, "total_aum": 0,
                "date_range_str": "2026", "plans": []}

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
        "plans": plans,
    }


def gather_recent_insights_data(session, days: int = 30) -> dict:
    """Collect meetings whose ``meeting_date`` falls within the last N days.

    Unlike :func:`gather_trends_data` (which uses a ``downloaded_at``
    filter inherited from :func:`get_new_meetings`), this gathers by
    true meeting date — meetings that actually happened recently rather
    than documents downloaded recently. Meant for the rolling-window
    CIO Insights variant.
    """
    cutoff = datetime.utcnow() - timedelta(days=days)
    recent_docs = (
        session.query(Document)
        .filter(Document.meeting_date >= cutoff)
        .order_by(Document.meeting_date.desc())
        .all()
    )

    # Group by (plan_id, meeting_date), same grouping shape that
    # get_new_meetings returns so format_meetings_for_prompt works.
    grouped: dict[tuple, dict] = {}
    for doc in recent_docs:
        key = (doc.plan_id, doc.meeting_date)
        if key not in grouped:
            plan = session.get(Plan, doc.plan_id)
            grouped[key] = {
                "plan": plan,
                "meeting_date": doc.meeting_date,
                "all_docs": [],
                "agenda_doc": None,
                "agenda_summary": None,
            }
        grouped[key]["all_docs"].append(doc)
        entry = grouped[key]
        if doc.doc_type == "agenda":
            entry["agenda_doc"] = doc
        elif entry["agenda_doc"] is None and doc.doc_type in ("board_pack", "minutes"):
            entry["agenda_doc"] = doc

    meetings = sorted(
        grouped.values(),
        key=lambda e: e["meeting_date"] or datetime.min,
        reverse=True,
    )

    if not meetings:
        return {"meetings": [], "plans_with_activity": 0, "total_aum": 0,
                "days": days, "date_range_str": f"Last {days} days",
                "plans": []}

    for m in meetings:
        m["all_summaries"] = _enrich_meeting_summaries(session, m)

    plan_ids = {m["plan"].id for m in meetings if m["plan"]}
    plans = session.query(Plan).filter(Plan.id.in_(plan_ids)).all()
    total_aum = sum(p.aum_billions or 0 for p in plans)

    dates = [m["meeting_date"] for m in meetings if m["meeting_date"]]
    if dates:
        earliest = min(dates)
        latest = max(dates)
        date_range_str = f"{earliest.strftime('%B %d, %Y')} – {latest.strftime('%B %d, %Y')}"
    else:
        date_range_str = f"Last {days} days"

    return {
        "meetings": meetings,
        "plans_with_activity": len(plan_ids),
        "total_aum": total_aum,
        "days": days,
        "date_range_str": date_range_str,
        "plans": plans,
    }


# ---------------------------------------------------------------------------
# Prompt construction
# ---------------------------------------------------------------------------

MAX_PROMPT_CHARS = 500_000  # ~125k tokens — well under Sonnet's 200k context


def _format_aum_table(plans: list) -> str:
    """Render the canonical PLAN AUM TABLE block injected into prompts.

    Sorted by registry AUM descending so the largest plans (which dominate
    the narrative) are at the top. Plans without a registry AUM are listed
    as "AUM unknown" so the model knows to source the figure from the
    meeting data with an as-of-date qualifier.
    """
    if not plans:
        return "(no plans referenced)"
    sorted_plans = sorted(
        plans, key=lambda p: -(p.aum_billions or 0)
    )
    lines = []
    for p in sorted_plans:
        abbr = p.abbreviation or p.name
        state = p.state or "?"
        if p.aum_billions:
            aum_str = f"~${p.aum_billions:.0f}B"
        else:
            aum_str = "AUM unknown"
        lines.append(f"- {abbr} ({state}): {aum_str}")
    return "\n".join(lines)


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


def format_weekly_date_range(date_range, days: int) -> str:
    """Render the human-readable date-range string used in the weekly H1.

    Same logic ``compose_weekly`` uses to validate the model's H1, so the
    title produced by the prompt and the title verified post-generation
    cannot drift.
    """
    today = datetime.utcnow()
    if date_range:
        start_dt, end_dt = date_range[0], date_range[1]
    else:
        start_dt, end_dt = today - timedelta(days=days), today
    start_month = start_dt.strftime("%B")
    end_month = end_dt.strftime("%B")
    if start_dt.month == end_dt.month and start_dt.year == end_dt.year:
        return f"{start_month} {start_dt.day}–{end_dt.day}, {end_dt.year}"
    return (
        f"{start_month} {start_dt.day} – "
        f"{end_month} {end_dt.day}, {end_dt.year}"
    )


def build_highlights_prompt(data: dict, days: int) -> str:
    """Build the Claude prompt for 7-day highlights generation."""
    today_str = datetime.utcnow().strftime("%B %d, %Y")
    date_range_title = format_weekly_date_range(data["date_range"], days)

    aum_table = _format_aum_table(data.get("plans") or [])
    meetings_text = format_meetings_for_prompt(data["meetings"])

    return f"""\
Write a 7-Day Highlights briefing covering U.S. public pension plan board and
investment committee activity for the period: {date_range_title}.

Below is structured data from {data['plans_with_activity']} pension plans that had meetings
or published materials in this period. Synthesize this into an analytical markdown document.

GROUNDING RULES (non-negotiable):
- Every specific figure (%, $, vote tally, fee bps, manager name, asset class \
allocation) MUST appear verbatim in the MEETING DATA below. If a number is not in \
the data, do not state one — use qualitative language instead ("a meaningful \
commitment", "increased materially") or omit the point.
- Do NOT compute new figures from source data. No subtracting a return from its \
benchmark to produce a basis-points alpha, no dividing counts to produce a \
percentage, no summing commitments to produce a total. If a derived figure isn't \
already stated verbatim in MEETING DATA, do not state it.
- Every manager, fund, or plan name must appear in the MEETING DATA. Do not \
introduce names from general knowledge.
- Use only the source's own language for WHY things happened or what they \
signify. Do not introduce connectives like "driven by", "reflects", "is \
consistent with", "a notable trend", "suggests", "indicates", or industry \
jargon ("market appreciation", "flight to quality", "crowding") unless that \
exact phrase appears in MEETING DATA. When linking two facts, juxtapose them \
neutrally ("TRS also reported X") rather than asserting a relationship the \
source does not state.
- Every claim must be traceable to at least one doc_id in the MEETING DATA. If \
you cannot cite a doc_id, do not make the claim.
- Prefer "no observation" over speculation. Better to write 600 well-sourced \
words than 900 with invented detail.
- Use the PLAN AUM TABLE below as the canonical AUM for each plan. When you \
mention a plan's AUM (in parentheses on first mention), use the value from \
the table. You MAY substitute a more recent or more precise figure from the \
MEETING DATA — but only if you append an "as of <date>" qualifier (e.g. \
"~$235.2B as of June 30, 2025") and use that same value consistently for \
that plan throughout the rest of the note. Do not switch back and forth.

FORMAT REQUIREMENTS:
- Start with exactly: # 7-Day Highlights: {date_range_title}
- Second line must be exactly: *Generated: {today_str}*
- Then a --- horizontal rule
- Use ## headings organized by THEME (not by plan). Good themes include:
  private equity commitments, manager hires/mandate changes, portfolio strategy,
  governance actions, performance data — but choose themes that fit the data
- Bold (**) plan names, dollar amounts, and manager names on first mention
- Include plan AUM in parentheses on first mention of each plan (see PLAN AUM TABLE)
- Every sentence containing a $ figure, %, bps, vote tally, or manager name \
must end with an inline citation as a parenthesised markdown link in the \
form ([source](?doc=42)). The cited doc must be the one whose summary in \
MEETING DATA contains that specific figure or name verbatim. If a sentence's \
figures come from two different docs, either split the sentence so each cite \
is unambiguous, or list both links: ([source](?doc=42), [source](?doc=58)). \
Concrete example of an acceptable sentence: "**LAFPP** approved a three-year \
contract extension with **MacKay Shields LLC** for high yield fixed income \
through June 30, 2029, with the manager managing **$771.9 million** at \
approximately **33 basis points** ([source](?doc=2069))." The section-level \
*Sources:* line (see below) remains as a summary at the end of each section.
- End with ## Upcoming Meetings to Watch. Today is {today_str}. Each bullet \
must include a dated trigger that has not yet ended as of {today_str}. \
"Has not yet ended" means: a specific calendar date strictly after \
{today_str}; a month whose last day is after {today_str}; a quarter whose \
last day is after {today_str}; or a half-year / "by year-end <YYYY>" whose \
end date is after {today_str}. Omit any bullet whose dated trigger has \
already passed. Omit bullets that reference only a past meeting with no \
forward-looking date — do not invent one. Omit open-ended items ("ongoing", \
"underway", "TBD", "in progress") that have no forward-dated trigger at all. \
If nothing qualifies, write a single bullet: \
"- _No forward-dated meetings or deadlines were identified in the source \
materials._"
- Hard cap 900 words total. If you reach it, drop the weakest-evidenced \
theme entirely rather than trimming a sentence from each.

SOURCE LINKS:
Each summary in MEETING DATA below includes a doc_id (e.g. ``doc_id=42``). \
You must cite sources two ways, and BOTH are required:

1. INLINE — every sentence containing a $ figure, %, bps, vote tally, or \
manager name ends with a parenthesised ``([source](?doc=ID))`` markdown \
link. Use the doc_id whose summary contains that specific figure or name \
verbatim. Multiple cites in one sentence are written \
``([source](?doc=42), [source](?doc=58))``. Do NOT write the literal text \
``(doc_id=42)`` — always write it as the markdown link \
``([source](?doc=42))``.

2. SECTION-LEVEL — at the end of each ## section, add a *Sources:* line \
listing the documents referenced in that section. Use this exact format \
for each link:
  [Plan Abbreviation — DocType — Date](?doc=ID)
Example: *Sources: [CalPERS — Agenda — April 02, 2026](?doc=42), [LACERA — Board Pack — March 11, 2026](?doc=58)*

Only cite documents whose content you actually used in that section.

BEFORE FINALISING — scan the draft for these specific patterns and verify each \
against MEETING DATA. If any item does not match the source, remove it or \
rewrite the sentence to juxtapose facts neutrally rather than asserting a \
relationship.
- bps / basis points figures (especially alpha or excess-return numbers — \
these are the most common arithmetic-derived hallucinations)
- multi-year returns (1-year, 3-year, 5-year, 10-year)
- ratios (Nx, N:1, N-quartile, N% of)
- list counts ("three plans", "all 11 portfolios", "two managers")
- the connective phrases: "consistent with", "reflects", "driven by", \
"a notable", "suggests", "indicates", "underscores"
- every inline ([source](?doc=N)) link: the cited doc must contain the \
specific figure or name in that sentence verbatim, not merely be on the \
same topic. Every sentence containing a $ figure, %, bps, vote tally, or \
manager name MUST have at least one such inline link — if any qualifying \
sentence is missing one, add it before finalising. Do NOT leave bare \
``(doc_id=N)`` text — convert any to ``([source](?doc=N))``.
- AUM consistency: each plan should appear with one and only one AUM value \
throughout the note (the PLAN AUM TABLE value, OR a single override with an \
"as of <date>" qualifier — never both for the same plan).
- ## Upcoming Meetings to Watch: re-read every bullet. Today is {today_str}. \
Delete any bullet whose dated trigger has already ended on or before \
{today_str} (a calendar date, month, or quarter whose end is in the past). \
Delete any bullet that has no forward-dated trigger at all (pure "ongoing", \
"underway", "TBD", "in progress"). If this empties the section, emit the \
single "_No forward-dated meetings or deadlines were identified in the \
source materials._" bullet instead.

PLAN AUM TABLE (canonical reference):
{aum_table}

MEETING DATA:
{meetings_text}"""


def build_insights_prompt(data: dict) -> str:
    """Build the Claude prompt for the CIO Insights note."""
    today_str = datetime.utcnow().strftime("%B %d, %Y")
    aum_trillions = data["total_aum"] / 1000
    aum_table = _format_aum_table(data.get("plans") or [])
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
- Do NOT compute new figures from source data. No subtracting a return from its \
benchmark to produce a basis-points alpha, no dividing counts to produce a \
percentage, no summing commitments to produce a total. If a derived figure isn't \
already stated verbatim in MEETING DATA, do not state it.
- Every manager, fund, or plan name must appear in the MEETING DATA. Do not introduce \
names from general knowledge.
- Use only the source's own language for WHY things happened or what they \
signify. Synthesis prose may use connectives like "driven by", "reflects", \
"is consistent with", "a notable trend", "suggests", "indicates", or industry \
jargon ("market appreciation", "flight to quality", "crowding") ONLY when (a) \
the exact phrase appears in MEETING DATA, OR (b) the connective claim is \
anchored to ≥2 specific named plans whose evidence in MEETING DATA supports \
the relationship being asserted. Otherwise juxtapose facts neutrally.
- Every claim must be traceable to at least one doc_id in the MEETING DATA. If you \
cannot cite a doc_id, do not make the claim.
- If a theme is supported by fewer than 3 plans in the data, either drop it or \
explicitly flag it as "*Emerging signal — limited data*".
- Prefer "no observation" over speculation. It is better to write 1,200 well-sourced \
words than 1,800 with invented detail.
- Use the PLAN AUM TABLE below as the canonical AUM for each plan. When you \
mention a plan's AUM (in parentheses on first mention), use the value from \
the table. You MAY substitute a more recent or more precise figure from the \
MEETING DATA — but only if you append an "as of <date>" qualifier and use \
that same value consistently for that plan throughout the rest of the note.

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
with an inline citation in the form (doc_id=42). The cited doc_id must be the one \
whose summary contains that specific figure or name verbatim. If a sentence's \
figures come from two different docs, split the sentence so each cite is \
unambiguous. The section-level *Sources:* line (see below) remains as a summary.
- Do NOT produce a section-by-section recap of each plan — synthesize across plans
- Hard cap 1,800 words total. If you reach it, drop the weakest-evidenced \
theme entirely rather than trimming a sentence from each.

SOURCE LINKS:
Each summary in the data below includes a doc_id (e.g. doc_id=42). Immediately before
the **Practical implication:** / **Bottom line:** sentence at the end of each ## section,
add a *Sources:* line listing the documents referenced in that section as markdown links.
Use this exact format for each link:
  [Plan Abbreviation — DocType — Date](?doc=ID)
Example: *Sources: [CalPERS — Agenda — April 02, 2026](?doc=42), [LACERA — Board Pack — March 11, 2026](?doc=58)*
Only cite documents whose content you actually used in that section.

BEFORE FINALISING — scan the draft for these specific patterns and verify each \
against MEETING DATA. If any item does not match the source, remove it or \
rewrite the sentence to juxtapose facts neutrally.
- bps / basis points figures (especially alpha or excess-return numbers — \
these are the most common arithmetic-derived hallucinations)
- multi-year returns (1-year, 3-year, 5-year, 10-year)
- ratios (Nx, N:1, N-quartile, N% of)
- list counts ("three plans", "all 11 portfolios", "two managers")
- the connective phrases: "consistent with", "reflects", "driven by", \
"a notable", "suggests", "indicates", "underscores" — each must either appear \
verbatim in MEETING DATA or be anchored to ≥2 specific named plans whose \
evidence supports the relationship
- every theme (##): at least 2 plans named with supporting evidence; if not, \
drop or soften
- every inline (doc_id=N): the cited doc must contain the specific figure or \
name in that sentence verbatim, not merely be on the same topic.
- AUM consistency: each plan should appear with one and only one AUM value \
throughout the note (the PLAN AUM TABLE value, OR a single override with an \
"as of <date>" qualifier — never both for the same plan).

PLAN AUM TABLE (canonical reference):
{aum_table}

MEETING DATA:
{meetings_text}"""


def build_recent_insights_prompt(data: dict) -> str:
    """Build the Claude prompt for the rolling-window (e.g. 30-day) CIO Insights note."""
    today_str = datetime.utcnow().strftime("%B %d, %Y")
    aum_trillions = data["total_aum"] / 1000
    aum_table = _format_aum_table(data.get("plans") or [])
    meetings_text = format_meetings_for_prompt(data["meetings"])
    days = data.get("days", 30)
    window_label = f"the past {days} days"
    date_range = data.get("date_range_str", window_label)

    return f"""\
Write a CIO Insights briefing synthesizing the most important strategic themes and \
implications from U.S. public pension plan board and investment committee activity \
over {window_label} ({date_range}).

Below is structured data from {data['plans_with_activity']} pension plans representing \
approximately ${aum_trillions:.1f} trillion in combined AUM that held meetings in this \
window.

Think like a Chief Investment Officer advising a large institutional investor. Go beyond \
describing what happened — extract the signals and identify what the pattern of decisions \
means. Where the data contradicts or complicates common assumptions about pension \
allocation trends, say so — but only when you can point to 2+ specific plans in the \
MEETING DATA that demonstrate it. Because this is a rolling {days}-day window the \
evidence base is smaller than a YTD briefing; weight observations accordingly and \
prefer depth on a few well-evidenced themes over many thin ones.

GROUNDING RULES (non-negotiable):
- Every specific figure (%, $, vote tally, fee bps, manager name, asset class \
allocation) MUST appear verbatim in the MEETING DATA below. If a number is not in \
the data, do not state one — use qualitative language instead ("increased materially", \
"a meaningful allocation") or omit the point.
- Do NOT compute new figures from source data. No subtracting a return from its \
benchmark to produce a basis-points alpha, no dividing counts to produce a \
percentage, no summing commitments to produce a total. If a derived figure isn't \
already stated verbatim in MEETING DATA, do not state it.
- Every manager, fund, or plan name must appear in the MEETING DATA. Do not introduce \
names from general knowledge.
- Use only the source's own language for WHY things happened or what they \
signify. Synthesis prose may use connectives like "driven by", "reflects", \
"is consistent with", "a notable trend", "suggests", "indicates", or industry \
jargon ("market appreciation", "flight to quality", "crowding") ONLY when (a) \
the exact phrase appears in MEETING DATA, OR (b) the connective claim is \
anchored to ≥2 specific named plans whose evidence in MEETING DATA supports \
the relationship being asserted. Otherwise juxtapose facts neutrally.
- Every claim must be traceable to at least one doc_id in the MEETING DATA. If you \
cannot cite a doc_id, do not make the claim.
- If a theme is supported by fewer than 2 plans in the data, either drop it or \
explicitly flag it as "*Emerging signal — limited data*". (Threshold relaxed from 3 \
to 2 because of the smaller window.)
- Prefer "no observation" over speculation. It is better to write 800 well-sourced \
words than 1,400 with invented detail.
- Use the PLAN AUM TABLE below as the canonical AUM for each plan. When you \
mention a plan's AUM (in parentheses on first mention), use the value from \
the table. You MAY substitute a more recent or more precise figure from the \
MEETING DATA — but only if you append an "as of <date>" qualifier and use \
that same value consistently for that plan throughout the rest of the note.

FORMAT REQUIREMENTS:
- Start with exactly: # CIO Insights: Past {days} Days
- Second line: *Synthesized from board and investment committee activity across \
{data['plans_with_activity']} U.S. public pension plans (~${aum_trillions:.1f} trillion AUM) \
— meetings held {date_range}*
- Third line must be exactly: *Generated: {today_str}*
- Then a --- horizontal rule
- Use numbered ## headings (## 1. Theme Name). Aim for 3–5 themes — fewer if the \
data is thin.
- Each section should end with a bold **Practical implication:** or **Bottom line:** sentence
- Bold plan names with AUM in parentheses on first mention, dollar amounts, manager names
- Include specific data (return percentages, commitment sizes, vote tallies, fee rates) \
ONLY when it appears in MEETING DATA — see GROUNDING RULES above
- Every sentence containing a $ figure, %, bps, vote tally, or manager name must end \
with an inline citation in the form (doc_id=42). The cited doc_id must be the one \
whose summary contains that specific figure or name verbatim. If a sentence's \
figures come from two different docs, split the sentence so each cite is \
unambiguous. The section-level *Sources:* line (see below) remains as a summary.
- Do NOT produce a section-by-section recap of each plan — synthesize across plans
- Hard cap 1,400 words total. If you reach it, drop the weakest-evidenced \
theme entirely rather than trimming a sentence from each.

SOURCE LINKS:
Each summary in the data below includes a doc_id (e.g. doc_id=42). Immediately before
the **Practical implication:** / **Bottom line:** sentence at the end of each ## section,
add a *Sources:* line listing the documents referenced in that section as markdown links.
Use this exact format for each link:
  [Plan Abbreviation — DocType — Date](?doc=ID)
Example: *Sources: [CalPERS — Agenda — April 02, 2026](?doc=42), [LACERA — Board Pack — March 11, 2026](?doc=58)*
Only cite documents whose content you actually used in that section.

BEFORE FINALISING — scan the draft for these specific patterns and verify each \
against MEETING DATA. If any item does not match the source, remove it or \
rewrite the sentence to juxtapose facts neutrally.
- bps / basis points figures (especially alpha or excess-return numbers — \
these are the most common arithmetic-derived hallucinations)
- multi-year returns (1-year, 3-year, 5-year, 10-year)
- ratios (Nx, N:1, N-quartile, N% of)
- list counts ("three plans", "all 11 portfolios", "two managers")
- the connective phrases: "consistent with", "reflects", "driven by", \
"a notable", "suggests", "indicates", "underscores" — each must either appear \
verbatim in MEETING DATA or be anchored to ≥2 specific named plans whose \
evidence supports the relationship
- every theme (##): at least 2 plans named with supporting evidence; if not, \
drop or soften
- every inline (doc_id=N): the cited doc must contain the specific figure or \
name in that sentence verbatim, not merely be on the same topic.
- AUM consistency: each plan should appear with one and only one AUM value \
throughout the note (the PLAN AUM TABLE value, OR a single override with an \
"as of <date>" qualifier — never both for the same plan).

PLAN AUM TABLE (canonical reference):
{aum_table}

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
    parser.add_argument("--insights-only", action="store_true",
                        help="Only generate both CIO insights variants (YTD and 30-day)")
    parser.add_argument("--insights-ytd-only", action="store_true",
                        help="Only generate the YTD CIO insights document")
    parser.add_argument("--insights-30day-only", action="store_true",
                        help="Only generate the rolling 30-day CIO insights document")
    parser.add_argument("--insights-30day-days", type=int, default=30,
                        help="Window length in days for the rolling CIO insights "
                             "note (default: 30)")
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
        only_one = (args.highlights_only or args.insights_only
                    or args.insights_ytd_only or args.insights_30day_only)
        do_highlights = args.highlights_only or not only_one
        # YTD insights run when: default, --insights-only, or --insights-ytd-only
        do_insights_ytd = (args.insights_ytd_only or args.insights_only
                           or not only_one)
        # 30-day insights run when: default, --insights-only, or --insights-30day-only
        do_insights_30day = (args.insights_30day_only or args.insights_only
                             or not only_one)

        def _validate_insights_note(note_path: Path, data: dict, label: str):
            """Fact-check a CIO insights note against the corpus it was built from."""
            if args.no_validate:
                return
            console.rule(f"[bold blue]Validate {label}[/bold blue]")
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
                content = generate_note(prompt, MAX_TOKENS_HIGHLIGHTS, model=MODEL_SONNET)
                today = datetime.utcnow().strftime("%Y-%m-%d")
                write_note(content, f"7day_highlights_{today}.md")

        # Step 3: Generate YTD CIO insights
        if do_insights_ytd:
            console.rule("[bold blue]Generate CIO Insights (YTD)[/bold blue]")
            data = gather_trends_data(session)
            if not data["meetings"]:
                console.print("[yellow]No 2026 meetings found. Skipping YTD insights.[/yellow]")
            else:
                prompt = build_insights_prompt(data)
                console.print(
                    f"Calling Claude Opus ({len(prompt):,} char prompt, "
                    f"{data['plans_with_activity']} plans, "
                    f"{len(data['meetings'])} meetings)...")
                content = generate_note(prompt, MAX_TOKENS_INSIGHTS, model=MODEL_OPUS)
                note_path = write_note(content, "2026_cio_insights.md")
                _validate_insights_note(note_path, data, "CIO Insights (YTD)")

        # Step 4: Generate rolling 30-day CIO insights
        if do_insights_30day:
            window_days = args.insights_30day_days
            console.rule(
                f"[bold blue]Generate CIO Insights ({window_days}-day)[/bold blue]"
            )
            data = gather_recent_insights_data(session, days=window_days)
            if not data["meetings"]:
                console.print(
                    f"[yellow]No meetings in the last {window_days} days. "
                    f"Skipping {window_days}-day insights.[/yellow]"
                )
            else:
                prompt = build_recent_insights_prompt(data)
                console.print(
                    f"Calling Claude Sonnet ({len(prompt):,} char prompt, "
                    f"{data['plans_with_activity']} plans, "
                    f"{len(data['meetings'])} meetings)...")
                content = generate_note(prompt, MAX_TOKENS_INSIGHTS)
                note_path = write_note(content, f"cio_insights_{window_days}day.md")
                _validate_insights_note(
                    note_path, data, f"CIO Insights ({window_days}-day)"
                )

        console.rule("[bold green]Notes generation complete[/bold green]")

    finally:
        session.close()


if __name__ == "__main__":
    main()
