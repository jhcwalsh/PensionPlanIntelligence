"""Composition adapters for the insights pipeline.

Three composition paths:

* **weekly** — fully reuses ``generate_notes.gather_highlights_data`` +
  ``build_highlights_prompt`` + ``generate_note``. Output is identical
  to ``python generate_notes.py --highlights-only`` for the same date
  window.
* **monthly** — net-new prompt that synthesizes 4 approved weekly
  digests into a monthly CIO Insights briefing.
* **annual** — net-new prompt that synthesizes 12 approved monthlies
  into a year-in-review.

The monthly/annual prompts intentionally mirror the grounding rules
from the existing CIO Insights prompt in ``generate_notes.py`` so the
voice stays consistent across cadences.

In ``INSIGHTS_MODE=mock`` every compose call returns canned Markdown
without touching Anthropic, so tests run in CI without an API key.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Iterable

from insights import config


# ---------------------------------------------------------------------------
# Mock content
# ---------------------------------------------------------------------------

def _mock_markdown(title: str, period_start: date, period_end: date) -> str:
    today = datetime.utcnow().strftime("%B %d, %Y")
    range_str = f"{period_start.isoformat()} – {period_end.isoformat()}"
    return (
        f"# {title}: {range_str}\n"
        f"*Generated: {today}*\n\n"
        "---\n\n"
        "## 1. Mock theme — private equity commitments\n\n"
        "**MockPlan ($100B)** committed **$500M** to a global fund (doc_id=1). "
        "**Bottom line:** This is a canned response; INSIGHTS_MODE=mock is set.\n\n"
        "*Sources: [MockPlan — Agenda — Apr 19, 2026](?doc=1)*\n"
    )


# ---------------------------------------------------------------------------
# Weekly — delegate to existing generate_notes pipeline
# ---------------------------------------------------------------------------

def compose_weekly(session, period_start: date, period_end: date) -> str:
    """Build the 7-Day Digest markdown for ``[period_start, period_end]``.

    Calls the existing ``generate_notes`` corpus gatherer and prompt
    builder so the editorial voice and grounding rules are unchanged.
    """
    if config.is_mock():
        return _mock_markdown("7-Day Highlights", period_start, period_end)

    # Import lazily so mock mode doesn't require anthropic SDK.
    from generate_notes import (
        MAX_TOKENS_HIGHLIGHTS,
        build_highlights_prompt,
        gather_highlights_data,
        generate_note,
    )
    from summarizer import MODEL_SONNET

    days = (period_end - period_start).days + 1
    data = gather_highlights_data(session, days=days)
    if not data["meetings"]:
        return (
            f"# 7-Day Highlights: {period_start.isoformat()} – {period_end.isoformat()}\n"
            f"*Generated: {datetime.utcnow().strftime('%B %d, %Y')}*\n\n"
            "---\n\n"
            "_No board or investment-committee activity recorded in this window._\n"
        )

    prompt = build_highlights_prompt(data, days=days)
    return generate_note(prompt, MAX_TOKENS_HIGHLIGHTS, model=MODEL_SONNET)


# ---------------------------------------------------------------------------
# Monthly — synthesize across 4 approved weeklies (NEW prompt)
# ---------------------------------------------------------------------------
#
# EDITORIAL: review with founder before flipping INSIGHTS_MODE=live for the
# first monthly run. The prompt below is net-new editorial work — see
# DECISIONS.md §1B and §5.

_MONTHLY_SYSTEM_PROMPT = """\
You are a senior investment analyst at a pension fund research firm.
You write monthly synthesis briefings for institutional investors based on
prior weekly briefings that the editor has already reviewed and approved.

Style:
- Precise: include dollar amounts, manager names, vote tallies, and return
  percentages exactly as they appear in the source weeklies.
- Analytical: connect themes that recur across the four weeks; surface
  divergences from the prior month's themes if visible.
- Concise: no filler, no disclaimers, no preamble. Start directly with the
  content.
- Faithful: every figure, manager name, vote tally, and plan position must
  appear verbatim in the source weeklies. Do NOT introduce names or numbers
  from general knowledge or invent supporting detail. If a theme is
  supported by fewer than 2 of the 4 weeks, drop it or flag it as
  *Emerging signal*.
Output clean markdown only — no code fences, no JSON, no commentary."""


def compose_monthly(weekly_markdowns: list[str],
                    period_start: date, period_end: date) -> str:
    """Synthesize 4 approved weekly digests into a monthly CIO Insights.

    The prior weeklies are passed in as their already-approved Markdown
    (``Publication.draft_markdown``). This keeps token cost bounded and
    keeps the narrative consistent with what the founder already signed
    off on.
    """
    if config.is_mock():
        return _mock_markdown("Monthly CIO Insights", period_start, period_end)

    from generate_notes import generate_note  # noqa: F401  (verifies import path)
    from summarizer import _get_client

    today_str = datetime.utcnow().strftime("%B %d, %Y")
    month_label = period_start.strftime("%B %Y")
    weeklies_block = "\n\n".join(
        f"=== Weekly {i + 1} ===\n{md.strip()}"
        for i, md in enumerate(weekly_markdowns)
    )

    user_prompt = f"""\
Write a Monthly CIO Insights briefing for {month_label} synthesizing the four \
approved weekly briefings below into one cohesive narrative for institutional \
investors.

GROUNDING RULES (non-negotiable):
- Every figure (%, $, vote tally, fee bps, manager name, asset class \
allocation) MUST appear verbatim in one of the WEEKLY BRIEFINGS below. If a \
number isn't there, do not state one — use qualitative language or omit.
- Every manager / fund / plan name must appear in the weeklies. Do not \
introduce names from general knowledge.
- Every claim must be traceable to at least one weekly. Drop themes \
supported by fewer than 2 of the 4 weeks, or flag them as \
*Emerging signal — limited data*.

FORMAT REQUIREMENTS:
- Start with exactly: # Monthly CIO Insights: {month_label}
- Second line: *Synthesized from four approved weekly briefings ({period_start.isoformat()} – {period_end.isoformat()})*
- Third line: *Generated: {today_str}*
- Then a --- horizontal rule
- Use numbered ## headings (## 1. Theme Name). Aim for 3–5 themes.
- Each section ends with a bold **Practical implication:** sentence.
- Bold plan names (with AUM in parentheses on first mention), dollar \
amounts, and manager names.
- Every sentence containing a $ figure, %, bps, vote tally, or manager name \
must end with an inline citation in the form (doc_id=42). The cited doc_id \
must be the one whose source weekly attached that specific figure or name to \
that doc_id. If a sentence's figures come from two different docs, split the \
sentence so each cite is unambiguous. The section-level *Sources:* line (see \
below) remains as a summary.
- Target 1,000–1,500 words. Err on the short side if the weeklies are thin.
- Do NOT produce a week-by-week recap — synthesize across weeks.

SOURCE LINKS:
The WEEKLY BRIEFINGS below include inline (doc_id=N) cites and section-level \
*Sources:* lines listing each underlying meeting document as a markdown link. \
Preserve those doc_ids end-to-end in your synthesis. At the end of each ## \
section in your output, add a *Sources:* line listing the documents \
referenced in that section as markdown links. Use this exact format for each \
link:
  [Plan Abbreviation — DocType — Date](?doc=ID)
Example: *Sources: [CalPERS — Agenda — April 02, 2026](?doc=42), [LACERA — Board Pack — March 11, 2026](?doc=58)*
Only cite documents whose content you actually used in that section. The \
*Sources:* line goes immediately before the **Practical implication:** \
sentence at the end of each section.

WEEKLY BRIEFINGS:
{weeklies_block}"""

    message = _get_client().messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        temperature=0.2,
        system=[
            {
                "type": "text",
                "text": _MONTHLY_SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=[{"role": "user", "content": user_prompt}],
    )
    return message.content[0].text


# ---------------------------------------------------------------------------
# Annual — synthesize 12 approved monthlies (NEW prompt)
# ---------------------------------------------------------------------------

_ANNUAL_SYSTEM_PROMPT = """\
You are a senior investment analyst at a pension fund research firm.
You write annual year-in-review briefings for institutional investors based
on twelve approved monthly briefings.

Style:
- Strategic: identify the year's defining themes, not a month-by-month log.
- Analytical: track how themes evolved through the year; call out reversals.
- Faithful: every figure, manager name, vote tally, and plan position must
  appear verbatim in the source monthlies. No general-knowledge embellishment.
- Concise: no filler, no preamble. Start directly with the content.
Output clean markdown only — no code fences, no JSON, no commentary."""


def compose_annual(monthly_markdowns: list[str],
                   period_start: date, period_end: date) -> str:
    """Synthesize 12 approved monthlies into an annual CIO Insights."""
    if config.is_mock():
        return _mock_markdown("Annual CIO Insights", period_start, period_end)

    from generate_notes import generate_note  # noqa: F401
    from summarizer import _get_client

    today_str = datetime.utcnow().strftime("%B %d, %Y")
    year = period_start.year
    monthlies_block = "\n\n".join(
        f"=== {(period_start.replace(month=i + 1)).strftime('%B %Y')} ===\n{md.strip()}"
        for i, md in enumerate(monthly_markdowns)
    )

    user_prompt = f"""\
Write an Annual CIO Insights year-in-review for {year} synthesizing the \
twelve approved monthly briefings below.

GROUNDING RULES (non-negotiable):
- Every figure, manager name, and plan position must appear verbatim in one \
of the MONTHLY BRIEFINGS below.
- Track how each theme evolved month-by-month — call out reversals or \
inflection points where you see them.
- Drop themes that appear in fewer than 3 months, or flag them as \
*Emerging signal*.

FORMAT REQUIREMENTS:
- Start with exactly: # CIO Insights: {year} Year in Review
- Second line: *Synthesized from twelve approved monthly briefings ({period_start.isoformat()} – {period_end.isoformat()})*
- Third line: *Generated: {today_str}*
- Then a --- horizontal rule
- Use numbered ## headings. Aim for 5–8 themes.
- Each section ends with a bold **Practical implication:** sentence.
- Open with a 2–3 sentence executive summary before the first ## section.
- Every sentence containing a $ figure, %, bps, vote tally, or manager name \
must end with an inline citation in the form (doc_id=42). The cited doc_id \
must be the one whose source monthly attached that specific figure or name to \
that doc_id. If a sentence's figures come from two different docs, split the \
sentence so each cite is unambiguous. The section-level *Sources:* line (see \
below) remains as a summary.
- Target 2,000–3,000 words.

SOURCE LINKS:
The MONTHLY BRIEFINGS below include inline (doc_id=N) cites and section-level \
*Sources:* lines listing each underlying meeting document as a markdown link. \
Preserve those doc_ids end-to-end in your synthesis. At the end of each ## \
section in your output, add a *Sources:* line listing the documents \
referenced in that section as markdown links. Use this exact format for each \
link:
  [Plan Abbreviation — DocType — Date](?doc=ID)
Example: *Sources: [CalPERS — Agenda — April 02, 2026](?doc=42), [LACERA — Board Pack — March 11, 2026](?doc=58)*
Only cite documents whose content you actually used in that section. The \
*Sources:* line goes immediately before the **Practical implication:** \
sentence at the end of each section.

MONTHLY BRIEFINGS:
{monthlies_block}"""

    message = _get_client().messages.create(
        model="claude-opus-4-6",
        max_tokens=8192,
        temperature=0.2,
        system=[
            {
                "type": "text",
                "text": _ANNUAL_SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=[{"role": "user", "content": user_prompt}],
    )
    return message.content[0].text


# ---------------------------------------------------------------------------
# Period helpers
# ---------------------------------------------------------------------------

def weekly_period_for(reference: date) -> tuple[date, date]:
    """Return ``(period_start, period_end)`` for the weekly cycle.

    Periods run Sunday→Saturday, identified by ``period_start`` (the
    Sunday). Given a reference date — typically the cron fire date —
    we pick the most recent fully-completed Sun→Sat week.

    Examples:
        reference=Sunday Apr 26 (cron fires) → Sun Apr 19 – Sat Apr 25
        reference=Wednesday Apr 22           → Sun Apr 12 – Sat Apr 18
    """
    # weekday(): Mon=0 .. Sun=6. Distance back to most recent Saturday:
    days_back_to_saturday = (reference.weekday() + 2) % 7  # Sat→0, Sun→1, ...
    period_end = reference - timedelta(days=days_back_to_saturday or 7)
    period_start = period_end - timedelta(days=6)
    return period_start, period_end


def monthly_period_for(reference: date) -> tuple[date, date]:
    """Return ``(first_of_prior_month, last_of_prior_month)``.

    Run on the 1st of each month, this picks up the month that just ended.
    """
    first_of_this_month = reference.replace(day=1)
    last_of_prior = first_of_this_month - timedelta(days=1)
    first_of_prior = last_of_prior.replace(day=1)
    return first_of_prior, last_of_prior


def annual_period_for(reference: date) -> tuple[date, date]:
    """Return ``(jan 1 of prior year, dec 31 of prior year)``."""
    prior_year = reference.year - 1
    return date(prior_year, 1, 1), date(prior_year, 12, 31)
