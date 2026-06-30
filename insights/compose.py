"""Composition adapters for the insights pipeline.

Three composition paths:

* **weekly** — fully reuses ``generate_notes.gather_highlights_data`` +
  ``build_highlights_prompt`` + ``generate_note``. Output is identical
  to ``python generate_notes.py --highlights-only`` for the same date
  window.
* **monthly** — net-new prompt that synthesizes 4 approved weekly
  digests into a monthly Insights briefing.
* **annual** — net-new prompt that synthesizes 12 approved monthlies
  into a year-in-review.

The monthly/annual prompts intentionally mirror the grounding rules
from the existing Insights prompt in ``generate_notes.py`` so the
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
        format_weekly_date_range,
        gather_highlights_data,
        generate_note,
        inject_highlights_preamble,
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
    markdown = generate_note(prompt, MAX_TOKENS_HIGHLIGHTS, model=MODEL_SONNET)

    expected_title = (
        f"# 7-Day Highlights: {format_weekly_date_range(data['date_range'], days)}"
    )
    actual_title = markdown.split("\n", 1)[0]
    if actual_title != expected_title:
        raise ValueError(
            f"Weekly H1 title mismatch — the model produced a non-conforming "
            f"title; aborting publish.\n"
            f"  expected: {expected_title!r}\n"
            f"  actual:   {actual_title!r}"
        )
    return inject_highlights_preamble(markdown, data["new_doc_count"], days)


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
    """Synthesize 4 approved weekly digests into a monthly Insights.

    The prior weeklies are passed in as their already-approved Markdown
    (``Publication.draft_markdown``). This keeps token cost bounded and
    keeps the narrative consistent with what the founder already signed
    off on.
    """
    if config.is_mock():
        return _mock_markdown("Monthly Insights", period_start, period_end)

    from generate_notes import generate_note  # noqa: F401  (verifies import path)
    from summarizer import _get_client

    today_str = datetime.utcnow().strftime("%B %d, %Y")
    month_label = period_start.strftime("%B %Y")
    weeklies_block = "\n\n".join(
        f"=== Weekly {i + 1} ===\n{md.strip()}"
        for i, md in enumerate(weekly_markdowns)
    )

    user_prompt = f"""\
Write a Monthly Insights briefing for {month_label} synthesizing the four \
approved weekly briefings below into one cohesive narrative for institutional \
investors.

GROUNDING RULES (non-negotiable):
- Every figure (%, $, vote tally, fee bps, manager name, asset class \
allocation) MUST appear verbatim in one of the WEEKLY BRIEFINGS below. If a \
number isn't there, do not state one — use qualitative language or omit.
- Do NOT compute new figures from source data. No subtracting a return from \
its benchmark to produce a basis-points alpha, no dividing counts to produce \
a percentage, no summing commitments to produce a total. If a derived figure \
isn't already stated verbatim in WEEKLY BRIEFINGS, do not state it.
- Every manager / fund / plan name must appear in the weeklies. Do not \
introduce names from general knowledge.
- Use only the source's own language for WHY things happened or what they \
signify. Synthesis prose may use connectives like "driven by", "reflects", \
"is consistent with", "a notable trend", "suggests", "indicates", or industry \
jargon ("market appreciation", "flight to quality", "crowding") ONLY when (a) \
the exact phrase appears in WEEKLY BRIEFINGS, OR (b) the connective claim is \
anchored to ≥2 specific named plans whose evidence in WEEKLY BRIEFINGS \
supports the relationship being asserted. Otherwise juxtapose facts neutrally.
- Every claim must be traceable to at least one weekly. Drop themes \
supported by fewer than 2 of the 4 weeks, or flag them as \
*Emerging signal — limited data*.
- AUM consistency: when the source weeklies show different AUM values for \
the same plan (e.g. one weekly says "TRS Texas (~$200B)" and another says \
"TRS Texas (~$235.2B as of June 30, 2025)"), pick ONE — preferring the \
value with an "as of <date>" qualifier — and use it consistently throughout \
the monthly. Do not switch values within a single note.

FORMAT REQUIREMENTS:
- Start with exactly: # Monthly Insights: {month_label}
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
- Hard cap 1,500 words total. If you reach it, drop the weakest-evidenced \
theme entirely rather than trimming a sentence from each.
- Do NOT produce a week-by-week recap — synthesize across weeks.

BEFORE FINALISING — scan the draft for these specific patterns and verify \
each against WEEKLY BRIEFINGS. If any item does not match the source, remove \
it or rewrite the sentence to juxtapose facts neutrally.
- bps / basis points figures (especially alpha or excess-return numbers — \
these are the most common arithmetic-derived hallucinations)
- multi-year returns (1-year, 3-year, 5-year, 10-year)
- ratios (Nx, N:1, N-quartile, N% of)
- list counts ("three plans", "all 11 portfolios", "two managers")
- the connective phrases: "consistent with", "reflects", "driven by", \
"a notable", "suggests", "indicates", "underscores" — each must either appear \
verbatim in WEEKLY BRIEFINGS or be anchored to ≥2 specific named plans whose \
evidence supports the relationship
- every theme (##): supported by at least 2 of the 4 weeks; if not, drop or \
flag as *Emerging signal — limited data*
- every inline (doc_id=N): the cited doc must be one whose source weekly \
attached that specific figure or name to that doc_id, not merely the same topic.

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
    """Synthesize 12 approved monthlies into an annual Insights."""
    if config.is_mock():
        return _mock_markdown("Annual Insights", period_start, period_end)

    from generate_notes import generate_note  # noqa: F401
    from summarizer import _get_client

    today_str = datetime.utcnow().strftime("%B %d, %Y")
    year = period_start.year
    monthlies_block = "\n\n".join(
        f"=== {(period_start.replace(month=i + 1)).strftime('%B %Y')} ===\n{md.strip()}"
        for i, md in enumerate(monthly_markdowns)
    )

    user_prompt = f"""\
Write an Annual Insights year-in-review for {year} synthesizing the \
twelve approved monthly briefings below.

GROUNDING RULES (non-negotiable):
- Every figure, manager name, and plan position must appear verbatim in one \
of the MONTHLY BRIEFINGS below.
- Do NOT compute new figures from source data. No subtracting a return from \
its benchmark to produce a basis-points alpha, no dividing counts to produce \
a percentage, no summing commitments to produce a total. If a derived figure \
isn't already stated verbatim in MONTHLY BRIEFINGS, do not state it.
- Use only the source's own language for WHY things happened or what they \
signify. Synthesis prose may use connectives like "driven by", "reflects", \
"is consistent with", "a notable trend", "suggests", "indicates", or industry \
jargon ("market appreciation", "flight to quality", "crowding") ONLY when (a) \
the exact phrase appears in MONTHLY BRIEFINGS, OR (b) the connective claim is \
anchored to ≥2 specific named plans whose evidence in MONTHLY BRIEFINGS \
supports the relationship being asserted. Otherwise juxtapose facts neutrally.
- Track how each theme evolved month-by-month — call out reversals or \
inflection points where you see them.
- Drop themes that appear in fewer than 3 months, or flag them as \
*Emerging signal*.
- AUM consistency: when the source monthlies show different AUM values for \
the same plan, pick ONE — preferring the value with an "as of <date>" \
qualifier — and use it consistently throughout the annual. Do not switch \
values within a single note.

FORMAT REQUIREMENTS:
- Start with exactly: # Insights: {year} Year in Review
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
- Hard cap 3,000 words total. If you reach it, drop the weakest-evidenced \
theme entirely rather than trimming a sentence from each.

BEFORE FINALISING — scan the draft for these specific patterns and verify \
each against MONTHLY BRIEFINGS. If any item does not match the source, \
remove it or rewrite the sentence to juxtapose facts neutrally.
- bps / basis points figures (especially alpha or excess-return numbers — \
these are the most common arithmetic-derived hallucinations)
- multi-year returns (1-year, 3-year, 5-year, 10-year)
- ratios (Nx, N:1, N-quartile, N% of)
- list counts ("three plans", "all 11 portfolios", "two managers")
- the connective phrases: "consistent with", "reflects", "driven by", \
"a notable", "suggests", "indicates", "underscores" — each must either appear \
verbatim in MONTHLY BRIEFINGS or be anchored to ≥2 specific named plans \
whose evidence supports the relationship
- every theme (##): supported by at least 3 of the 12 months; if not, drop \
or flag as *Emerging signal*
- every inline (doc_id=N): the cited doc must be one whose source monthly \
attached that specific figure or name to that doc_id, not merely the same topic.

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
# Weekly Consultant RFP brief — compose from structured RFPRecord rows
# ---------------------------------------------------------------------------
#
# Unlike weekly/monthly/annual, this cadence composes from already-structured
# ``RFPRecord`` rows (extracted by ``scripts.run_rfp_extraction``), not from
# free-text summaries. The records are grouped by where each RFP sits in the
# consultant-search lifecycle, rendered as deterministic tables (no figures
# the model could invent), and topped with a short LLM-written lead-in.

# Lifecycle stages, in forward order. Each label maps to one or more of the
# six ``status`` enum values in ``lib/rfp_schema.json``. The set of statuses
# across all buckets must stay an exact partition of that enum — the unit
# test ``test_stage_buckets_cover_every_schema_status`` enforces it, so a new
# schema status will fail CI until it is bucketed here.
_RFP_STAGE_BUCKETS: list[tuple[str, list[str]]] = [
    ("Initial plans", ["Planned"]),
    ("Launch",        ["Issued"]),
    ("Review",        ["ResponsesReceived", "FinalistsNamed"]),
    ("Decisions",     ["Awarded", "Withdrawn"]),
]

# status -> bucket label, derived once from the ordered bucket list above.
_RFP_STATUS_TO_BUCKET: dict[str, str] = {
    status: label for label, statuses in _RFP_STAGE_BUCKETS for status in statuses
}


def _rfp_outcome(payload: dict) -> str:
    """Classify an awarded RFP as ``Retained`` / ``Switched`` / ``—``.

    Compares the awarded manager against the incumbent (case- and
    whitespace-insensitive):

    * both present and equal      → ``"Retained"`` (incumbent kept the mandate)
    * both present and different  → ``"Switched"``
    * either missing/blank/None   → ``"—"`` (not enough info, e.g. a search
      still in flight or a first-time mandate with no incumbent)
    """
    awarded = (payload.get("awarded_manager") or "").strip()
    incumbent = (payload.get("incumbent_manager") or "").strip()
    if not awarded or not incumbent:
        return "—"
    return "Retained" if awarded.lower() == incumbent.lower() else "Switched"


def _gather_consultant_rfps(session, period_start: date,
                            period_end: date) -> list[dict]:
    """Return consultant RFPs extracted within ``[period_start, period_end]``.

    Filters ``RFPRecord`` to ``rfp_type == "Consultant"`` and to rows whose
    ``extracted_at`` falls within the inclusive day window (00:00:00 on
    ``period_start`` through 23:59:59.999999 on ``period_end``). Each parsed
    record payload is enriched with:

    * ``bucket`` — its lifecycle stage label (see ``_RFP_STAGE_BUCKETS``)
    * ``outcome`` — ``_rfp_outcome`` of the payload
    * ``plan_abbreviation`` / ``plan_name`` — joined from ``plans`` for display

    Returns ``[]`` when nothing matches. Results are ordered by lifecycle
    stage, then by extraction time, so downstream rendering is stable.
    """
    import json

    from database import Plan, RFPRecord

    window_start = datetime.combine(period_start, datetime.min.time())
    window_end = datetime.combine(period_end, datetime.max.time())

    rows = (
        session.query(RFPRecord)
        .filter(RFPRecord.extracted_at >= window_start)
        .filter(RFPRecord.extracted_at <= window_end)
        .all()
    )

    # plan_id -> (abbreviation, name) for display enrichment.
    plan_display = {
        p.id: (p.abbreviation, p.name) for p in session.query(Plan).all()
    }

    records: list[dict] = []
    for row in rows:
        payload = json.loads(row.record)
        if payload.get("rfp_type") != "Consultant":
            continue
        bucket = _RFP_STATUS_TO_BUCKET.get(payload.get("status"))
        if bucket is None:
            continue  # unknown/unbucketed status — skip defensively
        abbrev, name = plan_display.get(row.plan_id, (row.plan_id, row.plan_id))
        payload["bucket"] = bucket
        payload["outcome"] = _rfp_outcome(payload)
        payload["plan_abbreviation"] = abbrev
        payload["plan_name"] = name
        payload["extracted_at"] = row.extracted_at
        records.append(payload)

    bucket_order = {label: i for i, (label, _) in enumerate(_RFP_STAGE_BUCKETS)}
    records.sort(key=lambda r: (bucket_order[r["bucket"]], r["extracted_at"]))
    return records


def _render_rfp_tables(records: list[dict]) -> str:
    """Render gathered consultant RFPs as markdown tables, one per stage.

    Buckets with no records this week are omitted. The ``Outcome`` column is
    only meaningful for the Decisions stage, so it is shown there alone.
    """
    by_bucket: dict[str, list[dict]] = {}
    for rec in records:
        by_bucket.setdefault(rec["bucket"], []).append(rec)

    sections: list[str] = []
    for label, _statuses in _RFP_STAGE_BUCKETS:
        bucket_records = by_bucket.get(label)
        if not bucket_records:
            continue

        show_outcome = label == "Decisions"
        if show_outcome:
            header = "| Plan | RFP | Status | Outcome |\n|---|---|---|---|"
        else:
            header = "| Plan | RFP | Status |\n|---|---|---|"

        lines = [f"## {label}", "", header]
        for rec in bucket_records:
            plan = rec.get("plan_abbreviation") or rec.get("plan_id", "—")
            title = rec.get("title", "—")
            status = rec.get("status", "—")
            if show_outcome:
                lines.append(f"| {plan} | {title} | {status} | {rec['outcome']} |")
            else:
                lines.append(f"| {plan} | {title} | {status} |")
        sections.append("\n".join(lines))

    return "\n\n".join(sections)


_RFP_WEEKLY_SYSTEM_PROMPT = """\
You are an analyst at a pension fund research firm. You write a short lead-in
for a weekly brief on investment-consultant RFP activity across U.S. public
pension plans, for an audience of consulting firms tracking new-business
opportunities.

Style:
- Factual and concise: 2–4 sentences, no headline, no markdown headings.
- Faithful: mention only plans, consultant names, and RFP titles that appear
  in the DATA TABLE below. Do NOT introduce names from general knowledge or
  invent counts you cannot read off the table.
- Lead with what matters most to a consultant: newly issued searches and
  awarded mandates (especially switches away from an incumbent).
Output plain markdown prose only — no code fences, no headings, no commentary."""


def compose_rfp_weekly(session, period_start: date, period_end: date) -> str:
    """Build the Weekly Consultant RFP Brief markdown for the window.

    Gathers consultant RFPs extracted in ``[period_start, period_end]``,
    renders them as lifecycle-stage tables, and tops them with a short
    LLM-written lead-in. In ``INSIGHTS_MODE=mock`` the Claude call is
    short-circuited and canned markdown is returned unconditionally.
    """
    if config.is_mock():
        return _mock_markdown("Weekly Consultant RFP Brief", period_start, period_end)

    today_str = datetime.utcnow().strftime("%B %d, %Y")
    range_str = f"{period_start.isoformat()} – {period_end.isoformat()}"
    heading = (
        f"# Weekly Consultant RFP Brief: {range_str}\n"
        f"*Generated: {today_str}*\n\n"
        "---\n"
    )

    records = _gather_consultant_rfps(session, period_start, period_end)
    if not records:
        return (
            f"{heading}\n"
            "_No consultant RFP activity recorded in this window._\n"
        )

    tables = _render_rfp_tables(records)

    from summarizer import _get_client

    message = _get_client().messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        temperature=0.2,
        system=[
            {
                "type": "text",
                "text": _RFP_WEEKLY_SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=[{
            "role": "user",
            "content": (
                f"Write the lead-in for the week of {range_str}.\n\n"
                f"DATA TABLE:\n{tables}"
            ),
        }],
    )
    lead_in = message.content[0].text.strip()

    return f"{heading}\n{lead_in}\n\n{tables}\n"


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
