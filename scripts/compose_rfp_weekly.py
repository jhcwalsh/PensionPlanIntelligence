"""Compose a weekly Consultant RFP newsletter from rfp_records.

Each Sunday run produces a brief covering the trailing 7 days plus a
running 30-day list. Pulls every rfp_records row whose payload
``rfp_type == 'consultant'``, joins to Document/Plan for context, hands
the structured data to Claude, and writes the resulting markdown to
``notes/weekly_consultant_rfps_<period_end>.md`` where period_end is
the Sunday the brief covers.

This is its own briefing — independent of the Insights weekly /
monthly cycle — so it doesn't go through the Publication approval
flow. The Insights tab reads ``weekly_consultant_rfps_*.md`` directly.

Run:
    python -m scripts.compose_rfp_weekly                          # most recent Sunday
    python -m scripts.compose_rfp_weekly --period 2026-05-10      # any Sunday
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

from database import Document, Plan, RFPRecord, get_session

logger = logging.getLogger(__name__)

NOTES_DIR = Path(__file__).parent.parent / "notes"

_WEEK_DAYS = 7
_MONTH_DAYS = 30

_SYSTEM_PROMPT = """\
You are a senior investment analyst at a pension fund research firm.
You write concise, factual weekly briefings on consultant RFP activity
across U.S. public pension plans for institutional readers.

Style:
- Concise: tight prose, no filler, no preamble. Start with the content.
- Faithful: every plan name, RFP title, status, date, and dollar figure
  MUST appear verbatim in the structured data block. Do not introduce
  names, dates, or values from general knowledge or invent supporting
  detail.
- Analytical: group RFPs by stage (Planned → Issued → Responses Received
  → Awarded) and surface timing patterns where the data supports it.
- Precise: when a date or value is missing in the data, write "—" or omit
  rather than estimating.

Output clean markdown only. No code fences, no JSON, no commentary."""


def _parse_iso(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except (ValueError, TypeError):
        return None


_AWARDED_STATUSES = {"awarded", "closed"}
_ACTIVE_STALENESS = timedelta(days=365)
_AWARDED_STALENESS = timedelta(days=90)


def _is_relevant(rec: dict, start: date, end: date) -> bool:
    """True if the RFP belongs in the brief for [start, end].

    Anchor: the source document's ``meeting_date`` (or ``downloaded_at``
    for CAFRs and other non-meeting docs) must fall in the window.
    Staleness guard: drop records whose latest explicit date is older
    than 12 months (3 months for Awarded/Closed) before period_end.
    Records with no explicit dates pass the guard (newly surfaced).
    """
    anchor = _parse_iso(rec.get("meeting_date")) or _parse_iso(rec.get("downloaded_at"))
    if anchor is None or not (start <= anchor <= end):
        return False

    explicit = [
        _parse_iso(rec.get("release_date")),
        _parse_iso(rec.get("response_due_date")),
        _parse_iso(rec.get("award_date")),
    ]
    explicit = [d for d in explicit if d is not None]
    if not explicit:
        return True

    horizon = (_AWARDED_STALENESS
               if (rec.get("status") or "").lower() in _AWARDED_STATUSES
               else _ACTIVE_STALENESS)
    return max(explicit) >= end - horizon


def _gather_consultant_rfps(period_start: date, period_end: date) -> list[dict]:
    """Return all consultant-type RFP records relevant to [start, end]."""
    session = get_session()
    try:
        rows = (
            session.query(RFPRecord, Document, Plan)
            .join(Document, RFPRecord.document_id == Document.id)
            .outerjoin(Plan, RFPRecord.plan_id == Plan.id)
            .order_by(RFPRecord.extracted_at.desc())
            .all()
        )
        out = []
        for r, doc, plan in rows:
            try:
                payload = json.loads(r.record)
            except Exception:
                payload = {}
            if (payload.get("rfp_type") or "").lower() != "consultant":
                continue
            out.append({
                "plan_id": r.plan_id,
                "plan_name": (plan.name if plan else r.plan_id),
                "plan_abbrev": (plan.abbreviation if plan else "") or r.plan_id,
                "title": payload.get("title") or "",
                "status": payload.get("status") or "",
                "asset_class": payload.get("asset_class") or "",
                "release_date": payload.get("release_date") or "",
                "response_due_date": payload.get("response_due_date") or "",
                "award_date": payload.get("award_date") or "",
                "incumbent_manager": payload.get("incumbent_manager") or "",
                "shortlisted_managers": payload.get("shortlisted_managers") or [],
                "awarded_manager": payload.get("awarded_manager") or "",
                "mandate_size_usd_millions": payload.get("mandate_size_usd_millions"),
                "doc_id": doc.id,
                "doc_filename": doc.filename,
                "meeting_date": doc.meeting_date.isoformat() if doc.meeting_date else "",
                "downloaded_at": doc.downloaded_at.isoformat() if doc.downloaded_at else "",
                "extracted_at": r.extracted_at.isoformat() if r.extracted_at else "",
            })
        return [rec for rec in out if _is_relevant(rec, period_start, period_end)]
    finally:
        session.close()


def _tag_week(records: list[dict], week_start: date, week_end: date) -> list[dict]:
    """Mark each 30-day record with ``new_this_week`` so the LLM can flag it."""
    out = []
    for r in records:
        rec = dict(r)
        anchor = _parse_iso(r.get("meeting_date")) or _parse_iso(r.get("downloaded_at"))
        rec["new_this_week"] = bool(anchor and week_start <= anchor <= week_end)
        out.append(rec)
    return out


def _build_user_prompt(week_records: list[dict],
                       month_records: list[dict],
                       week_start: date, week_end: date,
                       month_start: date, month_end: date) -> str:
    today_str = datetime.utcnow().strftime("%B %d, %Y")
    week_label = f"Week ending {week_end.strftime('%b %d, %Y')}"
    data_block = json.dumps({
        "this_week": week_records,
        "past_30_days": month_records,
    }, indent=2, ensure_ascii=False)

    return f"""\
Write a Weekly Consultant RFP Brief covering {week_label}.

Inputs in the DATA block:
- ``this_week`` ({len(week_records)} records): consultant RFPs that
  surfaced in board materials dated {week_start.isoformat()} –
  {week_end.isoformat()}.
- ``past_30_days`` ({len(month_records)} records): the running 30-day
  list of consultant RFPs surfaced {month_start.isoformat()} –
  {month_end.isoformat()} (a superset of this_week — each record has a
  ``new_this_week`` boolean flag).

Records with stale explicit dates (active searches >12 months old, or
awards >3 months old) have already been filtered out upstream.

GROUNDING RULES (non-negotiable):
- Every plan name, RFP title, status, date, dollar figure, and vendor
  / manager / consultant name must appear verbatim in the DATA below.
  Only cite vendor names that are present in the awarded_manager,
  incumbent_manager, or shortlisted_managers fields, or that appear
  inside the title or asset_class strings. Do not infer vendor
  identities from general industry knowledge.
- Use the plan_abbrev field for plan references (e.g. **LACERA**,
  **MainePERS**) on first mention.
- Every RFP mentioned in prose must include an inline source link in
  the form ([source](?doc=N)) where N is the doc_id from the DATA.
- If a date is missing in the DATA, write "—". Do not estimate.

FORMAT REQUIREMENTS:
- Start with exactly: # Weekly Consultant RFP Brief: {week_label}
- Second line: *This week: {week_start.isoformat()} – {week_end.isoformat()}*
- Third line: *Past 30 days: {month_start.isoformat()} – {month_end.isoformat()}*
- Fourth line: *Generated: {today_str}*
- Then a --- horizontal rule.
- ## This week — open with one short paragraph (~2 sentences) covering
  the count of new RFPs this week, plans involved, and any pattern.
  If ``this_week`` is empty, write a single line stating no new
  consultant RFPs surfaced this week and skip the by-stage prose.
  Otherwise follow with short prose sections grouped by stage
  (Awarded → ResponsesReceived → Issued → Planned). For each RFP, one
  tight sentence covering plan, what's being procured, available
  timing or incumbent info, with an inline ([source](?doc=N)) link.
- ## Past 30 days — running summary table with columns:
  Plan | RFP | Stage | Released | Due | Awarded vendor | Incumbent | New | Source
  Plan: bold plan_abbrev. Stage: status field. Released: release_date.
  Due: response_due_date. Awarded vendor: awarded_manager (use "—" if
  empty). Incumbent: incumbent_manager (use "—" if empty).
  New: "•" if new_this_week is true, else "—".
  Source: a markdown link `[doc](?doc=N)`. Use "—" for any other
  missing field. List rows new-this-week first, then by stage
  (Awarded → ResponsesReceived → Issued → Planned).
- Target ~300–500 words total. Err on the short side.
- Do NOT speculate on outcomes or strategic implications.

DATA:
{data_block}"""


def compose(week_records: list[dict], month_records: list[dict],
            week_start: date, week_end: date,
            month_start: date, month_end: date) -> str:
    week_label = f"Week ending {week_end.strftime('%b %d, %Y')}"
    if not month_records:
        return (
            f"# Weekly Consultant RFP Brief: {week_label}\n"
            f"*This week: {week_start.isoformat()} – {week_end.isoformat()}*\n"
            f"*Past 30 days: {month_start.isoformat()} – {month_end.isoformat()}*\n"
            f"*Generated: {datetime.utcnow().strftime('%B %d, %Y')}*\n\n"
            "---\n\n"
            "_No consultant RFP records in the trailing 30 days._\n"
        )

    from summarizer import _get_client

    user_prompt = _build_user_prompt(
        week_records, month_records, week_start, week_end, month_start, month_end,
    )
    message = _get_client().messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        temperature=0.2,
        system=[
            {
                "type": "text",
                "text": _SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=[{"role": "user", "content": user_prompt}],
    )
    return message.content[0].text


def _last_sunday(today: date | None = None) -> date:
    """Return the most recent Sunday (today if today is Sunday)."""
    today = today or date.today()
    # Python weekday: Mon=0..Sun=6
    return today - timedelta(days=(today.weekday() + 1) % 7)


def _period_for(period_arg: str | None) -> tuple[date, date, date, date]:
    """Return (week_start, week_end, month_start, month_end).

    ``week_end`` is a Sunday; week is the trailing 7 days, month the
    trailing 30. Default: most recent Sunday.
    """
    end = date.fromisoformat(period_arg) if period_arg else _last_sunday()
    week_start = end - timedelta(days=_WEEK_DAYS - 1)
    month_start = end - timedelta(days=_MONTH_DAYS - 1)
    return week_start, end, month_start, end


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="compose_rfp_weekly")
    parser.add_argument(
        "--period",
        help="Sunday end-date as YYYY-MM-DD (default: most recent Sunday).",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    week_start, week_end, month_start, month_end = _period_for(args.period)

    month_records = _gather_consultant_rfps(month_start, month_end)
    month_records = _tag_week(month_records, week_start, week_end)
    week_records = [r for r in month_records if r["new_this_week"]]

    print(f"week records: {len(week_records)} ({week_start} → {week_end})")
    print(f"30-day records: {len(month_records)} ({month_start} → {month_end})")

    markdown = compose(week_records, month_records,
                       week_start, week_end, month_start, month_end)

    NOTES_DIR.mkdir(parents=True, exist_ok=True)
    out_path = NOTES_DIR / f"weekly_consultant_rfps_{week_end.isoformat()}.md"
    out_path.write_text(markdown, encoding="utf-8")
    print(f"wrote {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
