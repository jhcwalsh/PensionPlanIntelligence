"""Compose a monthly Consultant RFP newsletter from rfp_records.

Pulls every rfp_records row whose payload ``rfp_type == 'consultant'``,
joins to Document/Plan for context, hands the structured data to
Claude, and writes the resulting markdown to
``notes/monthly_consultant_rfps_<period_start>.md``.

This is its own briefing — independent of the CIO Insights weekly /
monthly cycle — so it doesn't go through the Publication approval
flow. The Notes tab reads ``monthly_consultant_rfps_*.md`` directly.

Run:
    python -m scripts.compose_rfp_monthly                   # April 2026
    python -m scripts.compose_rfp_monthly --period 2026-05  # any month
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path

from database import Document, Plan, RFPRecord, get_session

logger = logging.getLogger(__name__)

NOTES_DIR = Path(__file__).parent.parent / "notes"

_SYSTEM_PROMPT = """\
You are a senior investment analyst at a pension fund research firm.
You write concise, factual monthly briefings on consultant RFP activity
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


def _gather_consultant_rfps() -> list[dict]:
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
                "extracted_at": r.extracted_at.isoformat() if r.extracted_at else "",
            })
        return out
    finally:
        session.close()


def _build_user_prompt(records: list[dict],
                       period_start: date, period_end: date) -> str:
    today_str = datetime.utcnow().strftime("%B %d, %Y")
    month_label = period_start.strftime("%B %Y")
    data_block = json.dumps(records, indent=2, ensure_ascii=False)

    return f"""\
Write a Monthly Consultant RFP Brief for {month_label} summarizing the
{len(records)} consultant-type RFP records extracted from board materials
during the period.

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
- Stage counts in the "At a glance" paragraph must match the count of
  records in the DATA at each status. Recount before writing.

FORMAT REQUIREMENTS:
- Start with exactly: # Monthly Consultant RFP Brief: {month_label}
- Second line: *Compiled from board materials extracted during \
{period_start.isoformat()} – {period_end.isoformat()}*
- Third line: *Generated: {today_str}*
- Then a --- horizontal rule.
- ## At a glance — one short paragraph (~3 sentences) covering: total
  RFP count, # of plans involved, distribution across stages, and any
  notable theme (e.g. concentration in admin systems vs. investment
  consulting).
- ## Summary table — a markdown table with columns:
  Plan | RFP | Stage | Released | Due | Awarded vendor | Incumbent | Source
  Plan: bold plan_abbrev. Stage: status field. Released: release_date.
  Due: response_due_date. Awarded vendor: awarded_manager (use "—" if
  empty). Incumbent: incumbent_manager (use "—" if empty). Source: a
  markdown link `[doc](?doc=N)`. Use "—" for any other missing field.
  List rows in order Awarded → ResponsesReceived → Issued → Planned.
- ## By stage — short prose sections grouped by stage. For each RFP,
  one tight sentence covering plan, what's being procured, any
  available timing or incumbent info, with an inline ([source](?doc=N))
  link.
- Target ~400–600 words total. Err on the short side.
- Do NOT speculate on outcomes or strategic implications — this is a
  reference brief, not editorial.

DATA:
{data_block}"""


def compose(records: list[dict],
            period_start: date, period_end: date) -> str:
    if not records:
        return (
            f"# Monthly Consultant RFP Brief: {period_start.strftime('%B %Y')}\n"
            f"*Generated: {datetime.utcnow().strftime('%B %d, %Y')}*\n\n"
            "---\n\n"
            "_No consultant RFP records extracted in this window._\n"
        )

    from summarizer import _get_client

    user_prompt = _build_user_prompt(records, period_start, period_end)
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


def _period_for(period_arg: str | None) -> tuple[date, date]:
    """``2026-04`` or ``2026-04-01`` → (first, last) of that month.

    Default: prior calendar month relative to today.
    """
    if period_arg:
        s = period_arg if len(period_arg) > 7 else period_arg + "-01"
        start = date.fromisoformat(s).replace(day=1)
    else:
        today = date.today()
        start = (date(today.year - 1, 12, 1) if today.month == 1
                 else date(today.year, today.month - 1, 1))

    if start.month == 12:
        end = date(start.year, 12, 31)
    else:
        end = date.fromordinal(date(start.year, start.month + 1, 1).toordinal() - 1)
    return start, end


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="compose_rfp_monthly")
    parser.add_argument(
        "--period",
        help="Month as YYYY-MM (default: prior calendar month).",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    period_start, period_end = _period_for(args.period)

    records = _gather_consultant_rfps()
    print(f"consultant RFP records: {len(records)}")
    print(f"period: {period_start} to {period_end}")

    markdown = compose(records, period_start, period_end)

    NOTES_DIR.mkdir(parents=True, exist_ok=True)
    out_path = NOTES_DIR / f"monthly_consultant_rfps_{period_start.isoformat()}.md"
    out_path.write_text(markdown, encoding="utf-8")
    print(f"wrote {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
