"""
Extract structured periodic returns from ``doc_type='performance'`` documents.

Context: ``queries.performance_report_rows()`` sources its numbers from CAFRs,
so every figure it shows is a fiscal-year return. The 48 documents tagged
``doc_type='performance'`` were assumed to be the fix -- "true quarterly
reports [with] no structured extraction yet" -- but inspecting all five
plans that own one turned up only one plan whose documents are actually
that:

    nycrs_comptroller (30 docs)  Comptroller's "Monthly Performance Review"
                                  PDFs, one per constituent system (NYCERS,
                                  TRS, POLICE, FIRE, BERS) per month. Each
                                  contains a real Total Fund return table:
                                  1 Month / 3 Months / FYTD / FY Ending ...
                                  -- the 3-month figure is the quarterly
                                  number this table was missing.
    mn_msrs (10 docs)            MNDCP -- a supplemental 457/deferred-comp
                                  plan's *investment menu* returns, not the
                                  DB pension fund's total return.
    pera_colorado (6 docs)       Capital Accumulation Plans (the DC-plan
                                  overlay) performance review -- same
                                  problem as mn_msrs.
    calpers (1 doc)               A Performance, Compensation & Talent
                                  Management Subcommittee meeting transcript
                                  -- doc_type='performance' here describes
                                  the committee's name, not fund performance.
    dcrb (1 doc)                  A blank vendor "Past Performance
                                  Evaluation Form" -- likewise mistagged.

Extracting the last four as if they were fund performance would put a DC
plan's menu returns, or nothing at all, into a table that claims to be the
pension fund's return. So this script only ever processes nycrs_comptroller.
ALLOWED_PLAN_IDS exists to make that a deliberate, visible choice rather
than a silent filter -- widening it should mean a human looked at the new
plan's documents first, the way this docstring did for these five.

Reads ``Document.extracted_text`` rather than the local PDF: these are
short standalone reports (not CAFRs needing page-range section-location),
and unlike CAFR PDFs the source files are routinely gone from disk (E1) --
extracted_text survives that.

Usage:
    python extract_performance_reports.py                # all unextracted
    python extract_performance_reports.py --redo          # force re-extract
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys

import anthropic
from dotenv import load_dotenv
from rich.console import Console
from sqlalchemy.orm import undefer
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

# Reuses extract_cafr_investments's instrumented client factory rather than
# building a fifth copy of the same api_key/token-file boilerplate — see
# tests/test_usage_instrumentation.py's docstring for why a new module
# should join an existing factory rather than add another one.
from extract_cafr_investments import _get_client
from database import (
    utcnow,
    Document,
    Plan,
    PerformanceReportExtract,
    PerformanceReportReturn,
    get_session,
    init_db,
)

_ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(_ENV_PATH, override=True)

console = Console(legacy_windows=False)

MODEL = "claude-sonnet-4-6"
MAX_OUTPUT_TOKENS = 4096
MAX_TEXT_CHARS = 100_000  # these reports run well under this; a CAFR would not

# Only plans whose doc_type='performance' documents are genuinely periodic
# pension-fund return reports. See module docstring for why the other four
# plans that also hold documents tagged 'performance' are excluded.
ALLOWED_PLAN_IDS = {"nycrs_comptroller"}

# nycrs_comptroller files are named
# "Monthly-Performance-Review-Material_<MM>-<YYYY>-<FUND>.pdf"
_FUND_SUFFIX_RE = re.compile(r"-(NYCERS|TRS|POLICE|FIRE|BERS)\.pdf", re.IGNORECASE)


def fund_scope_from_url(url: str) -> str | None:
    m = _FUND_SUFFIX_RE.search(url or "")
    return m.group(1).upper() if m else None


# ---------------------------------------------------------------------------
# Claude tool-use extraction
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You extract structured periodic return data from a US public pension fund's periodic (monthly or quarterly) investment performance report.

You return two artefacts via the `record_performance_data` tool:

1. `as_of_date` -- the report's own reporting date (the date the returns are calculated through, e.g. "Performance Overview as of November 30, 2025" -> "2025-11-30"), as YYYY-MM-DD. Null if genuinely not stated.

2. `returns` -- one entry per (scope, period) return actually printed in the document.
   - `scope` is the literal string "total_fund" for the whole-fund return, or an asset-class name matching whatever the document itself calls it (do not normalise across plans).
   - `period` MUST be one of: "1mo" (one month), "3mo" (three months / one quarter), "fytd" (fiscal-year-to-date), or "fy_<YYYY>" where YYYY is the four-digit fiscal year the figure covers (e.g. a row labelled "FY Ending 6/30/25" -> "fy_2025"). Skip any period that doesn't fit one of these (e.g. 3y/5y/10y annualised, since-inception).
   - `return_pct` is a percentage (7.4 means 7.4%, not 0.074).
   - `benchmark_return_pct` / `benchmark_name`: the stated policy benchmark for that scope, if given. Either may be null.

Rules:
- Do NOT invent numbers. This text was extracted from a PDF that mixes chart labels with body text, so numbers may appear out of visual order -- only report a (scope, period, return) triple when the association between the label and the number is unambiguous from context (e.g. "1 Month - Total Fund ... Return: 0.61%").
- If the whole document is not a fund performance report (e.g. it is a meeting transcript, a blank evaluation form, or a defined-contribution investment-menu fund lineup rather than the plan's own total return), return an empty `returns` array and say why in `notes`. Do not force a match.
- Numbers reported as basis points: convert to percentage points (140 bps -> 1.4)."""


TOOL_SCHEMA = {
    "name": "record_performance_data",
    "description": "Record structured periodic return data extracted from a pension fund performance report.",
    "input_schema": {
        "type": "object",
        "properties": {
            "as_of_date": {"type": ["string", "null"], "description": "YYYY-MM-DD"},
            "returns": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "scope": {"type": "string"},
                        "period": {"type": "string"},
                        "return_pct": {"type": ["number", "null"]},
                        "benchmark_return_pct": {"type": ["number", "null"]},
                        "benchmark_name": {"type": ["string", "null"]},
                        "notes": {"type": ["string", "null"]},
                    },
                    "required": ["scope", "period"],
                },
            },
            "notes": {"type": ["string", "null"]},
        },
        "required": ["returns"],
    },
}

MOCK_PAYLOAD = {
    "as_of_date": "2026-01-31",
    "returns": [
        {"scope": "total_fund", "period": "1mo", "return_pct": 1.1,
         "benchmark_return_pct": None, "benchmark_name": None, "notes": None},
        {"scope": "total_fund", "period": "3mo", "return_pct": 3.2,
         "benchmark_return_pct": None, "benchmark_name": None, "notes": None},
    ],
    "notes": "mock",
}


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=4, max=30),
    retry=retry_if_exception_type((anthropic.APIConnectionError,
                                    anthropic.APIStatusError,
                                    anthropic.RateLimitError)),
)
def call_claude(plan_name: str, fund_scope: str | None, report_text: str) -> dict:
    """Send the report text to Claude; return parsed tool input.

    In mock mode (LLM_MODE=mock) this returns MOCK_PAYLOAD verbatim without
    calling the API, matching extract_cafr_actuarial.py's pattern.
    """
    if os.environ.get("LLM_MODE") == "mock":
        return MOCK_PAYLOAD

    user_message = (
        f"Plan: {plan_name}\n"
        f"Fund: {fund_scope or '(single fund)'}\n\n"
        f"--- PERFORMANCE REPORT TEXT ---\n{report_text}"
    )

    msg = _get_client().messages.create(
        model=MODEL,
        max_tokens=MAX_OUTPUT_TOKENS,
        system=[{
            "type": "text",
            "text": SYSTEM_PROMPT,
            "cache_control": {"type": "ephemeral"},
        }],
        tools=[TOOL_SCHEMA],
        tool_choice={"type": "tool", "name": "record_performance_data"},
        messages=[{"role": "user", "content": user_message}],
    )

    if msg.stop_reason == "max_tokens":
        console.print(
            f"  [yellow]warning: model hit max_tokens "
            f"(in={msg.usage.input_tokens} out={msg.usage.output_tokens})[/yellow]"
        )

    for block in msg.content:
        if block.type == "tool_use" and block.name == "record_performance_data":
            return block.input

    raise RuntimeError(
        f"Claude did not call record_performance_data; stop_reason={msg.stop_reason}"
    )


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def _coerce_float(v) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def save_extract(session, doc: Document, payload: dict, *,
                 fund_scope: str | None, text_hash: str) -> PerformanceReportExtract:
    existing = session.query(PerformanceReportExtract).filter_by(document_id=doc.id).first()
    if existing is not None:
        session.delete(existing)
        session.flush()

    extract = PerformanceReportExtract(
        plan_id=doc.plan_id,
        document_id=doc.id,
        fund_scope=fund_scope,
        as_of_date=payload.get("as_of_date"),
        extracted_at=utcnow(),
        model_used=MODEL,
        text_hash=text_hash,
        notes=payload.get("notes"),
    )
    session.add(extract)
    session.flush()

    for row in payload.get("returns", []):
        session.add(PerformanceReportReturn(
            extract_id=extract.id,
            scope=str(row.get("scope", ""))[:200],
            period=str(row.get("period", ""))[:30],
            return_pct=_coerce_float(row.get("return_pct")),
            benchmark_return_pct=_coerce_float(row.get("benchmark_return_pct")),
            benchmark_name=row.get("benchmark_name"),
            notes=row.get("notes"),
        ))

    session.commit()
    return extract


# ---------------------------------------------------------------------------
# Per-document orchestration
# ---------------------------------------------------------------------------

def extract_one(session, doc: Document, plan: Plan, *, force: bool = False) -> str:
    fund_scope = fund_scope_from_url(doc.url)
    label = f"{plan.abbreviation or doc.plan_id} {fund_scope or ''}".strip()

    text = doc.extracted_text or ""
    if len(text) < 200:
        console.print(f"  [yellow]{label}: too little text ({len(text)} chars)[/yellow]")
        return "too_short"

    if len(text) > MAX_TEXT_CHARS:
        text = text[:MAX_TEXT_CHARS]

    text_hash = hashlib.md5(text.encode("utf-8", errors="ignore")).hexdigest()
    if not force:
        existing = session.query(PerformanceReportExtract).filter_by(document_id=doc.id).first()
        if existing is not None and existing.text_hash == text_hash:
            console.print(f"  [dim]{label}: already extracted (hash matches)[/dim]")
            return "already_extracted"

    console.print(f"  [cyan]{label}: extracting ({len(text):,} chars)[/cyan]")

    try:
        payload = call_claude(plan.name, fund_scope, text)
    except Exception as e:
        console.print(f"  [red]{label}: Claude error: {e}[/red]")
        return "api_error"

    save_extract(session, doc, payload, fund_scope=fund_scope, text_hash=text_hash)
    n_returns = len(payload.get("returns", []))
    console.print(f"  [green]{label}: saved ({n_returns} return rows)[/green]")
    return "saved"


def run_extraction(force: bool = False) -> dict[str, int]:
    init_db()
    session = get_session()
    counts: dict[str, int] = {}

    try:
        q = (
            session.query(Document, Plan)
            .join(Plan, Document.plan_id == Plan.id)
            .filter(Document.doc_type == "performance")
            .filter(Document.plan_id.in_(ALLOWED_PLAN_IDS))
            .options(undefer(Document.extracted_text))
            .order_by(Document.downloaded_at)
        )

        if not force:
            extracted_doc_ids = {
                did for (did,) in session.query(PerformanceReportExtract.document_id).all()
            }
            docs = [(d, p) for d, p in q.all() if d.id not in extracted_doc_ids]
        else:
            docs = list(q.all())

        console.print(f"[bold]Extracting {len(docs)} performance report(s)[/bold]")
        for doc, plan in docs:
            console.rule(f"[bold]{plan.abbreviation or doc.plan_id}[/bold]")
            try:
                status = extract_one(session, doc, plan, force=force)
            except Exception as e:
                status = "error"
                console.print(f"  [red]{plan.abbreviation}: {e}[/red]")
            counts[status] = counts.get(status, 0) + 1
    finally:
        session.close()

    console.rule("[bold green]Extraction complete[/bold green]")
    for status in ("saved", "already_extracted", "too_short", "api_error", "error"):
        if counts.get(status):
            console.print(f"  {status:20s} {counts[status]}")
    return counts


def main():
    parser = argparse.ArgumentParser(
        description="Extract periodic returns from doc_type='performance' documents.")
    parser.add_argument("--redo", "--force", action="store_true",
                        help="Re-extract even if a PerformanceReportExtract row already exists.")
    args = parser.parse_args()

    counts = run_extraction(force=args.redo)
    sys.exit(0 if not counts.get("error") and not counts.get("api_error") else 1)


if __name__ == "__main__":
    main()
