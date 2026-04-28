"""
Extract structured Investment Section data from CAFR/ACFR PDFs.

For each CAFR document we:
  1. Locate the Investment Section pages (PDF TOC first; fallback to text search).
  2. Extract text from those pages with PyMuPDF.
  3. Send to Claude (Sonnet) with a tool-use schema requesting:
       - investment_policy_text (verbatim or close paraphrase of the policy summary)
       - asset_allocation: list of {asset_class, target_pct, actual_pct,
         target_range_low, target_range_high}
       - performance: list of {scope, period, return_pct, benchmark_return_pct,
         benchmark_name} where scope is "total_fund" or an asset class name.
  4. Write into cafr_extract / cafr_allocation / cafr_performance.

System prompt is cached (used identically across every CAFR), so the per-CAFR
incremental cost is just the section text + tool output.

Usage:
    python extract_cafr_investments.py                   # all unextracted CAFRs
    python extract_cafr_investments.py calpers ktrs      # specific plans
    python extract_cafr_investments.py --redo calpers    # force re-extract
"""

import argparse
import hashlib
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import anthropic
import fitz  # PyMuPDF
from dotenv import load_dotenv
from rich.console import Console
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from database import (
    CafrAllocation,
    CafrExtract,
    CafrPerformance,
    Document,
    Plan,
    get_session,
    init_db,
)

_ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(_ENV_PATH, override=True)

console = Console(legacy_windows=False)

MODEL = "claude-sonnet-4-6"
# 16K output covers the worst case observed: ~30 alloc rows + 70 perf rows
# + 4K-char policy text + benchmarks. With 8K we saw plans with rich
# allocation tables silently truncate before emitting the performance array.
MAX_OUTPUT_TOKENS = 16384
MAX_SECTION_CHARS = 200_000   # ~50k tokens; safe for Sonnet 4.6's 200k input

# Section-header phrases that mark the start of the Investment Section
INV_START_PATTERNS = [
    re.compile(r"^\s*investment\s+section\s*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^\s*investments\s*$", re.IGNORECASE | re.MULTILINE),
]
# Headers that mark its END (the section that follows Investments in CAFRs)
INV_END_PATTERNS = [
    re.compile(r"^\s*actuarial\s+section\s*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^\s*statistical\s+section\s*$", re.IGNORECASE | re.MULTILINE),
]


# ---------------------------------------------------------------------------
# Section location
# ---------------------------------------------------------------------------

def _locate_via_toc(doc: fitz.Document) -> tuple[int, int] | None:
    """Try to find Investment Section page range via the PDF outline (TOC)."""
    toc = doc.get_toc()
    if not toc:
        return None

    inv_idx = None
    inv_level = None
    for i, (level, title, page) in enumerate(toc):
        if re.search(r"\binvestment(?:s|\s+section)?\b", title, re.IGNORECASE):
            # Skip subheaders like "investment policy" if we already found a top-level "investments"
            if inv_idx is None or level < inv_level:
                inv_idx = i
                inv_level = level
                # Don't break — prefer the highest-level entry containing "investment"

    if inv_idx is None:
        return None

    start_page = toc[inv_idx][2]
    # End = next TOC entry at the same or higher level (numerically lower number)
    end_page = doc.page_count
    for j in range(inv_idx + 1, len(toc)):
        if toc[j][0] <= inv_level:
            end_page = toc[j][2] - 1
            break
    if end_page < start_page:
        return None
    return (start_page, end_page)


def _locate_via_text_search(doc: fitz.Document) -> tuple[int, int] | None:
    """Fallback: scan page text for section header phrases."""
    start = None
    end = None
    for i in range(doc.page_count):
        text = doc.load_page(i).get_text()
        if start is None:
            for pat in INV_START_PATTERNS:
                if pat.search(text):
                    start = i + 1  # PyMuPDF pages are 0-indexed; we use 1-indexed externally
                    break
        elif end is None:
            for pat in INV_END_PATTERNS:
                if pat.search(text):
                    end = i  # the page BEFORE the next section header
                    break
    if start is None:
        return None
    if end is None:
        end = doc.page_count
    if end < start:
        return None
    return (start, end)


def locate_investment_section(pdf_path: str) -> tuple[int, int] | None:
    """Return (start_page, end_page) 1-indexed for the Investment Section."""
    doc = fitz.open(pdf_path)
    try:
        rng = _locate_via_toc(doc) or _locate_via_text_search(doc)
        return rng
    finally:
        doc.close()


def extract_section_text(pdf_path: str, start_page: int, end_page: int) -> str:
    """Concatenate text from pages [start, end] (1-indexed, inclusive)."""
    doc = fitz.open(pdf_path)
    try:
        parts = []
        for p in range(start_page, end_page + 1):
            if p < 1 or p > doc.page_count:
                continue
            parts.append(f"[Page {p}]\n{doc.load_page(p - 1).get_text()}")
        return "\n\n".join(parts)
    finally:
        doc.close()


# ---------------------------------------------------------------------------
# Claude tool-use extraction
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You extract structured data from the Investment Section of US public pension Annual Comprehensive Financial Reports (CAFRs / ACFRs).

For each CAFR you receive, you return three artefacts via the `record_investment_data` tool:

1. `investment_policy_text` — a faithful excerpt or close paraphrase of the plan's substantive investment policy / philosophy, drawn from wherever it appears in the section. Sources to mine, in priority order:
   a. A labeled "Investment Policy", "Policy Summary", or "Investment Objectives" subsection (best case).
   b. The CIO / Investment Officer's narrative letter — these almost always describe objectives, allocation philosophy, and major decisions.
   c. The "Report on Investment Activity" narrative.
   d. Standalone paragraphs accompanying the asset-allocation table that explain the rationale.

   Focus content on:
   - Investment objectives and risk tolerance (target return, funded-status goals)
   - Asset-allocation philosophy (passive/active, in-house/external, public/private mix, diversification stance)
   - Rebalancing rules, liquidity considerations, ESG / divestment policies
   - Use of leverage, derivatives, securities lending
   - Notable tactical decisions or strategy shifts in the fiscal year

   Aim for 1500–4000 chars. Preserve concrete details (target return, risk metrics, liquidity tiers, dollar amounts, percentages). Drop pure boilerplate ("the Board has fiduciary responsibility…"). Only set this to null if the entire Investment Section is essentially unreadable; if there is *any* substantive narrative content, capture it.

2. `asset_allocation` — one entry per asset class with target and actual percentages.
   - Use the plan's own asset-class naming (e.g. "Global Equity", "Public Fixed Income", "Real Estate", "Private Equity", "Hedge Funds", "Cash"). Do not normalise across plans.
   - `target_pct` and `actual_pct` are percentages of total portfolio (so 25.0 means 25%, NOT 0.25). Both are floats. Either may be null if not reported.
   - `target_range_low` / `target_range_high` are the policy's allowable range bounds (also percentages); null if no range is stated.
   - Include a top-level "Total" row only if the plan reports one and it doesn't sum to 100 (e.g. when leverage is in play).

3. `performance` — one entry per (scope, period) cell in the returns table.
   - `scope` is either the literal string "total_fund" (for whole-portfolio returns) or the asset-class name (matching whatever the plan uses in its returns table).
   - `period` MUST be one of: "fy" (current fiscal year only), "1y", "3y", "5y", "10y", "since_inception". Map "1 year" -> "1y", "3 years annualized" -> "3y", "ITD" / "inception" -> "since_inception". If a period doesn't fit (e.g. "20y"), skip it.
   - `return_pct` is a percentage (so 7.4 means 7.4%, NOT 0.074).
   - `benchmark_return_pct` and `benchmark_name` are the plan's stated benchmark for that scope (the benchmark name often differs by asset class, e.g. "Russell 3000" for US equity). Either may be null.
   - Skip rows that are clearly noise (header repeats, footnote-only rows).

Rules:
- Do NOT invent numbers. If a cell is dashed, blank, or footnoted "n/a", omit that entry rather than guessing.
- Numbers reported as basis points: convert to percentage points (140 bps -> 1.4).
- If returns are reported gross AND net of fees, prefer NET. Note in the entry's `notes` field if you used gross.
- The Investment Section may include text outside the policy summary (e.g. CIO letter, manager lists). Pull from those for context but only the formal policy goes in `investment_policy_text`.
- If the section is mostly missing or not parseable, return empty arrays and explain in the top-level `notes` field; don't guess."""


TOOL_SCHEMA = {
    "name": "record_investment_data",
    "description": "Record structured Investment Section data extracted from a CAFR.",
    "input_schema": {
        "type": "object",
        "properties": {
            "investment_policy_text": {
                "type": ["string", "null"],
                "description": "Investment policy summary (1500-3500 chars). Null if not clearly stated.",
            },
            "asset_allocation": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "asset_class": {"type": "string"},
                        "target_pct": {"type": ["number", "null"]},
                        "actual_pct": {"type": ["number", "null"]},
                        "target_range_low": {"type": ["number", "null"]},
                        "target_range_high": {"type": ["number", "null"]},
                        "notes": {"type": ["string", "null"]},
                    },
                    "required": ["asset_class"],
                },
            },
            "performance": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "scope": {"type": "string", "description": "'total_fund' or asset class name"},
                        "period": {
                            "type": "string",
                            "enum": ["fy", "1y", "3y", "5y", "10y", "since_inception"],
                        },
                        "return_pct": {"type": ["number", "null"]},
                        "benchmark_return_pct": {"type": ["number", "null"]},
                        "benchmark_name": {"type": ["string", "null"]},
                        "notes": {"type": ["string", "null"]},
                    },
                    "required": ["scope", "period"],
                },
            },
            "notes": {
                "type": ["string", "null"],
                "description": "Top-level commentary on extraction quality / missing data.",
            },
        },
        "required": ["asset_allocation", "performance"],
    },
}


_client: anthropic.Anthropic | None = None


def _get_client() -> anthropic.Anthropic:
    global _client
    if _client is not None:
        return _client
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        load_dotenv(_ENV_PATH, override=True)
        api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        # Fallback to Claude Code session token (matches summarizer.py pattern)
        token_file = os.environ.get("CLAUDE_SESSION_INGRESS_TOKEN_FILE")
        if token_file and os.path.exists(token_file):
            with open(token_file) as f:
                auth_token = f.read().strip()
            if auth_token:
                _client = anthropic.Anthropic(auth_token=auth_token)
                return _client
        raise RuntimeError(f"ANTHROPIC_API_KEY not set. Check {_ENV_PATH}")
    _client = anthropic.Anthropic(api_key=api_key, base_url="https://api.anthropic.com")
    return _client


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=4, max=30),
    retry=retry_if_exception_type((anthropic.APIConnectionError,
                                    anthropic.APIStatusError,
                                    anthropic.RateLimitError)),
)
def call_claude(plan_name: str, fiscal_year: int | None,
                section_text: str) -> dict:
    """Send the Investment Section to Claude; return parsed tool input."""
    user_message = (
        f"Plan: {plan_name}\n"
        f"Fiscal year: {fiscal_year if fiscal_year else '(unknown)'}\n\n"
        f"--- INVESTMENT SECTION TEXT ---\n{section_text}"
    )

    msg = _get_client().messages.create(
        model=MODEL,
        max_tokens=MAX_OUTPUT_TOKENS,
        # System prompt cached so subsequent CAFRs pay only the cache-read price.
        system=[{
            "type": "text",
            "text": SYSTEM_PROMPT,
            "cache_control": {"type": "ephemeral"},
        }],
        tools=[TOOL_SCHEMA],
        tool_choice={"type": "tool", "name": "record_investment_data"},
        messages=[{"role": "user", "content": user_message}],
    )

    if msg.stop_reason == "max_tokens":
        # The tool-call JSON was almost certainly truncated mid-emission;
        # the partial input may parse but be missing trailing arrays.
        console.print(
            f"  [yellow]warning: model hit max_tokens "
            f"(in={msg.usage.input_tokens} out={msg.usage.output_tokens})[/yellow]"
        )

    for block in msg.content:
        if block.type == "tool_use" and block.name == "record_investment_data":
            return block.input

    raise RuntimeError(
        f"Claude did not call record_investment_data; stop_reason={msg.stop_reason}"
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
                 pages_used: str, text_hash: str) -> CafrExtract:
    # If a previous extract exists for this document, replace it cleanly.
    existing = session.query(CafrExtract).filter_by(document_id=doc.id).first()
    if existing is not None:
        session.delete(existing)
        session.flush()

    extract = CafrExtract(
        plan_id=doc.plan_id,
        document_id=doc.id,
        fiscal_year=doc.fiscal_year,
        investment_policy_text=payload.get("investment_policy_text"),
        extracted_at=datetime.utcnow(),
        model_used=MODEL,
        pages_used=pages_used,
        text_hash=text_hash,
        notes=payload.get("notes"),
    )
    session.add(extract)
    session.flush()  # populate extract.id

    for row in payload.get("asset_allocation", []):
        session.add(CafrAllocation(
            cafr_extract_id=extract.id,
            asset_class=str(row.get("asset_class", ""))[:200],
            target_pct=_coerce_float(row.get("target_pct")),
            actual_pct=_coerce_float(row.get("actual_pct")),
            target_range_low=_coerce_float(row.get("target_range_low")),
            target_range_high=_coerce_float(row.get("target_range_high")),
            notes=row.get("notes"),
        ))

    for row in payload.get("performance", []):
        session.add(CafrPerformance(
            cafr_extract_id=extract.id,
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

def extract_one(session, doc: Document, plan: Plan, *,
                force: bool = False,
                plan_meta: dict | None = None) -> str:
    """Process one CAFR document. Returns a status string."""
    label = f"{plan.abbreviation or doc.plan_id} FY{doc.fiscal_year or '?'}"

    if plan_meta and plan_meta.get("cafr_format") == "aggregator":
        console.print(f"  [dim]{label}: skipping (aggregator CAFR)[/dim]")
        return "aggregator_skipped"

    if not doc.local_path or not Path(doc.local_path).exists():
        console.print(f"  [yellow]{label}: missing local file[/yellow]")
        return "missing_file"

    rng = locate_investment_section(doc.local_path)
    if rng is None:
        console.print(f"  [yellow]{label}: Investment Section not found[/yellow]")
        return "no_section"
    start, end = rng

    section_text = extract_section_text(doc.local_path, start, end)
    if len(section_text) < 1000:
        console.print(f"  [yellow]{label}: section text too short ({len(section_text)} chars)[/yellow]")
        return "too_short"
    if len(section_text) > MAX_SECTION_CHARS:
        section_text = section_text[:MAX_SECTION_CHARS]
        truncated_note = f" (truncated to {MAX_SECTION_CHARS} chars)"
    else:
        truncated_note = ""

    text_hash = hashlib.md5(section_text.encode("utf-8", errors="ignore")).hexdigest()
    if not force:
        existing = session.query(CafrExtract).filter_by(document_id=doc.id).first()
        if existing is not None and existing.text_hash == text_hash:
            console.print(f"  [dim]{label}: already extracted (hash matches)[/dim]")
            return "already_extracted"

    pages_used = f"{start}-{end}"
    console.print(
        f"  [cyan]{label}: extracting from pages {pages_used} "
        f"({len(section_text):,} chars{truncated_note})[/cyan]"
    )

    try:
        payload = call_claude(plan.name, doc.fiscal_year, section_text)
    except Exception as e:
        console.print(f"  [red]{label}: Claude error: {e}[/red]")
        return "api_error"

    save_extract(session, doc, payload, pages_used=pages_used, text_hash=text_hash)
    n_alloc = len(payload.get("asset_allocation", []))
    n_perf = len(payload.get("performance", []))
    console.print(
        f"  [green]{label}: saved ({n_alloc} allocation rows, {n_perf} performance rows)[/green]"
    )
    return "saved"


def run_extraction(plan_ids: list[str] | None = None,
                   force: bool = False) -> dict[str, int]:
    init_db()
    # Load plan metadata (cafr_format etc.) from JSON since the DB schema
    # doesn't carry every per-plan field.
    from fetcher import load_plans
    plan_meta_by_id = {p["id"]: p for p in load_plans()}

    session = get_session()
    counts: dict[str, int] = {}

    try:
        q = (
            session.query(Document, Plan)
            .join(Plan, Document.plan_id == Plan.id)
            .filter(Document.doc_type == "cafr")
            .order_by(Plan.aum_billions.desc().nullslast())
        )
        if plan_ids:
            q = q.filter(Document.plan_id.in_(plan_ids))

        # Skip already-extracted unless force
        if not force:
            extracted_doc_ids = {
                did for (did,) in session.query(CafrExtract.document_id).all()
            }
            docs = [(d, p) for d, p in q.all() if d.id not in extracted_doc_ids]
        else:
            docs = list(q.all())

        console.print(f"[bold]Extracting {len(docs)} CAFR(s)[/bold]")
        for doc, plan in docs:
            console.rule(f"[bold]{plan.abbreviation or doc.plan_id}[/bold]")
            try:
                status = extract_one(session, doc, plan, force=force,
                                     plan_meta=plan_meta_by_id.get(doc.plan_id))
            except Exception as e:
                status = "error"
                console.print(f"  [red]{plan.abbreviation}: {e}[/red]")
            counts[status] = counts.get(status, 0) + 1
    finally:
        session.close()

    console.rule("[bold green]Extraction complete[/bold green]")
    for status in ("saved", "already_extracted", "aggregator_skipped",
                   "no_section", "too_short", "missing_file",
                   "api_error", "error"):
        if counts.get(status):
            console.print(f"  {status:20s} {counts[status]}")
    return counts


def main():
    parser = argparse.ArgumentParser(
        description="Extract Investment Section data from CAFR PDFs.")
    parser.add_argument("plan_ids", nargs="*",
                        help="Plan IDs to process (default: all CAFRs not yet extracted).")
    parser.add_argument("--redo", "--force", action="store_true",
                        help="Re-extract even if a CafrExtract row already exists.")
    args = parser.parse_args()

    counts = run_extraction(plan_ids=args.plan_ids or None, force=args.redo)
    sys.exit(0 if not counts.get("error") and not counts.get("api_error") else 1)


if __name__ == "__main__":
    main()
