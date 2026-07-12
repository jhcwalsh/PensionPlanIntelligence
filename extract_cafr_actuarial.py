"""
Extract structured funding / actuarial data from CAFR/ACFR PDFs.

For each CAFR document we:
  1. Locate the Actuarial Section pages (PDF TOC first; fallback to text
     search), reusing `extract_cafr_investments.locate_investment_section`
     with actuarial-specific `start_patterns`/`end_patterns`.
  2. If no Actuarial Section is found at all, fall back to the first page
     mentioning "net pension liability" (case-insensitive), +/- 3 pages
     (bounded to the document).
  3. Extract text from those pages with PyMuPDF.
  4. Send to Claude (Sonnet) with a tool-use schema requesting flat funding /
     actuarial metrics (funded ratio, AAL, discount rate, contribution
     rates, membership counts, etc).
  5. Write into `cafr_actuarial` (one row per document; unique on
     `document_id`).

Idempotent per `(document_id, prompt_version)`: if a `CafrActuarial` row
already exists for the document with a matching `text_hash` and
`prompt_version`, we skip it. Otherwise we delete the old row and replace it.

Usage:
    python extract_cafr_actuarial.py                     # all unextracted CAFRs
    python extract_cafr_actuarial.py calpers ktrs         # specific plans
    python extract_cafr_actuarial.py --limit 5            # cap the number of docs processed
"""

import argparse
import hashlib
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
    CafrActuarial,
    Document,
    Plan,
    get_session,
    init_db,
)
from extract_cafr_investments import extract_section_text, locate_investment_section

_ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(_ENV_PATH, override=True)

console = Console(legacy_windows=False)

MODEL = "claude-sonnet-4-6"
MAX_OUTPUT_TOKENS = 8192
MAX_SECTION_CHARS = 200_000

PROMPT_VERSION = "actuarial_v1"

# Section-header phrases that mark the start/end of the Actuarial Section.
ACTUARIAL_START = (
    re.compile(r"^\s*actuarial\s+section\s*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^\s*actuarial\s+valuation\s*$", re.IGNORECASE | re.MULTILINE),
)
ACTUARIAL_END = (
    re.compile(r"^\s*statistical\s+section\s*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^\s*investment\s+section\s*$", re.IGNORECASE | re.MULTILINE),
)

# Fallback phrase used when no Actuarial Section can be located at all.
_NET_PENSION_LIABILITY = re.compile(r"net\s+pension\s+liability", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Section location
# ---------------------------------------------------------------------------

def _fallback_net_pension_liability(pdf_path: str) -> tuple[int, int] | None:
    """Fallback when no Actuarial Section is found via TOC/text search.

    Scans pages for "net pension liability" (case-insensitive) and returns
    the first hit's page +/- 3 pages, bounded to the document. Returns None
    if the phrase never appears.
    """
    doc = fitz.open(pdf_path)
    try:
        hit_page = None
        for i in range(doc.page_count):
            text = doc.load_page(i).get_text()
            if _NET_PENSION_LIABILITY.search(text):
                hit_page = i + 1  # 1-indexed
                break
        if hit_page is None:
            return None
        start = max(1, hit_page - 3)
        end = min(doc.page_count, hit_page + 3)
        return (start, end)
    finally:
        doc.close()


# ---------------------------------------------------------------------------
# Claude tool-use extraction
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You extract structured funding / actuarial data from the Actuarial Section of US public pension Annual Comprehensive Financial Reports (CAFRs / ACFRs).

For each CAFR you receive, you return the plan's key funding metrics via the `record_actuarial_data` tool:

- `valuation_date` — the actuarial valuation date (YYYY-MM-DD) the reported figures are as of.
- `funded_ratio_pct` / `market_funded_ratio_pct` — the funded ratio on an actuarial-value-of-assets basis and, if separately reported, a market-value basis. Percentages (75.0 means 75%, NOT 0.75).
- `actuarial_value_assets_millions` / `actuarial_accrued_liability_millions` / `unfunded_aal_millions` — in millions of dollars.
- `net_pension_liability_millions` — the GASB 67/68 net pension liability, in millions.
- `discount_rate_pct` / `assumed_return_pct` / `inflation_pct` / `payroll_growth_pct` — key actuarial assumptions, as percentages.
- `amortization_years` — the amortization period (years) for the unfunded liability.
- `employer_contribution_rate_pct` / `employee_contribution_rate_pct` — contribution rates as a percentage of payroll.
- `adc_millions` — the Actuarially Determined Contribution, in millions.
- `adc_pct_contributed` — the percentage of the ADC actually contributed (100.0 means fully funded contribution).
- `members_active` / `members_retired` — membership counts (integers).
- `actuary_firm` — the name of the plan's actuary / consulting firm.
- `notes` — top-level commentary on extraction quality or missing data.

Rules:
- Do NOT invent numbers. If a figure is not stated, return null rather than guessing.
- Numbers reported as basis points: convert to percentage points (140 bps -> 1.4).
- If the section is mostly missing or not parseable, return nulls and explain in `notes`; don't guess."""


TOOL_SCHEMA = {
    "name": "record_actuarial_data",
    "description": "Record structured funding/actuarial data extracted from a CAFR.",
    "input_schema": {
        "type": "object",
        "properties": {
            "valuation_date": {"type": ["string", "null"], "description": "YYYY-MM-DD"},
            "funded_ratio_pct": {"type": ["number", "null"]},
            "market_funded_ratio_pct": {"type": ["number", "null"]},
            "actuarial_value_assets_millions": {"type": ["number", "null"]},
            "actuarial_accrued_liability_millions": {"type": ["number", "null"]},
            "unfunded_aal_millions": {"type": ["number", "null"]},
            "net_pension_liability_millions": {"type": ["number", "null"]},
            "discount_rate_pct": {"type": ["number", "null"]},
            "assumed_return_pct": {"type": ["number", "null"]},
            "inflation_pct": {"type": ["number", "null"]},
            "payroll_growth_pct": {"type": ["number", "null"]},
            "amortization_years": {"type": ["number", "null"]},
            "employer_contribution_rate_pct": {"type": ["number", "null"]},
            "employee_contribution_rate_pct": {"type": ["number", "null"]},
            "adc_millions": {"type": ["number", "null"]},
            "adc_pct_contributed": {"type": ["number", "null"]},
            "members_active": {"type": ["integer", "null"]},
            "members_retired": {"type": ["integer", "null"]},
            "actuary_firm": {"type": ["string", "null"]},
            "notes": {"type": ["string", "null"]},
        },
        "required": [],
    },
}


MOCK_PAYLOAD = {
    "valuation_date": "2025-06-30",
    "funded_ratio_pct": 75.0,
    "market_funded_ratio_pct": 78.0,
    "actuarial_value_assets_millions": 5000.0,
    "actuarial_accrued_liability_millions": 6600.0,
    "unfunded_aal_millions": 1600.0,
    "net_pension_liability_millions": 1500.0,
    "discount_rate_pct": 6.8,
    "assumed_return_pct": 6.8,
    "inflation_pct": 2.5,
    "payroll_growth_pct": 2.75,
    "amortization_years": 20.0,
    "employer_contribution_rate_pct": 25.0,
    "employee_contribution_rate_pct": 7.0,
    "adc_millions": 400.0,
    "adc_pct_contributed": 100.0,
    "members_active": 10000,
    "members_retired": 8000,
    "actuary_firm": "Cavanaugh Macdonald",
    "notes": None,
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
    """Send the Actuarial Section to Claude; return parsed tool input.

    In mock mode (LLM_MODE=mock) this returns MOCK_PAYLOAD verbatim without
    constructing a client or making any network call.
    """
    if os.environ.get("LLM_MODE") == "mock":
        return MOCK_PAYLOAD

    user_message = (
        f"Plan: {plan_name}\n"
        f"Fiscal year: {fiscal_year if fiscal_year else '(unknown)'}\n\n"
        f"--- ACTUARIAL SECTION TEXT ---\n{section_text}"
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
        tool_choice={"type": "tool", "name": "record_actuarial_data"},
        messages=[{"role": "user", "content": user_message}],
    )

    if msg.stop_reason == "max_tokens":
        console.print(
            f"  [yellow]warning: model hit max_tokens "
            f"(in={msg.usage.input_tokens} out={msg.usage.output_tokens})[/yellow]"
        )

    for block in msg.content:
        if block.type == "tool_use" and block.name == "record_actuarial_data":
            return block.input

    raise RuntimeError(
        f"Claude did not call record_actuarial_data; stop_reason={msg.stop_reason}"
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


def _coerce_int(v) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


_FLOAT_FIELDS = (
    "funded_ratio_pct",
    "market_funded_ratio_pct",
    "actuarial_value_assets_millions",
    "actuarial_accrued_liability_millions",
    "unfunded_aal_millions",
    "net_pension_liability_millions",
    "discount_rate_pct",
    "assumed_return_pct",
    "inflation_pct",
    "payroll_growth_pct",
    "amortization_years",
    "employer_contribution_rate_pct",
    "employee_contribution_rate_pct",
    "adc_millions",
    "adc_pct_contributed",
)
_INT_FIELDS = ("members_active", "members_retired")


def save_actuarial(session, doc: Document, payload: dict, *,
                   pages_used: str | None, text_hash: str) -> CafrActuarial:
    # If a previous row exists for this document, replace it cleanly.
    existing = session.query(CafrActuarial).filter_by(document_id=doc.id).first()
    if existing is not None:
        session.delete(existing)
        session.flush()

    kwargs = dict(
        plan_id=doc.plan_id,
        document_id=doc.id,
        fiscal_year=doc.fiscal_year,
        valuation_date=payload.get("valuation_date"),
        actuary_firm=payload.get("actuary_firm"),
        extracted_at=datetime.utcnow(),
        model_used=MODEL,
        prompt_version=PROMPT_VERSION,
        text_hash=text_hash,
        pages_used=pages_used,
        notes=payload.get("notes"),
    )
    for field in _FLOAT_FIELDS:
        kwargs[field] = _coerce_float(payload.get(field))
    for field in _INT_FIELDS:
        kwargs[field] = _coerce_int(payload.get(field))

    row = CafrActuarial(**kwargs)
    session.add(row)
    session.commit()
    return row


# ---------------------------------------------------------------------------
# Per-document orchestration
# ---------------------------------------------------------------------------

def extract_one(session, doc: Document, plan: Plan) -> str:
    """Process one CAFR document. Returns a status string."""
    label = f"{plan.abbreviation or doc.plan_id} FY{doc.fiscal_year or '?'}"

    # Mock mode bypasses PDF/file handling entirely.
    if os.environ.get("LLM_MODE") == "mock":
        text_hash = hashlib.md5(f"mock:{doc.id}".encode("utf-8")).hexdigest()
        existing = session.query(CafrActuarial).filter_by(document_id=doc.id).first()
        if (existing is not None and existing.text_hash == text_hash
                and existing.prompt_version == PROMPT_VERSION):
            console.print(f"  [dim]{label}: already extracted (mock hash + prompt_version match)[/dim]")
            return "already_have"

        payload = call_claude(plan.name, doc.fiscal_year, "")
        save_actuarial(session, doc, payload, pages_used=None, text_hash=text_hash)
        console.print(f"  [green]{label}: saved (mock)[/green]")
        return "saved"

    if not doc.local_path or not Path(doc.local_path).exists():
        console.print(f"  [yellow]{label}: missing local file[/yellow]")
        return "no_section"

    rng = locate_investment_section(doc.local_path, start_patterns=ACTUARIAL_START,
                                    end_patterns=ACTUARIAL_END)
    if rng is None:
        rng = _fallback_net_pension_liability(doc.local_path)
    if rng is None:
        console.print(f"  [yellow]{label}: Actuarial Section not found[/yellow]")
        return "no_section"
    start, end = rng

    section_text = extract_section_text(doc.local_path, start, end)
    if len(section_text) > MAX_SECTION_CHARS:
        section_text = section_text[:MAX_SECTION_CHARS]

    text_hash = hashlib.md5(section_text.encode("utf-8", errors="ignore")).hexdigest()
    pages_used = f"{start}-{end}"

    existing = session.query(CafrActuarial).filter_by(document_id=doc.id).first()
    if (existing is not None and existing.text_hash == text_hash
            and existing.prompt_version == PROMPT_VERSION):
        console.print(f"  [dim]{label}: already extracted (hash + prompt_version match)[/dim]")
        return "already_have"

    console.print(
        f"  [cyan]{label}: extracting from pages {pages_used} "
        f"({len(section_text):,} chars)[/cyan]"
    )

    try:
        payload = call_claude(plan.name, doc.fiscal_year, section_text)
    except Exception as e:
        console.print(f"  [red]{label}: Claude error: {e}[/red]")
        return "failed"

    save_actuarial(session, doc, payload, pages_used=pages_used, text_hash=text_hash)
    console.print(f"  [green]{label}: saved[/green]")
    return "saved"


def run_extraction(plan_ids: list[str] | None = None,
                   limit: int | None = None) -> dict[str, int]:
    init_db()
    session = get_session()
    counts: dict[str, int] = {"saved": 0, "already_have": 0, "no_section": 0, "failed": 0}

    try:
        q = (
            session.query(Document, Plan)
            .join(Plan, Document.plan_id == Plan.id)
            .filter(Document.doc_type == "cafr")
            .filter(Document.extraction_status == "done")
            .order_by(Plan.aum_billions.desc().nullslast())
        )
        if plan_ids:
            q = q.filter(Document.plan_id.in_(plan_ids))

        all_docs = q.all()

        # Latest per (plan_id, fiscal_year) — keep the highest document id
        # when a plan has re-fetched/re-extracted the same fiscal year.
        best_by_key: dict[tuple, tuple[Document, Plan]] = {}
        for doc, plan in all_docs:
            key = (doc.plan_id, doc.fiscal_year)
            cur = best_by_key.get(key)
            if cur is None or doc.id > cur[0].id:
                best_by_key[key] = (doc, plan)
        docs = list(best_by_key.values())

        if limit is not None:
            docs = docs[:limit]

        console.print(f"[bold]Extracting actuarial data for {len(docs)} CAFR(s)[/bold]")
        for doc, plan in docs:
            console.rule(f"[bold]{plan.abbreviation or doc.plan_id}[/bold]")
            try:
                status = extract_one(session, doc, plan)
            except Exception as e:
                status = "failed"
                console.print(f"  [red]{plan.abbreviation or doc.plan_id}: {e}[/red]")
            counts[status] = counts.get(status, 0) + 1
    finally:
        session.close()

    console.rule("[bold green]Extraction complete[/bold green]")
    for status in ("saved", "already_have", "no_section", "failed"):
        console.print(f"  {status:20s} {counts.get(status, 0)}")
    return counts


def main():
    parser = argparse.ArgumentParser(
        description="Extract funding/actuarial data from CAFR PDFs.")
    parser.add_argument("plan_ids", nargs="*",
                        help="Plan IDs to process (default: all unextracted CAFRs).")
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap the number of documents processed.")
    args = parser.parse_args()

    counts = run_extraction(plan_ids=args.plan_ids or None, limit=args.limit)
    sys.exit(0 if not counts.get("failed") else 1)


if __name__ == "__main__":
    main()
