"""
Extract structured data from verified Investment Policy Statements (IPS).

Unlike the CAFR extractor (`extract_cafr_investments.py`), the input here is
already-extracted plain text living in `IpsDocument.extracted_text` (a
GzippedText column decompressed transparently by the ORM) — no PDF section
location is needed. We only consider documents that passed the IPS
verification gate (`verification_verdict == "yes"`) in `fetch_ips.py`.

For each plan we take the latest (by `fetched_at`) verified IPS document and
send its text to Claude with a tool-use schema requesting:
  - objectives, rebalancing_policy, governance, manager_structure (JSON blobs)
  - asset_allocation: list of {asset_class, target_pct, range_low, range_high}
  - permitted_asset_classes / prohibited_investments (combined into one JSON blob)
  - esg_divestment_text, effective_date, adopted_date, notes

Extraction is idempotent per `(ips_document_id, prompt_version)`: if an
`IpsExtract` already exists for the document with a matching `text_hash`
(md5 of the extracted text) and `prompt_version`, we skip it. Otherwise we
delete the old extract (cascades to its `IpsAllocation` rows) and replace it.

Usage:
    python extract_ips.py                     # all plans with a verified IPS
    python extract_ips.py calpers ktrs         # specific plans
    python extract_ips.py --limit 5            # cap the number of plans processed
"""

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime

import anthropic
from dotenv import load_dotenv
from rich.console import Console
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from database import (
    IpsAllocation,
    IpsDocument,
    IpsExtract,
    Plan,
    get_session,
    init_db,
)

_ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(_ENV_PATH, override=True)

console = Console(legacy_windows=False)

MODEL = "claude-sonnet-4-6"
MAX_OUTPUT_TOKENS = 8192
MAX_INPUT_CHARS = 180_000

PROMPT_VERSION = "ips_v1"


# ---------------------------------------------------------------------------
# Claude tool-use extraction
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You extract structured data from Investment Policy Statements (IPS) of US public pension plans.

For each IPS you receive, you return structured data via the `record_ips_data` tool:

1. `objectives` — the plan's target return, risk tolerance, and funding goal, in the plan's own words where possible.
2. `asset_allocation` — one entry per asset class with the policy's target percentage and allowable range.
   - Use the plan's own asset-class naming (e.g. "Global Equity", "Public Fixed Income", "Real Estate", "Private Equity"). Do not normalise across plans.
   - `target_pct`, `range_low`, `range_high` are percentages of total portfolio (25.0 means 25%, NOT 0.25). Null if not stated.
3. `rebalancing_policy` — frequency, method, and any rebalancing bands.
4. `permitted_asset_classes` / `prohibited_investments` — lists drawn from the policy's stated permitted investments and any prohibitions (e.g. tobacco, sudan, direct commodities, leverage limits).
5. `governance` — approval authority (board vs. staff), staff delegation limits, and the named investment consultant and their role.
6. `manager_structure` — active/passive philosophy, internal/external management mix, and manager-selection criteria.
7. `esg_divestment_text` — any ESG or divestment policy language, verbatim or close paraphrase.
8. `effective_date` / `adopted_date` — YYYY-MM-DD if stated; otherwise null. Do not guess a date from context alone.
9. `notes` — top-level commentary on extraction quality or missing sections.

Rules:
- Do NOT invent numbers or dates. If a field is not stated, return null (or an empty array/list) rather than guessing.
- Numbers reported as basis points: convert to percentage points (140 bps -> 1.4).
- If the document is mostly missing or not parseable, return empty arrays/nulls and explain in `notes`; don't guess."""


TOOL_SCHEMA = {
    "name": "record_ips_data",
    "description": "Record structured data extracted from an Investment Policy Statement.",
    "input_schema": {
        "type": "object",
        "properties": {
            "objectives": {
                "type": "object",
                "properties": {
                    "target_return_pct": {"type": ["number", "null"]},
                    "risk_tolerance_text": {"type": ["string", "null"]},
                    "funding_goal_text": {"type": ["string", "null"]},
                },
            },
            "asset_allocation": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "asset_class": {"type": "string"},
                        "target_pct": {"type": ["number", "null"]},
                        "range_low": {"type": ["number", "null"]},
                        "range_high": {"type": ["number", "null"]},
                    },
                    "required": ["asset_class"],
                },
            },
            "rebalancing_policy": {
                "type": "object",
                "properties": {
                    "frequency": {"type": ["string", "null"]},
                    "method_text": {"type": ["string", "null"]},
                    "bands_text": {"type": ["string", "null"]},
                },
            },
            "permitted_asset_classes": {"type": "array", "items": {"type": "string"}},
            "prohibited_investments": {"type": "array", "items": {"type": "string"}},
            "governance": {
                "type": "object",
                "properties": {
                    "approval_authority_text": {"type": ["string", "null"]},
                    "staff_delegation_text": {"type": ["string", "null"]},
                    "consultant_name": {"type": ["string", "null"]},
                    "consultant_role": {"type": ["string", "null"]},
                },
            },
            "manager_structure": {
                "type": "object",
                "properties": {
                    "active_passive_text": {"type": ["string", "null"]},
                    "internal_external_text": {"type": ["string", "null"]},
                    "selection_criteria_text": {"type": ["string", "null"]},
                },
            },
            "esg_divestment_text": {"type": ["string", "null"]},
            "effective_date": {"type": ["string", "null"], "description": "YYYY-MM-DD"},
            "adopted_date": {"type": ["string", "null"]},
            "notes": {"type": ["string", "null"]},
        },
        "required": [],
    },
}


MOCK_PAYLOAD = {
    "objectives": {"target_return_pct": 7.0, "risk_tolerance_text": "moderate",
                   "funding_goal_text": "full funding by 2040"},
    "asset_allocation": [{"asset_class": "Global Equity", "target_pct": 40.0,
                          "range_low": 35.0, "range_high": 45.0}],
    "rebalancing_policy": {"frequency": "quarterly", "method_text": "to target",
                           "bands_text": "+/-3%"},
    "permitted_asset_classes": ["Global Equity"],
    "prohibited_investments": ["tobacco"],
    "governance": {"approval_authority_text": "board approves", "staff_delegation_text": None,
                   "consultant_name": "Meketa", "consultant_role": "general consultant"},
    "manager_structure": {"active_passive_text": None, "internal_external_text": None,
                          "selection_criteria_text": None},
    "esg_divestment_text": None,
    "effective_date": "2025-01-01", "adopted_date": None, "notes": None,
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
def call_claude(plan_name: str, ips_text: str) -> dict:
    """Send IPS text to Claude; return parsed tool input.

    In mock mode (LLM_MODE=mock) this returns MOCK_PAYLOAD verbatim without
    constructing a client or making any network call.
    """
    if os.environ.get("LLM_MODE") == "mock":
        return MOCK_PAYLOAD

    user_message = f"Plan: {plan_name}\n\n--- INVESTMENT POLICY STATEMENT TEXT ---\n{ips_text}"

    msg = _get_client().messages.create(
        model=MODEL,
        max_tokens=MAX_OUTPUT_TOKENS,
        # System prompt cached so subsequent IPS calls pay only the cache-read price.
        system=[{
            "type": "text",
            "text": SYSTEM_PROMPT,
            "cache_control": {"type": "ephemeral"},
        }],
        tools=[TOOL_SCHEMA],
        tool_choice={"type": "tool", "name": "record_ips_data"},
        messages=[{"role": "user", "content": user_message}],
    )

    if msg.stop_reason == "max_tokens":
        console.print(
            f"  [yellow]warning: model hit max_tokens "
            f"(in={msg.usage.input_tokens} out={msg.usage.output_tokens})[/yellow]"
        )

    for block in msg.content:
        if block.type == "tool_use" and block.name == "record_ips_data":
            return block.input

    raise RuntimeError(
        f"Claude did not call record_ips_data; stop_reason={msg.stop_reason}"
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


def save_extract(session, doc: IpsDocument, payload: dict, *, text_hash: str) -> IpsExtract:
    # If a previous extract exists for this document, replace it cleanly
    # (cascades to child IpsAllocation rows).
    existing = session.query(IpsExtract).filter_by(ips_document_id=doc.id).first()
    if existing is not None:
        session.delete(existing)
        session.flush()

    permitted_prohibited = {
        "permitted": payload.get("permitted_asset_classes") or [],
        "prohibited": payload.get("prohibited_investments") or [],
    }

    objectives = payload.get("objectives") or {}

    extract = IpsExtract(
        plan_id=doc.plan_id,
        ips_document_id=doc.id,
        extracted_at=datetime.utcnow(),
        model_used=MODEL,
        prompt_version=PROMPT_VERSION,
        text_hash=text_hash,
        target_return_pct=_coerce_float(objectives.get("target_return_pct")),
        effective_date=payload.get("effective_date"),
        adopted_date=payload.get("adopted_date"),
        objectives=json.dumps(objectives),
        rebalancing_policy=json.dumps(payload.get("rebalancing_policy") or {}),
        permitted_prohibited=json.dumps(permitted_prohibited),
        governance=json.dumps(payload.get("governance") or {}),
        manager_structure=json.dumps(payload.get("manager_structure") or {}),
        esg_divestment_text=payload.get("esg_divestment_text"),
        notes=payload.get("notes"),
    )
    session.add(extract)
    session.flush()  # populate extract.id

    for row in payload.get("asset_allocation", []):
        session.add(IpsAllocation(
            ips_extract_id=extract.id,
            asset_class=str(row.get("asset_class", ""))[:200],
            target_pct=_coerce_float(row.get("target_pct")),
            range_low=_coerce_float(row.get("range_low")),
            range_high=_coerce_float(row.get("range_high")),
        ))

    session.commit()
    return extract


# ---------------------------------------------------------------------------
# Per-plan orchestration
# ---------------------------------------------------------------------------

def _latest_verified_ips(session, plan_id: str) -> IpsDocument | None:
    """Latest (by fetched_at desc) verified, non-empty IPS document for a plan."""
    docs = (
        session.query(IpsDocument)
        .filter(IpsDocument.plan_id == plan_id)
        .filter(IpsDocument.verification_verdict == "yes")
        .order_by(IpsDocument.fetched_at.desc())
        .all()
    )
    for d in docs:
        if d.extracted_text:
            return d
    return None


def extract_one(session, plan: Plan) -> str:
    """Process one plan's latest verified IPS. Returns a status string."""
    label = plan.abbreviation or plan.id

    doc = _latest_verified_ips(session, plan.id)
    if doc is None:
        console.print(f"  [dim]{label}: no verified IPS with text[/dim]")
        return "no_candidates"

    text = doc.extracted_text
    if len(text) > MAX_INPUT_CHARS:
        text = text[:MAX_INPUT_CHARS]

    text_hash = hashlib.md5(text.encode("utf-8", errors="ignore")).hexdigest()

    existing = session.query(IpsExtract).filter_by(ips_document_id=doc.id).first()
    if (existing is not None
            and existing.text_hash == text_hash
            and existing.prompt_version == PROMPT_VERSION):
        console.print(f"  [dim]{label}: already extracted (hash + prompt_version match)[/dim]")
        return "already_have"

    console.print(f"  [cyan]{label}: extracting ({len(text):,} chars)[/cyan]")

    try:
        payload = call_claude(plan.name, text)
    except Exception as e:
        console.print(f"  [red]{label}: Claude error: {e}[/red]")
        return "failed"

    save_extract(session, doc, payload, text_hash=text_hash)
    n_alloc = len(payload.get("asset_allocation", []))
    console.print(f"  [green]{label}: saved ({n_alloc} allocation rows)[/green]")
    return "saved"


def run_extraction(plan_ids: list[str] | None = None,
                   limit: int | None = None) -> dict[str, int]:
    init_db()
    session = get_session()
    counts: dict[str, int] = {"saved": 0, "already_have": 0, "no_candidates": 0, "failed": 0}

    try:
        q = session.query(Plan).order_by(Plan.id.asc())
        if plan_ids:
            q = q.filter(Plan.id.in_(plan_ids))
        plans = q.all()
        if limit is not None:
            plans = plans[:limit]

        console.print(f"[bold]Extracting IPS data for {len(plans)} plan(s)[/bold]")
        for plan in plans:
            console.rule(f"[bold]{plan.abbreviation or plan.id}[/bold]")
            try:
                status = extract_one(session, plan)
            except Exception as e:
                status = "failed"
                console.print(f"  [red]{plan.abbreviation or plan.id}: {e}[/red]")
            counts[status] = counts.get(status, 0) + 1
    finally:
        session.close()

    console.rule("[bold green]Extraction complete[/bold green]")
    for status in ("saved", "already_have", "no_candidates", "failed"):
        console.print(f"  {status:20s} {counts.get(status, 0)}")
    return counts


def main():
    parser = argparse.ArgumentParser(
        description="Extract structured data from verified Investment Policy Statements.")
    parser.add_argument("plan_ids", nargs="*",
                        help="Plan IDs to process (default: all plans).")
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap the number of plans processed.")
    args = parser.parse_args()

    counts = run_extraction(plan_ids=args.plan_ids or None, limit=args.limit)
    sys.exit(0 if not counts.get("failed") else 1)


if __name__ == "__main__":
    main()
