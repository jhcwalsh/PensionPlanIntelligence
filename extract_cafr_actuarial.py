"""
Extract structured funding / actuarial data from CAFR/ACFR PDFs.

For each CAFR document we:
  1. Locate the Actuarial Section pages via `locate_actuarial_section`
     (PDF TOC first, then a contents-page-aware text scan, clamped to
     `MAX_SECTION_PAGES`).
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
from extract_cafr_investments import _locate_via_toc, extract_section_text

_ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(_ENV_PATH, override=True)

console = Console(legacy_windows=False)

MODEL = "claude-sonnet-4-6"
MAX_OUTPUT_TOKENS = 8192
MAX_SECTION_CHARS = 200_000

# Bound on the page span when no section end could be located and the range
# therefore runs to EOF. A wide range is NOT by itself suspicious — VRS's
# Actuarial Section legitimately spans 90 pages (pension + OPEB) — so this
# only applies when the end is unknown, never to a range whose end came from
# a real Statistical Section marker or the next top-level TOC entry.
MAX_SECTION_PAGES = 80

PROMPT_VERSION = "actuarial_v1"

# The Actuarial Section follows Introductory, Financial and Investment, so it
# never begins in a CAFR's front matter. Measured over the 44 CAFR PDFs on
# disk: every spurious start match landed on pages 3-8 (the table of contents,
# where PyMuPDF splits titles and page numbers onto separate lines so the
# anchored patterns below match a TOC line), while every genuine section start
# was page 37 or later. 20 sits in that gap with margin on both sides.
MIN_START_PAGE = 20

# Below this average, the located pages are scanned images whose text layer
# holds only headers, and every extracted field comes back null.
MIN_CHARS_PER_PAGE = 400

# Matched with `.search()` against a single-line TOC title. Requiring the word
# "section" matters: on flat outlines (KPERS ships 187 level-1 entries) a bare
# "actuarial" also matches "Actuarial Assumptions" in the Financial Section
# notes and anchors the range 28 pages too early. Prefixes and suffixes are
# fine — "III. Actuarial Section (Unaudited)", "Actuarial Section Cover".
ACTUARIAL_TOC_PATTERNS = (re.compile(r"\bactuarial\s+section\b", re.IGNORECASE),)

# Page-text section headers, strongest signal first: `_actuarial_text_search`
# only falls back to "Actuarial Valuation" when no "Actuarial Section" header
# exists, since the former also appears inside the Financial Section. Note
# `\s+` spans newlines, so on ACERA page 37 the pattern matches a table's
# stacked column header ("Actuarial / Valuation / Date") 96 pages early.
ACTUARIAL_START = (
    re.compile(r"^\s*actuarial\s+section\s*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^\s*actuarial\s+valuation\s*$", re.IGNORECASE | re.MULTILINE),
)

# Start patterns at these indices of ACTUARIAL_START are only honoured on a
# sparse page. A section divider carries little but its own title; the false
# positives this rules out are dense pages of running text and tables (ACERA
# page 37 is 3,974 characters of COLA notes).
_SPARSE_ONLY_START_INDEXES = frozenset({1})
_DIVIDER_MAX_CHARS = 1200
# Standard CAFR ordering is Introductory -> Financial -> Investment ->
# Actuarial -> Statistical, so only Statistical can legitimately follow the
# Actuarial Section. "Investment Section" was previously listed here, but it
# can only ever match ahead of a start that was itself matched too early.
ACTUARIAL_END = (
    re.compile(r"^\s*statistical\s+section\s*$", re.IGNORECASE | re.MULTILINE),
)

# Fallback phrase used when no Actuarial Section can be located at all.
_NET_PENSION_LIABILITY = re.compile(r"net\s+pension\s+liability", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Section location
# ---------------------------------------------------------------------------

def _find_section_end(doc: fitz.Document, start: int) -> int:
    """First page after `start` bearing an end header, minus one; else EOF."""
    for j in range(start, doc.page_count):  # 0-indexed j == page j+1, i.e. after start
        text = doc.load_page(j).get_text()
        if any(pat.search(text) for pat in ACTUARIAL_END):
            return j  # the page BEFORE the next section header
    return doc.page_count


def _actuarial_text_search(doc: fitz.Document) -> tuple[int, int] | None:
    """Scan page text for the Actuarial Section start, then its end.

    Candidates before `MIN_START_PAGE` are ignored, weak patterns are only
    honoured on a sparse page, and an "Actuarial Section" header always wins
    over an "Actuarial Valuation" one anywhere in the document. Returns a
    1-indexed inclusive range, or None.
    """
    first_hit: list[int | None] = [None] * len(ACTUARIAL_START)
    for i in range(MIN_START_PAGE - 1, doc.page_count):
        text = doc.load_page(i).get_text()
        sparse = len(text.strip()) <= _DIVIDER_MAX_CHARS
        for k, pat in enumerate(ACTUARIAL_START):
            if first_hit[k] is not None:
                continue
            if k in _SPARSE_ONLY_START_INDEXES and not sparse:
                continue
            if pat.search(text):
                first_hit[k] = i + 1  # PyMuPDF is 0-indexed; we use 1-indexed
        if first_hit[0] is not None:
            break  # strongest signal found; no weaker match can beat it

    start = next((p for p in first_hit if p is not None), None)
    if start is None:
        return None
    end = _find_section_end(doc, start)
    return (start, end) if end >= start else None


def locate_actuarial_section(pdf_path: str) -> tuple[int, int] | None:
    """Return the 1-indexed inclusive page range of the Actuarial Section.

    The PDF outline (TOC) supplies the start when it has one, falling back to
    a text scan; the end always comes from the page text. The TOC's own end
    is unusable because it is derived from outline hierarchy — on the flat
    outlines many plans ship (KPERS: 187 level-1 entries) the "next entry at
    the same level" is the next *heading*, so the section collapses to a
    single page. A TOC start inside the front matter is rejected as spurious
    (NMPERA's outline points its "Actuarial Section" entry at page 1;
    PERS-MS's at page -1). When no end can be found the range runs to EOF and
    is clamped to `MAX_SECTION_PAGES`; a wide range with a *located* end is
    left alone, since VRS's section really does span 90 pages.
    """
    doc = fitz.open(pdf_path)
    try:
        toc_rng = _locate_via_toc(doc, start_patterns=ACTUARIAL_TOC_PATTERNS)
        if toc_rng is not None and toc_rng[0] >= MIN_START_PAGE:
            start, end = toc_rng[0], _find_section_end(doc, toc_rng[0])
        else:
            rng = _actuarial_text_search(doc)
            if rng is None:
                return None
            start, end = rng
        if start > doc.page_count or end < start:
            return None  # corrupt outline pointing past the last page
        if end >= doc.page_count and end - start + 1 > MAX_SECTION_PAGES:
            clamped = start + MAX_SECTION_PAGES - 1
            console.print(
                f"  [yellow]no Actuarial Section end found; span {start}-{end} "
                f"({end - start + 1} pages) clamped to {start}-{clamped}[/yellow]"
            )
            end = clamped
        return (start, end)
    finally:
        doc.close()


def _has_usable_text(text: str, start: int, end: int) -> bool:
    """True if the located pages carry a real text layer, not just headers.

    Measured across the CAFRs on disk: text-bearing Actuarial Sections run
    ~2,000 characters per page (MN PERA 1,970; KPERS 2,345), while PERS-OR's
    image-based one yields 89.
    """
    return len(text) / max(1, end - start + 1) >= MIN_CHARS_PER_PAGE


def _fallback_net_pension_liability(pdf_path: str) -> tuple[int, int] | None:
    """Fallback when no Actuarial Section is found via TOC/text search.

    Returns the span of pages mentioning "net pension liability"
    (case-insensitive), or None if the phrase never appears. GASB 67/68
    disclosures run across the notes and the RSI schedules, so the span is
    what carries the figures — a narrow probe around the first hit lands in
    MD&A and comes back with a funded ratio and nothing else. A lone hit
    still gets +/- 3 pages of context. Capped at `MAX_SECTION_PAGES`.

    The scan skips the front matter for the same reason the section locator
    does: a CAFR's contents page lists the phrase, so an unfiltered scan
    anchors on it (PERS-OR hits page 5 and returns pages 2-8 of cover art).
    Documents too short to have front matter are scanned whole.
    """
    doc = fitz.open(pdf_path)
    try:
        first_page = MIN_START_PAGE if doc.page_count > MIN_START_PAGE else 1
        hits = [i + 1 for i in range(first_page - 1, doc.page_count)
                if _NET_PENSION_LIABILITY.search(doc.load_page(i).get_text())]
        if not hits:
            return None
        if len(hits) == 1:
            start, end = hits[0] - 3, hits[0] + 3
        else:
            start, end = hits[0], hits[-1]
        start = max(1, start)
        end = min(doc.page_count, end, start + MAX_SECTION_PAGES - 1)
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

    rng = locate_actuarial_section(doc.local_path)
    if rng is None:
        rng = _fallback_net_pension_liability(doc.local_path)
    if rng is None:
        console.print(f"  [yellow]{label}: Actuarial Section not found[/yellow]")
        return "no_section"
    start, end = rng
    section_text = extract_section_text(doc.local_path, start, end)

    # Locating the right pages is not enough: some plans publish the Actuarial
    # Section as scanned images, so the pages carry a text layer of headers and
    # nothing else (PERS-OR FY2024 yields 89 chars/page across 46 pages, and
    # every extracted field comes back null). Prefer the net-pension-liability
    # window in that case — it lands in the text-bearing Financial Section.
    if not _has_usable_text(section_text, start, end):
        fallback = _fallback_net_pension_liability(doc.local_path)
        if fallback is not None:
            fallback_text = extract_section_text(doc.local_path, *fallback)
            if len(fallback_text) > len(section_text):
                console.print(
                    f"  [yellow]{label}: pages {start}-{end} have no usable text "
                    f"layer ({len(section_text):,} chars); falling back to "
                    f"pages {fallback[0]}-{fallback[1]}[/yellow]"
                )
                start, end = fallback
                section_text = fallback_text

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
