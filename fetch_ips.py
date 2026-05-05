"""IPS (Investment Policy Statement) discovery, fetch and verification.

Designed to run from a residential IP (Windows Task Scheduler) so we don't
fight cloud-IP WAFs that block ~5-7% of plan sites from GHA datacenter
ranges. Three discovery layers, in priority order:

  1. plan["ips_url"] override in known_plans.json (highest confidence).
  2. Mine the plan's already-extracted documents for IPS URLs that
     appear in board materials or CAFRs.
  3. Site crawl: probe seed paths under plan.website and scrape any
     "investment policy" anchors on the resulting pages.

Each candidate URL is downloaded, validated (size + %PDF- magic header),
hash-deduplicated against existing IpsDocument rows for the plan, then
verified by Claude Haiku 4.5: "Is this {plan_name}'s primary IPS?"
Only verdict='yes' survives; partial / no rows get logged but not saved.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from database import Document, IpsDocument
from fetcher import (
    DOWNLOADS_DIR,
    download_document,
    fetch_page_requests,
    load_plans,
)


MIN_IPS_BYTES = 50_000  # IPS PDFs are typically 200KB-2MB; 50KB rejects HTML/blank.
MAX_TEXT_PAGES_FOR_VERIFY = 3   # send first ~3 pages of text to the verifier.


# ---------------------------------------------------------------------------
# Keyword + URL hints
# ---------------------------------------------------------------------------

# Match anchor text *or* URL slug for IPS-shaped documents.
IPS_REGEX = re.compile(
    r"("
    r"investment[\s\-_]*polic"           # "investment policy", "investment policies"
    r"|statement[\s\-_]*of[\s\-_]*investment[\s\-_]*polic"
    r"|\bIPS\b"
    r"|\bSIP\b"                          # statement of investment policy
    r")",
    re.IGNORECASE,
)

# Narrower URL-only pattern (for free-text mining, where false positives are common).
IPS_URL_HINT = re.compile(
    r"(investment[-_]?polic"
    r"|investment[-_]?policy[-_]?statement"
    r"|statement[-_]?of[-_]?investment[-_]?polic"
    r"|/ips[-_/.]"
    r"|[-_/]ips\.pdf$"
    r")",
    re.IGNORECASE,
)

# Seed paths to probe under plan.website if no override / mining hit.
IPS_SEED_PATHS = [
    "/investment-policy-statement",
    "/investment-policy",
    "/investment-policies",
    "/investments/investment-policy-statement",
    "/investments/investment-policy",
    "/investments/policies",
    "/investments",
    "/policies/investment-policy-statement",
    "/policies/investment-policy",
    "/policies",
    "/about/policies",
    "/about/investment-policy",
    "/governance/policies",
    "/governance/investment-policy",
    "/ips",
]


URL_RE = re.compile(r"https?://[^\s)>\"'\]}]+", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Discovery layer 1: explicit override
# ---------------------------------------------------------------------------

def override_url(plan: dict) -> str | None:
    """Return plan['ips_url'] verbatim if set."""
    return plan.get("ips_url") or None


# ---------------------------------------------------------------------------
# Discovery layer 2: mine already-extracted documents
# ---------------------------------------------------------------------------

def mine_existing_for_ips_urls(plan_id: str, session) -> list[str]:
    """Scan this plan's extracted documents for IPS-shaped PDF URLs."""
    docs = (
        session.query(Document)
        .filter(Document.plan_id == plan_id)
        .filter(Document.extracted_text.isnot(None))
        .all()
    )
    seen: set[str] = set()
    urls: list[str] = []
    for doc in docs:
        for m in URL_RE.finditer(doc.extracted_text or ""):
            url = m.group(0).rstrip(".,;:)>]}'\"")
            base = url.lower().split("?")[0].split("#")[0]
            if not base.endswith(".pdf"):
                continue
            if not IPS_URL_HINT.search(url):
                continue
            if url in seen:
                continue
            seen.add(url)
            urls.append(url)
    return urls


# ---------------------------------------------------------------------------
# Discovery layer 3: site crawl
# ---------------------------------------------------------------------------

def extract_ips_links_from_page(soup: BeautifulSoup,
                                base_url: str) -> list[dict]:
    """Return anchors on a page whose text or URL mentions IPS / 'investment policy'."""
    results: list[dict] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip().rstrip("'\">")
        text = a.get_text(" ", strip=True)
        url = urljoin(base_url, href)
        if url in seen or not url.lower().startswith("http"):
            continue
        combined = f"{text} {url}"
        if not IPS_REGEX.search(combined):
            continue
        seen.add(url)
        is_pdf = url.lower().split("?")[0].split("#")[0].endswith(".pdf")
        results.append({"url": url, "text": text, "is_pdf": is_pdf})
    return results


def site_crawl_for_ips(plan: dict) -> list[str]:
    """Probe seed paths under plan.website + any anchor pages discovered.

    Returns PDF URLs only; non-PDF "policy" landing pages are followed
    one hop to surface the eventual PDF link.
    """
    website = plan.get("website")
    if not website:
        return []

    pdf_urls: list[str] = []
    seen_pdfs: set[str] = set()
    visited: set[str] = set()
    landing_pages: list[str] = []

    def add_pdf(u: str):
        if u not in seen_pdfs:
            seen_pdfs.add(u)
            pdf_urls.append(u)

    def harvest(soup: BeautifulSoup, base_url: str):
        for link in extract_ips_links_from_page(soup, base_url):
            if link["is_pdf"]:
                add_pdf(link["url"])
            else:
                landing_pages.append(link["url"])

    root = website.rstrip("/")
    candidates = [root] + [root + p for p in IPS_SEED_PATHS]
    for url in candidates:
        if url in visited:
            continue
        visited.add(url)
        soup = fetch_page_requests(url)
        if soup is None:
            continue
        harvest(soup, url)

    # Follow non-PDF policy pages one hop
    for url in list(landing_pages):
        if url in visited:
            continue
        visited.add(url)
        soup = fetch_page_requests(url)
        if soup is None:
            continue
        for link in extract_ips_links_from_page(soup, url):
            if link["is_pdf"]:
                add_pdf(link["url"])

    return pdf_urls


# ---------------------------------------------------------------------------
# Combined discovery
# ---------------------------------------------------------------------------

def discover_ips_urls(plan: dict, session) -> list[tuple[str, str]]:
    """Return [(url, source)] candidates in priority order.

    Sources: 'override' | 'mine_existing' | 'site_crawl'.
    De-duplicated across sources; first-seen wins.
    """
    seen: set[str] = set()
    out: list[tuple[str, str]] = []

    def add(url: str, source: str):
        if url and url not in seen:
            seen.add(url)
            out.append((url, source))

    if (u := override_url(plan)):
        add(u, "override")

    for u in mine_existing_for_ips_urls(plan["id"], session):
        add(u, "mine_existing")

    for u in site_crawl_for_ips(plan):
        add(u, "site_crawl")

    return out


# ---------------------------------------------------------------------------
# Validation: PDF magic + content hash
# ---------------------------------------------------------------------------

def looks_like_pdf(path: Path) -> bool:
    """True if file starts with the %PDF- magic header."""
    try:
        with open(path, "rb") as f:
            return f.read(5) == b"%PDF-"
    except OSError:
        return False


def file_sha256(path: Path) -> str:
    """Hex sha256 of the file at `path`."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# LLM verification (Haiku 4.5)
# ---------------------------------------------------------------------------

VERIFY_PROMPT = """\
You are verifying whether a PDF is the comprehensive Investment Policy
Statement (IPS) for a specific U.S. public-pension plan.

Plan: {plan_name}

The following is the first ~3 pages of extracted text from the PDF:

```
{text}
```

Classify the document. The COMPREHENSIVE IPS typically contains:
asset allocation targets and ranges, rebalancing rules, performance
benchmarks, manager guidelines, risk parameters, governance roles.
Adjacent single-policy documents (proxy voting, securities lending,
asset allocation policy as a standalone, rebalancing-only) are NOT
the comprehensive IPS.

Respond with valid JSON only, no commentary:

{{
  "is_ips": true | false,
  "confidence": "high" | "medium" | "low",
  "doc_type": "ips" | "asset_allocation_policy" | "rebalancing_policy" | \
"investment_beliefs" | "proxy_voting_policy" | "securities_lending" | \
"other_policy" | "manager_guidelines" | "unknown",
  "reason": "<one short sentence>"
}}
"""


def _mock_verify(text: str) -> dict:
    """Deterministic mock for tests / dry-runs without an API key.

    Heuristic: if the text contains a strong IPS phrase, say yes.
    """
    head = (text or "")[:3000].lower()
    strong = (
        "investment policy statement" in head
        or "statement of investment policy" in head
    )
    return {
        "is_ips": strong,
        "confidence": "high" if strong else "low",
        "doc_type": "ips" if strong else "unknown",
        "reason": "mock verification" + (" — strong title match" if strong else ""),
    }


def verify_is_ips(plan_name: str, extracted_text: str) -> dict:
    """LLM yes/no on whether `extracted_text` is `plan_name`'s primary IPS.

    Sends only the first ~3 pages (truncated by MAX_TEXT_PAGES_FOR_VERIFY).
    Returns dict with keys: is_ips, confidence, doc_type, reason.

    Honors IPS_MODE=mock for tests / offline runs (no API call).
    """
    if os.environ.get("IPS_MODE") == "mock":
        return _mock_verify(extracted_text)

    # Trim to first ~3 page-markers, fall back to first 8000 chars.
    sentinel = "[Page "
    pages: list[str] = []
    text = extracted_text or ""
    if sentinel in text:
        parts = text.split(sentinel)
        pages = parts[: MAX_TEXT_PAGES_FOR_VERIFY + 1]
        head = sentinel.join(pages)
    else:
        head = text[:8000]
    head = head[:8000]

    # Reuse the summarizer's Anthropic client — same auth path, same retry.
    from summarizer import _get_client, MODEL_HAIKU

    msg = _get_client().messages.create(
        model=MODEL_HAIKU,
        max_tokens=300,
        messages=[
            {"role": "user",
             "content": VERIFY_PROMPT.format(plan_name=plan_name, text=head)},
        ],
    )
    if not msg.content:
        # Treat empty response as "low-confidence no" rather than raising
        return {
            "is_ips": False, "confidence": "low",
            "doc_type": "unknown",
            "reason": f"empty LLM response (stop_reason={msg.stop_reason})",
        }
    raw = msg.content[0].text.strip()
    # Strip markdown fences if Claude added them
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\n?", "", raw)
        raw = re.sub(r"\n?```$", "", raw)
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {
            "is_ips": False, "confidence": "low",
            "doc_type": "unknown",
            "reason": f"LLM returned non-JSON: {raw[:120]!r}",
        }
    # Defensive: ensure expected keys exist.
    return {
        "is_ips": bool(parsed.get("is_ips")),
        "confidence": parsed.get("confidence") or "low",
        "doc_type": parsed.get("doc_type") or "unknown",
        "reason": parsed.get("reason") or "",
    }


# ---------------------------------------------------------------------------
# Filename helper
# ---------------------------------------------------------------------------

def make_ips_filename(url: str, plan_abbrev: str) -> str:
    name = Path(urlparse(url).path).name
    if not name.lower().endswith(".pdf"):
        name = f"{plan_abbrev.lower()}-ips.pdf"
    return re.sub(r"[^\w\-.]+", "-", name)[:120]
