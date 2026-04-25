"""
CAFR / ACFR report discovery and downloader.

For each pension plan, find the URL of its Comprehensive Annual Financial
Report (CAFR) — renamed to Annual Comprehensive Financial Report (ACFR) by
GFOA in 2021 — and download it.

Discovery uses three strategies in order, reusing what we already know
about each plan from prior pipeline runs:

  1. Mine extracted text from documents already in the DB (agendas,
     board packs often link directly to the plan's CAFR).
  2. Crawl the plan's `website` — probe common paths (/cafr, /acfr,
     /publications, /financial-reports, ...) and follow CAFR-named
     links one level deep.
  3. DuckDuckGo HTML search scoped to the plan's domain (no API key).

Usage:
    python fetch_cafr.py                       # all plans
    python fetch_cafr.py calpers calstrs       # specific plans
    python fetch_cafr.py --max-per-plan 3      # multiple years
    python fetch_cafr.py --min-year 2023       # drop older candidates
"""

import argparse
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus, unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from rich.console import Console

from database import Document, document_exists, get_session, init_db
from fetcher import (
    DOWNLOADS_DIR,
    HEADERS,
    download_document,
    fetch_page_requests,
    load_plans,
)

console = Console(legacy_windows=False)

# ---------------------------------------------------------------------------
# CAFR detection
# ---------------------------------------------------------------------------

# Matches link text / combined URL+text. Broad on purpose — we filter
# further by requiring PDFs + an investment/finance signal.
CAFR_REGEX = re.compile(
    r"(\bcafr\b"
    r"|\bacfr\b"
    r"|comprehensive[\s\-]*annual[\s\-]*financial[\s\-]*report"
    r"|annual[\s\-]*comprehensive[\s\-]*financial[\s\-]*report)",
    re.IGNORECASE,
)

# Narrower check for URLs alone (URL path rarely contains full phrase).
CAFR_URL_HINT = re.compile(
    r"(cafr|acfr|comprehensive[-_]?annual|annual[-_]?comprehensive"
    r"|annual[-_]financial[-_]report)",
    re.IGNORECASE,
)

# Common paths to probe under a plan's website root.
CAFR_SEED_PATHS = [
    "/cafr", "/acfr",
    "/financial-reports", "/financial-report",
    "/annual-report", "/annual-reports",
    "/publications", "/publications/financial-reports",
    "/reports/cafr", "/reports/acfr", "/reports",
    "/about/cafr", "/about/acfr", "/about/financial-reports",
    "/resources/publications",
    "/investments/financial-reports",
]

# Minimum plausible CAFR PDF size — most are 5-30 MB, but guard against
# 1-page redirect stubs / error HTML served with .pdf names.
MIN_PDF_BYTES = 100_000


# ---------------------------------------------------------------------------
# URL / filename helpers
# ---------------------------------------------------------------------------

URL_RE = re.compile(r"https?://[^\s)>\"'\]}]+", re.IGNORECASE)
YEAR_RE = re.compile(r"(20[0-3]\d|19\d{2})")


def year_from_url(url: str) -> int | None:
    """Pick the latest plausible fiscal year out of a URL path."""
    years = [int(y) for y in YEAR_RE.findall(url)]
    years = [y for y in years if 1990 <= y <= datetime.utcnow().year + 1]
    return max(years) if years else None


def make_cafr_filename(url: str, plan_abbrev: str) -> str:
    name = Path(urlparse(url).path).name
    if not name.lower().endswith(".pdf"):
        year = year_from_url(url)
        name = f"{plan_abbrev.lower()}-cafr-{year or 'unknown'}.pdf"
    return re.sub(r"[^\w\-.]+", "-", name)[:120]


def extract_cafr_urls_from_text(text: str) -> list[str]:
    """Pull CAFR-like PDF URLs out of free-form extracted text."""
    if not text:
        return []
    urls: list[str] = []
    seen: set[str] = set()
    for m in URL_RE.finditer(text):
        url = m.group(0).rstrip(".,;:)>]}'\"")
        base = url.lower().split("?")[0].split("#")[0]
        if not base.endswith(".pdf"):
            continue
        if not CAFR_URL_HINT.search(url):
            continue
        if url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls


def extract_cafr_links_from_page(soup: BeautifulSoup,
                                 base_url: str) -> list[dict]:
    """Return anchors on a page whose text or URL mentions CAFR/ACFR."""
    results = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip().rstrip("'\">")
        text = a.get_text(" ", strip=True)
        url = urljoin(base_url, href)
        if url in seen or not url.lower().startswith("http"):
            continue
        combined = f"{text} {url}"
        if not CAFR_REGEX.search(combined):
            continue
        seen.add(url)
        is_pdf = url.lower().split("?")[0].split("#")[0].endswith(".pdf")
        results.append({"url": url, "text": text, "is_pdf": is_pdf})
    return results


# ---------------------------------------------------------------------------
# Discovery strategies
# ---------------------------------------------------------------------------

def strategy_mine_existing(plan_id: str, session) -> list[str]:
    """Scan this plan's already-extracted documents for CAFR PDF URLs."""
    docs = (
        session.query(Document)
        .filter(Document.plan_id == plan_id)
        .filter(Document.extracted_text.isnot(None))
        .all()
    )
    urls: list[str] = []
    seen: set[str] = set()
    for doc in docs:
        for url in extract_cafr_urls_from_text(doc.extracted_text):
            if url not in seen:
                seen.add(url)
                urls.append(url)
    return urls


def strategy_site_crawl(plan: dict) -> list[str]:
    """Probe seed paths on plan.website, then follow CAFR links one hop."""
    website = plan.get("website")
    if not website:
        return []

    pdf_urls: list[str] = []
    seen_pdfs: set[str] = set()
    visited: set[str] = set()

    def add_pdf(u: str):
        if u not in seen_pdfs:
            seen_pdfs.add(u)
            pdf_urls.append(u)

    root = website.rstrip("/")
    candidates = [root] + [root + p for p in CAFR_SEED_PATHS]

    landing_pages: list[str] = []
    for url in candidates:
        if url in visited:
            continue
        visited.add(url)
        soup = fetch_page_requests(url)
        if soup is None:
            continue
        for link in extract_cafr_links_from_page(soup, url):
            if link["is_pdf"]:
                add_pdf(link["url"])
            else:
                landing_pages.append(link["url"])
        time.sleep(0.2)

    # Follow CAFR-named non-PDF links one level deeper.
    for page_url in landing_pages[:10]:
        if page_url in visited:
            continue
        visited.add(page_url)
        soup = fetch_page_requests(page_url)
        if soup is None:
            continue
        for link in extract_cafr_links_from_page(soup, page_url):
            if link["is_pdf"]:
                add_pdf(link["url"])
        time.sleep(0.3)

    return pdf_urls


def strategy_duckduckgo(plan: dict) -> list[str]:
    """Domain-scoped DuckDuckGo HTML search. No API key required."""
    name = plan.get("name") or ""
    website = plan.get("website") or ""
    domain = urlparse(website).netloc.replace("www.", "") if website else ""
    query = f'"{name}" (CAFR OR ACFR) filetype:pdf'
    if domain:
        query += f" site:{domain}"
    endpoint = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
    try:
        resp = requests.get(endpoint, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
    except Exception as e:
        console.print(f"  [yellow]DuckDuckGo search failed: {e}[/yellow]")
        return []

    results: list[str] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = a.get_text(" ", strip=True)
        # DDG wraps result URLs: /l/?uddg=<url-encoded>&...
        m = re.search(r"uddg=([^&]+)", href)
        actual = unquote(m.group(1)) if m else href
        if not actual.lower().startswith("http"):
            continue
        base = actual.lower().split("?")[0].split("#")[0]
        if not base.endswith(".pdf"):
            continue
        if not (CAFR_URL_HINT.search(actual) or CAFR_REGEX.search(text)):
            continue
        if actual in seen:
            continue
        seen.add(actual)
        results.append(actual)
    return results[:20]


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def discover_cafr_urls(plan: dict, session) -> list[str]:
    """Run all strategies, return deduped URLs sorted newest-first."""
    urls: list[str] = []
    seen: set[str] = set()

    def extend(new_urls: list[str], label: str):
        before = len(urls)
        for u in new_urls:
            if u not in seen:
                seen.add(u)
                urls.append(u)
        console.print(f"    {label}: +{len(urls) - before}")

    console.print("  [dim]strategies:[/dim]")
    extend(strategy_mine_existing(plan["id"], session), "mine-existing")
    extend(strategy_site_crawl(plan), "site-crawl")
    if not urls:
        extend(strategy_duckduckgo(plan), "duckduckgo")

    # Newest year first; unknown-year go last.
    urls.sort(key=lambda u: (year_from_url(u) or 0), reverse=True)
    return urls


def run_cafr_fetcher(plan_ids: list[str] = None,
                     max_per_plan: int = 1,
                     min_year: int | None = None):
    init_db()
    plans = load_plans()
    if plan_ids:
        plans = [p for p in plans if p["id"] in plan_ids]

    session = get_session()
    total_new = 0

    try:
        for plan in plans:
            console.rule(f"[bold]{plan['abbreviation']}[/bold] CAFR")

            urls = discover_cafr_urls(plan, session)
            if not urls:
                console.print("  [yellow]No CAFR candidates found.[/yellow]")
                continue

            console.print(f"  [cyan]{len(urls)} candidate URL(s)[/cyan]")
            plan_dir = DOWNLOADS_DIR / plan["id"] / "cafr"
            saved = 0

            for url in urls:
                if saved >= max_per_plan:
                    break

                year = year_from_url(url)
                if min_year is not None and (year or 0) < min_year:
                    continue

                if document_exists(session, url):
                    console.print(f"  [dim]skip (in DB): {url}[/dim]")
                    continue

                filename = make_cafr_filename(url, plan["abbreviation"])
                console.print(f"  [cyan]→ {url}[/cyan]")
                local_path, size = download_document(url, plan_dir, filename)
                if not local_path:
                    continue

                if size < MIN_PDF_BYTES:
                    console.print(
                        f"  [yellow]file only {size} bytes; "
                        f"not a real CAFR — discarding[/yellow]"
                    )
                    try:
                        local_path.unlink()
                    except OSError:
                        pass
                    continue

                meeting_date = datetime(year, 12, 31) if year else None
                session.add(Document(
                    plan_id=plan["id"],
                    url=url,
                    filename=local_path.name,
                    doc_type="cafr",
                    local_path=str(local_path),
                    file_size_bytes=size,
                    downloaded_at=datetime.utcnow(),
                    extraction_status="pending",
                    meeting_date=meeting_date,
                ))
                session.commit()
                saved += 1
                total_new += 1
                time.sleep(0.5)

            console.print(f"  [green]{saved} CAFR(s) saved[/green]")

    finally:
        session.close()

    console.print(
        f"\n[bold green]Done. {total_new} new CAFR document(s).[/bold green]"
    )
    return total_new


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Fetch CAFR/ACFR reports for pension plans."
    )
    parser.add_argument("plan_ids", nargs="*",
                        help="Plan IDs to process (default: all).")
    parser.add_argument("--max-per-plan", type=int, default=1,
                        help="Max CAFRs to download per plan (default 1 — latest).")
    parser.add_argument("--min-year", type=int,
                        help="Skip candidate CAFRs older than this fiscal year.")
    args = parser.parse_args()

    run_cafr_fetcher(
        plan_ids=args.plan_ids or None,
        max_per_plan=args.max_per_plan,
        min_year=args.min_year,
    )


if __name__ == "__main__":
    main()
