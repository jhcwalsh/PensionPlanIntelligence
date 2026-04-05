"""
Document discovery and PDF/DOCX downloader.

For each plan in known_plans.json, fetches the materials page, finds
links to PDFs/Word docs, and downloads any that haven't been seen before.

Plans with materials_type="playwright" use a headless browser to render
JavaScript-heavy pages before link extraction.
"""

import json
import os
import re
import time
import hashlib
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from rich.console import Console

from database import Document, Plan, get_session, init_db, upsert_plan, document_exists

console = Console()

DOWNLOADS_DIR = Path(__file__).parent / "downloads"
PLANS_FILE = Path(__file__).parent / "data" / "known_plans.json"

DOC_EXTENSIONS = {".pdf", ".docx", ".doc", ".xlsx"}

# URL patterns that serve documents without a file extension
DOC_URL_PATTERNS = [
    r"/documents/[^/]+/download",   # CalPERS
    r"/media/\d+/download",         # IPERS, Drupal-based sites
    r"/download\?",                  # Generic inline downloads
    r"/files/.*\?",                  # Some Drupal sites
]

# A document link must match at least one of these to be kept
RELEVANT_KEYWORDS = [
    "agenda", "minutes", "board", "investment", "committee",
    "pack", "materials", "meeting", "report", "performance", "trustee"
]

# ---------------------------------------------------------------------------
# Investment-focus filter
# Links/pages must match at least one of these to be kept.
# All other committees (audit, benefits, legislative, finance, etc.) are dropped.
# ---------------------------------------------------------------------------
INVESTMENT_FOCUS = re.compile(
    r"(investment[\s\-_]committee|investment[\s\-_]board|board[\s\-_]of[\s\-_]invest"
    r"|investment[\s\-_]advisory|portfolio[\s\-_]committee|pctm"
    r"|investment[\s\-_]staff|investment[\s\-_]meeting|invest[\s\-_]material"
    r"|board[\s\-_]invest|boi[\b\-_/])",
    re.IGNORECASE,
)

# Pages/committees to explicitly exclude even if they contain the word "board"
EXCLUDE_COMMITTEES = re.compile(
    r"(audit[\s\-_]committee|finance[\s\-_]committee|benefit[\s\-_]review"
    r"|legislative[\s\-_]committee|compensation[\s\-_]committee"
    r"|personnel[\s\-_]committee|governance[\s\-_]committee"
    r"|real[\s\-_]estate[\s\-_]committee|general[\s\-_]counsel"
    r"|administration[\s\-_]committee|full[\s\-_]board(?!.*invest)"
    r"|board[\s\-_]of[\s\-_]retirement|board[\s\-_]of[\s\-_]trustee"
    r"|board[\s\-_]of[\s\-_]director)",
    re.IGNORECASE,
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    )
}


# ---------------------------------------------------------------------------
# Date / type helpers
# ---------------------------------------------------------------------------

DATE_PATTERNS = [
    (r"(\d{4})[_\-](\d{2})[_\-](\d{2})", "%Y-%m-%d"),
    (r"(\w+ \d{1,2},? \d{4})", "%B %d %Y"),
    (r"(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})", "%m/%d/%Y"),
]


def parse_date_from_text(text: str) -> datetime | None:
    text = text.replace(",", "").strip()
    for pattern, fmt in DATE_PATTERNS:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            raw = " ".join(g for g in m.groups() if g)
            try:
                return datetime.strptime(raw.strip(), fmt)
            except ValueError:
                continue
    return None


def guess_doc_type(url: str, link_text: str) -> str:
    combined = (url + " " + link_text).lower()
    if any(w in combined for w in ["minute", "minutes"]):
        return "minutes"
    if any(w in combined for w in ["agenda"]):
        return "agenda"
    if any(w in combined for w in ["performance", "return", "investment report"]):
        return "performance"
    if any(w in combined for w in ["pack", "material", "board book", "board packet"]):
        return "board_pack"
    return "board_pack"


# ---------------------------------------------------------------------------
# Page fetching — requests (fast) or Playwright (JS-rendered)
# ---------------------------------------------------------------------------

def load_plans() -> list[dict]:
    with open(PLANS_FILE) as f:
        return json.load(f)


def fetch_page_requests(url: str) -> BeautifulSoup | None:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "lxml")
    except Exception as e:
        console.print(f"  [red]requests failed: {e}[/red]")
        return None


def fetch_page_playwright(url: str, wait_selector: str = None,
                          scroll: bool = True) -> BeautifulSoup | None:
    """
    Render a JS-heavy page with a headless Chromium browser and return its
    fully-rendered HTML as a BeautifulSoup object.

    - wait_selector: CSS selector to wait for before extracting HTML
    - scroll: scroll to bottom to trigger lazy-loaded content
    """
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=HEADERS["User-Agent"],
                viewport={"width": 1280, "height": 900},
            )
            page = context.new_page()

            console.print(f"  [dim]Playwright: loading {url}[/dim]")
            try:
                page.goto(url, wait_until="networkidle", timeout=45_000)
            except Exception:
                # networkidle can timeout on pages with long-polling; fall back
                page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                time.sleep(3)

            # Wait for a specific element if specified
            if wait_selector:
                try:
                    page.wait_for_selector(wait_selector, timeout=15_000)
                except PWTimeout:
                    console.print(f"  [yellow]wait_selector '{wait_selector}' timed out, continuing[/yellow]")

            # Scroll to trigger lazy loading
            if scroll:
                for _ in range(5):
                    page.evaluate("window.scrollBy(0, window.innerHeight)")
                    time.sleep(0.4)

            # Extra settle time for SPAs
            time.sleep(1.5)

            html = page.content()
            browser.close()

        return BeautifulSoup(html, "lxml")

    except Exception as e:
        console.print(f"  [red]Playwright failed: {e}[/red]")
        return None


def fetch_page(plan: dict, url: str = None) -> BeautifulSoup | None:
    """Dispatch to requests or Playwright based on plan config."""
    target = url or plan["materials_url"]
    materials_type = plan.get("materials_type", "html_links")

    if materials_type == "playwright":
        wait_sel = plan.get("playwright_wait_selector")
        return fetch_page_playwright(target, wait_selector=wait_sel)
    else:
        soup = fetch_page_requests(target)
        if soup is None:
            # Fallback to Playwright on requests failure
            console.print(f"  [yellow]Falling back to Playwright...[/yellow]")
            return fetch_page_playwright(target)
        return soup


# ---------------------------------------------------------------------------
# Link extraction
# ---------------------------------------------------------------------------

def is_doc_url(url: str, link_text: str) -> bool:
    """Return True if this link points to a downloadable document."""
    parsed = urlparse(url)
    ext = Path(parsed.path).suffix.lower()
    if ext in DOC_EXTENSIONS:
        return True
    # Match extensionless download patterns (CalPERS, IPERS, Drupal, etc.)
    for pattern in DOC_URL_PATTERNS:
        if re.search(pattern, url, re.IGNORECASE):
            return True
    # Link text explicitly mentions a doc type
    if re.search(r"\.(pdf|docx?|xlsx?)\b", link_text, re.IGNORECASE):
        return True
    return False


def make_filename(url: str, link_text: str) -> str:
    """Generate a clean filename for a document URL."""
    parsed = urlparse(url)
    name = Path(parsed.path).name
    ext = Path(parsed.path).suffix.lower()

    # If we have a real filename with extension, use it
    if ext in DOC_EXTENSIONS and name:
        return name

    # Try to extract a meaningful slug from the URL path
    slug = parsed.path.rstrip("/").split("/")[-2] if "/download" in parsed.path else name
    slug = re.sub(r"[^\w\-]", "-", slug)[:60].strip("-") or hashlib.md5(url.encode()).hexdigest()[:12]

    # Determine extension from link text if not in URL
    text_ext_match = re.search(r"\.(pdf|docx?|xlsx?)\b", link_text, re.IGNORECASE)
    if text_ext_match:
        ext = "." + text_ext_match.group(1).lower()
    elif not ext:
        ext = ".pdf"  # assume PDF for pension sites

    return f"{slug}{ext}"


def is_investment_related(url: str, link_text: str, page_url: str = "") -> bool:
    """
    Return True only if this document or its page context is investment
    committee / board of investments related.

    When a plan uses investment_only=True (default), non-investment committee
    documents are dropped here. Plans can set investment_only=False to keep all.
    """
    combined = f"{url} {link_text} {page_url}"
    if EXCLUDE_COMMITTEES.search(combined):
        return False
    # If the page URL itself is investment-focused, trust all docs on that page
    if INVESTMENT_FOCUS.search(page_url):
        return True
    # Otherwise require an investment signal in the link/URL
    if INVESTMENT_FOCUS.search(combined):
        return True
    return False


def extract_doc_links(soup: BeautifulSoup, base_url: str,
                      investment_only: bool = True) -> list[dict]:
    """Extract document links from a BeautifulSoup page."""
    found = []
    seen_urls = set()

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        link_text = a_tag.get_text(strip=True)

        full_url = urljoin(base_url, href)

        if not is_doc_url(full_url, link_text):
            continue
        if full_url in seen_urls:
            continue
        seen_urls.add(full_url)

        combined = (full_url + " " + link_text).lower()
        if not any(kw in combined for kw in RELEVANT_KEYWORDS):
            continue

        if investment_only and not is_investment_related(full_url, link_text, base_url):
            continue

        filename = make_filename(full_url, link_text)
        meeting_date = parse_date_from_text(link_text) or parse_date_from_text(full_url)
        doc_type = guess_doc_type(full_url, link_text)

        found.append({
            "url": full_url,
            "filename": filename,
            "doc_type": doc_type,
            "link_text": link_text,
            "meeting_date": meeting_date,
        })

    return found


def find_sub_pages(soup: BeautifulSoup, base_url: str, pattern: str,
                   max_sub_pages: int = 12) -> list[str]:
    """
    Find internal sub-page links matching a regex pattern.
    Used for two-level crawls (e.g. CalPERS main → per-meeting sub-pages).
    """
    sub_urls = []
    seen = set()
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        link_text = a_tag.get_text(strip=True)
        full_url = urljoin(base_url, href)
        if full_url in seen:
            continue
        if re.search(pattern, full_url, re.IGNORECASE):
            # Still exclude non-investment sub-pages
            if not EXCLUDE_COMMITTEES.search(full_url + " " + link_text):
                seen.add(full_url)
                sub_urls.append(full_url)
        if len(sub_urls) >= max_sub_pages:
            break
    return sub_urls


def discover_document_links(plan: dict) -> list[dict]:
    """
    Fetch the plan's materials page and return a list of document dicts.
    Handles static HTML, JS-rendered, and two-level crawl patterns.
    Plans with investment_only=False skip the investment filter (default True).
    """
    materials_url = plan["materials_url"]
    investment_only = plan.get("investment_only", True)
    console.print(f"[cyan]Discovering documents for {plan['abbreviation']}...[/cyan]")

    soup = fetch_page(plan)
    if soup is None:
        return []

    found = extract_doc_links(soup, materials_url, investment_only=investment_only)

    # Two-level crawl: follow sub-pages matching a URL pattern before extracting docs
    sub_page_pattern = plan.get("sub_page_pattern")
    if sub_page_pattern:
        sub_pages = find_sub_pages(soup, materials_url, sub_page_pattern,
                                   max_sub_pages=plan.get("max_sub_pages", 12))
        console.print(f"  Following [dim]{len(sub_pages)}[/dim] sub-pages...")
        for sub_url in sub_pages:
            sub_soup = fetch_page(plan, url=sub_url)
            if sub_soup:
                found.extend(extract_doc_links(sub_soup, sub_url,
                                               investment_only=investment_only))
            time.sleep(0.5)

    # Additional explicit extra_pages (e.g. LACERA Board of Investments)
    for sub_url in plan.get("extra_pages", []):
        sub_soup = fetch_page(plan, url=sub_url)
        if sub_soup:
            # extra_pages are already investment-scoped, skip filter
            found.extend(extract_doc_links(sub_soup, sub_url, investment_only=False))

    # Deduplicate
    seen = set()
    unique = []
    for d in found:
        if d["url"] not in seen:
            seen.add(d["url"])
            unique.append(d)

    console.print(f"  Found [green]{len(unique)}[/green] document links")
    return unique


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_document(url: str, dest_dir: Path, filename: str) -> tuple[Path | None, int]:
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / filename

    if dest.exists():
        return dest, dest.stat().st_size

    try:
        resp = requests.get(url, headers=HEADERS, timeout=60, stream=True)
        resp.raise_for_status()

        cd = resp.headers.get("Content-Disposition", "")
        cd_match = re.search(r'filename="?([^";\n]+)"?', cd)
        if cd_match:
            filename = cd_match.group(1).strip()
            dest = dest_dir / filename

        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        return dest, dest.stat().st_size

    except Exception as e:
        console.print(f"  [red]Download failed for {url}: {e}[/red]")
        return None, 0


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

MIN_DATE = datetime(2025, 1, 1)


def run_fetcher(plan_ids: list[str] = None, max_docs_per_plan: int = 50):
    init_db()
    plans = load_plans()

    if plan_ids:
        plans = [p for p in plans if p["id"] in plan_ids]

    session = get_session()
    total_new = 0

    try:
        for plan_data in plans:
            upsert_plan(session, plan_data)
        session.commit()

        for plan_data in plans:
            console.rule(f"[bold]{plan_data['abbreviation']}[/bold]")

            doc_links = discover_document_links(plan_data)
            plan_dir = DOWNLOADS_DIR / plan_data["id"]
            new_count = 0

            for doc_info in doc_links[:max_docs_per_plan]:
                url = doc_info["url"]

                # Skip documents dated before 2025 (date=None means unknown, keep it)
                doc_date = doc_info.get("meeting_date")
                if doc_date and doc_date < MIN_DATE:
                    continue

                if document_exists(session, url):
                    continue

                local_path, size = download_document(url, plan_dir, doc_info["filename"])
                now = datetime.utcnow()

                doc = Document(
                    plan_id=plan_data["id"],
                    url=url,
                    filename=doc_info["filename"],
                    doc_type=doc_info["doc_type"],
                    local_path=str(local_path) if local_path else None,
                    file_size_bytes=size,
                    downloaded_at=now if local_path else None,
                    extraction_status="pending" if local_path else "failed",
                    meeting_date=doc_info.get("meeting_date"),
                )
                session.add(doc)
                new_count += 1
                total_new += 1

                time.sleep(0.5)

            session.commit()
            console.print(f"  [green]{new_count} new documents saved[/green]")

    finally:
        session.close()

    console.print(f"\n[bold green]Done. {total_new} new documents across all plans.[/bold green]")
    return total_new


if __name__ == "__main__":
    import sys
    plan_ids = sys.argv[1:] if len(sys.argv) > 1 else None
    run_fetcher(plan_ids=plan_ids)
