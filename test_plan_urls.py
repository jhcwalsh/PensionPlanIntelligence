"""
Test materials_url health for plans without documents.

For each plan in known_plans.json that has no documents in the DB:
  1. Fetch materials_url with requests (HTTP status, response time)
  2. Run extract_doc_links over the static HTML to count discoverable docs
  3. If materials_type is "playwright", optionally also try Playwright

Prints a table sorted by AUM descending plus per-plan diagnostics. Non-200 or
zero-doc results are flagged so you can decide which plans need URL fixes
before running the full pipeline against them.

Usage:
    python test_plan_urls.py                     # all plans without docs
    python test_plan_urls.py --plan ucrp         # one plan
    python test_plan_urls.py --all               # every plan, even those with docs
    python test_plan_urls.py --use-playwright    # also exercise Playwright
    python test_plan_urls.py --csv out.csv       # write results to CSV
    python test_plan_urls.py --limit 10          # stop after N plans
"""

import argparse
import csv
import json
import sys
import time
from pathlib import Path

import requests

from database import Document, Plan, get_session, init_db

PLANS_FILE = Path(__file__).parent / "data" / "known_plans.json"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Upgrade-Insecure-Requests": "1",
}
TIMEOUT = 25


def load_plan_configs() -> dict[str, dict]:
    with open(PLANS_FILE, encoding="utf-8") as f:
        return {p["id"]: p for p in json.load(f)}


def fetch_with_requests(url: str) -> tuple[int | None, float, str | None, str]:
    """Return (status_code, elapsed_seconds, html_or_None, error_message)."""
    start = time.time()
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
        elapsed = time.time() - start
        return resp.status_code, elapsed, resp.text, ""
    except requests.exceptions.Timeout:
        return None, time.time() - start, None, "timeout"
    except requests.exceptions.SSLError as e:
        return None, time.time() - start, None, f"ssl: {e}"
    except requests.exceptions.ConnectionError as e:
        return None, time.time() - start, None, f"connection: {e}"
    except Exception as e:
        return None, time.time() - start, None, f"{type(e).__name__}: {e}"


def count_doc_links(html: str, base_url: str, investment_only: bool) -> tuple[int, int]:
    """Return (raw_doc_links, filtered_doc_links_after_pipeline_rules)."""
    if not html:
        return 0, 0
    try:
        from bs4 import BeautifulSoup
        from fetcher import extract_doc_links, is_doc_url
    except ImportError as e:
        print(f"  (skipping doc-link analysis: {e})", file=sys.stderr)
        return 0, 0

    soup = BeautifulSoup(html, "lxml")
    # Raw count: any link that looks like a document URL
    raw = 0
    from urllib.parse import urljoin
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = a.get_text(strip=True)
        full = urljoin(base_url, href)
        if is_doc_url(full, text):
            raw += 1
    # Filtered count: same logic the pipeline uses
    filtered = len(extract_doc_links(soup, base_url, investment_only=investment_only))
    return raw, filtered


def fetch_with_playwright(plan_cfg: dict) -> tuple[str | None, str]:
    """Return (html, error_message). Lazy-imports playwright."""
    try:
        from fetcher import fetch_page_playwright
    except ImportError as e:
        return None, f"import: {e}"
    try:
        wait_sel = plan_cfg.get("playwright_wait_selector")
        soup = fetch_page_playwright(plan_cfg["materials_url"], wait_selector=wait_sel)
        if soup is None:
            return None, "playwright returned None"
        return str(soup), ""
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def main():
    parser = argparse.ArgumentParser(description="Test materials_url health for plans")
    parser.add_argument("--plan", help="Test one plan by id")
    parser.add_argument("--all", action="store_true",
                        help="Include plans that already have documents")
    parser.add_argument("--use-playwright", action="store_true",
                        help="Also fetch via Playwright (slow)")
    parser.add_argument("--csv", help="Write results to CSV file")
    parser.add_argument("--limit", type=int, help="Stop after N plans")
    parser.add_argument("--delay", type=float, default=1.0,
                        help="Seconds between requests (default: 1.0)")
    args = parser.parse_args()

    init_db()
    session = get_session()
    plan_configs = load_plan_configs()

    # Build target list
    if args.plan:
        plans = session.query(Plan).filter(Plan.id == args.plan).all()
        if not plans:
            print(f"No plan with id={args.plan!r}", file=sys.stderr)
            sys.exit(1)
    else:
        plans = (
            session.query(Plan)
            .order_by(Plan.aum_billions.desc().nulls_last())
            .all()
        )
        if not args.all:
            plans = [p for p in plans
                     if session.query(Document).filter_by(plan_id=p.id).count() == 0]

    if args.limit:
        plans = plans[:args.limit]

    print(f"Testing {len(plans)} plan(s)\n")

    results = []
    for i, plan in enumerate(plans, 1):
        cfg = plan_configs.get(plan.id, {})
        url = plan.materials_url or cfg.get("materials_url")
        materials_type = cfg.get("materials_type", "html_links")
        investment_only = cfg.get("investment_only", True)
        aum = f"${plan.aum_billions:.0f}B" if plan.aum_billions else "?"

        print(f"[{i}/{len(plans)}] {plan.id} ({aum}) [{materials_type}]")
        print(f"  URL: {url}")

        if not url:
            print("  [SKIP] no materials_url\n")
            results.append({
                "plan_id": plan.id, "name": plan.name, "aum_b": plan.aum_billions,
                "url": "", "materials_type": materials_type,
                "status": "no_url", "elapsed_s": 0, "raw_links": 0, "filtered_links": 0,
                "playwright_links": "", "error": "no materials_url",
                "verdict": "BAD",
            })
            continue

        status, elapsed, html, err = fetch_with_requests(url)
        raw, filtered = count_doc_links(html or "", url, investment_only) if html else (0, 0)

        playwright_filtered = ""
        if args.use_playwright and materials_type == "playwright":
            print("  Trying Playwright...")
            pw_html, pw_err = fetch_with_playwright(cfg)
            if pw_html:
                _, playwright_filtered = count_doc_links(pw_html, url, investment_only)
            else:
                playwright_filtered = f"err:{pw_err}"

        # Verdict — interpret status in context of materials_type.
        # 403 on playwright sites usually means "anti-bot blocks requests; use a
        # real browser" (likely fixable via Playwright). 404 always means the
        # URL itself is wrong.
        if err or status is None:
            verdict = "FAIL"
        elif status == 404:
            verdict = "BAD"
        elif status >= 400 and materials_type == "playwright":
            verdict = "JS"  # Site blocks bots; need browser
        elif status >= 400:
            verdict = "BAD"
        elif filtered > 0:
            verdict = "OK"
        elif materials_type == "playwright":
            verdict = "JS"  # Static HTML has no docs; needs browser
        else:
            verdict = "EMPTY"

        marker = {
            "OK": "[OK]   ",
            "JS": "[JS]   ",
            "EMPTY": "[EMPTY]",
            "BAD": "[BAD]  ",
            "FAIL": "[FAIL] ",
        }[verdict]
        suffix = ""
        if isinstance(playwright_filtered, int):
            suffix = f"  pw={playwright_filtered}"
        elif playwright_filtered:
            suffix = f"  pw={playwright_filtered}"
        print(f"  {marker} HTTP {status if status else '---'}  "
              f"{elapsed:.1f}s  raw={raw}  filtered={filtered}{suffix}"
              + (f"  err={err}" if err else "") + "\n")

        results.append({
            "plan_id": plan.id, "name": plan.name, "aum_b": plan.aum_billions,
            "url": url, "materials_type": materials_type,
            "status": status if status else "",
            "elapsed_s": round(elapsed, 2),
            "raw_links": raw, "filtered_links": filtered,
            "playwright_links": playwright_filtered,
            "error": err,
            "verdict": verdict,
        })

        if args.delay > 0 and i < len(plans):
            time.sleep(args.delay)

    # Summary
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    by_verdict: dict[str, list] = {}
    for r in results:
        by_verdict.setdefault(r["verdict"], []).append(r)

    legend = {
        "OK": "URL works AND yielded document links via static HTML",
        "JS": "Playwright-configured site (anti-bot block or JS-rendered) - rerun with --use-playwright",
        "EMPTY": "URL works but no doc links found - URL or filters likely wrong",
        "BAD": "URL returned HTTP 404 (or 4xx/5xx on a non-playwright plan)",
        "FAIL": "Network error - DNS / SSL / timeout / connection refused",
    }
    for v in ["OK", "JS", "EMPTY", "BAD", "FAIL"]:
        rows = by_verdict.get(v, [])
        print(f"  {v:6s} {len(rows):3d}  {legend[v]}")

    print()
    for v in ["BAD", "FAIL", "EMPTY"]:
        rows = by_verdict.get(v, [])
        if rows:
            print(f"--- Action needed: {v} ({len(rows)} plans) ---")
            for r in sorted(rows, key=lambda x: -(x["aum_b"] or 0)):
                aum = f"${r['aum_b']:.0f}B" if r["aum_b"] else "?"
                detail = r["error"] or f"HTTP {r['status']}" if r["status"] else "no response"
                print(f"  [{aum:>5s}] {r['plan_id']:18s} {detail}")
            print()

    if args.csv:
        with open(args.csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
            writer.writeheader()
            writer.writerows(results)
        print(f"Wrote {len(results)} rows to {args.csv}")


if __name__ == "__main__":
    main()
