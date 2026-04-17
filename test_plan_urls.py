"""
Test materials_url health for plans without documents.

For each plan in known_plans.json (or a targeted subset) this script:
  1. Fetches materials_url with requests (HTTP status, response time)
  2. Runs extract_doc_links over the static HTML to count discoverable docs
  3. If --use-playwright is set, optionally renders with a real browser

Prints per-plan diagnostics plus a summary grouped by verdict. CSV output is
written incrementally so partial runs survive Ctrl+C.

Typical workflows:

    # Quick triage (requests only, ~1 minute for 53 plans)
    python test_plan_urls.py --csv url_test.csv

    # Deep dive using a real browser (slow; needs playwright install)
    python test_plan_urls.py --use-playwright --csv url_test_pw.csv --delay 2

    # After fixing URLs in known_plans.json, re-test only what previously failed
    python test_plan_urls.py --use-playwright --retry-failed url_test_pw.csv \\
        --csv url_test_pw_v2.csv

    # Single plan
    python test_plan_urls.py --plan ucrp --use-playwright

Verdicts:
    OK     URL works and yielded document links via static HTML
    JS     Playwright-configured (anti-bot block or JS-rendered) - try --use-playwright
    EMPTY  URL works but no doc links found - URL or filters likely wrong
    BAD    HTTP 404 (or 4xx/5xx on a non-playwright plan)
    FAIL   Network error - DNS / SSL / timeout / connection refused
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

CSV_FIELDS = [
    "plan_id", "name", "aum_b", "url", "materials_type",
    "status", "elapsed_s", "raw_links", "filtered_links",
    "playwright_links", "error", "verdict",
]


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
        return None, time.time() - start, None, f"ssl: {str(e)[:120]}"
    except requests.exceptions.ConnectionError as e:
        return None, time.time() - start, None, f"connection: {str(e)[:120]}"
    except Exception as e:
        return None, time.time() - start, None, f"{type(e).__name__}: {str(e)[:120]}"


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

    from urllib.parse import urljoin
    soup = BeautifulSoup(html, "lxml")
    raw = 0
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = a.get_text(strip=True)
        full = urljoin(base_url, href)
        if is_doc_url(full, text):
            raw += 1
    filtered = len(extract_doc_links(soup, base_url, investment_only=investment_only))
    return raw, filtered


def fetch_with_playwright(plan_cfg: dict) -> tuple[str | None, str]:
    """Return (html, error_message). Lazy-imports playwright via fetcher.py."""
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
        return None, f"{type(e).__name__}: {str(e)[:120]}"


def classify(status, err, filtered, materials_type) -> str:
    """Interpret the fetch result into one of: OK, JS, EMPTY, BAD, FAIL."""
    if err or status is None:
        return "FAIL"
    if status == 404:
        return "BAD"
    if status >= 400 and materials_type == "playwright":
        return "JS"
    if status >= 400:
        return "BAD"
    if filtered > 0:
        return "OK"
    if materials_type == "playwright":
        return "JS"
    return "EMPTY"


def format_eta(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    if seconds < 3600:
        return f"{int(seconds // 60)}m{int(seconds % 60):02d}s"
    return f"{int(seconds // 3600)}h{int((seconds % 3600) // 60):02d}m"


def load_retry_csv(path: str) -> set[str]:
    """Return plan_ids whose verdict in the CSV is NOT 'OK'."""
    ids: set[str] = set()
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if (row.get("verdict") or "").strip() != "OK":
                pid = (row.get("plan_id") or "").strip()
                if pid:
                    ids.add(pid)
    return ids


def main():
    parser = argparse.ArgumentParser(
        description="Test materials_url health for plans",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--plan", help="Test one plan by id")
    parser.add_argument("--plans", help="Comma-separated list of plan ids")
    parser.add_argument("--all", action="store_true",
                        help="Include plans that already have documents")
    parser.add_argument("--use-playwright", action="store_true",
                        help="Also fetch via Playwright (slow)")
    parser.add_argument("--csv", help="Write results to CSV file (incremental)")
    parser.add_argument("--limit", type=int, help="Stop after N plans")
    parser.add_argument("--delay", type=float, default=1.0,
                        help="Seconds between requests (default: 1.0)")
    parser.add_argument("--retry-failed", metavar="CSV",
                        help="Read a previous CSV and retest only non-OK plans")
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
    elif args.plans:
        ids = [s.strip() for s in args.plans.split(",") if s.strip()]
        plans = session.query(Plan).filter(Plan.id.in_(ids)).all()
        if len(plans) != len(ids):
            found = {p.id for p in plans}
            missing = sorted(set(ids) - found)
            print(f"Warning: unknown plan ids: {missing}", file=sys.stderr)
    elif args.retry_failed:
        retry_ids = load_retry_csv(args.retry_failed)
        if not retry_ids:
            print(f"No non-OK rows in {args.retry_failed}. Nothing to retry.")
            sys.exit(0)
        plans = (
            session.query(Plan)
            .filter(Plan.id.in_(retry_ids))
            .order_by(Plan.aum_billions.desc().nulls_last())
            .all()
        )
        print(f"Retrying {len(plans)} plan(s) flagged non-OK in {args.retry_failed}")
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

    print(f"Testing {len(plans)} plan(s)"
          + (f" via Playwright" if args.use_playwright else " via requests only")
          + f" (delay={args.delay}s)\n")

    # Open CSV for incremental writes so Ctrl+C preserves progress
    csv_fh = None
    csv_writer = None
    if args.csv:
        csv_fh = open(args.csv, "w", newline="", encoding="utf-8")
        csv_writer = csv.DictWriter(csv_fh, fieldnames=CSV_FIELDS)
        csv_writer.writeheader()
        csv_fh.flush()

    results: list[dict] = []
    start_time = time.time()
    try:
        for i, plan in enumerate(plans, 1):
            cfg = plan_configs.get(plan.id, {})
            url = plan.materials_url or cfg.get("materials_url")
            materials_type = cfg.get("materials_type", "html_links")
            investment_only = cfg.get("investment_only", True)
            aum = f"${plan.aum_billions:.0f}B" if plan.aum_billions else "?"

            # Progress line with ETA (visible when there's >1 plan)
            if len(plans) > 1 and i > 1:
                elapsed_total = time.time() - start_time
                avg = elapsed_total / (i - 1)
                remaining = avg * (len(plans) - i + 1)
                progress = (
                    f"[{i}/{len(plans)}] {plan.id} ({aum}) [{materials_type}]"
                    f"  elapsed {format_eta(elapsed_total)}  "
                    f"ETA {format_eta(remaining)}"
                )
            else:
                progress = f"[{i}/{len(plans)}] {plan.id} ({aum}) [{materials_type}]"
            print(progress)
            print(f"  URL: {url}")

            if not url:
                print("  [SKIP]  no materials_url\n")
                row = {
                    "plan_id": plan.id, "name": plan.name, "aum_b": plan.aum_billions,
                    "url": "", "materials_type": materials_type,
                    "status": "no_url", "elapsed_s": 0, "raw_links": 0, "filtered_links": 0,
                    "playwright_links": "", "error": "no materials_url",
                    "verdict": "BAD",
                }
                results.append(row)
                if csv_writer:
                    csv_writer.writerow(row)
                    csv_fh.flush()
                continue

            status, elapsed, html, err = fetch_with_requests(url)
            raw, filtered = count_doc_links(html or "", url, investment_only) if html else (0, 0)

            playwright_filtered = ""
            # Run playwright when requested AND either:
            #   (a) plan is configured for it, OR
            #   (b) requests hit an anti-bot 4xx — playwright may bypass
            should_try_pw = args.use_playwright and (
                materials_type == "playwright"
                or (status is not None and status >= 400)
            )
            if should_try_pw:
                print("  Trying Playwright...")
                pw_html, pw_err = fetch_with_playwright(cfg)
                if pw_html:
                    _, playwright_filtered = count_doc_links(pw_html, url, investment_only)
                    # If playwright found docs, this plan is effectively OK
                    # (classification below uses playwright_filtered if present)
                else:
                    playwright_filtered = f"err:{pw_err}"

            # Promote to OK when playwright succeeds where requests failed
            if isinstance(playwright_filtered, int) and playwright_filtered > 0:
                verdict = "OK"
            else:
                verdict = classify(status, err, filtered, materials_type)

            marker = {
                "OK": "[OK]   ",
                "JS": "[JS]   ",
                "EMPTY": "[EMPTY]",
                "BAD": "[BAD]  ",
                "FAIL": "[FAIL] ",
            }[verdict]
            suffix = ""
            if playwright_filtered != "":
                suffix = f"  pw={playwright_filtered}"
            print(f"  {marker} HTTP {status if status else '---'}  "
                  f"{elapsed:.1f}s  raw={raw}  filtered={filtered}{suffix}"
                  + (f"  err={err}" if err else "") + "\n")

            row = {
                "plan_id": plan.id, "name": plan.name, "aum_b": plan.aum_billions,
                "url": url, "materials_type": materials_type,
                "status": status if status else "",
                "elapsed_s": round(elapsed, 2),
                "raw_links": raw, "filtered_links": filtered,
                "playwright_links": playwright_filtered,
                "error": err,
                "verdict": verdict,
            }
            results.append(row)
            if csv_writer:
                csv_writer.writerow(row)
                csv_fh.flush()

            if args.delay > 0 and i < len(plans):
                time.sleep(args.delay)

    except KeyboardInterrupt:
        print("\n\nInterrupted. Partial results below (CSV preserved on disk if --csv was set).\n")

    finally:
        if csv_fh is not None:
            csv_fh.close()

    # Summary
    if not results:
        print("(No plans tested.)")
        return

    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    by_verdict: dict[str, list] = {}
    for r in results:
        by_verdict.setdefault(r["verdict"], []).append(r)

    legend = {
        "OK": "URL works AND yielded document links",
        "JS": "Playwright-configured site (anti-bot block or JS-rendered)",
        "EMPTY": "URL works but no doc links found - URL or filters likely wrong",
        "BAD": "HTTP 404 (or 4xx/5xx on a non-playwright plan)",
        "FAIL": "Network error - DNS / SSL / timeout / connection refused",
    }
    for v in ["OK", "JS", "EMPTY", "BAD", "FAIL"]:
        rows = by_verdict.get(v, [])
        print(f"  {v:6s} {len(rows):3d}  {legend[v]}")

    # AUM at risk per verdict
    print()
    print("AUM by verdict:")
    for v in ["OK", "JS", "EMPTY", "BAD", "FAIL"]:
        rows = by_verdict.get(v, [])
        aum = sum((r["aum_b"] or 0) for r in rows)
        if rows:
            print(f"  {v:6s} ${aum:,.0f}B")

    # Action lists
    print()
    for v in ["BAD", "FAIL", "EMPTY"]:
        rows = by_verdict.get(v, [])
        if rows:
            print(f"--- Action needed: {v} ({len(rows)} plans) ---")
            for r in sorted(rows, key=lambda x: -(x["aum_b"] or 0)):
                aum = f"${r['aum_b']:.0f}B" if r["aum_b"] else "?"
                detail = r["error"] or (f"HTTP {r['status']}" if r["status"] else "no response")
                print(f"  [{aum:>5s}] {r['plan_id']:18s} {detail}")
            print()

    if args.csv:
        print(f"Wrote {len(results)} rows to {args.csv}")


if __name__ == "__main__":
    main()
