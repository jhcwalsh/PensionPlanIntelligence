"""Probe whether GitHub Actions runners can scrape pension-plan board-
materials pages without being blocked by cloud-IP-aware bot mitigation.

The pipeline scrapes 148 pension-fund sites, mostly state-government
domains. They work fine from a residential IP locally, but the host
running this ($GITHUB_ACTIONS=true on Azure runners) might be flagged by
Cloudflare/Akamai bot mode that some sites enable. This script issues a
single read-only request to each plan's ``materials_url``, reports the
HTTP outcome and any WAF telltales, and exits 0 unless EVERY plan fails
(which would suggest a systemic block rather than per-site flakiness).

Usage:
    python -m scripts.probe_scrape                          # default 17-plan sample
    python -m scripts.probe_scrape calpers calstrs lacera   # specific plans
    python -m scripts.probe_scrape --all                    # every plan in registry

Read-only — does NOT touch the database or download PDFs.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path

# Same User-Agent the production fetcher uses (kept inline so the probe
# doesn't transitively import bs4/lxml from fetcher.py).
PROBE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    )
}

# Curated mix: both materials_types, varied states, common platforms,
# big-name plans the user actually cares about.
DEFAULT_SAMPLE = [
    "calpers",       # CA, playwright, custom site
    "calstrs",       # CA, playwright
    "nycers",        # NY, html_links
    "trs_texas",     # TX, playwright
    "lacera",        # CA, html_links
    "wsib",          # WA, html_links
    "ipers",         # IA, playwright
    "mn_sbi",        # MN, playwright
    "kyret_ky",      # KY, ?
    "njdpb",         # NJ, html_links
    "vrs",           # VA, playwright
    "asrs",          # AZ, playwright
    "psers",         # PA, playwright
    "opers",         # OH, playwright
    "or_pers",       # OR
    "lacers",        # CA, html_links
    "hpops",         # TX, playwright
]

# Cheap WAF/bot-mitigation telltales that suggest IP-based blocking
# rather than per-site flakiness (404s, app errors, etc).
WAF_FINGERPRINTS = [
    ("cloudflare-ray", "cf-ray header"),
    ("just a moment", "Cloudflare 'Just a moment' challenge"),
    ("attention required", "Cloudflare 'Attention Required' challenge"),
    ("access denied", "generic access-denied page"),
    ("recaptcha", "recaptcha challenge"),
    ("akamai", "Akamai bot mitigation"),
    ("incapsula", "Imperva/Incapsula"),
    ("perimeterx", "PerimeterX"),
    ("datadome", "DataDome"),
]


def _detect_waf(status: int, headers: dict, body: str) -> str | None:
    """Return a short reason if the response looks WAF-blocked, else None."""
    if status in (403, 429, 503):
        # Check headers + body for fingerprints
        hay = " ".join(f"{k}:{v}" for k, v in (headers or {}).items()).lower()
        body_lc = (body[:4000] or "").lower()
        for needle, label in WAF_FINGERPRINTS:
            if needle in hay or needle in body_lc:
                return f"HTTP {status} ({label})"
        return f"HTTP {status} (no WAF fingerprint, but blocked status)"
    return None


def probe_one_requests(url: str) -> dict:
    """Probe a static / requests-based plan. Read-only HTTP GET."""
    import requests

    t0 = time.time()
    try:
        resp = requests.get(url, headers=PROBE_HEADERS, timeout=30, allow_redirects=True)
    except Exception as exc:
        return {
            "ok": False,
            "status": None,
            "size": 0,
            "links": 0,
            "elapsed_s": time.time() - t0,
            "error": f"{type(exc).__name__}: {exc}",
        }
    elapsed = time.time() - t0

    waf = _detect_waf(resp.status_code, dict(resp.headers), resp.text)
    if waf:
        return {
            "ok": False,
            "status": resp.status_code,
            "size": len(resp.content),
            "links": 0,
            "elapsed_s": elapsed,
            "error": f"BLOCKED: {waf}",
        }

    if resp.status_code >= 400:
        return {
            "ok": False,
            "status": resp.status_code,
            "size": len(resp.content),
            "links": 0,
            "elapsed_s": elapsed,
            "error": f"HTTP {resp.status_code}",
        }

    # Count anchor tags as a sanity measure (cheap, no parsing)
    link_count = len(re.findall(r"<a\s", resp.text, re.IGNORECASE))
    return {
        "ok": True,
        "status": resp.status_code,
        "size": len(resp.content),
        "links": link_count,
        "elapsed_s": elapsed,
        "error": None,
    }


def probe_one_playwright(url: str, wait_selector: str | None = None) -> dict:
    """Probe a JS-heavy plan. Spawns headless Chromium per plan — slow but
    matches the production fetcher's behaviour exactly."""
    t0 = time.time()
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        return {
            "ok": False, "status": None, "size": 0, "links": 0,
            "elapsed_s": time.time() - t0,
            "error": f"playwright not installed: {exc}",
        }

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=PROBE_HEADERS["User-Agent"],
                viewport={"width": 1280, "height": 900},
            )
            page = context.new_page()
            response = page.goto(url, wait_until="networkidle", timeout=45_000)
            if wait_selector:
                try:
                    page.wait_for_selector(wait_selector, timeout=10_000)
                except Exception:
                    pass
            html = page.content()
            status = response.status if response else None
            headers = response.headers if response else {}
            browser.close()
    except Exception as exc:
        return {
            "ok": False, "status": None, "size": 0, "links": 0,
            "elapsed_s": time.time() - t0,
            "error": f"{type(exc).__name__}: {exc}",
        }
    elapsed = time.time() - t0

    waf = _detect_waf(status or 0, headers, html)
    if waf:
        return {
            "ok": False, "status": status, "size": len(html), "links": 0,
            "elapsed_s": elapsed, "error": f"BLOCKED: {waf}",
        }
    if status and status >= 400:
        return {
            "ok": False, "status": status, "size": len(html), "links": 0,
            "elapsed_s": elapsed, "error": f"HTTP {status}",
        }
    link_count = len(re.findall(r"<a\s", html, re.IGNORECASE))
    return {
        "ok": True, "status": status, "size": len(html), "links": link_count,
        "elapsed_s": elapsed, "error": None,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="scripts.probe_scrape")
    parser.add_argument("plan_ids", nargs="*",
                        help="Plan IDs to probe (default: 17-plan sample)")
    parser.add_argument("--all", action="store_true",
                        help="Probe every plan in the registry")
    args = parser.parse_args(argv)

    repo_root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(repo_root))

    with open(repo_root / "data" / "known_plans.json", encoding="utf-8") as f:
        registry = {p["id"]: p for p in json.load(f)}

    if args.all or (args.plan_ids and args.plan_ids == ["all"]):
        plan_ids = list(registry.keys())
    elif args.plan_ids:
        plan_ids = args.plan_ids
    else:
        plan_ids = DEFAULT_SAMPLE

    is_gha = os.environ.get("GITHUB_ACTIONS") == "true"
    print(f"Probe scrape — {len(plan_ids)} plans"
          f"  ({'GitHub Actions' if is_gha else 'local'} environment)\n")

    results = []
    for pid in plan_ids:
        plan = registry.get(pid)
        if plan is None:
            print(f"  ✗ {pid:15s} NOT FOUND in registry")
            results.append({"plan_id": pid, "ok": False, "error": "unknown plan"})
            continue
        url = plan.get("materials_url")
        if not url:
            print(f"  ✗ {pid:15s} no materials_url")
            results.append({"plan_id": pid, "ok": False, "error": "no materials_url"})
            continue
        mt = plan.get("materials_type", "html_links")
        wait_sel = plan.get("playwright_wait_selector")
        if mt == "playwright":
            r = probe_one_playwright(url, wait_selector=wait_sel)
        else:
            r = probe_one_requests(url)
        r["plan_id"] = pid
        r["abbreviation"] = plan.get("abbreviation", pid)
        r["state"] = plan.get("state", "?")
        r["materials_type"] = mt

        mark = "✓" if r["ok"] else "✗"
        size_kb = r.get("size", 0) // 1024
        print(
            f"  {mark} {pid:15s} {r['abbreviation']:12s} {r['state']:3s} "
            f"{mt:11s} HTTP={r.get('status','-')!s:>5s}  "
            f"{size_kb:5d} KB  {r.get('links',0):4d} links  "
            f"{r.get('elapsed_s',0):4.1f}s  "
            f"{r.get('error') or 'OK'}"
        )
        results.append(r)

    n = len(results)
    n_ok = sum(1 for r in results if r.get("ok"))
    n_blocked = sum(
        1 for r in results
        if not r.get("ok") and (r.get("error") or "").startswith("BLOCKED:")
    )
    n_other = n - n_ok - n_blocked

    print("\nSummary")
    print(f"  passed:               {n_ok}/{n}")
    print(f"  WAF / IP-blocked:     {n_blocked}/{n}")
    print(f"  other failures:       {n_other}/{n}")

    if n_ok == 0:
        print("\nALL plans failed — likely systemic IP block. Investigate before "
              "migrating production cron.")
        return 1
    if n_blocked > 0:
        print(f"\n{n_blocked} plan(s) appear to be cloud-IP blocked. Hybrid "
              "deployment recommended (those run locally, the rest from GHA).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
