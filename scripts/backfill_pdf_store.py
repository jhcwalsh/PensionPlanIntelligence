"""Backfill the R2 document store from the existing corpus.

One-shot, resumable, safe to re-run. Not scheduled -- run it by hand.

Measured 2026-08-29: 4,542 documents, of which 1,909 still had a local PDF
and 2,633 did not. A 20-URL sample of the missing ones found 19 still
fetchable, so the re-fetch path recovers most but not all of them, and that
proportion only falls as link rot accumulates.

Ordering is deliberate: local files first (free, no network, no link-rot
race), then re-fetches newest-first, since recent documents are both more
likely to still resolve and more likely to matter.

Usage:
    python -m scripts.backfill_pdf_store              # everything
    python -m scripts.backfill_pdf_store --limit 50   # a taste first
    python -m scripts.backfill_pdf_store --no-refetch # local files only
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys
import time

import requests
from rich.console import Console

import pdf_store
from database import Document, get_session, init_db, utcnow
from fetcher import HEADERS

console = Console(legacy_windows=False)

ROOT = pathlib.Path(__file__).resolve().parents[1]
WAF_FILE = ROOT / "data" / "waf_blocked_plans.json"
REQUEST_DELAY_SECONDS = 0.5


def _waf_blocked_plan_ids() -> set[str]:
    """Plan ids skipped everywhere -- mirrors pipeline.py:_load_waf_blocked_ids.

    data/waf_blocked_plans.json is a dict shaped
    {"_doc": ..., "plans": [{"id": ...}, ...], "_how_it_works": ...}, not a
    flat list. Calling set() on the parsed dict would return its top-level
    keys ("_doc", "plans", "_how_it_works") instead of a single plan id, so
    the WAF skip would silently never fire.
    """
    try:
        with open(WAF_FILE, encoding="utf-8") as f:
            return {p["id"] for p in json.load(f)["plans"]}
    except (OSError, ValueError, KeyError):
        return set()


def _fetch_bytes(url: str) -> bytes | None:
    """Download `url`, or None if it no longer resolves as a PDF."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=60)
        if resp.status_code != 200 or not resp.content.startswith(b"%PDF"):
            return None
        return resp.content
    except Exception:                            # noqa: BLE001 - deliberate
        return None


def run(limit: int | None = None, refetch: bool = True, cfg=None) -> dict:
    init_db()
    cfg = cfg or pdf_store.config_from_env()
    if cfg is None:
        console.print("[red]R2 not configured (need R2_* env vars)[/red]")
        return {}

    blocked = _waf_blocked_plan_ids()
    counts: dict[str, int] = {key: 0 for key in (
        "stored_local", "stored_refetch", "already", "unrecoverable",
        "skipped_waf", "failed", "deferred_refetch")}

    def bump(key):
        counts[key] = counts.get(key, 0) + 1

    session = get_session()
    try:
        docs = (session.query(Document)
                .order_by(Document.downloaded_at.desc().nullslast())
                .all())
        # Local files first: free, and an interrupted run still made progress.
        docs.sort(key=lambda d: 0 if (d.local_path and
                                      pathlib.Path(d.local_path).exists())
                  else 1)
        if limit:
            docs = docs[:limit]

        for doc in docs:
            if doc.content_sha256:
                bump("already")
                continue

            local = pathlib.Path(doc.local_path) if doc.local_path else None
            if local and local.exists():
                try:
                    sha = pdf_store.put(cfg, local.read_bytes())
                    doc.content_sha256 = sha
                    doc.r2_uploaded_at = utcnow()
                    session.commit()
                    bump("stored_local")
                except Exception as e:           # noqa: BLE001
                    console.print(f"  [red]{doc.url}: {e}[/red]")
                    session.rollback()
                    bump("failed")
                continue

            if not refetch:
                # Deliberately deferred, not lost -- worth surfacing so a
                # --no-refetch operator knows how many are still waiting.
                bump("deferred_refetch")
                continue

            if doc.plan_id in blocked:
                bump("skipped_waf")
                continue

            data = _fetch_bytes(doc.url)
            time.sleep(REQUEST_DELAY_SECONDS)
            if data is None:
                # Permanent, and worth counting: this is the floor of what
                # can never be recovered.
                bump("unrecoverable")
                continue
            try:
                sha = pdf_store.put(cfg, data)
                doc.content_sha256 = sha
                doc.r2_uploaded_at = utcnow()
                session.commit()
                bump("stored_refetch")
            except Exception as e:               # noqa: BLE001
                console.print(f"  [red]{doc.url}: {e}[/red]")
                session.rollback()
                bump("failed")
    finally:
        session.close()

    console.rule("[bold green]Backfill complete[/bold green]")
    for key in ("stored_local", "stored_refetch", "already", "unrecoverable",
                "skipped_waf", "failed", "deferred_refetch"):
        if counts.get(key):
            console.print(f"  {key:16s} {counts[key]}")
    return counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--no-refetch", action="store_true",
                        help="Only upload PDFs already on disk.")
    args = parser.parse_args()
    counts = run(limit=args.limit, refetch=not args.no_refetch)
    sys.exit(1 if counts.get("failed") else 0)


if __name__ == "__main__":
    main()
