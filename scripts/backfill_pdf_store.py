"""Backfill the R2 document store from the existing corpus.

One-shot, resumable, safe to re-run. Not scheduled -- run it by hand.

Measured 2026-08-29: 4,542 documents, of which 1,909 still had a local PDF
and 2,633 did not. A 20-URL sample of the missing ones found 19 still
fetchable, so the re-fetch path recovers most but not all of them, and that
proportion only falls as link rot accumulates.

Ordering is deliberate: local files first (free, no network, no link-rot
race), then re-fetches newest-first, since recent documents are both more
likely to still resolve and more likely to matter.

`--limit` applies to each phase independently -- N local uploads *and* N
re-fetches. Applying it to the merged list instead would mean `--limit 50`
never reached the re-fetch path at all, so the "taste first" run would
sample only the safe half and tell the operator nothing about the risky one.

A re-fetch that fails is recorded on the document, not just counted:
`retention_status` is set to "unrecoverable" (a definitive 404-class
failure) or "transient" (timeout, 5xx, anything unclassified). Resume skips
the unrecoverable ones without a network call and retries the transient
ones; `--retry-unrecoverable` forces another attempt at both.

Usage:
    python -m scripts.backfill_pdf_store                    # everything
    python -m scripts.backfill_pdf_store --limit 50         # a taste of each phase
    python -m scripts.backfill_pdf_store --no-refetch       # local files only
    python -m scripts.backfill_pdf_store --retry-unrecoverable
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

# Stop after this many consecutive R2 failures. One failure is a bad object;
# twenty-five in a row is R2, and continuing just burns bandwidth re-fetching
# documents nothing will store.
CONSECUTIVE_FAILURE_LIMIT = 25

COUNT_KEYS = (
    "stored_local", "stored_refetch", "already", "unrecoverable", "transient",
    "skipped_unrecoverable", "skipped_waf", "failed", "deferred_refetch",
)


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


def _fetch_bytes(url: str) -> tuple[bytes | None, str | None]:
    """Download `url`. Returns (data, None) on success, else (None, reason).

    `reason` is "unrecoverable" or "transient", and the distinction is the
    whole point: the caller writes it to the document row, so folding a
    90-second network drop into "gone forever" would permanently mislabel
    every document the run touched while the network was down.

    Classification errs toward transient deliberately. Mislabelling a live
    document as dead costs the document; mislabelling a dead one as
    retryable costs one wasted request on the next run.
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=60)
    except Exception:                            # noqa: BLE001 - deliberate
        # Timeouts, connection resets, TLS failures, DNS. All retryable.
        return None, "transient"

    status = resp.status_code
    if status == 200:
        if resp.content.startswith(b"%PDF"):
            return resp.content, None
        # A 200 that isn't a PDF is a login wall or a "page moved" stub. The
        # bytes we wanted are not at this URL and a retry gets the same page.
        return None, "unrecoverable"
    if status in (408, 429):
        return None, "transient"                 # explicitly "come back later"
    if 400 <= status < 500:
        return None, "unrecoverable"             # 404, 403, 410, gone for good
    return None, "transient"                     # 5xx, and anything unclassified


def run(limit: int | None = None, refetch: bool = True, cfg=None,
        retry_unrecoverable: bool = False) -> dict:
    init_db()
    cfg = cfg or pdf_store.config_from_env()
    if cfg is None:
        console.print("[red]R2 not configured (need R2_* env vars)[/red]")
        return {"unconfigured": 1}

    try:
        pdf_store.preflight(cfg)
    except Exception as e:                       # noqa: BLE001 - deliberate
        console.print(
            f"[red]R2 preflight failed: bucket {cfg.bucket!r} on account "
            f"{cfg.account_id!r} is not reachable: {e}[/red]")
        console.print("[red]Check R2_ACCOUNT_ID / R2_ACCESS_KEY_ID / "
                      "R2_SECRET_ACCESS_KEY / R2_BUCKET. No documents were "
                      "processed.[/red]")
        return {"preflight_failed": 1}

    blocked = _waf_blocked_plan_ids()
    counts: dict[str, int] = {key: 0 for key in COUNT_KEYS}

    def bump(key):
        counts[key] = counts.get(key, 0) + 1

    session = get_session()
    try:
        docs = (session.query(Document)
                .order_by(Document.downloaded_at.desc().nullslast())
                .all())

        # Split into the two phases before applying --limit, so the limit
        # samples each phase rather than being consumed by the free one.
        local_phase, refetch_phase = [], []
        for doc in docs:
            if doc.content_sha256:
                bump("already")
                continue
            if doc.local_path and pathlib.Path(doc.local_path).exists():
                local_phase.append(doc)
                continue
            if doc.retention_status == "unrecoverable" and not retry_unrecoverable:
                # Already established as permanently gone. Re-asking costs a
                # request and gets the same 404. --retry-unrecoverable
                # overrides, for when a plan restores its archive.
                bump("skipped_unrecoverable")
                continue
            if not refetch:
                # Deliberately deferred, not lost -- worth surfacing so a
                # --no-refetch operator knows how many are still waiting.
                bump("deferred_refetch")
                continue
            if doc.plan_id in blocked:
                bump("skipped_waf")
                continue
            refetch_phase.append(doc)

        if limit:
            local_phase = local_phase[:limit]
            refetch_phase = refetch_phase[:limit]

        consecutive_failures = 0
        for doc in local_phase + refetch_phase:
            local = pathlib.Path(doc.local_path) if doc.local_path else None
            if local and local.exists():
                try:
                    sha = pdf_store.put(cfg, local.read_bytes())
                    doc.content_sha256 = sha
                    doc.r2_uploaded_at = utcnow()
                    session.commit()
                    bump("stored_local")
                    consecutive_failures = 0
                except Exception as e:           # noqa: BLE001
                    console.print(f"  [red]{doc.url}: {e}[/red]")
                    session.rollback()
                    bump("failed")
                    consecutive_failures += 1
                if consecutive_failures >= CONSECUTIVE_FAILURE_LIMIT:
                    break
                continue

            data, reason = _fetch_bytes(doc.url)
            time.sleep(REQUEST_DELAY_SECONDS)
            if data is None:
                # Recorded, not merely counted: null content_sha256 alone
                # cannot tell "not yet stored" from "gone forever".
                doc.retention_status = reason
                session.commit()
                bump(reason)
                continue
            try:
                sha = pdf_store.put(cfg, data)
                doc.content_sha256 = sha
                doc.r2_uploaded_at = utcnow()
                session.commit()
                bump("stored_refetch")
                consecutive_failures = 0
            except Exception as e:               # noqa: BLE001
                console.print(f"  [red]{doc.url}: {e}[/red]")
                session.rollback()
                bump("failed")
                consecutive_failures += 1
            if consecutive_failures >= CONSECUTIVE_FAILURE_LIMIT:
                break

        if consecutive_failures >= CONSECUTIVE_FAILURE_LIMIT:
            console.print(
                f"[red]{CONSECUTIVE_FAILURE_LIMIT} consecutive upload "
                f"failures -- R2 looks unhealthy. Stopping early; re-run once "
                f"it is back.[/red]")
    finally:
        session.close()
        # Inside the finally so Ctrl-C still prints the counts. The plan
        # tells the operator to interrupt this run freely; a summary that
        # only appears on a clean exit makes that advice a lie.
        console.rule("[bold green]Backfill complete[/bold green]")
        for key in COUNT_KEYS:
            if counts.get(key):
                console.print(f"  {key:22s} {counts[key]}")

    return counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap each phase independently: at most N local "
                             "uploads and at most N re-fetches.")
    parser.add_argument("--no-refetch", action="store_true",
                        help="Only upload PDFs already on disk.")
    parser.add_argument("--retry-unrecoverable", action="store_true",
                        help="Re-attempt documents previously recorded as "
                             "unrecoverable (a plan may have restored its "
                             "archive).")
    args = parser.parse_args()
    counts = run(limit=args.limit, refetch=not args.no_refetch,
                 retry_unrecoverable=args.retry_unrecoverable)
    failed = (counts.get("failed") or counts.get("unconfigured")
              or counts.get("preflight_failed"))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
