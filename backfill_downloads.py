"""
Bulk backfill source documents to the persistent disk.

Iterates every Document in the DB and re-downloads any file that isn't
present at its local_path (or has a local_path pointing to a path that
doesn't exist on the current machine — e.g. a Windows path after
deploying to Render). Updates Document.local_path and file_size_bytes
to reflect the new disk location.

Runs on Render via the shell:
    python backfill_downloads.py

Usage:
    python backfill_downloads.py                  # backfill all missing
    python backfill_downloads.py --plan calpers   # one plan only
    python backfill_downloads.py --dry-run        # report what would happen
    python backfill_downloads.py --limit 50       # stop after N fetches
    python backfill_downloads.py --delay 0.5      # seconds between requests
"""

import argparse
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

from database import Document, get_session, init_db

DOWNLOADS_DIR = Path(os.environ.get("DOWNLOADS_DIR") or (Path(__file__).parent / "downloads"))
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; PensionPlanIntelligence/1.0)"}


def fetch_one(doc, delay: float) -> tuple[Path | None, int, str]:
    """Download a single document to DOWNLOADS_DIR/{plan}/{filename}.

    Returns (path, size_bytes, error_message). On success error_message is "".
    """
    plan_id = doc.plan_id or "unknown"
    filename = doc.filename or f"doc_{doc.id}.pdf"
    dest_dir = DOWNLOADS_DIR / plan_id
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / filename

    if dest.exists():
        return dest, dest.stat().st_size, ""

    if not doc.url:
        return None, 0, "no URL on record"

    try:
        resp = requests.get(doc.url, headers=HEADERS, timeout=60, stream=True)
        resp.raise_for_status()

        cd = resp.headers.get("Content-Disposition", "")
        cd_match = re.search(r'filename="?([^";\n]+)"?', cd)
        if cd_match:
            dest = dest_dir / cd_match.group(1).strip()

        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
    except Exception as exc:
        return None, 0, str(exc)

    if delay > 0:
        time.sleep(delay)

    return dest, dest.stat().st_size, ""


def main():
    parser = argparse.ArgumentParser(description="Backfill source PDFs to the persistent disk")
    parser.add_argument("--plan", help="Limit to a specific plan_id")
    parser.add_argument("--dry-run", action="store_true", help="Report only; don't fetch")
    parser.add_argument("--limit", type=int, help="Stop after N successful fetches")
    parser.add_argument("--delay", type=float, default=0.5,
                        help="Seconds between requests (default: 0.5)")
    args = parser.parse_args()

    init_db()
    session = get_session()

    query = session.query(Document)
    if args.plan:
        query = query.filter(Document.plan_id == args.plan)
    docs = query.order_by(Document.id).all()

    print(f"Downloads directory: {DOWNLOADS_DIR}")
    print(f"Documents to check: {len(docs):,}")

    # Classify up-front so we can show a useful summary before any network calls
    already_present = 0
    missing = []
    for doc in docs:
        if doc.local_path and Path(doc.local_path).exists():
            already_present += 1
            continue
        # Also check whether the canonical dest already exists (from a
        # previous partial run or lazy fetch via the app)
        dest = DOWNLOADS_DIR / (doc.plan_id or "unknown") / (doc.filename or f"doc_{doc.id}.pdf")
        if dest.exists():
            # Repair the DB record so the app sees it
            doc.local_path = str(dest)
            doc.file_size_bytes = dest.stat().st_size
            already_present += 1
            continue
        missing.append(doc)

    session.commit()
    print(f"Already on disk: {already_present:,}")
    print(f"Missing (to fetch): {len(missing):,}")

    if args.dry_run:
        for doc in missing[:20]:
            print(f"  would fetch: [{doc.id}] {doc.plan_id} / {doc.filename} -> {doc.url}")
        if len(missing) > 20:
            print(f"  ... and {len(missing) - 20:,} more")
        return

    if not missing:
        print("Nothing to do.")
        return

    ok = 0
    failed = []
    start = time.time()

    for i, doc in enumerate(missing, 1):
        if args.limit and ok >= args.limit:
            print(f"Reached --limit {args.limit}, stopping.")
            break

        path, size, err = fetch_one(doc, args.delay)
        if path:
            doc.local_path = str(path)
            doc.file_size_bytes = size
            # Stamp downloaded_at so the Admin pipeline-coverage view sees
            # these records as freshly downloaded rather than "never".
            if doc.downloaded_at is None:
                doc.downloaded_at = datetime.utcnow()
            session.commit()
            ok += 1
            print(f"[{i}/{len(missing)}] OK   {doc.plan_id}/{path.name} ({size:,} bytes)")
        else:
            failed.append((doc, err))
            print(f"[{i}/{len(missing)}] FAIL {doc.plan_id}/{doc.filename}: {err}", file=sys.stderr)

    elapsed = time.time() - start
    print()
    print(f"Done in {elapsed:.1f}s. Success: {ok:,}  Failed: {len(failed):,}")

    if failed:
        print("\nFailed documents (URL may have moved or expired):")
        for doc, err in failed[:30]:
            print(f"  [{doc.id}] {doc.plan_id}/{doc.filename}: {err}")
        if len(failed) > 30:
            print(f"  ... and {len(failed) - 30:,} more")

    session.close()


if __name__ == "__main__":
    main()
