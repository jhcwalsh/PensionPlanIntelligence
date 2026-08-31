"""Re-download documents whose PDF is gone, so they can be extracted again.

    python -m scripts.refetch_missing_files            # report, download nothing
    python -m scripts.refetch_missing_files --download

A `documents` row can outlive its file: the fetch succeeded on a machine
whose disk we no longer have, or the download failed after the row was
written. Those rows carry `extraction_status='failed'` and an
`extraction_details` reason of `file_missing`, and nothing retries them --
the extractor needs a file and the fetcher will not re-download a URL that
`document_exists` already knows about.

Diagnosed on 2026-08-30: 42 such documents across dgrs_mi (36 of 36),
trsnyc and ga_trs, which is why those three plans have no performance data
at all. The rows are fine; only the bytes are missing.

**Makes no API calls.** Downloading is free; the re-extraction afterwards is
a separate, explicit step:

    python pipeline.py dgrs_mi trsnyc ga_trs --retry-failed --no-ocr

`--retry-failed` is the only flag that extracts *without* also summarising
-- `--extract-only` summarises too, despite its name.

WAF-blocked plans are skipped, as everywhere else: no runner can reach them.
"""
from __future__ import annotations

import argparse
import io
import json
import sys
import time
from pathlib import Path

from rich.console import Console
from sqlalchemy import text

import database
from database import Document, ExtractionDetail

console = Console(legacy_windows=False)

REQUEST_DELAY_SECONDS = 0.5


def _waf_blocked_plan_ids() -> set[str]:
    blocked: set[str] = set()
    for name in ("waf_blocked_plans.json", "waf_blocked_cafr_plans.json"):
        try:
            with io.open(f"data/{name}", encoding="utf-8") as f:
                blocked |= {p["id"] for p in json.load(f)["plans"]}
        except FileNotFoundError:
            pass
    return blocked


def missing_file_documents(session, plan_ids: list[str] | None,
                           only_unextracted: bool = True):
    """Rows whose local file is absent *and* which have no text to show for it.

    Keyed on the file rather than on `extraction_details`, because a row can
    lose its file without anything having re-run to record why.

    The text condition is not optional in practice. This script was written
    when the pipeline ran locally, where an absent file meant lost work. The
    pipeline is cloud-only now: GHA runners fetch, extract and discard, so
    2,557 of 5,084 documents have no local file and nearly all of them are
    perfectly fine — text extracted, summary written, nothing wrong. Without
    this filter the script proposes re-downloading all of them, which would
    hammer a hundred plan websites to recover nothing. The rows that actually
    need their bytes back are the 121 that failed extraction because the file
    was gone.

    ``only_unextracted=False`` restores the original behaviour, for the case
    where a document holds text but you want the PDF itself back — a
    re-extraction at a higher page cap, or a retention backfill into R2.
    """
    q = (session.query(Document)
         .outerjoin(ExtractionDetail, ExtractionDetail.document_id == Document.id)
         .filter(Document.url.isnot(None)))
    if plan_ids:
        q = q.filter(Document.plan_id.in_(plan_ids))
    if only_unextracted:
        q = q.filter(Document.extracted_text.is_(None))
    out = []
    for doc in q:
        path = Path(doc.local_path) if doc.local_path else None
        if path is None or not path.exists():
            out.append(doc)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("plan_ids", nargs="*", help="limit to these plans")
    ap.add_argument("--download", action="store_true",
                    help="actually download; without it this only reports")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--include-extracted", action="store_true",
                    help="also re-download documents that already hold text. "
                         "2,557 documents have no local file because the "
                         "pipeline runs on GHA runners; almost all are fine.")
    args = ap.parse_args()

    from fetcher import DOWNLOADS_DIR, download_document

    blocked = _waf_blocked_plan_ids()
    session = database.SessionLocal()
    try:
        docs = missing_file_documents(
            session, args.plan_ids or None,
            only_unextracted=not args.include_extracted)
        docs = [d for d in docs if d.plan_id not in blocked]
        if args.limit:
            docs = docs[:args.limit]

        if not docs:
            console.print("No documents are missing their file.")
            return 0

        by_plan: dict[str, int] = {}
        for d in docs:
            by_plan[d.plan_id] = by_plan.get(d.plan_id, 0) + 1
        scope = ("have a row but no file" if args.include_extracted
                 else "have no file and no extracted text")
        console.print(f"\n[bold]{len(docs)}[/bold] documents {scope}:")
        for plan, n in sorted(by_plan.items(), key=lambda x: -x[1]):
            console.print(f"   {plan:22}{n:>5}")

        if not args.download:
            console.print("\n[yellow]Nothing downloaded. Re-run with --download."
                          "[/yellow]\n")
            return 0

        got = gone = 0
        for doc in docs:
            dest_dir = DOWNLOADS_DIR / doc.plan_id
            try:
                path, size = download_document(doc.url, dest_dir, doc.filename)
            except Exception as e:                       # noqa: BLE001
                console.print(f"  [red]{doc.filename}: {e}[/red]")
                path, size = None, 0
            time.sleep(REQUEST_DELAY_SECONDS)

            if not path:
                gone += 1
                continue

            doc.local_path = str(path)
            doc.file_size_bytes = size
            # Back to pending so the extractor picks it up. The old failure
            # reason is deleted rather than kept: it recorded a missing file,
            # and the file is no longer missing.
            doc.extraction_status = "pending"
            session.query(ExtractionDetail).filter(
                ExtractionDetail.document_id == doc.id).delete()
            session.commit()
            got += 1
            console.print(f"  [green]{got:>4}[/green] {str(doc.filename)[:52]:54}"
                          f"{size:>10,} bytes")

        console.print(f"\nrecovered [bold]{got}[/bold], still gone [bold]{gone}[/bold]")
        console.print("[dim]extract them with: python pipeline.py "
                      f"{' '.join(sorted(by_plan))} --retry-failed --no-ocr[/dim]")
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
