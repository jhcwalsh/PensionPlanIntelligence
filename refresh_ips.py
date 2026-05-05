"""
Monthly IPS refresh: for each plan, discover the current Investment Policy
Statement, fetch it, hash-dedupe against existing IpsDocument rows, run a
Claude Haiku 4.5 verification gate, and save as a new version when changed.

Strategy per plan:
  1. discover_ips_urls(): override > mine_existing > site_crawl
  2. For each candidate URL (until one verifies):
       a. download → size + %PDF- check
       b. compute sha256
       c. if (plan_id, hash) already in DB → status=already_have, return
       d. extract first ~3 pages of text (pdfplumber)
       e. verify_is_ips(plan_name, text) via Haiku 4.5 (or IPS_MODE=mock)
       f. if verdict.is_ips → save IpsDocument; status=saved
       g. else → log verification_failed, try next candidate
  3. If all candidates exhausted → status=verification_failed | no_candidates

Usage (local, no GHA):
    python refresh_ips.py                    # all 148 plans
    python refresh_ips.py calpers nystrs     # subset
    python refresh_ips.py --discover-only    # don't save, just print candidates
    IPS_MODE=mock python refresh_ips.py      # offline / test (no Anthropic call)
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

from rich.console import Console

from database import (
    IpsDocument,
    IpsRefreshLog,
    Plan,
    get_session,
    init_db,
)
from fetch_ips import (
    MIN_IPS_BYTES,
    discover_ips_urls,
    file_sha256,
    looks_like_pdf,
    make_ips_filename,
    verify_is_ips,
)
from fetcher import DOWNLOADS_DIR, download_document, load_plans

console = Console(legacy_windows=False)


def log_outcome(session, plan_id: str, run_at: datetime, status: str,
                url_tried: str | None = None,
                discovery_source: str | None = None,
                document_id: int | None = None,
                notes: str | None = None) -> None:
    session.add(IpsRefreshLog(
        plan_id=plan_id,
        run_at=run_at,
        status=status,
        url_tried=url_tried,
        discovery_source=discovery_source,
        document_id=document_id,
        notes=(notes or None),
    ))
    session.commit()


def _extract_first_pages(local_path: Path) -> tuple[str, int]:
    """Run the project's pdfplumber extractor and return (full_text, page_count).

    The full text gets stored on IpsDocument.extracted_text for downstream
    scoring; verify_is_ips() only sees the first ~3 pages.
    """
    from extractor import extract_pdf_pdfplumber, extract_pdf_pymupdf
    text, n_pages = extract_pdf_pdfplumber(str(local_path))
    if not text.strip():
        text, n_pages = extract_pdf_pymupdf(str(local_path))
    return text, n_pages


def refresh_plan(session, plan: dict, run_at: datetime,
                 discover_only: bool = False) -> str:
    """Process one plan; return the status string."""
    plan_id = plan["id"]
    abbrev = plan.get("abbreviation", plan_id)
    plan_name = plan.get("name") or plan_id

    candidates = discover_ips_urls(plan, session)
    if not candidates:
        console.print(f"  [yellow]{abbrev}: no IPS candidates discovered[/yellow]")
        log_outcome(session, plan_id, run_at, "no_candidates")
        return "no_candidates"

    if discover_only:
        for url, source in candidates:
            console.print(f"  [cyan]{abbrev}: candidate ({source}) {url}[/cyan]")
        return "discover_only"

    plan_dir = Path(DOWNLOADS_DIR) / plan_id / "ips"
    plan_dir.mkdir(parents=True, exist_ok=True)

    last_status = "no_candidates"
    last_notes = None

    for url, source in candidates:
        filename = make_ips_filename(url, abbrev)
        console.print(f"  [cyan]{abbrev}: trying ({source}) {url}[/cyan]")
        local_path, size = download_document(url, plan_dir, filename)
        if not local_path:
            log_outcome(session, plan_id, run_at, "url_failed",
                        url_tried=url, discovery_source=source,
                        notes="download_document returned no file")
            last_status = "url_failed"
            continue

        if size < MIN_IPS_BYTES:
            try: local_path.unlink()
            except OSError: pass
            log_outcome(session, plan_id, run_at, "validation_failed",
                        url_tried=url, discovery_source=source,
                        notes=f"file only {size} bytes")
            last_status = "validation_failed"
            continue

        if not looks_like_pdf(local_path):
            try: local_path.unlink()
            except OSError: pass
            log_outcome(session, plan_id, run_at, "validation_failed",
                        url_tried=url, discovery_source=source,
                        notes="not a PDF (magic header check failed)")
            last_status = "validation_failed"
            continue

        # Hash-dedupe against existing rows for this plan
        digest = file_sha256(local_path)
        existing = (session.query(IpsDocument)
                    .filter_by(plan_id=plan_id, content_hash=digest)
                    .first())
        if existing is not None:
            try: local_path.unlink()
            except OSError: pass
            console.print(
                f"  [dim]{abbrev}: content_hash already in DB "
                f"(doc {existing.id}); no change[/dim]"
            )
            log_outcome(session, plan_id, run_at, "already_have",
                        url_tried=url, discovery_source=source,
                        document_id=existing.id,
                        notes=f"sha256={digest[:12]}…")
            return "already_have"

        # Extract first pages for verification + downstream scoring
        text, page_count = _extract_first_pages(local_path)
        if not text.strip():
            log_outcome(session, plan_id, run_at, "validation_failed",
                        url_tried=url, discovery_source=source,
                        notes="text extraction produced empty result")
            last_status = "validation_failed"
            continue

        verdict = verify_is_ips(plan_name, text)
        if not verdict["is_ips"]:
            try: local_path.unlink()
            except OSError: pass
            note = (f"verifier said no (doc_type={verdict['doc_type']}, "
                    f"confidence={verdict['confidence']}): {verdict['reason']}")
            console.print(f"  [yellow]{abbrev}: {note}[/yellow]")
            log_outcome(session, plan_id, run_at, "verification_failed",
                        url_tried=url, discovery_source=source, notes=note)
            last_status = "verification_failed"
            last_notes = note
            continue

        # Save
        doc = IpsDocument(
            plan_id=plan_id,
            content_hash=digest,
            url=url,
            filename=filename,
            local_path=str(local_path),
            file_size_bytes=size,
            fetched_at=run_at,
            extracted_text=text,
            extraction_status="done",
            page_count=page_count,
            verification_verdict="yes",
            verification_confidence=verdict["confidence"],
            verification_notes=verdict["reason"],
        )
        session.add(doc)
        session.commit()
        log_outcome(session, plan_id, run_at, "saved",
                    url_tried=url, discovery_source=source,
                    document_id=doc.id,
                    notes=f"sha256={digest[:12]}…")
        console.print(
            f"  [green]{abbrev}: saved IPS (doc {doc.id}, "
            f"confidence={verdict['confidence']})[/green]"
        )
        return "saved"

    return last_status


def run_refresh(plan_ids: list[str] | None = None,
                discover_only: bool = False) -> dict[str, int]:
    init_db()
    plans = load_plans()
    if plan_ids:
        wanted = set(plan_ids)
        plans = [p for p in plans if p["id"] in wanted]

    run_at = datetime.utcnow()
    counts: dict[str, int] = {}

    session = get_session()
    try:
        for plan in plans:
            console.rule(f"[bold]{plan.get('abbreviation', plan['id'])}[/bold]")
            try:
                status = refresh_plan(session, plan, run_at,
                                      discover_only=discover_only)
            except Exception as e:
                status = "error"
                console.print(f"  [red]error: {e}[/red]")
                log_outcome(session, plan["id"], run_at, "error",
                            notes=f"{type(e).__name__}: {e}"[:500])
            counts[status] = counts.get(status, 0) + 1
    finally:
        session.close()

    console.rule("[bold green]IPS refresh complete[/bold green]")
    for status in ("saved", "already_have", "verification_failed",
                   "validation_failed", "url_failed", "no_candidates",
                   "discover_only", "error"):
        if status in counts:
            console.print(f"  {status:22s} {counts[status]}")
    return counts


def main():
    parser = argparse.ArgumentParser(
        description="Monthly IPS refresh: discover, fetch, verify, save."
    )
    parser.add_argument("plan_ids", nargs="*",
                        help="Plan IDs to process (default: all 148).")
    parser.add_argument("--discover-only", action="store_true",
                        help="List candidate URLs per plan without "
                             "downloading or saving anything.")
    args = parser.parse_args()

    counts = run_refresh(plan_ids=args.plan_ids or None,
                         discover_only=args.discover_only)
    sys.exit(0 if counts.get("error", 0) == 0 else 1)


if __name__ == "__main__":
    main()
