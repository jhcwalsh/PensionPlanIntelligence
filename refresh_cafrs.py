"""
Monthly CAFR refresh: for each plan, check whether the most recently
closed fiscal year's ACFR is in the DB; if not, try to fetch it.

Strategy per plan:
  1. Compute `expected_year` from the plan's `fiscal_year_end` and today's date.
  2. If we already have a CAFR for that plan + year, skip (status `already_have`).
  3. Resolve a URL to try via `fetch_cafr.resolve_cafr_url_for_year`:
     uses `cafr_url_template` first, then `cafr_landing`, then static
     `cafr_url` (only if its embedded year matches `expected_year`).
  4. Download the PDF; reject anything < `MIN_PDF_BYTES`.
  5. Open the PDF and read the cover for a fiscal year. If it matches
     `expected_year`, save with `doc_type='cafr'` and `fiscal_year=expected_year`.
  6. Log the outcome to `cafr_refresh_log`.

Usage:
    python refresh_cafrs.py                    # all plans
    python refresh_cafrs.py calpers nystrs     # specific plans
    python refresh_cafrs.py --year 2025        # force a target year
"""

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

from rich.console import Console

from cafr_year_check import fiscal_year_from_pdf
from database import (
    CafrRefreshLog,
    Document,
    Plan,
    document_exists,
    get_session,
    init_db,
)
from fetch_cafr import (
    DOWNLOADS_DIR,
    MIN_PDF_BYTES,
    make_cafr_filename,
    resolve_cafr_url_for_year,
)
from fetcher import download_document, load_plans

console = Console(legacy_windows=False)


def expected_fiscal_year(today: datetime, fy_end_md: str) -> int:
    """Most recent FY that has already closed as of `today`.

    `fy_end_md` is "MM-DD" (e.g. "06-30"). If today is on/after that month-day
    in the current calendar year, the most recently closed FY is `today.year`;
    otherwise it's `today.year - 1`.
    """
    m, d = (int(x) for x in fy_end_md.split("-"))
    if (today.month, today.day) >= (m, d):
        return today.year
    return today.year - 1


def already_have_cafr_for_year(session, plan_id: str, year: int) -> Document | None:
    return (
        session.query(Document)
        .filter(
            Document.plan_id == plan_id,
            Document.doc_type == "cafr",
            Document.fiscal_year == year,
        )
        .first()
    )


def log_outcome(session, plan_id: str, run_at: datetime, expected_year: int,
                status: str, url_tried: str | None = None,
                document_id: int | None = None, notes: str | None = None) -> None:
    session.add(CafrRefreshLog(
        plan_id=plan_id,
        run_at=run_at,
        expected_year=expected_year,
        status=status,
        url_tried=url_tried,
        document_id=document_id,
        notes=notes,
    ))
    session.commit()


def refresh_plan(session, plan: dict, run_at: datetime,
                 force_year: int | None = None) -> str:
    """Process one plan; return the status string."""
    plan_id = plan["id"]
    abbrev = plan.get("abbreviation", plan_id)
    fy_end = plan.get("fiscal_year_end")
    if not fy_end:
        console.print(f"  [yellow]{abbrev}: no fiscal_year_end; skipping[/yellow]")
        log_outcome(session, plan_id, run_at, 0, "no_strategy",
                    notes="missing fiscal_year_end")
        return "no_strategy"

    target_year = force_year if force_year is not None else \
                  expected_fiscal_year(run_at, fy_end)

    # Skip if we already have it
    existing = already_have_cafr_for_year(session, plan_id, target_year)
    if existing is not None:
        console.print(f"  [dim]{abbrev}: FY{target_year} already saved (doc {existing.id})[/dim]")
        log_outcome(session, plan_id, run_at, target_year, "already_have",
                    document_id=existing.id)
        return "already_have"

    # Resolve a URL guess for the target year
    url = resolve_cafr_url_for_year(plan, target_year)
    if not url:
        console.print(f"  [yellow]{abbrev}: no URL resolves for FY{target_year}[/yellow]")
        log_outcome(session, plan_id, run_at, target_year, "no_strategy",
                    notes="no template/landing/static URL produced a candidate")
        return "no_strategy"

    if document_exists(session, url):
        existing = session.query(Document).filter_by(url=url).first()
        # Same URL was previously saved (perhaps with a different year tag) —
        # don't re-download.
        log_outcome(session, plan_id, run_at, target_year, "already_have",
                    url_tried=url, document_id=existing.id if existing else None,
                    notes="URL already in DB under another fiscal_year")
        return "already_have"

    # Download
    plan_dir = Path(DOWNLOADS_DIR) / plan_id / "cafr"
    plan_dir.mkdir(parents=True, exist_ok=True)
    filename = make_cafr_filename(url, abbrev)

    console.print(f"  [cyan]{abbrev}: trying {url}[/cyan]")
    local_path, size = download_document(url, plan_dir, filename)
    if not local_path:
        log_outcome(session, plan_id, run_at, target_year, "url_failed",
                    url_tried=url, notes="download_document returned no file")
        return "url_failed"

    if size < MIN_PDF_BYTES:
        try:
            local_path.unlink()
        except OSError:
            pass
        log_outcome(session, plan_id, run_at, target_year, "validation_failed",
                    url_tried=url, notes=f"file only {size} bytes")
        return "validation_failed"

    # Cover-page year check
    cover_year = fiscal_year_from_pdf(local_path, max_year=run_at.year + 1)
    if cover_year != target_year:
        # Could be: (a) plan re-posted last year's, (b) cover unreadable,
        # (c) URL pointed to wrong file. Don't save — keep the file for
        # human review and log the mismatch.
        log_outcome(
            session, plan_id, run_at, target_year, "validation_failed",
            url_tried=url,
            notes=(f"cover_year={cover_year}, expected={target_year}; "
                   f"file kept at {local_path} for review"),
        )
        console.print(
            f"  [yellow]{abbrev}: cover year {cover_year} != expected {target_year}; "
            f"not saving[/yellow]"
        )
        return "validation_failed"

    # Save
    doc = Document(
        plan_id=plan_id,
        url=url,
        filename=filename,
        doc_type="cafr",
        local_path=str(local_path),
        file_size_bytes=size,
        downloaded_at=run_at,
        extraction_status="pending",
        fiscal_year=cover_year,
    )
    session.add(doc)
    session.commit()
    log_outcome(session, plan_id, run_at, target_year, "saved",
                url_tried=url, document_id=doc.id)
    console.print(f"  [green]{abbrev}: saved FY{cover_year} CAFR (doc {doc.id})[/green]")
    time.sleep(0.5)
    return "saved"


def run_refresh(plan_ids: list[str] | None = None,
                force_year: int | None = None) -> dict[str, int]:
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
                status = refresh_plan(session, plan, run_at, force_year=force_year)
            except Exception as e:
                status = "error"
                console.print(f"  [red]error: {e}[/red]")
                log_outcome(session, plan["id"], run_at, 0, "error",
                            notes=f"{type(e).__name__}: {e}"[:500])
            counts[status] = counts.get(status, 0) + 1
    finally:
        session.close()

    console.rule("[bold green]Refresh complete[/bold green]")
    for status in ("saved", "already_have", "validation_failed", "url_failed",
                   "no_strategy", "error"):
        console.print(f"  {status:20s} {counts.get(status, 0)}")
    return counts


def main():
    parser = argparse.ArgumentParser(
        description="Monthly CAFR refresh: pull next year's ACFR per plan.")
    parser.add_argument("plan_ids", nargs="*",
                        help="Plan IDs to process (default: all).")
    parser.add_argument("--year", type=int,
                        help="Force a specific target fiscal year (default: "
                             "computed from each plan's fiscal_year_end).")
    args = parser.parse_args()

    counts = run_refresh(plan_ids=args.plan_ids or None, force_year=args.year)
    sys.exit(0 if counts.get("error", 0) == 0 else 1)


if __name__ == "__main__":
    main()
