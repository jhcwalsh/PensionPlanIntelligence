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
    python refresh_cafrs.py                    # all plans (GHA-skip applies)
    python refresh_cafrs.py calpers nystrs     # specific plans (overrides filters)
    python refresh_cafrs.py --year 2025        # force a target year
    python refresh_cafrs.py --local-only       # only the WAF-blocked plans (Task Scheduler)

When run from a hosted GitHub Actions runner ($GITHUB_ACTIONS=true on Azure
runners), the plans listed in data/local_only_cafr_plans.json are skipped --
their CAFR sources are fronted by Cloudflare/Akamai bot mitigation that
blocks cloud datacenter IPs. Those plans are picked up by the parallel
local Windows Task Scheduler invocation that uses --local-only. Explicit
plan_ids on the CLI bypass both filters.
"""

import argparse
import json
import os
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


LOCAL_ONLY_FILE = Path(__file__).parent / "data" / "local_only_cafr_plans.json"


def _load_local_only_ids() -> list[str]:
    with open(LOCAL_ONLY_FILE, encoding="utf-8") as f:
        return [p["id"] for p in json.load(f)["plans"]]


def _resolve_plan_ids(explicit: list[str] | None, local_only: bool) -> list[str] | None:
    """Determine which plan IDs to actually process.

    Precedence: explicit CLI args > --local-only > GITHUB_ACTIONS env var > all.
    Returns None to mean "all plans" (run_refresh's default).
    """
    if explicit:
        return explicit
    if local_only:
        return _load_local_only_ids()
    if os.environ.get("GITHUB_ACTIONS") == "true":
        skip = set(_load_local_only_ids())
        registry = Path(__file__).parent / "data" / "known_plans.json"
        with open(registry, encoding="utf-8") as f:
            return [p["id"] for p in json.load(f) if p["id"] not in skip]
    return None


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
    """Process one plan; return the status string.

    Tries `expected_fiscal_year` first, then falls back to the prior FY
    (since ACFRs typically lag FY-close by 4-9 months and the most recent
    FY may not be published yet). The cover-year check also accepts any
    year in the try-set, so a static URL still pointing at last year's
    PDF resolves cleanly. --year overrides skip the fallback.
    """
    plan_id = plan["id"]
    abbrev = plan.get("abbreviation", plan_id)
    fy_end = plan.get("fiscal_year_end")
    if not fy_end:
        console.print(f"  [yellow]{abbrev}: no fiscal_year_end; skipping[/yellow]")
        log_outcome(session, plan_id, run_at, 0, "no_strategy",
                    notes="missing fiscal_year_end")
        return "no_strategy"

    initial_target = force_year if force_year is not None else \
                     expected_fiscal_year(run_at, fy_end)

    years_to_try = [initial_target]
    if force_year is None:
        years_to_try.append(initial_target - 1)
    acceptable_years = set(years_to_try)

    last_result = "no_strategy"
    for target_year in years_to_try:
        existing = already_have_cafr_for_year(session, plan_id, target_year)
        if existing is not None:
            console.print(f"  [dim]{abbrev}: FY{target_year} already saved (doc {existing.id})[/dim]")
            log_outcome(session, plan_id, run_at, target_year, "already_have",
                        document_id=existing.id)
            return "already_have"

        url = resolve_cafr_url_for_year(plan, target_year)
        if not url:
            console.print(f"  [yellow]{abbrev}: no URL resolves for FY{target_year}[/yellow]")
            log_outcome(session, plan_id, run_at, target_year, "no_strategy",
                        notes="no template/landing/static URL produced a candidate")
            last_result = "no_strategy"
            continue

        if document_exists(session, url):
            existing = session.query(Document).filter_by(url=url).first()
            log_outcome(session, plan_id, run_at, target_year, "already_have",
                        url_tried=url, document_id=existing.id if existing else None,
                        notes="URL already in DB under another fiscal_year")
            return "already_have"

        plan_dir = Path(DOWNLOADS_DIR) / plan_id / "cafr"
        plan_dir.mkdir(parents=True, exist_ok=True)
        filename = make_cafr_filename(url, abbrev)

        console.print(f"  [cyan]{abbrev}: trying FY{target_year} {url}[/cyan]")
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

        # PDF magic-header check — landing pages occasionally serve an HTML
        # download that passes the size threshold but isn't a real PDF (this
        # is how the `nycppf-cafr-unknown.pdf` junk row got created in 2026-05).
        try:
            with open(local_path, "rb") as f:
                head = f.read(5)
        except OSError as e:
            head = b""
            console.print(f"  [yellow]{abbrev}: could not read downloaded "
                          f"file ({e}); treating as not-a-PDF[/yellow]")
        if not head.startswith(b"%PDF-"):
            try:
                local_path.unlink()
            except OSError:
                pass
            log_outcome(
                session, plan_id, run_at, target_year, "validation_failed",
                url_tried=url,
                notes=f"not a PDF (header={head!r}); likely HTML masquerading as PDF",
            )
            console.print(
                f"  [yellow]{abbrev}: not a PDF (header {head!r}); not saving[/yellow]"
            )
            last_result = "validation_failed"
            continue

        cover_year = fiscal_year_from_pdf(local_path, max_year=run_at.year + 1)
        if cover_year not in acceptable_years:
            log_outcome(
                session, plan_id, run_at, target_year, "validation_failed",
                url_tried=url,
                notes=(f"cover_year={cover_year}, acceptable="
                       f"{sorted(acceptable_years, reverse=True)}; "
                       f"file kept at {local_path} for review"),
            )
            console.print(
                f"  [yellow]{abbrev}: cover year {cover_year} not in "
                f"{sorted(acceptable_years, reverse=True)}; not saving[/yellow]"
            )
            last_result = "validation_failed"
            continue

        # Save with the actual cover year — may equal target_year or be
        # the fallback. already_have_cafr_for_year on the next loop
        # iteration will catch any duplicate.
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
        log_outcome(
            session, plan_id, run_at, target_year, "saved",
            url_tried=url, document_id=doc.id,
            notes=None if cover_year == target_year else
                  f"saved as FY{cover_year} (target was FY{target_year})",
        )
        console.print(f"  [green]{abbrev}: saved FY{cover_year} CAFR (doc {doc.id})[/green]")
        time.sleep(0.5)
        return "saved"

    return last_result


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
                        help="Plan IDs to process (default: all, with GHA-"
                             "skip filter applied when running on a hosted "
                             "runner).")
    parser.add_argument("--year", type=int,
                        help="Force a specific target fiscal year (default: "
                             "computed from each plan's fiscal_year_end).")
    parser.add_argument("--local-only", action="store_true",
                        help="Process only the plans listed in "
                             "data/local_only_cafr_plans.json (use from "
                             "Windows Task Scheduler — those plans' CAFR "
                             "sources block cloud datacenter IPs).")
    args = parser.parse_args()

    plan_ids = _resolve_plan_ids(args.plan_ids or None, args.local_only)
    counts = run_refresh(plan_ids=plan_ids, force_year=args.year)
    sys.exit(0 if counts.get("error", 0) == 0 else 1)


if __name__ == "__main__":
    main()
