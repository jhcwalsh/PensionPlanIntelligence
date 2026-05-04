"""
Pipeline orchestrator: fetch → extract → summarize

Run this script to process one or more pension plans end-to-end.

Usage:
    python pipeline.py                         # all plans
    python pipeline.py calpers calstrs         # specific plans
    python pipeline.py --local-only            # only the WAF-blocked plans (Task Scheduler)
    python pipeline.py --extract-only          # skip fetch, just extract + summarize
    python pipeline.py --summarize-only        # skip fetch + extract, just summarize

Hybrid GHA / local split: when GITHUB_ACTIONS=true is set (auto on hosted
runners), the plans listed in data/local_only_plans.json are skipped --
those have Cloudflare/WAF that blocks Azure IPs, so they stay on the
local Windows Task Scheduler invocation that uses --local-only. Explicit
positional plan_ids on the CLI bypass both filters.
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

# Force UTF-8 output on Windows — filenames from pension sites can contain
# characters outside cp1252, which crashes the default Windows console encoder.
if sys.platform == 'win32' and hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

from database import Document, Plan, Summary, get_session, init_db

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
console = Console(legacy_windows=False)


def print_status(session):
    """Print a summary table of current DB state."""
    plans = session.query(Plan).all()
    table = Table(title="Pipeline Status", show_lines=True)
    table.add_column("Plan", style="cyan")
    table.add_column("Docs", justify="right")
    table.add_column("Extracted", justify="right")
    table.add_column("Summarized", justify="right")

    for plan in plans:
        total = session.query(Document).filter_by(plan_id=plan.id).count()
        extracted = session.query(Document).filter_by(
            plan_id=plan.id, extraction_status="done").count()
        summarized = (
            session.query(Summary)
            .join(Document)
            .filter(Document.plan_id == plan.id)
            .count()
        )
        if total > 0:
            table.add_row(plan.abbreviation or plan.name, str(total),
                          str(extracted), str(summarized))

    console.print(table)


LOCAL_ONLY_FILE = Path(__file__).parent / "data" / "local_only_plans.json"


def _load_local_only_ids() -> list[str]:
    with open(LOCAL_ONLY_FILE, encoding="utf-8") as f:
        return [p["id"] for p in json.load(f)["plans"]]


def _resolve_plan_ids(explicit: list[str] | None, local_only: bool) -> list[str] | None:
    """Determine which plan IDs to actually process.

    Precedence: explicit CLI args > --local-only > GITHUB_ACTIONS env var > all.
    Returns None to mean "all plans" (the fetcher's default).
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


def run_pipeline(
    plan_ids: list[str] = None,
    do_fetch: bool = True,
    do_extract: bool = True,
    do_summarize: bool = True,
    max_docs_per_plan: int = 50,
    min_year: int = 2026,
    retry_failed: bool = False,
):
    init_db()
    start = datetime.utcnow()
    console.rule("[bold blue]Pension Plan Intelligence Pipeline[/bold blue]")

    if do_fetch:
        console.rule("[bold]Step 1: Fetch Documents[/bold]")
        import fetcher as _fetcher
        from fetcher import run_fetcher
        _fetcher.MIN_DATE = datetime(min_year, 1, 1)
        run_fetcher(plan_ids=plan_ids, max_docs_per_plan=max_docs_per_plan)

    if do_extract:
        console.rule("[bold]Step 2: Extract Text[/bold]")
        from extractor import run_extractor
        run_extractor(retry_failed=retry_failed)

    if do_summarize:
        console.rule("[bold]Step 3: Summarize with Claude[/bold]")
        from summarizer import run_summarizer
        run_summarizer()

    session = get_session()
    try:
        console.rule("[bold]Pipeline Complete[/bold]")
        elapsed = (datetime.utcnow() - start).seconds
        console.print(f"Total time: {elapsed}s\n")
        print_status(session)
    finally:
        session.close()


def main():
    parser = argparse.ArgumentParser(description="Pension Plan Intelligence Pipeline")
    parser.add_argument("plan_ids", nargs="*", help="Plan IDs to process (default: all)")
    parser.add_argument("--fetch-only", action="store_true")
    parser.add_argument("--extract-only", action="store_true")
    parser.add_argument("--summarize-only", action="store_true")
    parser.add_argument("--max-docs", type=int, default=50,
                        help="Max documents to download per plan (default: 50)")
    parser.add_argument("--min-year", type=int, default=2026,
                        help="Only fetch documents from this year onward (default: 2025)")
    parser.add_argument("--retry-failed", action="store_true",
                        help="Re-attempt failed extractions (with OCR fallback)")
    parser.add_argument("--status", action="store_true",
                        help="Just print pipeline status and exit")
    parser.add_argument("--updates", action="store_true",
                        help="Print new meetings with agenda summaries and material links")
    parser.add_argument("--updates-days", type=int, default=14,
                        help="Lookback window for --updates (default: 14 days)")
    parser.add_argument("--local-only", action="store_true",
                        help="Process only the WAF-blocked plans listed in "
                             "data/local_only_plans.json (use from Windows "
                             "Task Scheduler in the hybrid GHA/local model)")
    args = parser.parse_args()

    if args.status:
        init_db()
        session = get_session()
        try:
            print_status(session)
        finally:
            session.close()
        return

    if args.updates:
        init_db()
        session = get_session()
        try:
            from database import get_new_meetings
            meetings = get_new_meetings(session, days=args.updates_days)
            if not meetings:
                console.print(f"[yellow]No new meetings in the last {args.updates_days} days.[/yellow]")
                return
            console.rule(f"[bold]New Meetings (last {args.updates_days} days)[/bold]")
            for m in meetings:
                plan = m["plan"]
                plan_str = (plan.abbreviation or plan.name) if plan else "Unknown"
                date_str = m["meeting_date"].strftime("%B %d, %Y") if m["meeting_date"] else "Date unknown"
                console.print(f"\n[bold cyan]{plan_str}[/bold cyan] — {date_str}")
                if m["agenda_summary"]:
                    console.print(f"  {m['agenda_summary'].summary_text}")
                else:
                    console.print("  [dim]No summary yet — run pipeline to process.[/dim]")
                console.print("  [bold]Materials:[/bold]")
                for d in m["all_docs"]:
                    doc_type = (d.doc_type or "document").replace("_", " ").title()
                    console.print(f"    {doc_type}: {d.url}")
        finally:
            session.close()
        return

    do_fetch = not (args.extract_only or args.summarize_only)
    do_extract = not (args.fetch_only or args.summarize_only)
    do_summarize = not (args.fetch_only or args.extract_only)

    # If a specific step flag was set, only do that step
    if args.fetch_only:
        do_fetch, do_extract, do_summarize = True, False, False
    elif args.extract_only:
        do_fetch, do_extract, do_summarize = False, True, True  # extract + summarize
    elif args.summarize_only:
        do_fetch, do_extract, do_summarize = False, False, True
    elif args.retry_failed:
        do_fetch, do_extract, do_summarize = False, True, False

    run_pipeline(
        plan_ids=_resolve_plan_ids(args.plan_ids, args.local_only),
        do_fetch=do_fetch,
        do_extract=do_extract,
        do_summarize=do_summarize,
        max_docs_per_plan=args.max_docs,
        min_year=args.min_year,
        retry_failed=args.retry_failed,
    )


if __name__ == "__main__":
    main()
