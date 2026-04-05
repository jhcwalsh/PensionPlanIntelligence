"""
Pipeline orchestrator: fetch → extract → summarize

Run this script to process one or more pension plans end-to-end.

Usage:
    python pipeline.py                         # all plans
    python pipeline.py calpers calstrs         # specific plans
    python pipeline.py --extract-only          # skip fetch, just extract + summarize
    python pipeline.py --summarize-only        # skip fetch + extract, just summarize
"""

import argparse
import os
import sys
from datetime import datetime

from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

from database import Document, Plan, Summary, get_session, init_db

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
console = Console()


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


def run_pipeline(
    plan_ids: list[str] = None,
    do_fetch: bool = True,
    do_extract: bool = True,
    do_summarize: bool = True,
    max_docs_per_plan: int = 50,
):
    init_db()
    start = datetime.utcnow()
    console.rule("[bold blue]Pension Plan Intelligence Pipeline[/bold blue]")

    if do_fetch:
        console.rule("[bold]Step 1: Fetch Documents[/bold]")
        from fetcher import run_fetcher
        run_fetcher(plan_ids=plan_ids, max_docs_per_plan=max_docs_per_plan)

    if do_extract:
        console.rule("[bold]Step 2: Extract Text[/bold]")
        from extractor import run_extractor
        run_extractor()

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
    parser.add_argument("--status", action="store_true",
                        help="Just print pipeline status and exit")
    args = parser.parse_args()

    if args.status:
        init_db()
        session = get_session()
        try:
            print_status(session)
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

    run_pipeline(
        plan_ids=args.plan_ids if args.plan_ids else None,
        do_fetch=do_fetch,
        do_extract=do_extract,
        do_summarize=do_summarize,
        max_docs_per_plan=args.max_docs,
    )


if __name__ == "__main__":
    main()
