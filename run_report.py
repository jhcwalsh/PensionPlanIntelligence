"""
Run report: show documents added in the latest pipeline run, summarization status,
and anything excluded (failed extraction, missing summary, no downloaded_at).

Usage:
    python run_report.py          # last 7 days — summarized/excluded breakdown
    python run_report.py --days 1 # last 24 hours
    python run_report.py --list   # flat list of all downloaded docs with title
    python run_report.py --csv    # export all docs to documents.csv
"""

import argparse
import csv
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from rich.console import Console
from rich.table import Table

from database import Document, Plan, Summary, get_session, init_db

console = Console(legacy_windows=False)


def main():
    parser = argparse.ArgumentParser(description="Report on the latest pipeline run")
    parser.add_argument("--days", type=int, default=7,
                        help="Lookback window in days (default: 7)")
    parser.add_argument("--list", action="store_true",
                        help="Print a flat list of all downloaded documents with title")
    parser.add_argument("--csv", action="store_true",
                        help="Export all documents to documents.csv")
    args = parser.parse_args()

    init_db()
    session = get_session()

    cutoff = datetime.utcnow() - timedelta(days=args.days)

    docs = (
        session.query(Document)
        .filter(Document.downloaded_at >= cutoff)
        .order_by(Document.downloaded_at.desc())
        .all()
    )

    if not docs:
        console.print(f"[yellow]No documents downloaded in the last {args.days} day(s).[/yellow]")
        return

    if args.csv:
        out_path = Path(__file__).parent / "documents.csv"
        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Plan", "Title", "Filename", "Type", "Meeting Date", "Downloaded At", "Summarized"])
            for doc in docs:
                plan = session.get(Plan, doc.plan_id)
                plan_name = plan.abbreviation or plan.name if plan else doc.plan_id
                meeting_date = doc.meeting_date.strftime("%Y-%m-%d") if doc.meeting_date else ""
                downloaded = doc.downloaded_at.strftime("%Y-%m-%d %H:%M") if doc.downloaded_at else ""
                title = doc.filename or ""
                if doc.summary and doc.summary.key_topics:
                    topics = json.loads(doc.summary.key_topics or "[]")
                    if topics:
                        title = topics[0]
                summarized = "yes" if doc.summary else "no"
                writer.writerow([plan_name, title, doc.filename or "", doc.doc_type or "",
                                  meeting_date, downloaded, summarized])
        console.print(f"[green]Wrote {len(docs)} documents to {out_path}[/green]")
        session.close()
        return

    if args.list:
        t = Table(
            title=f"Documents downloaded — last {args.days} day(s)  "
                  f"[dim](cutoff: {cutoff.strftime('%Y-%m-%d %H:%M UTC')})[/dim]",
            show_lines=True,
        )
        t.add_column("#", justify="right", style="dim")
        t.add_column("Plan", style="cyan")
        t.add_column("Title / Filename")
        t.add_column("Type")
        t.add_column("Meeting date")
        t.add_column("Downloaded")

        for i, doc in enumerate(docs, 1):
            plan = session.get(Plan, doc.plan_id)
            plan_name = plan.abbreviation or plan.name if plan else doc.plan_id
            meeting_date = doc.meeting_date.strftime("%Y-%m-%d") if doc.meeting_date else "unknown"
            downloaded = doc.downloaded_at.strftime("%Y-%m-%d %H:%M") if doc.downloaded_at else "—"

            # Prefer summary title from key_topics[0] or fall back to filename
            title = doc.filename or "—"
            if doc.summary and doc.summary.key_topics:
                topics = json.loads(doc.summary.key_topics or "[]")
                if topics:
                    title = topics[0]

            t.add_row(str(i), plan_name, title, doc.doc_type or "—", meeting_date, downloaded)

        console.print()
        console.print(t)
        console.print(f"\n[dim]Total: {len(docs)} documents[/dim]")
        session.close()
        return

    summarized = []
    excluded = []

    for doc in docs:
        plan = session.get(Plan, doc.plan_id)
        plan_name = plan.abbreviation or plan.name if plan else doc.plan_id
        meeting_date = doc.meeting_date.strftime("%Y-%m-%d") if doc.meeting_date else "unknown"
        downloaded = doc.downloaded_at.strftime("%Y-%m-%d %H:%M") if doc.downloaded_at else "—"

        base = {
            "plan": plan_name,
            "filename": doc.filename or "—",
            "doc_type": doc.doc_type or "—",
            "meeting_date": meeting_date,
            "downloaded": downloaded,
        }

        has_summary = doc.summary is not None

        if doc.extraction_status == "failed":
            excluded.append({**base, "reason": "extraction failed"})
        elif doc.extraction_status != "done":
            excluded.append({**base, "reason": f"extraction {doc.extraction_status or 'pending'}"})
        elif not has_summary:
            excluded.append({**base, "reason": "not yet summarized"})
        else:
            summarized.append(base)

    # --- Summary counts ---
    console.print()
    console.print(f"[bold]Pipeline Run Report[/bold] — last {args.days} day(s)  "
                  f"([dim]cutoff: {cutoff.strftime('%Y-%m-%d %H:%M UTC')}[/dim])")
    console.print(f"  Documents downloaded : [bold]{len(docs)}[/bold]")
    console.print(f"  Summarized           : [green]{len(summarized)}[/green]")
    console.print(f"  Excluded             : [red]{len(excluded)}[/red]")
    console.print()

    # --- Summarized table ---
    if summarized:
        t = Table(title=f"Summarized ({len(summarized)})", show_lines=True)
        t.add_column("Plan", style="cyan")
        t.add_column("File")
        t.add_column("Type")
        t.add_column("Meeting date")
        t.add_column("Downloaded")
        for r in summarized:
            t.add_row(r["plan"], r["filename"], r["doc_type"], r["meeting_date"], r["downloaded"])
        console.print(t)
        console.print()

    # --- Excluded table ---
    if excluded:
        t = Table(title=f"Excluded ({len(excluded)})", show_lines=True)
        t.add_column("Plan", style="cyan")
        t.add_column("File")
        t.add_column("Type")
        t.add_column("Meeting date")
        t.add_column("Downloaded")
        t.add_column("Reason", style="red")
        for r in excluded:
            t.add_row(r["plan"], r["filename"], r["doc_type"],
                      r["meeting_date"], r["downloaded"], r["reason"])
        console.print(t)

    session.close()


if __name__ == "__main__":
    main()
