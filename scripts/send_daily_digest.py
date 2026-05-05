"""Daily pipeline digest email.

Sends a daily summary of new documents fetched by the document pipeline.
Reads FetchRun rows from the last N hours (default 26), aggregates each
run's new_document_ids, joins to Document + Plan, and renders a
per-plan grouped list. Same data shape the Admin "Recent Runs" tab
shows, just emailed.

Triggered at the end of .github/workflows/daily-pipeline.yml after the
137-plan GHA run completes. The 26-hour window also catches any local
Task Scheduler FetchRun for the 11 WAF-blocked plans (which fires
earlier ET than the GHA cron).

Honors INSIGHTS_MODE=mock for offline tests — writes to tmp/sent_emails/
instead of calling Resend.

Usage:
    python -m scripts.send_daily_digest                # default 26-hour window
    python -m scripts.send_daily_digest --hours 48     # wider window
    python -m scripts.send_daily_digest user@x.com     # one-off recipient override
    INSIGHTS_MODE=mock python -m scripts.send_daily_digest
"""
from __future__ import annotations

import argparse
import html as _html
import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta

from insights import config
from insights.approval import ApprovalEmail, send_email
from database import Document, FetchRun, Plan, get_session


DEFAULT_HOURS = 26


def collect_recent_runs(session, hours: int) -> list[dict]:
    """Return [{run, plan_to_docs}] for FetchRuns started in the last `hours`.

    plan_to_docs maps (plan_id, plan_name, abbreviation) -> list of
    (doc_id, filename) for each newly-inserted document.
    """
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    runs = (
        session.query(FetchRun)
        .filter(FetchRun.started_at >= cutoff)
        .order_by(FetchRun.started_at.asc())
        .all()
    )

    out: list[dict] = []
    for run in runs:
        try:
            doc_ids = json.loads(run.new_document_ids or "[]")
        except (TypeError, ValueError):
            doc_ids = []

        plan_to_docs: dict[tuple, list[tuple[int, str]]] = defaultdict(list)
        if doc_ids:
            rows = (
                session.query(
                    Document.id, Document.plan_id, Document.filename,
                    Plan.name, Plan.abbreviation,
                )
                .join(Plan, Plan.id == Document.plan_id)
                .filter(Document.id.in_(doc_ids))
                .order_by(Plan.abbreviation, Document.id)
                .all()
            )
            for doc_id, plan_id, filename, plan_name, abbrev in rows:
                plan_to_docs[(plan_id, plan_name or "", abbrev or plan_id)].append(
                    (doc_id, filename or "(no filename)")
                )

        out.append({"run": run, "plan_to_docs": dict(plan_to_docs)})
    return out


def _fmt_run_header(run) -> str:
    """One-line label for a run: '✓ Run #14 (gha) — 2026-05-05 11:00 UTC, 24m 22s — success'."""
    elapsed = ""
    if run.completed_at and run.started_at:
        secs = int((run.completed_at - run.started_at).total_seconds())
        elapsed = f", {secs // 60}m {secs % 60}s"
    marker = {"success": "OK", "failed": "FAIL", "running": "RUNNING"}.get(run.status, "?")
    return (
        f"[{marker}] Run #{run.id} ({run.source}) — "
        f"{run.started_at.strftime('%Y-%m-%d %H:%M UTC')}{elapsed} — "
        f"status={run.status}"
    )


def render_email(runs_data: list[dict]) -> tuple[str, str, int]:
    """Return (html_body, text_body, total_new_docs)."""
    total_docs = sum(
        sum(len(d) for d in r["plan_to_docs"].values()) for r in runs_data
    )
    today = datetime.utcnow().strftime("%Y-%m-%d")

    # ---- HTML body
    html_parts: list[str] = [
        '<html><body style="font-family:-apple-system,sans-serif;max-width:720px;'
        'margin:1.5em auto;line-height:1.5;color:#222;">',
        f'<h2 style="margin-bottom:0.2em;">Daily pipeline — {today}</h2>',
        (f'<p style="color:#555;margin-top:0;">{total_docs} new document'
         f'{"s" if total_docs != 1 else ""} across {len(runs_data)} run'
         f'{"s" if len(runs_data) != 1 else ""} in the last 26 hours.</p>'),
    ]

    # ---- Text body
    text_parts: list[str] = [
        f"Daily pipeline — {today}",
        "=" * 40,
        f"{total_docs} new document(s) across {len(runs_data)} run(s) "
        "in the last 26 hours.",
        "",
    ]

    if not runs_data:
        html_parts.append('<p>No pipeline runs in the window.</p>')
        text_parts.append("No pipeline runs in the window.")

    for r in runs_data:
        run = r["run"]
        plan_to_docs = r["plan_to_docs"]
        header = _fmt_run_header(run)

        html_parts.append(
            '<h3 style="margin-top:1.5em;border-bottom:1px solid #ddd;'
            f'padding-bottom:0.3em;">{_html.escape(header)}</h3>'
        )
        text_parts.append(header)
        text_parts.append("-" * 40)

        if run.status == "failed" and run.error_message:
            err = (run.error_message or "")[:400]
            html_parts.append(
                f'<p style="color:#c00;"><b>Failure:</b> '
                f'<code>{_html.escape(err)}</code></p>'
            )
            text_parts.extend([f"FAILURE: {err}", ""])
            continue

        if not plan_to_docs:
            html_parts.append('<p style="color:#888;">No new documents.</p>')
            text_parts.extend(["No new documents.", ""])
            continue

        n_docs = sum(len(d) for d in plan_to_docs.values())
        n_plans = len(plan_to_docs)
        html_parts.append(
            f'<p style="color:#555;">{n_docs} new doc(s) across '
            f'{n_plans} plan(s):</p><ul style="padding-left:1.5em;">'
        )
        text_parts.append(f"{n_docs} new doc(s) across {n_plans} plan(s):")

        plans_sorted = sorted(plan_to_docs.items(), key=lambda kv: kv[0][2])
        for (plan_id, plan_name, abbrev), docs in plans_sorted:
            label = f"{abbrev} ({plan_name})" if plan_name else abbrev
            html_parts.append(
                f'<li><b>{_html.escape(label)}</b> — {len(docs)} doc(s)'
                '<ul style="margin-top:0.3em;">'
            )
            text_parts.append(f"  {label} — {len(docs)} doc(s)")
            for _doc_id, filename in docs:
                html_parts.append(
                    f'<li style="color:#444;font-size:0.95em;">'
                    f'<code>{_html.escape(filename)}</code></li>'
                )
                text_parts.append(f"    - {filename}")
            html_parts.append('</ul></li>')
        html_parts.append('</ul>')
        text_parts.append("")

    html_parts.append(
        '<hr style="margin-top:2em;border:0;border-top:1px solid #eee;">'
        '<p style="color:#888;font-size:0.85em;">Sent by '
        '<code>scripts/send_daily_digest.py</code>. The same view lives in '
        'the Streamlit app under <b>Admin → Recent Runs</b>.</p>'
        '</body></html>'
    )
    return "\n".join(html_parts), "\n".join(text_parts), total_docs


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="scripts.send_daily_digest")
    parser.add_argument(
        "--hours", type=int, default=DEFAULT_HOURS,
        help=f"Window for recent FetchRun rows (default: {DEFAULT_HOURS}).",
    )
    parser.add_argument(
        "recipient", nargs="?",
        help="Override APPROVAL_EMAIL_RECIPIENT for this send.",
    )
    args = parser.parse_args(argv)

    session = get_session()
    try:
        runs_data = collect_recent_runs(session, args.hours)
    finally:
        session.close()

    html, text, total_docs = render_email(runs_data)
    today = datetime.utcnow().strftime("%Y-%m-%d")
    subject = (f"[PensionGraph] Daily pipeline — {total_docs} new doc"
               f"{'s' if total_docs != 1 else ''} ({today})")

    email = ApprovalEmail(
        subject=subject, html=html, text=text,
        pdf_attachment=None, pdf_filename=None,
    )

    recipient = args.recipient or config.APPROVAL_EMAIL_RECIPIENT
    print(f"Daily digest send")
    print(f"  mode:      {'mock' if config.is_mock() else 'live'}")
    print(f"  to:        {recipient}")
    print(f"  subject:   {subject}")
    print(f"  runs:      {len(runs_data)}")
    print(f"  new docs:  {total_docs}")

    delivery_id = send_email(email, to=recipient)
    print(f"  delivery:  {delivery_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
