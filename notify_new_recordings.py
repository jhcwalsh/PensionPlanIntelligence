"""Send a digest email of newly-discovered meeting recordings.

Phase 2 part C. Reads MeetingRecording rows where alert_sent_at IS NULL,
composes a digest email listing them (one section per plan, newest
first), POSTs to Resend, and stamps alert_sent_at on every row included
so the next run only sees recordings discovered since.

Reuses the same Resend account / env vars as scripts/notify_failure.py:
  RESEND_API_KEY            (required to actually send)
  APPROVAL_EMAIL_RECIPIENT  (where the digest goes)
  APPROVAL_EMAIL_FROM       (verified sender, e.g. onboarding@resend.dev)

If either RESEND_API_KEY or APPROVAL_EMAIL_RECIPIENT is missing, the
script writes the digest body to stdout and exits 0 — same dev-friendly
behaviour as notify_failure.

Usage:
  python notify_new_recordings.py                # send digest, mark alerts sent
  python notify_new_recordings.py --dry-run      # print what would be sent
  python notify_new_recordings.py --min-count 1  # don't bother emailing for tiny digests (default 1)
"""
from __future__ import annotations

import argparse
import os
import socket
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

from database import MeetingRecording, Plan, SessionLocal, init_db


REPO_ROOT = Path(__file__).resolve().parent
load_dotenv(REPO_ROOT / ".env")

# Cap recordings per email so a one-time backfill doesn't produce a wall.
DEFAULT_DIGEST_CAP = 50


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _human_duration(seconds: int | None) -> str:
    if not seconds:
        return ""
    h, rem = divmod(int(seconds), 3600)
    m, _ = divmod(rem, 60)
    return f"{h}h{m:02d}m" if h else f"{m}m"


def _fetch_pending(session) -> list[tuple[MeetingRecording, Plan]]:
    rows = (
        session.query(MeetingRecording, Plan)
        .join(Plan, MeetingRecording.plan_id == Plan.id)
        .filter(MeetingRecording.alert_sent_at.is_(None))
        .order_by(
            MeetingRecording.meeting_date_inferred.desc().nullslast(),
            MeetingRecording.published_at.desc().nullslast(),
            MeetingRecording.discovered_at.desc(),
        )
        .all()
    )
    return rows


def _format_text(rows: list[tuple[MeetingRecording, Plan]]) -> str:
    lines = [f"{len(rows)} new pension-plan meeting recording(s) discovered.\n"]
    by_plan: dict[str, list] = {}
    for rec, plan in rows:
        key = plan.abbreviation or plan.id
        by_plan.setdefault(key, []).append((rec, plan))
    for plan_label in sorted(by_plan):
        recs = by_plan[plan_label]
        lines.append(f"\n{plan_label} ({len(recs)})")
        lines.append("-" * (len(plan_label) + 6))
        for rec, _ in recs:
            md = (rec.meeting_date_inferred or rec.published_at)
            md_s = md.strftime("%Y-%m-%d") if md else "no date"
            dur = _human_duration(rec.duration_seconds)
            title = (rec.title or rec.video_id)[:90]
            local = (f"   local: {rec.local_path}" if rec.local_path
                     else "   (not yet downloaded)")
            lines.append(f"  {md_s}  {dur:>5}  {title}")
            lines.append(f"   url:   {rec.video_url}")
            lines.append(local)
    return "\n".join(lines) + "\n"


def _format_html(rows: list[tuple[MeetingRecording, Plan]]) -> str:
    by_plan: dict[str, list] = {}
    for rec, plan in rows:
        key = plan.abbreviation or plan.id
        by_plan.setdefault(key, []).append((rec, plan))

    parts = [
        f"<p>{len(rows)} new pension-plan meeting recording(s) discovered.</p>",
    ]
    for plan_label in sorted(by_plan):
        recs = by_plan[plan_label]
        parts.append(
            f"<h3 style='margin:18px 0 4px 0;font-size:14px'>"
            f"{plan_label} <span style='color:#666;font-weight:400'>"
            f"({len(recs)})</span></h3>"
            f"<ul style='margin:0;padding-left:18px;font-size:13px'>"
        )
        for rec, _ in recs:
            md = (rec.meeting_date_inferred or rec.published_at)
            md_s = md.strftime("%Y-%m-%d") if md else "<em>no date</em>"
            dur = _human_duration(rec.duration_seconds)
            title = (rec.title or rec.video_id).replace("<", "&lt;")
            local = (f"<br><span style='color:#888'>local: <code>"
                     f"{rec.local_path}</code></span>"
                     if rec.local_path
                     else "<br><span style='color:#aaa'>not yet downloaded</span>")
            parts.append(
                f"<li style='margin-bottom:6px'>"
                f"<strong>{md_s}</strong> "
                f"<span style='color:#666'>{dur}</span> — "
                f"<a href='{rec.video_url}'>{title}</a>"
                f"{local}"
                f"</li>"
            )
        parts.append("</ul>")
    return "".join(parts)


def send_digest(rows: list[tuple[MeetingRecording, Plan]]) -> tuple[bool, str]:
    """POST the digest to Resend. Returns (ok, message)."""
    recipient = os.environ.get("APPROVAL_EMAIL_RECIPIENT", "")
    sender = os.environ.get("APPROVAL_EMAIL_FROM", "onboarding@resend.dev")
    api_key = os.environ.get("RESEND_API_KEY", "")
    if not (recipient and api_key):
        return False, "Resend not configured (RESEND_API_KEY / APPROVAL_EMAIL_RECIPIENT missing)"

    subject = f"[Recordings] {len(rows)} new pension meeting recording(s)"
    text_body = _format_text(rows)
    html_body = _format_html(rows)

    try:
        resp = requests.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "from": sender,
                "to": [recipient],
                "subject": subject,
                "text": text_body,
                "html": html_body,
            },
            timeout=15,
        )
    except Exception as exc:  # noqa: BLE001
        return False, f"Resend request raised: {exc}"

    if resp.status_code >= 400:
        return False, f"Resend returned {resp.status_code}: {resp.text[:200]}"
    delivery_id = resp.json().get("id", "?")
    return True, f"sent ({delivery_id}) to {recipient}"


def run(*, dry_run: bool, min_count: int, cap: int,
        baseline: bool = False) -> int:
    init_db()
    session = SessionLocal()
    try:
        rows = _fetch_pending(session)
        if not rows:
            print("No pending recording alerts.")
            return 0

        # Baseline mode: mark every currently-pending row as already-alerted
        # without sending email. Run once after Phase-1 discovery so the
        # first real digest doesn't dump the entire historical catalogue.
        if baseline:
            now = _utcnow()
            print(f"Baseline mode: marking {len(rows)} existing pending row(s) "
                  f"as already-alerted (no email sent).")
            if dry_run:
                print("(dry run — no changes committed)")
                return 0
            for rec, _ in rows:
                rec.alert_sent_at = now
            session.commit()
            return 0
        if len(rows) < min_count:
            print(f"Only {len(rows)} pending alert(s) — below min-count={min_count}; skipping.")
            return 0
        if len(rows) > cap:
            print(f"Capping digest at {cap} of {len(rows)} pending rows; "
                  f"the rest will be in the next digest.")
            rows = rows[:cap]

        if dry_run:
            print(_format_text(rows))
            print(f"(dry run — would email {len(rows)} recording(s))")
            return 0

        ok, msg = send_digest(rows)
        print(f"[notify_new_recordings] {msg}")
        if not ok:
            return 1

        now = _utcnow()
        for rec, _ in rows:
            rec.alert_sent_at = now
        session.commit()
        return 0
    finally:
        session.close()


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dry-run", action="store_true",
                   help="Print the digest body without sending or marking rows")
    p.add_argument("--min-count", type=int, default=1,
                   help="Minimum pending alerts before sending (default 1)")
    p.add_argument("--cap", type=int, default=DEFAULT_DIGEST_CAP,
                   help=f"Max recordings per digest (default {DEFAULT_DIGEST_CAP})")
    p.add_argument("--baseline", action="store_true",
                   help="One-time: mark every currently-pending row as already-alerted "
                        "without sending an email. Run once after Phase-1 discovery so "
                        "the first real digest doesn't dump the entire historical catalogue.")
    args = p.parse_args()
    return run(dry_run=args.dry_run, min_count=args.min_count, cap=args.cap,
               baseline=args.baseline)


if __name__ == "__main__":
    raise SystemExit(main())
