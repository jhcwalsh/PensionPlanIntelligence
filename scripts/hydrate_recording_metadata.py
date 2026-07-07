"""Hydrate MeetingRecording rows with full per-video yt-dlp metadata.

refresh_recordings.py uses flat-playlist extraction, which is one request
per channel but returns no timestamps and sometimes no duration. This
script does the expensive per-video pass for rows that are missing
published_at, title, or duration_seconds:

  - published_at        <- upload timestamp / upload_date
  - title               <- if missing
  - duration_seconds    <- if missing
  - meeting_date_inferred <- title parse first; falls back to the upload
    date (a board recording is published within days of the meeting, and
    the column is explicitly an inference)

Rows whose video is gone (deleted / private) get download_status='gone'
so they stop being candidates without losing the catalogue entry.

Undated rows are processed first, so a --limit run spends its budget on
the rows that matter most. Idempotent: hydrated rows fall out of the
candidate query. Network-heavy but free; ~1-2 s per video, so use
--limit for incremental runs (e.g. from the weekly task) and no limit
for a full backfill.

Usage:
  python -m scripts.hydrate_recording_metadata               # everything missing
  python -m scripts.hydrate_recording_metadata --limit 200   # incremental
  python -m scripts.hydrate_recording_metadata --plan calpers
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone

from sqlalchemy import or_

from database import MeetingRecording, SessionLocal, init_db
from refresh_recordings import parse_meeting_date_from_title

_GONE_MARKERS = ("Private video", "video unavailable", "This video has been removed",
                 "account associated with this video has been terminated")


def _hydrate_one(ydl, row) -> str:
    try:
        info = ydl.extract_info(row.video_url, download=False)
    except Exception as exc:  # noqa: BLE001
        msg = str(exc)
        if any(m.lower() in msg.lower() for m in _GONE_MARKERS):
            row.download_status = "gone"
            row.download_error = msg[:500]
            return "gone"
        return "error"

    ts = info.get("timestamp")
    if ts:
        row.published_at = datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None)
    elif info.get("upload_date"):
        row.published_at = datetime.strptime(info["upload_date"], "%Y%m%d")

    if not row.title and info.get("title"):
        row.title = info["title"]
    if not row.duration_seconds and info.get("duration"):
        row.duration_seconds = int(info["duration"])

    if row.meeting_date_inferred is None:
        row.meeting_date_inferred = (parse_meeting_date_from_title(row.title)
                                     or row.published_at)
    row.updated_at = datetime.utcnow()
    return "hydrated"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="hydrate_recording_metadata")
    parser.add_argument("--limit", type=int, help="Stop after N videos")
    parser.add_argument("--plan", help="Restrict to one plan_id")
    parser.add_argument("--delay", type=float, default=0.5,
                        help="Seconds between requests (default 0.5)")
    args = parser.parse_args(argv)

    import yt_dlp

    init_db()
    session = SessionLocal()
    q = (session.query(MeetingRecording)
         .filter(MeetingRecording.download_status != "gone")
         .filter(or_(MeetingRecording.published_at.is_(None),
                     MeetingRecording.title.is_(None),
                     MeetingRecording.duration_seconds.is_(None)))
         # Undated rows first — they're the cataloguing gap that matters.
         .order_by(MeetingRecording.meeting_date_inferred.isnot(None),
                   MeetingRecording.id))
    rows = q.all()
    if args.plan:
        rows = [r for r in rows if r.plan_id == args.plan]
    if args.limit:
        rows = rows[:args.limit]
    print(f"hydrating {len(rows)} recordings", flush=True)

    counts = {"hydrated": 0, "gone": 0, "error": 0}
    ydl = yt_dlp.YoutubeDL({"quiet": True, "no_warnings": True, "skip_download": True})
    try:
        for i, row in enumerate(rows, 1):
            outcome = _hydrate_one(ydl, row)
            counts[outcome] += 1
            session.commit()
            if i % 50 == 0:
                print(f"  {i}/{len(rows)} ({counts})", flush=True)
            time.sleep(args.delay)
    finally:
        session.close()

    print(f"done: {counts}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
