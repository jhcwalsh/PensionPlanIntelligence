"""Download pending meeting recordings to the local D: drive.

Phase 2 part B. Reads MeetingRecording rows where download_status='pending'
and uses yt-dlp to fetch each video to:

  D:\\PensionGraph\\meetingrecordings\\{plan_id}\\{YYYY-MM-DD}_{video_id}.{ext}

Updates the row's local_path / file_size_bytes / content_hash /
download_status / last_download_attempt_at / download_attempts. Skips
sources whose platform is not yet supported by yt-dlp out of the box
(currently anything other than youtube / vimeo).

Long-running by design: a typical board meeting recording is 1-3 GB
and takes 5-15 minutes to download. Run from Task Scheduler overnight,
not in foreground.

Usage:
  python download_recordings.py                    # all pending recordings
  python download_recordings.py calpers calstrs    # restrict to plan ids
  python download_recordings.py --limit 5          # cap downloads per run
  python download_recordings.py --dry-run          # show plan only
  python download_recordings.py --skip-livestreams # default off — livestreams have no archive yet
"""
from __future__ import annotations

import argparse
import hashlib
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from database import MeetingRecording, SessionLocal, init_db
from video_storage import RECORDINGS_DIR, plan_dir, recording_path


# High-confidence meeting-title keywords. A title that matches at least one
# of these is treated as a real board / committee meeting recording rather
# than a member-facing promo or educational video. Used by --latest-per-plan
# to skip plans whose newest content is "Quick Tip: Plan Benefits" instead
# of an actual meeting.
_MEETING_TITLE_RE = re.compile(
    r"(?:"
    r"board\s+meeting|"
    r"board\s+of\s+(?:administration|trustees|directors|managers|education)|"
    r"investment\s+committee|"
    r"audit\s+committee|"
    r"finance\s+committee|"
    r"executive\s+committee|"
    r"governance\s+committee|"
    r"trustees?\s+meeting|"
    r"retirement\s+board|"
    r"pension\s+board|"
    r"regular\s+meeting|"
    r"special\s+meeting|"
    r"quarterly\s+meeting|"
    r"annual\s+meeting|"
    r"committee\s+meeting|"
    r"public\s+meeting"
    r")",
    re.IGNORECASE,
)


def is_meeting_title(title: str | None) -> bool:
    if not title:
        return False
    return bool(_MEETING_TITLE_RE.search(title))


# Platforms yt-dlp can fetch directly. Others (granicus, swagit, cablecast,
# civicplus, boxcast, website) need custom Phase-3 scrapers.
DOWNLOADABLE_PLATFORMS = ("youtube", "vimeo")

# Default download retry budget. yt-dlp does its own internal retries; we
# count whole-row attempts so a row that consistently fails is shelved.
MAX_ATTEMPTS = 3


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _expected_path(rec: MeetingRecording, ext: str = "mp4") -> Path:
    return recording_path(
        rec.plan_id, rec.video_id,
        meeting_date=rec.meeting_date_inferred,
        published_at=rec.published_at,
        ext=ext,
    )


def download_one(rec: MeetingRecording, *, dry_run: bool) -> dict:
    """Download a single recording. Returns a result dict."""
    result = {"status": "skipped", "path": None, "size": None,
              "hash": None, "error": None}

    if rec.platform not in DOWNLOADABLE_PLATFORMS:
        result["error"] = f"platform '{rec.platform}' not supported by yt-dlp downloader"
        return result

    if rec.is_livestream and not rec.published_at:
        result["error"] = "scheduled livestream — no archive available yet"
        return result

    target_dir = plan_dir(rec.plan_id)
    expected = _expected_path(rec)

    if dry_run:
        result["status"] = "dry_run"
        result["path"] = str(expected)
        return result

    # yt-dlp's outtmpl puts the actual extension on the file. We strip the
    # `.mp4` from `expected` and let yt-dlp pick the real container.
    target_dir.mkdir(parents=True, exist_ok=True)
    out_template = str(expected.with_suffix(".%(ext)s"))

    import yt_dlp

    ydl_opts = {
        "outtmpl": out_template,
        # Best mp4-compatible single file when possible — avoids needing
        # ffmpeg for muxing on the user's box. Falls back to best.
        "format": "best[ext=mp4]/best",
        "quiet": True,
        "noprogress": True,
        "noplaylist": True,
        "retries": 5,
        "fragment_retries": 5,
        "concurrent_fragment_downloads": 4,
        "merge_output_format": None,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(rec.video_url, download=True)
    except Exception as exc:  # noqa: BLE001
        result["status"] = "failed"
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result

    # yt-dlp returns the resolved filename via 'requested_downloads' or
    # by computing it through the template. Walk the plan directory for
    # any file whose stem matches our expected stem (the extension may be
    # mp4, webm, m4a, etc. depending on what was available).
    actual: Path | None = None
    if info and info.get("requested_downloads"):
        candidate = info["requested_downloads"][0].get("filepath")
        if candidate and Path(candidate).exists():
            actual = Path(candidate)
    if actual is None:
        for f in target_dir.iterdir():
            if f.is_file() and f.stem == expected.stem:
                actual = f
                break

    if actual is None or not actual.exists():
        result["status"] = "failed"
        result["error"] = "yt-dlp returned without writing a file"
        return result

    result["status"] = "done"
    result["path"] = str(actual)
    result["size"] = actual.stat().st_size
    result["hash"] = _sha256_file(actual)
    if info:
        result["info"] = {
            "duration": info.get("duration"),
            "title": info.get("title"),
            "upload_date": info.get("upload_date"),  # YYYYMMDD string
            "release_timestamp": info.get("release_timestamp"),
        }
    return result


def _select_latest_per_plan(session, plan_ids, *,
                            meetings_only: bool = True) -> list[MeetingRecording]:
    """One MeetingRecording per plan — the newest one that's a real meeting.

    "Newest": by meeting_date_inferred if set, else published_at, else
    discovered_at.

    With meetings_only=True (default) only rows whose title matches the
    meeting-title heuristic are considered. Plans whose newest content is
    a promotional / educational video — and that have no clearly-tagged
    meeting recording in the catalogue — are skipped entirely rather than
    yielding a junk download. Pass meetings_only=False to fall back to
    the unfiltered "latest video by date" behaviour.
    """
    q = (
        session.query(MeetingRecording)
        .filter(MeetingRecording.download_status.in_(("pending", "failed", "done")))
        .filter(MeetingRecording.platform.in_(DOWNLOADABLE_PLATFORMS))
    )
    if plan_ids:
        q = q.filter(MeetingRecording.plan_id.in_(list(plan_ids)))
    rows = q.all()

    if meetings_only:
        rows = [r for r in rows if is_meeting_title(r.title)]

    by_plan: dict[str, MeetingRecording] = {}
    for r in rows:
        key_date = r.meeting_date_inferred or r.published_at or r.discovered_at
        existing = by_plan.get(r.plan_id)
        if existing is None:
            by_plan[r.plan_id] = r
            continue
        ex_date = (existing.meeting_date_inferred or existing.published_at
                   or existing.discovered_at)
        if key_date and (ex_date is None or key_date > ex_date):
            by_plan[r.plan_id] = r

    # Filter out rows that are already done — only return those still
    # needing a download.
    return [r for r in by_plan.values()
            if r.download_status in ("pending", "failed")
            and (r.download_attempts or 0) < MAX_ATTEMPTS]


def run(plan_ids: Iterable[str] | None, *, limit: int | None,
        dry_run: bool, include_livestreams: bool,
        latest_per_plan: bool, meetings_only: bool = True) -> None:
    init_db()
    session = SessionLocal()
    try:
        if latest_per_plan:
            rows = _select_latest_per_plan(session, plan_ids,
                                           meetings_only=meetings_only)
            # Sort newest-first for display.
            rows.sort(
                key=lambda r: (r.meeting_date_inferred or r.published_at
                               or r.discovered_at),
                reverse=True,
            )
            if limit:
                rows = rows[:limit]
        else:
            q = (
                session.query(MeetingRecording)
                .filter(MeetingRecording.download_status.in_(("pending", "failed")))
                .filter(MeetingRecording.platform.in_(DOWNLOADABLE_PLATFORMS))
                .filter(MeetingRecording.download_attempts < MAX_ATTEMPTS)
            )
            if not include_livestreams:
                q = q.filter(MeetingRecording.is_livestream.is_(False)
                             | MeetingRecording.published_at.isnot(None))
            if plan_ids:
                q = q.filter(MeetingRecording.plan_id.in_(list(plan_ids)))
            # Newest first — most likely to be what the user wants on hand.
            q = q.order_by(
                MeetingRecording.meeting_date_inferred.desc().nullslast(),
                MeetingRecording.published_at.desc().nullslast(),
                MeetingRecording.discovered_at.desc(),
            )
            if limit:
                q = q.limit(limit)
            rows = q.all()

        if not rows:
            print("Nothing to download.")
            return

        print(f"Downloading to: {RECORDINGS_DIR}")
        print(f"Pending downloads: {len(rows)}{' (dry run)' if dry_run else ''}\n")

        ok = 0
        failed = 0
        for rec in rows:
            label = (f"[{rec.plan_id}] {rec.platform} "
                     f"{(rec.title or rec.video_id)[:70]}")
            print(label)
            now = _utcnow()
            rec.last_download_attempt_at = now
            rec.download_attempts = (rec.download_attempts or 0) + 1
            rec.download_status = "downloading"
            session.commit()  # checkpoint state

            result = download_one(rec, dry_run=dry_run)

            if result["status"] == "dry_run":
                print(f"   would write: {result['path']}")
                rec.download_status = "pending"  # restore
                session.commit()
                continue

            if result["status"] == "done":
                rec.download_status = "done"
                rec.local_path = result["path"]
                rec.file_size_bytes = result["size"]
                rec.content_hash = result["hash"]
                rec.download_error = None
                if result.get("info", {}).get("duration") and not rec.duration_seconds:
                    rec.duration_seconds = int(result["info"]["duration"])
                ok += 1
                size_mb = (result["size"] or 0) / 1_048_576
                print(f"   OK ({size_mb:.1f} MB) -> {result['path']}")
            elif result["status"] == "skipped":
                rec.download_status = "skipped"
                rec.download_error = result["error"]
                print(f"   skip: {result['error']}")
            else:
                rec.download_status = "failed"
                rec.download_error = result["error"]
                failed += 1
                print(f"   FAIL: {result['error']}")

            rec.updated_at = _utcnow()
            session.commit()

        print(f"\n--- summary ---")
        print(f"downloaded: {ok}")
        print(f"failed:     {failed}")
        print(f"total considered: {len(rows)}")
    finally:
        session.close()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("plan_ids", nargs="*",
                   help="Restrict to specific plan ids (default: all pending)")
    p.add_argument("--limit", type=int,
                   help="Cap number of downloads per run (default: no cap)")
    p.add_argument("--dry-run", action="store_true",
                   help="Print expected paths without fetching anything")
    p.add_argument("--include-livestreams", action="store_true",
                   help="Also consider scheduled-livestream rows (off by default — "
                        "they have no archive until the stream ends)")
    p.add_argument("--latest-per-plan", action="store_true",
                   help="Pick exactly the newest pending recording per plan and download "
                        "those. Combined with --limit caps the breadth of the run.")
    p.add_argument("--no-meetings-filter", action="store_true",
                   help="With --latest-per-plan, also consider non-meeting videos. "
                        "Default is to skip plans whose newest video is a promo / "
                        "educational clip rather than a real board meeting.")
    args = p.parse_args()
    run(args.plan_ids or None, limit=args.limit, dry_run=args.dry_run,
        include_livestreams=args.include_livestreams,
        latest_per_plan=args.latest_per_plan,
        meetings_only=not args.no_meetings_filter)


if __name__ == "__main__":
    main()
