"""Poll each active plan_video_sources row for new meeting recordings.

Phase 2 of the meeting-video subsystem. Reads every PlanVideoSource where
status='active', uses yt-dlp's flat-playlist extractor to list videos on
the channel/playlist/archive page (metadata only — no download), and
inserts a MeetingRecording row for any video_id we don't already have.

Output: NEW recordings have download_status='pending' and
alert_sent_at=None, which makes them visible to:
  - download_recordings.py (Phase 2 part B)
  - notify_new_recordings.py (Phase 2 part C)

This is the trigger for "alert me when something new appears" — discovery
of a new MeetingRecording row IS the alertable event. Downloads happen
afterward; alerts can fire either at discovery or at download-complete
depending on user preference (see notify_new_recordings.py).

Usage:
  python refresh_recordings.py                    # all active sources
  python refresh_recordings.py calpers calstrs    # restrict to plan ids
  python refresh_recordings.py --max-per-source 50  # how many videos to list per source (default 25)

Notes:
  - Granicus / Swagit / Cablecast / Boxcast / CivicPlus archives are
    HTML viewers, not yt-dlp-native extractors. We skip them in this
    pass; Phase-3 will add custom scrapers per platform.
  - Self-hosted "website" sources (e.g. CalPERS webcast widget) likewise
    need a custom scraper and are skipped here.
  - Vimeo channels and YouTube channels/playlists are fully supported.
"""
from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime, timezone
from typing import Iterable

from database import (
    MeetingRecording,
    Plan,
    PlanVideoSource,
    SessionLocal,
    VideoRefreshLog,
    init_db,
)
from recording_scrapers import PLATFORM_SCRAPERS


# Platforms yt-dlp can list natively via flat-playlist extraction.
YTDLP_PLATFORMS = ("youtube", "vimeo")
# Total set of platforms the refresh can poll, including custom scrapers.
SUPPORTED_PLATFORMS = YTDLP_PLATFORMS + tuple(PLATFORM_SCRAPERS.keys())

# YouTube channel ids are 24 chars starting with UC; legitimate video ids
# are exactly 11 chars. Use this to filter out yt-dlp's occasional
# "channel-as-entry" hallucination at the top of a flat-playlist listing.
_YT_CHANNEL_ID_RE = re.compile(r"^UC[0-9A-Za-z_-]{22}$")

# Title-based meeting-date extraction. Pension plans tend to embed the
# meeting date verbatim in the video title — much more reliable than
# trying to coax timestamps out of flat-playlist mode (which often
# returns null for them on YouTube).
_MONTHS = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}
_DATE_PATTERNS = [
    # "April 14, 2026" or "Apr 14 2026"
    re.compile(
        r"\b(?P<m>jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec"
        r"|january|february|march|april|may|june|july|august|september|"
        r"october|november|december)\.?\s+"
        r"(?P<d>\d{1,2})(?:st|nd|rd|th)?,?\s+(?P<y>\d{4})\b",
        re.IGNORECASE,
    ),
    # "14 April 2026" (less common but seen)
    re.compile(
        r"\b(?P<d>\d{1,2})\s+"
        r"(?P<m>jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec"
        r"|january|february|march|april|may|june|july|august|september|"
        r"october|november|december)\.?\s+(?P<y>\d{4})\b",
        re.IGNORECASE,
    ),
    # "2026-04-14" / "2026/04/14"
    re.compile(r"\b(?P<y>\d{4})[-/](?P<mo>\d{1,2})[-/](?P<d>\d{1,2})\b"),
    # "04/14/2026" / "4-14-2026" — assume US ordering for these plans.
    re.compile(r"\b(?P<mo>\d{1,2})[/-](?P<d>\d{1,2})[/-](?P<y>\d{4})\b"),
    # "04/14/26" — short year (US ordering, assume 2000s)
    re.compile(r"\b(?P<mo>\d{1,2})[/-](?P<d>\d{1,2})[/-](?P<y>\d{2})\b"),
]


def parse_meeting_date_from_title(title: str | None) -> datetime | None:
    """Best-effort extraction of a meeting date from a video title.

    Returns naive UTC datetime at 00:00 on the parsed day, or None.
    Tries patterns in priority order; first match wins.
    """
    if not title:
        return None
    for i, pat in enumerate(_DATE_PATTERNS):
        m = pat.search(title)
        if not m:
            continue
        try:
            if "m" in m.groupdict() and m.group("m"):
                month = _MONTHS[m.group("m").lower().rstrip(".")]
            else:
                month = int(m.group("mo"))
            day = int(m.group("d"))
            year = int(m.group("y"))
            if year < 100:  # short-year pattern
                year += 2000
            if not (1 <= month <= 12 and 1 <= day <= 31 and 2000 <= year <= 2100):
                continue
            return datetime(year, month, day)
        except (KeyError, ValueError):
            continue
    return None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _list_videos(source_url: str, max_videos: int) -> list[dict]:
    """Use yt-dlp to list videos on a channel / playlist URL.

    max_videos <= 0 means "no cap" — yt-dlp will return the channel's
    entire history. For YouTube channels with hundreds of videos this
    can take 30-60s per source, so reserve unlimited mode for full
    re-indexing rather than routine polling.
    """
    import yt_dlp

    ydl_opts = {
        "quiet": True,
        "skip_download": True,
        "extract_flat": "in_playlist",
        "noprogress": True,
        "ignoreerrors": True,
    }
    if max_videos and max_videos > 0:
        ydl_opts["playlistend"] = max_videos
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(source_url, download=False)

    if info is None:
        return []
    entries = info.get("entries") or []
    out: list[dict] = []
    for e in entries:
        if not e:
            continue
        vid = e.get("id")
        if not vid:
            continue
        vid = str(vid)
        # yt-dlp sometimes emits the channel itself as the first flat-playlist
        # entry on a YouTube channel URL — its id is the 24-char UC... channel
        # id rather than an 11-char video id. Drop these silently.
        if _YT_CHANNEL_ID_RE.match(vid):
            continue
        # Defensive: also drop any other YouTube ids that aren't 11 chars.
        if e.get("ie_key", "").lower().startswith("youtube") or "youtube" in (e.get("url") or ""):
            if len(vid) != 11:
                continue
        out.append({
            "id": vid,
            "title": e.get("title"),
            "url": e.get("url") or e.get("webpage_url"),
            "duration": e.get("duration"),
            "timestamp": e.get("timestamp") or e.get("release_timestamp"),
            "thumbnail": e.get("thumbnail"),
            "is_live": e.get("is_live") or e.get("live_status") in ("is_live", "is_upcoming"),
        })
    return out


def _normalise_video_url(platform: str, video_id: str, raw_url: str | None) -> str:
    """Return the canonical viewer URL for a recording.

    Custom scrapers already supply canonical URLs (e.g. Granicus
    MediaPlayer.php URLs); for those we trust raw_url. yt-dlp output
    on YouTube/Vimeo is normalised here.
    """
    if platform == "youtube":
        return f"https://www.youtube.com/watch?v={video_id}"
    if platform == "vimeo":
        return f"https://vimeo.com/{video_id}"
    return raw_url or video_id


def _ts_to_dt(ts) -> datetime | None:
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).replace(tzinfo=None)
    except (TypeError, ValueError, OSError):
        return None


def refresh_source(session, source: PlanVideoSource, *,
                   max_videos: int) -> dict:
    """Poll one source. Returns a result dict with status / counts / error."""
    result = {
        "status": "checked_no_new",
        "found": 0,
        "new": 0,
        "error": None,
    }
    if source.platform not in SUPPORTED_PLATFORMS:
        result["status"] = "no_source"
        result["error"] = f"platform '{source.platform}' not yet supported by refresh"
        return result

    try:
        if source.platform in YTDLP_PLATFORMS:
            entries = _list_videos(source.source_url, max_videos)
        else:
            scraper = PLATFORM_SCRAPERS[source.platform]
            entries = scraper(source.source_url)
            if max_videos and max_videos > 0:
                entries = entries[:max_videos]
    except Exception as exc:  # noqa: BLE001
        result["status"] = "fetch_failed"
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result

    result["found"] = len(entries)
    if not entries:
        return result

    now = _utcnow()
    newest_published: datetime | None = None
    for e in entries:
        video_id = e["id"]
        existing = (
            session.query(MeetingRecording)
            .filter_by(platform=source.platform, video_id=video_id)
            .first()
        )
        published = _ts_to_dt(e.get("timestamp"))
        if published and (newest_published is None or published > newest_published):
            newest_published = published
        canonical_url = _normalise_video_url(source.platform, video_id, e.get("url"))
        title = e.get("title")
        meeting_date = parse_meeting_date_from_title(title)
        if existing is None:
            row = MeetingRecording(
                plan_id=source.plan_id,
                video_source_id=source.id,
                platform=source.platform,
                video_id=video_id,
                video_url=canonical_url,
                title=title,
                duration_seconds=int(e["duration"]) if e.get("duration") else None,
                published_at=published,
                meeting_date_inferred=meeting_date,
                thumbnail_url=e.get("thumbnail"),
                is_livestream=bool(e.get("is_live")),
                download_status="pending",
                discovered_at=now,
                updated_at=now,
            )
            session.add(row)
            result["new"] += 1
        else:
            # Backfill missing fields and link to source if we discovered it
            # earlier without channel context.
            if existing.video_source_id is None:
                existing.video_source_id = source.id
            if existing.title is None and title:
                existing.title = title
            if existing.duration_seconds is None and e.get("duration"):
                existing.duration_seconds = int(e["duration"])
            if existing.published_at is None and published:
                existing.published_at = published
            if existing.meeting_date_inferred is None and meeting_date:
                existing.meeting_date_inferred = meeting_date
            if existing.thumbnail_url is None and e.get("thumbnail"):
                existing.thumbnail_url = e["thumbnail"]
            existing.updated_at = now

    source.last_checked_at = now
    if newest_published and (
        source.last_recording_seen_at is None
        or newest_published > source.last_recording_seen_at
    ):
        source.last_recording_seen_at = newest_published

    result["status"] = "new_recordings" if result["new"] > 0 else "checked_no_new"
    return result


def run(plan_ids: Iterable[str] | None, *, max_videos: int) -> None:
    init_db()
    session = SessionLocal()
    try:
        q = session.query(PlanVideoSource).filter(PlanVideoSource.status == "active")
        if plan_ids:
            q = q.filter(PlanVideoSource.plan_id.in_(list(plan_ids)))
        sources = q.order_by(PlanVideoSource.plan_id, PlanVideoSource.platform).all()

        if not sources:
            print("No active sources matched.")
            return

        plans_seen: set[str] = set()
        plans_with_new: set[str] = set()
        totals = {"checked": 0, "new": 0, "fetch_failed": 0, "skipped": 0}

        for s in sources:
            plans_seen.add(s.plan_id)
            print(f"[{s.plan_id}] {s.platform:8} {s.source_url}")
            result = refresh_source(session, s, max_videos=max_videos)

            log = VideoRefreshLog(
                plan_id=s.plan_id,
                video_source_id=s.id,
                run_at=_utcnow(),
                status=result["status"],
                recordings_found=result["found"],
                recordings_new=result["new"],
                url_tried=s.source_url,
                discovery_source="poll",
                notes=result["error"],
            )
            session.add(log)

            if result["status"] == "fetch_failed":
                totals["fetch_failed"] += 1
                print(f"   FAIL: {result['error']}")
            elif result["status"] == "no_source":
                totals["skipped"] += 1
                print(f"   skip: {result['error']}")
            else:
                totals["checked"] += 1
                if result["new"]:
                    plans_with_new.add(s.plan_id)
                    print(f"   NEW {result['new']} of {result['found']} videos")
                else:
                    print(f"   no new (saw {result['found']})")

            # Commit per source so a later failure doesn't lose earlier work.
            session.commit()

            totals["new"] += result["new"]

        print(f"\n--- summary ---")
        print(f"sources polled:   {len(sources)}")
        print(f"plans seen:       {len(plans_seen)}")
        print(f"plans with new:   {len(plans_with_new)}")
        print(f"new recordings:   {totals['new']}")
        print(f"fetch failures:   {totals['fetch_failed']}")
        print(f"skipped (other):  {totals['skipped']}")
    finally:
        session.close()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("plan_ids", nargs="*", help="Restrict to specific plan ids (default: all active)")
    p.add_argument("--max-per-source", type=int, default=25,
                   help="Max videos to list per source per poll (default 25; "
                        "newer videos appear first on YouTube/Vimeo)")
    args = p.parse_args()
    run(args.plan_ids or None, max_videos=args.max_per_source)


if __name__ == "__main__":
    main()
