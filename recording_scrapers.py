"""Platform-specific scrapers for non-yt-dlp video archives.

Granicus, Swagit, Cablecast, Boxcast, CivicPlus, and self-hosted plan
players each have a different HTML/API surface that yt-dlp doesn't
extract by default. This module supplies one function per platform
that takes a PlanVideoSource and returns a list of normalised
recording-metadata dicts:

    {
        "id": str,           # platform-native id, unique per source
        "title": str | None,
        "url": str,           # canonical viewer URL
        "duration": int | None,    # seconds
        "timestamp": int | None,   # unix epoch (best available)
        "is_live": bool,
    }

These dicts are shape-compatible with refresh_recordings._list_videos
output, so the orchestrator dispatches by platform and merges the
results into the same MeetingRecording rows.
"""
from __future__ import annotations

import re
from typing import Callable
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    ),
}


# ---------------------------------------------------------------------------
# Granicus
# ---------------------------------------------------------------------------
# Public archive page: https://{client}.granicus.com/ViewPublisher.php?view_id=N
# Canonical viewer:    https://{client}.granicus.com/MediaPlayer.php?view_id=N&clip_id=M
# Each archived row contains an anchor whose href matches /clip_id=\d+/.
# The Date cell is prefixed by a hidden Unix timestamp ("1773903600 Mar 19, 2026"),
# which we use as the authoritative meeting date. Duration cell is "Xh Ym".

_CLIP_ID_RE = re.compile(r"clip_id=(\d+)", re.IGNORECASE)
_VIEW_ID_RE = re.compile(r"view_id=(\d+)", re.IGNORECASE)
_DURATION_RE = re.compile(r"(\d+)\s*h\s*(\d+)\s*m", re.IGNORECASE)
_LEADING_TS_RE = re.compile(r"^(\d{9,11})\b")


def _resolve_url(base: str, href: str) -> str:
    if href.startswith("//"):
        return f"https:{href}"
    return urljoin(base, href)


def list_granicus(source_url: str) -> list[dict]:
    """Scrape a Granicus ViewPublisher page for archived clips.

    Returns [] gracefully on empty archives ("Currently there are no
    archived videos."), 404s, or unparseable layouts. Raises on hard
    network failures so the caller can log them per-source.
    """
    resp = requests.get(source_url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    parsed = urlparse(source_url)
    host = f"{parsed.scheme}://{parsed.netloc}"
    view_id_match = _VIEW_ID_RE.search(source_url)
    view_id = view_id_match.group(1) if view_id_match else "2"

    out: list[dict] = []
    seen: set[str] = set()

    for row in soup.find_all("tr"):
        clip_anchor = None
        for a in row.find_all("a", href=True):
            if "clip_id=" in a["href"]:
                clip_anchor = a
                break
        if clip_anchor is None:
            continue
        clip_match = _CLIP_ID_RE.search(clip_anchor["href"])
        if not clip_match:
            continue
        clip_id = clip_match.group(1)
        if clip_id in seen:
            continue
        seen.add(clip_id)

        cells = row.find_all(["td", "th"])
        # Granicus archive rows have at least: Name, Video, Date, Duration
        name = cells[0].get_text(" ", strip=True) if cells else None

        # Date cell is whichever cell starts with a Unix timestamp.
        timestamp = None
        date_human = None
        for c in cells:
            txt = c.get_text(" ", strip=True)
            m = _LEADING_TS_RE.match(txt)
            if m:
                try:
                    timestamp = int(m.group(1))
                    date_human = txt[m.end():].strip()
                except ValueError:
                    pass
                break

        # Duration cell: "03h 49m"
        duration = None
        for c in cells:
            txt = c.get_text(" ", strip=True)
            m = _DURATION_RE.search(txt)
            if m and not _LEADING_TS_RE.match(txt):
                hours = int(m.group(1))
                minutes = int(m.group(2))
                duration = hours * 3600 + minutes * 60
                break

        canonical = f"{host}/MediaPlayer.php?view_id={view_id}&clip_id={clip_id}"
        title = name or date_human or f"clip_{clip_id}"

        out.append({
            "id": clip_id,
            "title": title,
            "url": canonical,
            "duration": duration,
            "timestamp": timestamp,
            "thumbnail": None,
            "is_live": False,
        })
    return out


# ---------------------------------------------------------------------------
# Dispatch table
# ---------------------------------------------------------------------------

# platform -> scraper function. yt-dlp-native platforms (youtube, vimeo)
# stay in refresh_recordings._list_videos; this table covers the rest.
PLATFORM_SCRAPERS: dict[str, Callable[[str], list[dict]]] = {
    "granicus": list_granicus,
}
