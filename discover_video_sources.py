"""Discover meeting-video sources for each plan.

Phase 1 of the meeting-video subsystem. Two discovery layers:

  1. mine — scan documents.extracted_text for archive/channel URLs.
     Cheap, offline, but limited: most board PDFs don't embed video
     links. Mostly catches Vimeo/Granicus references in the few plans
     whose minutes name them, plus the occasional YouTube channel.

  2. site_crawl — fetch the plan's materials_url via fetcher.fetch_page()
     (Playwright-aware) and harvest <iframe>/<a> video links from the
     rendered HTML. This is where most plans' YouTube channels and
     Granicus viewers actually live.

Both layers normalise URLs to a channel/archive home (not single videos),
classify the platform, and upsert into plan_video_sources without any
LLM verification — that's a Phase-1.5 step. Idempotent on
(plan_id, platform, source_url): re-running collapses duplicates.

Usage:
  python discover_video_sources.py                          # all plans, mine only
  python discover_video_sources.py calpers --site-crawl     # one plan, both layers
  python discover_video_sources.py --site-crawl             # all plans, both layers (slow)
  python discover_video_sources.py --dry-run                # report only, don't write
"""
from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict
from datetime import datetime
from typing import Iterable
from urllib.parse import urlparse

from database import (
    Document,
    MeetingRecording,
    Plan,
    PlanVideoSource,
    SessionLocal,
    init_db,
)


URL_RE = re.compile(r"https?://[^\s)>\"'\]}]+", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Platform classification
# ---------------------------------------------------------------------------

# Each entry: (platform, host_pattern, normaliser). The normaliser collapses
# any single-video URL down to its channel / archive home so the directory
# row points at a useful "list all recordings" page rather than one clip.
# It returns (canonical_source_url, channel_id_or_None) or None to drop.

_YT_VIDEO_HOSTS = ("youtube.com", "youtu.be", "m.youtube.com", "www.youtube.com")
_YT_CHANNEL_RE = re.compile(r"youtube\.com/(channel/UC[0-9A-Za-z_-]{20,}|@[A-Za-z0-9_.\-]+|c/[A-Za-z0-9_.\-]+|user/[A-Za-z0-9_.\-]+)", re.IGNORECASE)
# Legacy vanity URLs: youtube.com/{Name} with no /c/ /user/ /channel/ prefix.
# Reserved-path alternatives are anchored to a full segment ([/?#] or end)
# so `c` doesn't accidentally exclude "CalPERS" by matching its leading "C".
_YT_VANITY_RE = re.compile(
    r"youtube\.com/(?!"
    r"(?:watch|embed|shorts|playlist|results|feed|channel|c|user|"
    r"about|account|t|hashtag|live)(?:[/?#]|$)"
    r")([A-Za-z0-9_.\-]{3,})(?:[/?#]|$)",
    re.IGNORECASE,
)
_YT_PLAYLIST_RE = re.compile(r"youtube\.com/playlist\?list=([A-Za-z0-9_\-]{10,})", re.IGNORECASE)
_YT_VIDEO_ID_RE = re.compile(r"(?:v=|youtu\.be/|/embed/|/shorts/)([A-Za-z0-9_-]{11})")

_VIMEO_USER_RE = re.compile(r"vimeo\.com/(channels/[A-Za-z0-9_\-]+|showcase/\d+|user\d+|[a-z][a-z0-9_\-]{2,})(?:[/?#]|$)", re.IGNORECASE)
# Vimeo path segments that look username-shaped but are actually site routes.
_VIMEO_RESERVED_SLUGS = {
    "event", "events", "watch", "explore", "categories", "channels",
    "showcase", "ondemand", "stock", "features", "pricing", "upgrade",
    "log_in", "join", "search", "settings", "help", "manage", "live",
    "about", "jobs", "blog",
    "video", "videos", "api", "embed", "player", "create", "home",
    "feed", "following", "library", "tags", "categories", "manage",
}

_GRANICUS_RE = re.compile(r"https?://([a-z0-9\-]+)\.granicus\.com(?:/[A-Za-z]+\.php)?", re.IGNORECASE)
_SWAGIT_RE = re.compile(r"https?://([a-z0-9\-]+)\.swagit\.com", re.IGNORECASE)
_CABLECAST_RE = re.compile(r"https?://([a-z0-9\-.]+\.cablecast\.tv)", re.IGNORECASE)
_BOXCAST_RE = re.compile(r"https?://(?:www\.)?boxcast\.(?:com|tv)/(channel|view|s)/([A-Za-z0-9_\-]+)", re.IGNORECASE)
_CIVICPLUS_RE = re.compile(r"https?://([a-z0-9\-.]+\.civicclerk\.com|[a-z0-9\-.]+\.civicplus\.com)", re.IGNORECASE)
_ZOOM_RE = re.compile(r"https?://([a-z0-9\-]+\.)?zoom\.us/(j|rec|webinar)/", re.IGNORECASE)
_FACEBOOK_RE = re.compile(r"https?://(?:www\.|m\.|web\.)?facebook\.com/([A-Za-z0-9.\-]+)/?(?:videos|live|posts)?", re.IGNORECASE)


def classify(url: str, *, allow_unresolved: bool = False) -> tuple[str, str, str, str | None] | None:
    """Return (platform, kind, canonical_url, identifier) for a video URL.

    `kind` is 'directory' (channel/playlist/archive viewer — belongs in
    plan_video_sources) or 'video' (single-recording URL — belongs in
    meeting_recordings). The identifier is platform-specific: a channel
    slug for directories, a video id for videos.

    Returns None if the URL is not a recognised video host, or if it's a
    single-video URL on a host where we couldn't recover anything useful
    and allow_unresolved is False. Mining (corpus scan) calls with
    allow_unresolved=False to suppress noise; site-crawl (plan's own
    meetings page) calls with allow_unresolved=True to also capture
    individual recordings.
    """
    try:
        host = (urlparse(url).hostname or "").lower()
    except ValueError:
        return None
    if not host:
        return None

    # YouTube
    if host in _YT_VIDEO_HOSTS:
        m = _YT_CHANNEL_RE.search(url)
        if m:
            slug = m.group(1)
            return ("youtube", "directory", f"https://www.youtube.com/{slug}", slug)
        m = _YT_PLAYLIST_RE.search(url)
        if m:
            list_id = m.group(1)
            return ("youtube", "directory",
                    f"https://www.youtube.com/playlist?list={list_id}",
                    f"playlist:{list_id}")
        m = _YT_VANITY_RE.search(url)
        if m:
            slug = m.group(1)
            return ("youtube", "directory", f"https://www.youtube.com/{slug}", slug)
        m = _YT_VIDEO_ID_RE.search(url)
        if m and allow_unresolved:
            return ("youtube", "video", url, m.group(1))
        return None

    if "vimeo.com" in host:
        m = _VIMEO_USER_RE.search(url)
        if m:
            slug = m.group(1)
            if not slug.isdigit() and slug.lower() not in _VIMEO_RESERVED_SLUGS:
                return ("vimeo", "directory", f"https://vimeo.com/{slug}", slug)
        # Bare numeric vimeo URL = single video.
        path = (urlparse(url).path or "").strip("/")
        if path.isdigit() and allow_unresolved:
            return ("vimeo", "video", url, path)
        return None

    if host.endswith("granicus.com"):
        m = _GRANICUS_RE.search(url)
        if m:
            client = m.group(1)
            return ("granicus", "directory",
                    f"https://{client}.granicus.com/ViewPublisher.php?view_id=2",
                    client)
        return None

    if host.endswith("swagit.com"):
        m = _SWAGIT_RE.search(url)
        if m:
            client = m.group(1)
            return ("swagit", "directory", f"https://{client}.swagit.com/", client)
        return None

    if host.endswith("cablecast.tv"):
        m = _CABLECAST_RE.search(url)
        if m:
            return ("cablecast", "directory", f"https://{m.group(1)}/", m.group(1))
        return None

    if "boxcast" in host:
        m = _BOXCAST_RE.search(url)
        if m:
            return ("boxcast", "directory",
                    f"https://www.boxcast.com/channel/{m.group(2)}", m.group(2))
        return None

    if host.endswith("civicclerk.com") or host.endswith("civicplus.com"):
        m = _CIVICPLUS_RE.search(url)
        if m:
            return ("civicplus", "directory", f"https://{m.group(1)}/", m.group(1))
        return None

    if "zoom.us" in host:
        path = (urlparse(url).path or "").lower()
        if not path.startswith("/rec/"):
            return None
        return ("zoom", "directory", f"https://{host}/rec/", host)

    if "facebook.com" in host:
        m = _FACEBOOK_RE.search(url)
        if m:
            page = m.group(1)
            if page.lower() in ("videos", "live", "watch", "posts"):
                return None
            return ("facebook", "directory", f"https://www.facebook.com/{page}/", page)
        return None

    return None


# ---------------------------------------------------------------------------
# Mining
# ---------------------------------------------------------------------------

def mine_plan(session, plan_id: str) -> dict[tuple[str, str], dict]:
    """Mine extracted_text across one plan's documents for directory URLs.

    Mining is conservative — only directory-shaped URLs (channels, playlists,
    archive viewers) survive. Individual watch URLs in PDF text are too
    noisy / often stale to record as recordings without a verification
    pass, so they're dropped here (allow_unresolved=False).

    Returns a dict keyed by (platform, source_url).
    """
    docs = (
        session.query(Document)
        .filter(Document.plan_id == plan_id)
        .filter(Document.extracted_text.isnot(None))
        .all()
    )
    found: dict[tuple[str, str], dict] = {}
    for doc in docs:
        text = doc.extracted_text or ""
        seen_in_doc: set[str] = set()
        for m in URL_RE.finditer(text):
            raw = m.group(0).rstrip(".,;:)>]}'\"")
            classification = classify(raw, allow_unresolved=False)
            if classification is None:
                continue
            platform, kind, canonical, channel_id = classification
            if kind != "directory":
                continue
            key = (platform, canonical)
            if key in seen_in_doc:
                continue
            seen_in_doc.add(key)
            entry = found.setdefault(key, {
                "platform": platform,
                "source_url": canonical,
                "channel_id": channel_id,
                "document_count": 0,
                "sample_url": raw,
            })
            entry["document_count"] += 1
    return found


# ---------------------------------------------------------------------------
# Site-crawl: fetch the plan's materials page and harvest video links
# ---------------------------------------------------------------------------

# Regex to find platform URLs anywhere in raw HTML — catches links that
# don't appear as <a href> or <iframe src> (e.g. inline JS player config).
_HTML_VIDEO_URL_RE = re.compile(
    r"https?://[^\s\"'<>]+?(?:"
    r"youtube\.com|youtu\.be|vimeo\.com|granicus\.com|swagit\.com|"
    r"cablecast\.tv|boxcast\.com|civicclerk\.com|civicplus\.com|"
    r"zoom\.us|facebook\.com"
    r")[^\s\"'<>]*",
    re.IGNORECASE,
)

_LIVE_HINT_RE = re.compile(r"(live\s*stream|watch\s*live|broadcast\s*live|live\s*broadcast)", re.IGNORECASE)
_ARCHIVE_HINT_RE = re.compile(r"(archive|recording|past\s*meeting|video\s*library|on[-\s]?demand|replay)", re.IGNORECASE)

# A self-hosted player on the plan's own domain — match iframe sources whose
# host is the plan website AND whose path/query suggests streaming.
_SELFHOST_PATH_RE = re.compile(
    r"(webcast|livestream|live[-_]?stream|/live[/?]|/stream[/?]|video[-_]?player|broadcast)",
    re.IGNORECASE,
)


def _plan_root_host(plan: dict) -> str | None:
    site = plan.get("website") or plan.get("materials_url")
    if not site:
        return None
    try:
        host = urlparse(site).hostname or ""
    except ValueError:
        return None
    return host.lower().lstrip("www.") or None


# Seed paths to probe under plan.website when the materials_url page yielded
# nothing useful. Pension plans tend to bury the meeting-video link under
# one of these — same pattern fetch_ips.py uses for IPS discovery.
DEEP_SEED_PATHS = [
    "/board-meetings",
    "/board",
    "/meetings",
    "/videos",
    "/watch",
    "/live",
    "/webcasts",
    "/webcast",
    "/archive",
    "/media",
    "/about/board",
    "/about/board-meetings",
    "/governance/board",
    "/governance/meetings",
    "/news/videos",
    "/resources/videos",
]


def deep_crawl_plan(plan: dict, *, max_pages: int = 8
                    ) -> tuple[dict[tuple[str, str], dict],
                               dict[tuple[str, str], dict]]:
    """Probe seed paths under plan.website and harvest video links from each.

    Used as a third discovery layer for plans whose materials_url didn't
    yield anything via crawl_plan. We're explicit about the paths to
    keep the network burst bounded — most pension plans use one of the
    DEEP_SEED_PATHS conventions, so 8 page loads cover the majority.

    Returns (sources, recordings) in the same shape as crawl_plan.
    """
    from fetcher import fetch_page_requests, fetch_page_playwright

    website = plan.get("website")
    if not website:
        return {}, {}

    sources: dict[tuple[str, str], dict] = {}
    recordings: dict[tuple[str, str], dict] = {}

    base = website.rstrip("/")
    visited: set[str] = set()
    pages_fetched = 0

    for seed in DEEP_SEED_PATHS:
        if pages_fetched >= max_pages:
            break
        url = base + seed
        if url in visited:
            continue
        visited.add(url)
        soup = fetch_page_requests(url)
        if soup is None:
            # Some plan sites need JS; try Playwright. This is slow so
            # only fall back when requests returned None (4xx/5xx/timeout).
            soup = fetch_page_playwright(url)
        if soup is None:
            continue
        pages_fetched += 1
        page_sources, page_recordings = _harvest_html(plan, soup)
        for k, v in page_sources.items():
            sources.setdefault(k, v)
        for k, v in page_recordings.items():
            recordings.setdefault(k, v)
    return sources, recordings


def _harvest_html(plan: dict, soup) -> tuple[dict[tuple[str, str], dict],
                                              dict[tuple[str, str], dict]]:
    """Shared HTML-harvesting body used by crawl_plan / deep_crawl_plan."""
    plan_host = _plan_root_host(plan)
    sources: dict[tuple[str, str], dict] = {}
    recordings: dict[tuple[str, str], dict] = {}
    candidates: list[tuple[str, str]] = []

    for iframe in soup.find_all("iframe", src=True):
        src = iframe["src"].strip()
        candidates.append((src, ""))
        if plan_host:
            try:
                host = (urlparse(src).hostname or "").lower().lstrip("www.")
            except ValueError:
                host = ""
            if host and host == plan_host and _SELFHOST_PATH_RE.search(src):
                key = ("website", src)
                sources.setdefault(key, {
                    "platform": "website",
                    "source_url": src,
                    "channel_id": None,
                    "sample_url": src,
                    "live_hint": True,
                    "archive_hint": True,
                    "context_samples": ["self-hosted player iframe on plan domain"],
                })
    for a in soup.find_all("a", href=True):
        candidates.append((a["href"].strip(), a.get_text(" ", strip=True)))
    for source in soup.find_all("source", src=True):
        candidates.append((source["src"].strip(), ""))

    raw_html = str(soup)
    for m in _HTML_VIDEO_URL_RE.finditer(raw_html):
        candidates.append((m.group(0).rstrip(".,;:)>]}'\""), ""))

    seen_urls: set[str] = set()
    for url, ctx in candidates:
        if not url or not url.lower().startswith("http"):
            continue
        if url in seen_urls:
            continue
        seen_urls.add(url)
        result = classify(url, allow_unresolved=True)
        if result is None:
            continue
        platform, kind, canonical, identifier = result
        live_hint = bool(_LIVE_HINT_RE.search(ctx))
        archive_hint = bool(_ARCHIVE_HINT_RE.search(ctx))
        if kind == "directory":
            key = (platform, canonical)
            entry = sources.setdefault(key, {
                "platform": platform,
                "source_url": canonical,
                "channel_id": identifier,
                "sample_url": url,
                "live_hint": False,
                "archive_hint": False,
                "context_samples": [],
            })
            entry["live_hint"] = entry["live_hint"] or live_hint
            entry["archive_hint"] = entry["archive_hint"] or archive_hint
            if ctx and len(entry["context_samples"]) < 3:
                entry["context_samples"].append(ctx[:120])
        else:
            key = (platform, identifier)
            recordings.setdefault(key, {
                "platform": platform,
                "video_id": identifier,
                "video_url": canonical,
                "title": ctx[:200] if ctx else None,
                "is_live_hint": live_hint,
            })
    return sources, recordings


def crawl_plan(plan: dict) -> tuple[dict[tuple[str, str], dict],
                                     dict[tuple[str, str], dict]]:
    """Fetch the plan's materials_url and harvest the page.

    Returns (sources, recordings) where each is keyed by a stable tuple:
      sources    — {(platform, canonical_url): metadata for plan_video_sources}
      recordings — {(platform, video_id): metadata for meeting_recordings}
    """
    from fetcher import fetch_page

    soup = fetch_page(plan)
    if soup is None:
        return {}, {}
    return _harvest_html(plan, soup)


def upsert_source(session, plan_id: str, found: dict, *,
                  method: str) -> tuple[bool, PlanVideoSource]:
    """Upsert one discovered source into plan_video_sources.

    method: 'mined' | 'site_crawl' — both share the schema row but produce
    slightly different note/recording_policy hints.

    Returns (created?, row).
    """
    existing = (
        session.query(PlanVideoSource)
        .filter_by(
            plan_id=plan_id,
            platform=found["platform"],
            source_url=found["source_url"],
        )
        .first()
    )
    if method == "mined":
        note = (f"discovered via mine_existing_documents; "
                f"appeared in {found['document_count']} extracted document(s); "
                f"sample={found['sample_url']}")
        live_streamed = None
        recording_policy = None
    elif method == "deep_crawl":
        ctx = " | ".join(found.get("context_samples") or [])
        note = (f"discovered via deep_crawl of plan.website seed paths; "
                f"sample={found['sample_url']}"
                + (f"; context={ctx}" if ctx else ""))
        live_streamed = True if found.get("live_hint") else None
        recording_policy = "always" if found.get("archive_hint") else None
    else:  # site_crawl
        ctx = " | ".join(found.get("context_samples") or [])
        note = (f"discovered via site_crawl of plan.materials_url; "
                f"sample={found['sample_url']}"
                + (f"; context={ctx}" if ctx else ""))
        live_streamed = True if found.get("live_hint") else None
        recording_policy = "always" if found.get("archive_hint") else None

    now = datetime.utcnow()
    if existing is None:
        row = PlanVideoSource(
            plan_id=plan_id,
            platform=found["platform"],
            source_url=found["source_url"],
            channel_id=found.get("channel_id"),
            live_streamed=live_streamed,
            recording_policy=recording_policy,
            discovery_method=method,
            status="active",
            notes=note,
            created_at=now,
            updated_at=now,
        )
        session.add(row)
        return True, row

    # Existing row: enrich without clobbering manual edits.
    if not existing.channel_id and found.get("channel_id"):
        existing.channel_id = found["channel_id"]
    if existing.live_streamed is None and live_streamed is not None:
        existing.live_streamed = live_streamed
    if existing.recording_policy is None and recording_policy is not None:
        existing.recording_policy = recording_policy
    # Only refresh notes for auto-discovered rows; never overwrite manual notes.
    auto_methods = ("mined", "site_crawl", "deep_crawl")
    if existing.discovery_method in auto_methods:
        existing.notes = note
        # Method confidence ranking: site_crawl > deep_crawl > mined.
        # Upgrade an existing weaker tag to the strongest source we've seen.
        rank = {"mined": 0, "deep_crawl": 1, "site_crawl": 2}
        if rank.get(method, 0) > rank.get(existing.discovery_method, 0):
            existing.discovery_method = method
    existing.updated_at = now
    return False, existing


def upsert_recording(session, plan_id: str, found: dict) -> tuple[bool, MeetingRecording]:
    """Upsert one discovered recording into meeting_recordings.

    Idempotent on (platform, video_id). video_source_id stays null until
    a Phase-2 enrichment can resolve a watch URL to its parent channel.
    """
    existing = (
        session.query(MeetingRecording)
        .filter_by(platform=found["platform"], video_id=found["video_id"])
        .first()
    )
    now = datetime.utcnow()
    if existing is None:
        row = MeetingRecording(
            plan_id=plan_id,
            platform=found["platform"],
            video_id=found["video_id"],
            video_url=found["video_url"],
            title=found.get("title"),
            is_livestream=bool(found.get("is_live_hint")),
            download_status="pending",
            discovered_at=now,
            updated_at=now,
        )
        session.add(row)
        return True, row
    # Refresh non-clobbering fields.
    if not existing.title and found.get("title"):
        existing.title = found["title"]
    existing.updated_at = now
    return False, existing


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _process_plan(session, plan_row, *, do_mine: bool, do_crawl: bool,
                  do_deep: bool, dry_run: bool, totals, per_platform) -> int:
    """Run the requested discovery layers for one plan.

    Returns the total candidate count across sources and recordings.
    """
    plan_dict = None
    if do_crawl or do_deep:
        # fetcher.fetch_page consumes a plan dict from known_plans.json,
        # which has materials_type / materials_url / playwright_wait_selector.
        # Look it up lazily so a missing entry doesn't break the mining pass.
        try:
            from fetcher import load_plans
            plans_json = {p["id"]: p for p in load_plans()}
            plan_dict = plans_json.get(plan_row.id)
        except Exception as e:
            print(f"  WARN: could not load known_plans.json for {plan_row.id}: {e}", file=sys.stderr)

    sources: list[tuple[str, dict]] = []   # (method, data)
    recordings: list[dict] = []

    if do_mine:
        mined = mine_plan(session, plan_row.id)
        for data in mined.values():
            sources.append(("mined", data))

    if do_crawl and plan_dict:
        try:
            crawled_sources, crawled_recordings = crawl_plan(plan_dict)
        except Exception as e:
            print(f"  ERROR: site_crawl failed for {plan_row.id}: {e}", file=sys.stderr)
            crawled_sources, crawled_recordings = {}, {}
        for data in crawled_sources.values():
            sources.append(("site_crawl", data))
        recordings.extend(crawled_recordings.values())

    if do_deep and plan_dict:
        try:
            deep_sources, deep_recordings = deep_crawl_plan(plan_dict)
        except Exception as e:
            print(f"  ERROR: deep_crawl failed for {plan_row.id}: {e}", file=sys.stderr)
            deep_sources, deep_recordings = {}, {}
        # Don't double-record sources we already saw via shallower layers.
        existing_keys = {(m, d["source_url"]) for m, d in sources}
        for data in deep_sources.values():
            if (data["platform"], data["source_url"]) in {
                (d["platform"], d["source_url"]) for _, d in sources
            }:
                continue
            sources.append(("deep_crawl", data))
        # Merge deep recordings (de-dupe by (platform, video_id))
        existing_rec_keys = {(r["platform"], r["video_id"]) for r in recordings}
        for data in deep_recordings.values():
            if (data["platform"], data["video_id"]) in existing_rec_keys:
                continue
            recordings.append(data)

    if not sources and not recordings:
        return 0

    print(f"\n[{plan_row.id}] {plan_row.name} — "
          f"{len(sources)} source(s), {len(recordings)} recording(s):")

    for method, data in sources:
        per_platform[data["platform"]] += 1
        line_extra = (f"docs={data.get('document_count', '-')}"
                      if method == "mined"
                      else f"live={data.get('live_hint')} archive={data.get('archive_hint')}")
        if dry_run:
            print(f"  - [{method:11}] {data['platform']:10} {data['source_url']}  ({line_extra})")
            continue
        created, _ = upsert_source(session, plan_row.id, data, method=method)
        tag = "NEW" if created else "upd"
        totals["new_sources" if created else "updated_sources"] += 1
        print(f"  {tag} [{method:11}] {data['platform']:10} {data['source_url']}  ({line_extra})")

    for data in recordings:
        if dry_run:
            print(f"  - [recording ] {data['platform']:10} {data['video_url']}")
            continue
        created, _ = upsert_recording(session, plan_row.id, data)
        tag = "NEW" if created else "upd"
        totals["new_recordings" if created else "updated_recordings"] += 1
        print(f"  {tag} [recording ] {data['platform']:10} {data['video_url']}")

    return len(sources) + len(recordings)


def run(plan_ids: Iterable[str] | None, *, do_mine: bool, do_crawl: bool,
        do_deep: bool, deep_only_for_gaps: bool, dry_run: bool) -> None:
    init_db()
    session = SessionLocal()
    try:
        if plan_ids:
            plans = session.query(Plan).filter(Plan.id.in_(list(plan_ids))).all()
            missing = set(plan_ids) - {p.id for p in plans}
            if missing:
                print(f"WARN: unknown plan ids: {sorted(missing)}", file=sys.stderr)
        else:
            plans = session.query(Plan).order_by(Plan.id).all()

        # Deep-only-for-gaps: skip plans that already have an active source row,
        # so a deep crawl doesn't re-hit network for plans we already covered.
        gap_plan_ids: set[str] | None = None
        if do_deep and deep_only_for_gaps:
            with_active = {
                pid for (pid,) in session.query(PlanVideoSource.plan_id)
                .filter(PlanVideoSource.status == "active")
                .distinct().all()
            }
            gap_plan_ids = {p.id for p in plans} - with_active
            print(f"deep crawl restricted to {len(gap_plan_ids)} gap plan(s)")

        totals = defaultdict(int)
        per_platform = defaultdict(int)
        plans_with_hits = 0

        for plan in plans:
            plan_do_deep = do_deep
            if do_deep and gap_plan_ids is not None and plan.id not in gap_plan_ids:
                plan_do_deep = False
            n = _process_plan(session, plan,
                              do_mine=do_mine, do_crawl=do_crawl,
                              do_deep=plan_do_deep,
                              dry_run=dry_run,
                              totals=totals, per_platform=per_platform)
            if n:
                plans_with_hits += 1
            # Commit per plan when crawling so a later failure doesn't lose
            # everything (mining is fast enough to commit at the end).
            if (do_crawl or do_deep) and not dry_run:
                session.commit()

        if not dry_run:
            session.commit()

        print(f"\n--- summary ---")
        print(f"layers run:             mine={do_mine} site_crawl={do_crawl} deep={do_deep}")
        print(f"plans scanned:          {len(plans)}")
        print(f"plans with hits:        {plans_with_hits}")
        print(f"new source rows:        {totals['new_sources']}")
        print(f"updated source rows:    {totals['updated_sources']}")
        print(f"new recording rows:     {totals['new_recordings']}")
        print(f"updated recording rows: {totals['updated_recordings']}")
        if per_platform:
            print(f"by platform:")
            for plat, n in sorted(per_platform.items(), key=lambda x: -x[1]):
                print(f"  {plat:12} {n}")
        if dry_run:
            print("(dry run — no changes committed)")
    finally:
        session.close()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("plan_ids", nargs="*", help="Restrict to specific plan ids (default: all)")
    p.add_argument("--dry-run", action="store_true", help="Print findings without writing to DB")
    p.add_argument("--site-crawl", action="store_true",
                   help="Also fetch each plan's materials_url and harvest video links from the HTML")
    p.add_argument("--deep", action="store_true",
                   help="Also probe seed paths under plan.website (e.g. /board, /videos, /watch). "
                        "By default this only runs against plans that have NO active source row "
                        "— pass --deep-all to run it everywhere.")
    p.add_argument("--deep-all", action="store_true",
                   help="With --deep, run deep crawl on every plan (not just coverage gaps).")
    p.add_argument("--no-mine", action="store_true",
                   help="Skip the corpus-mining layer (only run site_crawl / deep)")
    args = p.parse_args()
    do_mine = not args.no_mine
    do_crawl = args.site_crawl
    do_deep = args.deep or args.deep_all
    deep_only_for_gaps = args.deep and not args.deep_all
    if not (do_mine or do_crawl or do_deep):
        print("ERROR: nothing to do — pass --site-crawl / --deep, or omit --no-mine",
              file=sys.stderr)
        sys.exit(2)
    run(args.plan_ids or None, do_mine=do_mine, do_crawl=do_crawl,
        do_deep=do_deep, deep_only_for_gaps=deep_only_for_gaps,
        dry_run=args.dry_run)


if __name__ == "__main__":
    main()
