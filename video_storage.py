"""Local storage layout for downloaded meeting recordings.

The video files themselves never go into the SQLite DB and never go
onto Render's persistent disk — they live on the user's local D: drive
under PensionGraph\\meetingrecordings. The DB only tracks the path so
the catalogue can find the file again.

Layout:
  D:\\PensionGraph\\meetingrecordings\\
      {plan_id}\\
          {YYYY-MM-DD}_{video_id}.{ext}

YYYY-MM-DD is the inferred meeting date when known, else the recording
publish date, else "undated". video_id is the platform-native id, which
keeps two recordings on the same day (board + investment committee)
from colliding.

The path is overridable via the RECORDINGS_DIR env var so the same
code can run on a non-Windows test box.
"""
from __future__ import annotations

import os
import re
from datetime import datetime
from pathlib import Path


RECORDINGS_DIR = Path(
    os.environ.get("RECORDINGS_DIR", r"D:\PensionGraph\meetingrecordings")
)


_FILENAME_SAFE_RE = re.compile(r"[^A-Za-z0-9._\-]+")


def _safe_segment(s: str) -> str:
    """Strip path-unsafe chars; collapse whitespace to underscores."""
    s = s.strip().replace(" ", "_")
    return _FILENAME_SAFE_RE.sub("", s) or "x"


def recording_path(plan_id: str, video_id: str, *,
                   meeting_date: datetime | None = None,
                   published_at: datetime | None = None,
                   ext: str = "mp4") -> Path:
    """Build the canonical local path for a recording's downloaded file.

    Does NOT create the directory or the file — just returns where the
    Phase-2 downloader should write to and where the catalogue tab
    should expect the file to live.
    """
    when = meeting_date or published_at
    date_seg = when.strftime("%Y-%m-%d") if when else "undated"
    fname = f"{date_seg}_{_safe_segment(video_id)}.{ext.lstrip('.')}"
    return RECORDINGS_DIR / _safe_segment(plan_id) / fname


def plan_dir(plan_id: str) -> Path:
    """Per-plan recordings directory."""
    return RECORDINGS_DIR / _safe_segment(plan_id)
