"""A mock-mode cycle must never touch the committed notes/ directory.

Regression test for a real incident: when auto-publish landed, `archive=True`
called `publish.write_note` in mock mode too, and a single test run
overwrote notes/annual_cio_insights_2026.md and
notes/quarterly_cio_insights_2026-04-01.md — two real published briefings —
with canned mock text. The filename is derived from the period, so any mock
cycle for an already-published period lands exactly on that file.
"""

from __future__ import annotations

import hashlib
from datetime import date

import pytest

from database import Publication
from insights import publish


def _sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture()
def draft_publication():
    return Publication(
        id=999,
        cadence="annual",
        period_start=date(2026, 1, 1),
        period_end=date(2026, 12, 31),
        status="generating",
        draft_markdown="# Mock draft\n\nThis must not reach notes/.\n",
    )


def test_mock_write_note_redirects_away_from_committed_notes(draft_publication):
    committed = publish.NOTES_DIR / publish._filename_for(draft_publication)
    before = _sha(committed) if committed.exists() else None

    path = publish.write_note(draft_publication)

    assert publish.MOCK_NOTES_DIR in path.parents, path
    assert path.read_text(encoding="utf-8") == draft_publication.draft_markdown
    if before is not None:
        assert _sha(committed) == before, "committed note was modified!"


def test_redirect_only_applies_to_the_committed_directory(
        draft_publication, tmp_path, monkeypatch):
    """A test that deliberately points NOTES_DIR elsewhere still writes there."""
    monkeypatch.setattr(publish, "NOTES_DIR", tmp_path)
    path = publish.write_note(draft_publication)
    assert path.parent == tmp_path
