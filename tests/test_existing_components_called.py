"""Ensure the cycles call the existing summarizer/notes pipeline rather than re-implementing it.

In live mode, ``compose.compose_weekly`` MUST go through
``generate_notes.generate_note`` and ``generate_notes.gather_highlights_data``.
This test patches those functions and asserts they're invoked.
"""

from __future__ import annotations

from datetime import date

import pytest

from insights import compose


def test_weekly_compose_delegates_to_generate_notes(monkeypatch):
    """In live mode the weekly compose path goes through generate_notes."""
    monkeypatch.setenv("INSIGHTS_MODE", "live")
    # Reload config so is_mock() picks up the new env var.
    import importlib
    import insights.config as ic
    importlib.reload(ic)
    importlib.reload(compose)

    calls = {"gather": 0, "build": 0, "generate": 0}

    import generate_notes

    fake_data = {"meetings": [{"plan": None, "meeting_date": None,
                                "all_docs": [], "all_summaries": [],
                                "agenda_doc": None, "agenda_summary": None}],
                  "date_range": None, "plans_with_activity": 1, "total_aum": 0}

    def fake_gather(*a, **kw):
        calls["gather"] += 1
        return fake_data

    def fake_build(*a, **kw):
        calls["build"] += 1
        return "fake prompt"

    def fake_generate(prompt, max_tokens, model=None):
        calls["generate"] += 1
        return "# Fake delegated content"

    monkeypatch.setattr(generate_notes, "gather_highlights_data", fake_gather)
    monkeypatch.setattr(generate_notes, "build_highlights_prompt", fake_build)
    monkeypatch.setattr(generate_notes, "generate_note", fake_generate)

    result = compose.compose_weekly(session=None,
                                    period_start=date(2026, 4, 19),
                                    period_end=date(2026, 4, 25))
    assert calls == {"gather": 1, "build": 1, "generate": 1}
    assert "Fake delegated content" in result
