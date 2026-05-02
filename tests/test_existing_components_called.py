"""Ensure the cycles call the existing summarizer/notes pipeline rather than re-implementing it.

In live mode, ``compose.compose_weekly`` MUST go through
``generate_notes.generate_note`` and ``generate_notes.gather_highlights_data``.
This test patches those functions and asserts they're invoked.
"""

from __future__ import annotations

from datetime import date, datetime

import pytest

from insights import compose


def _enter_live_mode(monkeypatch):
    monkeypatch.setenv("INSIGHTS_MODE", "live")
    import importlib
    import insights.config as ic
    importlib.reload(ic)
    importlib.reload(compose)


def _fake_data(date_range):
    return {
        "meetings": [{
            "plan": None, "meeting_date": None,
            "all_docs": [], "all_summaries": [],
            "agenda_doc": None, "agenda_summary": None,
        }],
        "date_range": date_range,
        "plans_with_activity": 1,
        "total_aum": 0,
    }


def test_weekly_compose_delegates_to_generate_notes(monkeypatch):
    """In live mode the weekly compose path goes through generate_notes."""
    _enter_live_mode(monkeypatch)

    calls = {"gather": 0, "build": 0, "generate": 0}

    import generate_notes

    date_range = (datetime(2026, 4, 19), datetime(2026, 4, 25))
    expected_title = (
        f"# 7-Day Highlights: "
        f"{generate_notes.format_weekly_date_range(date_range, days=7)}"
    )

    def fake_gather(*a, **kw):
        calls["gather"] += 1
        return _fake_data(date_range)

    def fake_build(*a, **kw):
        calls["build"] += 1
        return "fake prompt"

    def fake_generate(prompt, max_tokens, model=None):
        calls["generate"] += 1
        return f"{expected_title}\n*Generated: today*\n\n## body\n"

    monkeypatch.setattr(generate_notes, "gather_highlights_data", fake_gather)
    monkeypatch.setattr(generate_notes, "build_highlights_prompt", fake_build)
    monkeypatch.setattr(generate_notes, "generate_note", fake_generate)

    result = compose.compose_weekly(session=None,
                                    period_start=date(2026, 4, 19),
                                    period_end=date(2026, 4, 25))
    assert calls == {"gather": 1, "build": 1, "generate": 1}
    assert result.startswith(expected_title)


def test_weekly_compose_rejects_h1_mismatch(monkeypatch):
    """If the model produces a non-conforming H1, compose_weekly aborts.

    Catches the failure mode that produced 7day_highlights_2026-04-13.md's
    rogue "February 16-23, 2027" title — the model defied the prompt's
    "Start with exactly: # 7-Day Highlights: <date_range_title>" rule.
    """
    _enter_live_mode(monkeypatch)

    import generate_notes

    date_range = (datetime(2026, 4, 7), datetime(2026, 4, 13))

    def fake_gather(*a, **kw):
        return _fake_data(date_range)

    def fake_build(*a, **kw):
        return "fake prompt"

    def fake_generate_with_bad_title(prompt, max_tokens, model=None):
        return "# 7-Day Highlights: February 16–23, 2027\n*Generated: today*\n"

    monkeypatch.setattr(generate_notes, "gather_highlights_data", fake_gather)
    monkeypatch.setattr(generate_notes, "build_highlights_prompt", fake_build)
    monkeypatch.setattr(generate_notes, "generate_note",
                        fake_generate_with_bad_title)

    with pytest.raises(ValueError, match="Weekly H1 title mismatch"):
        compose.compose_weekly(session=None,
                               period_start=date(2026, 4, 7),
                               period_end=date(2026, 4, 13))
