"""The daily digest has no schedule (since 2026-09-12).

Retired at the owner's request in favour of the weekly cadences. The
workflow stays for manual dispatch, so the thing to pin is that nothing
fires it on a timer, not that the file is gone.
"""
import pathlib

import yaml

WF = pathlib.Path(__file__).resolve().parents[1] / ".github" / "workflows" / "daily-digest.yml"


def test_daily_digest_has_no_cron_schedule():
    wf = yaml.safe_load(WF.read_text(encoding="utf-8"))
    triggers = wf.get("on") or wf.get(True)  # PyYAML reads a bare `on:` as True
    assert "schedule" not in triggers
    assert "workflow_dispatch" in triggers
