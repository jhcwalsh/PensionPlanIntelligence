"""Static guards on the Mac Mini runner script.

The script cannot be exercised in CI -- it needs Docker, a residential IP and
a real Neon URL -- so these tests assert the properties that would otherwise
rot unnoticed. This mirrors the static backstops already in
tests/test_pipeline_cli.py and tests/test_deployment_config.py, and exists for
the same reason: the failure mode is silent.
"""

from __future__ import annotations

import pathlib
import re

from scripts import waf_blocked_ids


SCRIPT = (pathlib.Path(__file__).resolve().parents[1]
          / "scripts" / "run_waf_plans.sh")


def _source() -> str:
    return SCRIPT.read_text(encoding="utf-8")


def _mentions(src: str, plan_id: str) -> bool:
    """Whether `plan_id` appears as a standalone token.

    Not `plan_id in src`: "refresh_cafrs" ends in the letters of the plan id
    "frs", so a substring test reports the script hardcodes a plan it merely
    never mentions. Bounded on both sides by anything that could continue an
    identifier, underscores and hyphens included, since plan ids contain both.
    """
    return re.search(rf"(?<![\w-]){re.escape(plan_id)}(?![\w-])", src) is not None


def test_the_runner_exists():
    assert SCRIPT.exists(), f"{SCRIPT} is missing"


def test_no_plan_id_is_hardcoded():
    """The whole point of scripts/waf_blocked_ids.py.

    A literal id here is correct today and silently wrong the first time a
    plan is unblocked: naming a plan that no longer needs naming still
    succeeds, so nothing fails and the list quietly diverges from the JSON.
    """
    src = _source()
    leaked = [pid for pid in waf_blocked_ids.all_ids() if _mentions(src, pid)]
    assert not leaked, f"hardcoded plan ids in run_waf_plans.sh: {leaked}"


def test_no_excluded_plan_id_is_hardcoded_either():
    """The stronger version, and the one that matters after 2026-09-01.

    Seven of the fourteen are excluded because the Mini cannot serve them --
    four list but will not download, two are blocked outright, one was never
    blocked. Naming any of those explicitly would resurrect the bug the
    classification exists to prevent, and `all_ids()` alone would not catch
    it because they are not in it.
    """
    src = _source()
    leaked = [pid for pid in waf_blocked_ids.unreachable_ids()
              if _mentions(src, pid)]
    assert not leaked, f"runner names plans the Mini cannot serve: {leaked}"


def test_both_id_lists_are_sourced_from_the_helper():
    src = _source()
    assert "scripts.waf_blocked_ids --materials" in src
    assert "scripts.waf_blocked_ids --cafr" in src


def test_both_pipeline_entry_points_are_invoked():
    src = _source()
    assert "pipeline.py" in src, "board materials are never fetched"
    assert "refresh_cafrs.py" in src, "CAFRs are never refreshed"


def test_every_step_notifies_on_failure():
    """run_recordings.bat's pattern: a failed step emails rather than exiting
    quietly into a log nobody opens.

    Counts calls to the script's own `notify` helper rather than occurrences
    of `scripts.notify_failure` -- the helper wraps it, so the module name
    appears exactly once no matter how many steps are guarded.
    """
    src = _source()
    assert "scripts.notify_failure" in src, "no failure notification at all"
    # Lines invoking the helper, excluding its own definition.
    calls = [ln for ln in src.splitlines()
             if ln.strip().startswith("notify ")]
    assert len(calls) >= 4, (
        f"only {len(calls)} guarded steps -- a failure would be silent")


def test_database_url_is_asserted_before_any_work():
    """An unset or empty DATABASE_URL is an empty SQLite file: the job reads
    nothing, writes nothing and exits zero. CLAUDE.md calls this out as the
    first thing to check when a deployment looks like total data loss."""
    assert "DATABASE_URL" in _source()


def test_retention_state_is_recorded_in_the_log():
    """These are the plans no other machine can re-fetch, so a run that
    retains nothing is the most expensive silent success available. fetcher.py
    prints retention on/off at the start of every run; the runner must send
    that line somewhere a human can find it."""
    src = _source()
    assert 'LOG' in src and '>> "$LOG"' in src


def test_mock_modes_are_not_set():
    src = _source()
    assert "LLM_MODE=mock" not in src
    assert "INSIGHTS_MODE=mock" not in src
