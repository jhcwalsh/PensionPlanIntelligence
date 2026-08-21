"""The pipeline CLI must actually parse its arguments.

On 2026-08-16 a commit removing the `--local-only` flag deleted one line too
many, taking `args = parser.parse_args()` with it. `main()` then referenced
`args` unbound. The branch carrying that change sat unmerged for five days, so
production kept running the old code and stayed green — the moment it merged,
the next scheduled run died with:

    File "pipeline.py", line 202, in main
    NameError: name 'args' is not defined

and fetched nothing.

Nothing caught it. The suite exercises the pipeline's internals directly and
never invokes `main()`, so the entry point had no coverage at all. These tests
are that coverage: they call `main()` the way GitHub Actions does.
"""

from __future__ import annotations

import pathlib

import pytest

import pipeline


def test_status_flag_runs_without_a_nameerror(tmp_db, monkeypatch, capsys):
    """The exact invocation that broke: `python pipeline.py --status`.

    A NameError here is the regression. `--status` is used because it is the
    one path that returns without fetching, extracting or calling an LLM.
    """
    monkeypatch.setattr("sys.argv", ["pipeline.py", "--status"])
    pipeline.main()
    out = capsys.readouterr().out
    assert out.strip(), "--status produced no output"


def test_bare_invocation_reaches_the_run_stage(tmp_db, monkeypatch):
    """No flags at all — the form the daily cron actually uses.

    `run_pipeline` is stubbed so nothing is fetched or summarised; the point is
    that argument parsing and the flag arithmetic above it complete, and that
    the defaults arrive as the cron expects.
    """
    seen = {}

    def fake_run_pipeline(**kwargs):
        seen.update(kwargs)

    monkeypatch.setattr(pipeline, "run_pipeline", fake_run_pipeline)
    monkeypatch.setattr("sys.argv", ["pipeline.py"])
    pipeline.main()

    assert seen, "run_pipeline was never reached — argument handling failed"
    # The cron runs all three stages; none of the -only flags should be set.
    assert seen.get("do_fetch") is True
    assert seen.get("do_extract") is True
    assert seen.get("do_summarize") is True
    assert seen.get("retry_failed") is False
    # No --plans means every eligible plan, resolved by _resolve_plan_ids.
    plan_ids = seen.get("plan_ids")
    assert isinstance(plan_ids, list) and plan_ids, plan_ids


def test_parse_args_is_actually_called():
    """A static backstop for the specific line that went missing.

    The tests above would already fail, but this one names the cause, so a
    future reader of a red suite is pointed at the line rather than at a
    NameError deep in a traceback.
    """
    src = pathlib.Path(pipeline.__file__).read_text(encoding="utf-8")
    assert "parser.parse_args()" in src, (
        "pipeline.main() builds an ArgumentParser but never calls "
        "parse_args() — every reference to `args` below it is unbound")
