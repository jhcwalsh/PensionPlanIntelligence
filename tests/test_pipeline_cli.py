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


# ---------------------------------------------------------------------------
# The same defect, in a second module, found four days later
#
# Commit 67c842b removed the --local-only flag from pipeline.py and
# refresh_cafrs.py, and took `args = parser.parse_args()` with it in BOTH. The
# pipeline half was fixed on 2026-08-21 (PR #27) and nobody checked the other,
# so the monthly CAFR refresh went on failing: 2026-08-01 died with the same
# NameError while June and July had succeeded.
#
# Fixing the instance and not the class is what cost the extra three weeks, so
# this test enumerates every entry point rather than naming one.
# ---------------------------------------------------------------------------

import ast


def _modules_with_a_parser() -> list[pathlib.Path]:
    root = pathlib.Path(__file__).resolve().parents[1]
    out = []
    for path in sorted(root.rglob("*.py")):
        rel = path.relative_to(root).as_posix()
        if any(rel.startswith(s) for s in
               (".venv/", ".claude/", "build/", "tests/")):
            continue
        if "ArgumentParser(" in path.read_text(encoding="utf-8", errors="replace"):
            out.append(path)
    return out


def test_the_sweep_finds_the_known_entry_points():
    """Guard on the guard: if the discovery breaks, the test below passes on
    an empty list and proves nothing."""
    names = {p.name for p in _modules_with_a_parser()}
    assert {"pipeline.py", "refresh_cafrs.py"} <= names, names


@pytest.mark.parametrize(
    "path", _modules_with_a_parser(),
    ids=lambda p: p.name)
def test_every_parser_is_actually_parsed(path):
    """A module that builds a parser and never parses it references `args`
    unbound — a NameError on the first real run, and green in every test that
    does not invoke main()."""
    src = path.read_text(encoding="utf-8")
    if "parse_args" in src:
        return
    tree = ast.parse(src)
    uses_args = any(isinstance(n, ast.Name) and n.id == "args"
                    for n in ast.walk(tree))
    assert not uses_args, (
        "%s builds an ArgumentParser, references `args`, and never calls "
        "parse_args() — every reference below it is unbound" % path.name)


def test_refresh_cafrs_main_does_not_raise_nameerror(monkeypatch):
    """The specific regression, exercised the way the monthly cron does."""
    import refresh_cafrs

    called = {}
    monkeypatch.setattr(refresh_cafrs, "_resolve_plan_ids",
                        lambda ids: called.setdefault("ids", ids) or ["x"])
    monkeypatch.setattr(refresh_cafrs, "run_refresh",
                        lambda **kw: called.update(kw) or {"error": 0})
    monkeypatch.setattr("sys.argv", ["refresh_cafrs.py", "--year", "2025"])

    with pytest.raises(SystemExit) as exc:
        refresh_cafrs.main()
    assert exc.value.code == 0
    assert called.get("force_year") == 2025, called
