"""app.py must import with only requirements.txt installed.

Render's web service runs `pip install -r requirements.txt`. Everything else —
Playwright, rich, the PDF tooling — lives in requirements-pipeline.txt for the
GitHub Actions jobs. CI and every dev machine install the *pipeline* set, so
they are blind to anything app.py needs that only the pipeline provides.

That blindness took the site down. PR #25 added
`from twin_builder import visible_relationships` to app.py; twin_builder had
`from rich.console import Console` at module level; rich is pipeline-only. The
whole app served a ModuleNotFoundError traceback for days, unnoticed, because
there are no users yet and nothing checks this.

These tests simulate the web service's narrower environment by hiding modules
it would not have, rather than by installing anything.
"""

from __future__ import annotations

import builtins
import importlib
import pathlib
import re
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _declared(filename: str) -> set[str]:
    """Distribution names pinned in a requirements file."""
    names = set()
    for line in (ROOT / filename).read_text(encoding="utf-8").splitlines():
        line = line.split("#")[0].strip()
        if line:
            names.add(re.split(r"[<>=\[]", line)[0].strip().lower())
    return names


def test_the_two_requirement_sets_really_do_differ():
    """Guard on the guard.

    If requirements.txt ever became a superset, these tests would pass while
    testing nothing — and the difference is the whole hazard.
    """
    web = _declared("requirements.txt")
    pipeline = _declared("requirements-pipeline.txt")
    assert pipeline - web, "requirements-pipeline.txt adds nothing over the web set"
    assert "rich" in pipeline - web, (
        "rich moved into requirements.txt — this test's premise changed")


@pytest.fixture()
def without_pipeline_only_modules(monkeypatch):
    """Make pipeline-only imports fail the way they do on Render."""
    hidden = {"rich", "playwright", "pdfplumber", "fitz", "yt_dlp"}
    real_import = builtins.__import__

    def guarded(name, *args, **kwargs):
        root = name.split(".")[0]
        if root in hidden:
            raise ModuleNotFoundError("No module named %r" % root, name=root)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded)
    for mod in list(sys.modules):
        if mod.split(".")[0] in hidden:
            monkeypatch.delitem(sys.modules, mod, raising=False)
    yield


# Evicted from sys.modules before a re-import, so the import really re-runs.
# `database` is deliberately NOT in this list: reloading it orphans the ORM
# classes and breaks SQLAlchemy's mapper registry (see tests/conftest.py). None
# of these define models, so evicting them is safe.
_EVICT = ("app", "twin_builder", "queries", "auth")


def _reimport(module_name: str, monkeypatch):
    """Import fresh, not from cache.

    Deleting only the target module is not enough and the omission is silent:
    `import app` would find an already-imported twin_builder in sys.modules and
    never re-execute the import that fails. The first version of this file made
    that mistake, and its app-level test passed against the very bug it was
    written for.
    """
    for name in _EVICT:
        monkeypatch.delitem(sys.modules, name, raising=False)
    return importlib.import_module(module_name)


def test_twin_builder_imports_without_rich(without_pipeline_only_modules,
                                           monkeypatch):
    """The exact failure. app.py imports this module for one pure function."""
    module = _reimport("twin_builder", monkeypatch)
    assert hasattr(module, "visible_relationships")


def test_visible_relationships_works_without_rich(without_pipeline_only_modules,
                                                  monkeypatch):
    """Importable is not enough — the function app.py calls must run."""
    module = _reimport("twin_builder", monkeypatch)
    rels = [{"role": "Consultant", "basis": "rfp_awarded"},
            {"role": "Actuary", "basis": "cafr_actuarial"}]
    assert module.visible_relationships(rels) == [rels[1]]


def test_the_cli_still_gets_its_console():
    """Laziness must not have broken the thing rich is actually for."""
    import twin_builder
    console = twin_builder._console()
    assert hasattr(console, "print")
    assert twin_builder._console() is console, "a new Console per call"


def test_app_imports_without_pipeline_only_modules(without_pipeline_only_modules,
                                                   monkeypatch):
    """The test that mirrors Render.

    Importing app.py is exactly what the web service does at startup, and it is
    the only check that catches a pipeline-only dependency reaching the web
    path — whichever module introduces it next.
    """
    module = _reimport("app", monkeypatch)
    assert hasattr(module, "main")
