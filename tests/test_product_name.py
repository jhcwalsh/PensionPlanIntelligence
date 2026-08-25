"""One product name, in everything a reader sees.

The project shipped with two. Publication notices went out subject-lined
``[PensionGraph]`` while the page title, the Subscribe page and both subscriber
emails said "Pension Plan Intelligence" — so a reader who signed up and then
received a briefing saw two different products.

Unified on PensionGraph, which is also the domain (pensiongraph.com), the local
recordings root, and arguably the more accurate name: the twins, manager rosters
and consultant relationships are a graph of the pension world, where
"intelligence" describes only the briefings.

The repository is still named PensionPlanIntelligence and stays that way —
renaming it would break every clone, checkout path and Actions URL for a name
no reader ever sees.
"""

from __future__ import annotations

import pathlib
import re

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]

# The display forms. Not the spaceless one — that is the repo, a path, or a
# User-Agent token, and is checked separately below.
_OLD_DISPLAY = re.compile(r"Pension\s+(Plan\s+)?Intelligence")

_SKIP_PREFIXES = (
    ".venv/", ".claude/", "build/", "node_modules/",
    # Dated records of decisions taken at the time. Rewriting them would be
    # falsifying the history that explains why the name changed.
    "docs/superpowers/specs/", "docs/superpowers/plans/",
    "notes/",
)


def _source_files():
    for path in sorted(ROOT.rglob("*.py")):
        rel = path.relative_to(ROOT).as_posix()
        if any(rel.startswith(p) for p in _SKIP_PREFIXES):
            continue
        if rel.startswith("tests/") and path.name == "test_product_name.py":
            continue
        yield rel, path


def test_no_python_file_shows_the_old_display_name():
    offenders = []
    for rel, path in _source_files():
        text = path.read_text(encoding="utf-8", errors="replace")
        for i, line in enumerate(text.splitlines(), 1):
            if _OLD_DISPLAY.search(line):
                offenders.append("%s:%d: %s" % (rel, i, line.strip()[:90]))
    assert offenders == [], (
        "these still show the old product name to a reader:\n  "
        + "\n  ".join(offenders))


def test_the_readme_uses_the_current_name():
    text = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "PensionGraph" in text
    assert not _OLD_DISPLAY.search(text), "README still shows the old name"


def test_the_repository_name_is_left_alone():
    """Deliberately unchanged.

    insights/github_dispatch.py addresses the real GitHub repo. Renaming it to
    match the product would break every clone, checkout path and Actions URL —
    for a name no reader ever sees.
    """
    src = (ROOT / "insights" / "github_dispatch.py").read_text(encoding="utf-8")
    assert 'REPO_NAME = "PensionPlanIntelligence"' in src


@pytest.mark.parametrize("path,needle", [
    ("insights/notice.py", "[PensionGraph]"),
    ("insights/subscribers.py", "PensionGraph"),
    ("app.py", "PensionGraph"),
])
def test_the_reader_facing_surfaces_say_pensiongraph(path, needle):
    """Not just the absence of the old name — the presence of the new one."""
    text = (ROOT / path).read_text(encoding="utf-8")
    assert needle in text, "%s does not name the product" % path


def test_the_user_agent_identifies_the_product():
    """Plan webmasters see this in their logs; it should match the site."""
    for rel in ("app.py", "backfill_downloads.py"):
        text = (ROOT / rel).read_text(encoding="utf-8")
        if "User-Agent" in text:
            assert "PensionGraph/1.0" in text, rel
