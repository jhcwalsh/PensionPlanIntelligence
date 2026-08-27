"""Reader-facing links point at the domain we own.

Briefings embed absolute links back to the site, baked in at compose time
(``insights/daily.py`` builds ``{base_url}/?document={id}``) rather than
rewritten at render time. So whatever host is configured when a briefing is
composed is the host its links carry forever.

That host was ``pensionplanintelligence.onrender.com`` -- a Render-assigned
subdomain, not a name we control. It still resolves today, which is exactly
why this is worth a test rather than a fix-when-it-breaks: 934 links across
30 published notes already carry it, and nothing will fail loudly on the day
the subdomain stops answering.

Five separate places hardcoded it, which is the other half of the problem: a
single ``APP_BASE_URL`` env var would have been one edit, but the value was
duplicated across two modules, a CLI print, a workflow env block and
``render.yaml``. Two of those -- the workflow and render.yaml -- *override*
the code default, so fixing only the Python looks correct locally while
production keeps emitting old links. render.yaml was in fact missed on the
first pass and caught by this test.
"""

from __future__ import annotations

import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]

CANONICAL = "https://pensiongraph.com"
RETIRED_HOST = "pensionplanintelligence.onrender.com"

_SKIP_PREFIXES = (
    ".venv/", ".claude/", "build/", "node_modules/", "tmp/",
    # Dated records of decisions taken at the time -- rewriting them would
    # falsify the history that explains why the host changed.
    "docs/superpowers/specs/", "docs/superpowers/plans/",
    # Already-published briefings. Their links are a separate, deliberate
    # backfill decision; see scripts/rewrite_published_links.py.
    "notes/",
)


# Files that must name the retired host, because rewriting it away is their
# whole job. Exempted by exact path, not by directory, so a new script under
# scripts/ is still caught.
_SELF_REFERENTIAL = (
    "tests/test_app_base_url.py",
    "scripts/rewrite_published_links.py",
)


def _scanned_files():
    for pattern in ("*.py", "*.yml", "*.yaml", "*.toml"):
        for path in sorted(ROOT.rglob(pattern)):
            rel = path.relative_to(ROOT).as_posix()
            if any(rel.startswith(p) for p in _SKIP_PREFIXES):
                continue
            if rel in _SELF_REFERENTIAL:
                continue
            yield rel, path


def test_no_source_file_hardcodes_the_retired_host():
    offenders = []
    for rel, path in _scanned_files():
        text = path.read_text(encoding="utf-8", errors="replace")
        for i, line in enumerate(text.splitlines(), 1):
            if RETIRED_HOST in line:
                offenders.append("%s:%d: %s" % (rel, i, line.strip()[:90]))
    assert offenders == [], (
        "these still point at the Render-assigned subdomain instead of "
        "%s:\n  %s" % (CANONICAL, "\n  ".join(offenders)))


@pytest.mark.parametrize("path,symbol", [
    ("insights/render.py", "DEFAULT_APP_BASE_URL"),
    ("insights/config.py", "APPROVAL_BASE_URL"),
])
def test_the_defaults_name_the_canonical_host(path, symbol):
    """Presence, not just absence of the old name."""
    text = (ROOT / path).read_text(encoding="utf-8")
    assert symbol in text, "%s no longer defines %s" % (path, symbol)
    assert CANONICAL in text, "%s does not name %s" % (path, CANONICAL)


def test_the_digest_workflow_agrees_with_the_code_default():
    """Two sources of truth for one value; they must not drift apart.

    daily-digest.yml sets APPROVAL_BASE_URL explicitly, overriding the
    default in insights/config.py. If only one is updated, the digest keeps
    composing links to the old host while every local run looks correct.
    """
    wf = (ROOT / ".github/workflows/daily-digest.yml").read_text(encoding="utf-8")
    if "APPROVAL_BASE_URL" in wf:
        assert CANONICAL in wf, (
            "daily-digest.yml overrides APPROVAL_BASE_URL but not with the "
            "canonical host — the digest would keep emitting old links")


def test_absolute_url_uses_the_canonical_host_by_default(monkeypatch):
    """Behavioural: the function callers actually reach."""
    monkeypatch.delenv("APP_BASE_URL", raising=False)
    from insights.render import absolute_url

    assert absolute_url("?document=42") == CANONICAL + "/?document=42"
    assert absolute_url("/notes/x.md") == CANONICAL + "/notes/x.md"


def test_absolute_url_still_respects_the_env_override(monkeypatch):
    monkeypatch.setenv("APP_BASE_URL", "https://staging.example/")
    from insights.render import absolute_url

    assert absolute_url("?document=42") == "https://staging.example/?document=42"


def test_absolute_urls_are_left_alone(monkeypatch):
    """Already-absolute hrefs must not be double-prefixed."""
    monkeypatch.delenv("APP_BASE_URL", raising=False)
    from insights.render import absolute_url

    for href in ("https://example.invalid/x", "http://example.invalid/x",
                 "mailto:a@example.invalid"):
        assert absolute_url(href) == href


def test_the_migration_script_names_both_hosts():
    """The one file that must mention the retired host.

    It is exempted from the scan above, so assert positively that it still
    does the job that earned the exemption -- otherwise the exemption could
    outlive the script and silently widen the blind spot.
    """
    src = (ROOT / "scripts/rewrite_published_links.py").read_text(encoding="utf-8")
    assert RETIRED_HOST in src, "migration script no longer names the old host"
    assert CANONICAL in src, "migration script no longer names the new host"
