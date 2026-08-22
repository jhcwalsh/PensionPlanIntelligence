"""The cutover's invariants, pinned where they can be checked.

None of this can be exercised locally: a workflow only runs on GitHub and
render.yaml only means anything to Render. The failure modes are quiet ones —
a job that reads an empty SQLite file and reports success, or a commit step
that resurrects a database no longer in git — so the config gets asserted on
directly rather than trusted.

See docs/superpowers/specs/2026-08-16-low-maintenance-app-design.md, step 5.
"""

from __future__ import annotations

import pathlib

import pytest
import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"

# Jobs that talk to the database. The rest (probe-pipeline, test-email, and
# test.yml's own jobs) deliberately do not.
DB_WORKFLOWS = [
    "annual-insights.yml",
    "daily-digest.yml",
    "daily-pipeline.yml",
    "monthly-cafr-refresh.yml",
    "monthly-insights.yml",
    "monthly-ips.yml",
    "quarterly-insights.yml",
    "weekly-insights.yml",
]


def _load(name: str) -> dict:
    return yaml.safe_load((WORKFLOWS / name).read_text(encoding="utf-8"))


def _jobs(name: str):
    return _load(name).get("jobs", {}).items()


@pytest.mark.parametrize("name", DB_WORKFLOWS)
def test_every_db_job_receives_the_dsn(name):
    """At job level, not per step.

    Declared once for the job so a step added later cannot silently fall back:
    database.resolve_database_url() reads an unset or empty DATABASE_URL as
    "use DB_PATH", which on a runner is an empty file. The job would read
    nothing, write nothing, and exit zero.
    """
    for job_name, job in _jobs(name):
        env = job.get("env") or {}
        assert "DATABASE_URL" in env, \
            f"{name}:{job_name} has no job-level DATABASE_URL"
        assert "secrets.DATABASE_URL" in str(env["DATABASE_URL"]), env


@pytest.mark.parametrize("name", DB_WORKFLOWS)
def test_no_workflow_commits_the_database(name):
    """db/pension.db left git in the cutover.

    A `git add db/pension.db` that survived would either fail the step or, on
    a checkout that still had the file, commit a stale snapshot over and over
    — reviving the 100 MB ceiling the cutover exists to remove.
    """
    text = (WORKFLOWS / name).read_text(encoding="utf-8")
    assert "db/pension.db" not in text, \
        f"{name} still references db/pension.db"


@pytest.mark.parametrize("name", DB_WORKFLOWS)
def test_no_workflow_still_calls_db_sync(name):
    """scripts/db_sync.py is deleted; a leftover call is an instant failure."""
    text = (WORKFLOWS / name).read_text(encoding="utf-8")
    assert "db_sync" not in text, f"{name} still calls db_sync"
    assert "R2_" not in text, f"{name} still passes R2 credentials"


@pytest.mark.parametrize("name", DB_WORKFLOWS)
def test_declared_inputs_are_all_used(name):
    """A dispatch input nothing reads is a trap.

    Both daily-pipeline and monthly-ips offered "Skip the final commit/push of
    db/pension.db" after the commit step it referred to had gone. Ticking it
    would have done nothing at all.
    """
    import re

    text = (WORKFLOWS / name).read_text(encoding="utf-8")
    doc = _load(name)
    # YAML 1.1 parses a bare `on:` key as the boolean True.
    triggers = doc.get("on", doc.get(True)) or {}
    declared = set(((triggers.get("workflow_dispatch") or {}).get("inputs") or {}))
    used = set(re.findall(r"github\.event\.inputs\.(\w+)", text))
    assert declared == used, (
        f"{name}: declared-but-unused {sorted(declared - used)}, "
        f"used-but-undeclared {sorted(used - declared)}")


def test_render_has_one_service_and_it_uses_postgres():
    render = yaml.safe_load((ROOT / "render.yaml").read_text(encoding="utf-8"))
    services = render["services"]
    assert len(services) == 1, [s["name"] for s in services]

    web = services[0]
    keys = {e["key"] for e in web["envVars"]}
    assert "DATABASE_URL" in keys
    assert "DB_PATH" not in keys, \
        "DB_PATH would send the app back to a SQLite file that no longer exists"


def test_render_no_longer_mounts_a_disk():
    """The disk existed only to hold the committed database."""
    render = yaml.safe_load((ROOT / "render.yaml").read_text(encoding="utf-8"))
    assert "disk" not in render["services"][0], render["services"][0].get("disk")


def test_the_dsn_is_never_committed():
    """Every DATABASE_URL declaration must be sync: false or a secret ref."""
    render = yaml.safe_load((ROOT / "render.yaml").read_text(encoding="utf-8"))
    for service in render["services"]:
        for entry in service["envVars"]:
            if entry["key"] == "DATABASE_URL":
                assert entry.get("sync") is False, entry
                assert "value" not in entry, "a literal DSN in git: %s" % entry


def test_r2_dependencies_are_gone():
    """boto3 existed solely for db_sync's R2 client, moto solely to mock it."""
    for name in ("requirements.txt", "requirements-pipeline.txt"):
        text = (ROOT / name).read_text(encoding="utf-8")
        assert "boto3" not in text, f"{name} still pins boto3"
    ci = (WORKFLOWS / "test.yml").read_text(encoding="utf-8")
    assert "moto" not in ci, "CI still installs moto"


def test_the_local_recordings_job_does_not_push():
    """The one remaining local job.

    Its git commit/push existed to get catalogue rows to Render. Both now read
    the same Postgres, which is what removed its conflict-avoidance time slot.
    """
    bat = (ROOT / "scripts" / "run_recordings.bat").read_text(encoding="utf-8")
    assert "db_sync" not in bat
    assert "git commit" not in bat
    assert "git push" not in bat
