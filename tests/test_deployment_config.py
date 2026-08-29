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


R2_WORKFLOWS = ["daily-pipeline.yml"]
R2_VARS = ("R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID",
           "R2_SECRET_ACCESS_KEY", "R2_BUCKET")


@pytest.mark.parametrize("name", R2_WORKFLOWS)
def test_r2_credentials_declared_at_job_level(name):
    """Same rationale as the DSN: declared once for the job so a step added
    later cannot silently skip retention.

    pdf_store.config_from_env() returns None when any var is missing, and
    store_document treats None as "skip quietly" -- correct for local dev,
    invisible on a runner. Asserting the config directly is the only place
    that distinction gets caught.
    """
    for job_name, job in _jobs(name):
        env = job.get("env") or {}
        for var in R2_VARS:
            assert var in env, f"{name}:{job_name} has no job-level {var}"
            assert f"secrets.{var}" in str(env[var]), env


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
    """scripts/db_sync.py is deleted; a leftover call is an instant failure.

    R2 credentials belong only in workflows that fetch PDFs. daily-pipeline.yml
    needs them for PDF retention (asserted positively by
    test_r2_credentials_declared_at_job_level); other workflows should not
    carry credentials they don't use.
    """
    text = (WORKFLOWS / name).read_text(encoding="utf-8")
    assert "db_sync" not in text, f"{name} still calls db_sync"
    if name not in R2_WORKFLOWS:
        assert "R2_" not in text, f"{name} has R2 credentials but doesn't need them"


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


def test_r2_dependencies_are_present_for_the_pdf_store():
    """boto3/moto's presence tracks a decision that reversed once already.

    They were removed in the 2026-08-16 low-maintenance cutover, when R2 was
    only a database-sync bus (scripts/db_sync.py) and Postgres replaced that
    role outright. They're back for a second, unrelated reason: R2 as a
    content-addressed PDF object store (pdf_store.py), per
    docs/superpowers/specs/2026-08-29-pdf-retention-design.md §3.3.

    The two split across the two requirements files, and the split matters:

    - boto3 is a *runtime* dependency of pdf_store.py, so it belongs in
      requirements.txt. requirements-pipeline.txt starts with
      `-r requirements.txt`, so the daily pipeline gets it transitively
      without a second, redundant pin.
    - moto[s3] is a test-only mock and must stay OUT of requirements.txt,
      because render.yaml builds the public Streamlit service from that file
      -- moto would put werkzeug, Jinja2, cryptography, responses, xmltodict
      and py-partiql-parser into a production web service that never calls
      S3. It lives in requirements-pipeline.txt, which is what CI installs
      (.github/workflows/test.yml) and which already carries pytest and
      freezegun.

    tests/conftest.py imports boto3 and moto inside the `r2` fixture rather
    than at module level for the same reason: a module-level import would
    make the entire suite fail to collect wherever moto is absent.

    The db_sync invariant this test used to also carry is unaffected by any
    of this and is still covered elsewhere: test_no_workflow_still_calls_db_sync
    (this file) and the scripts/db_sync.py absence implied by
    test_the_local_recordings_job_does_not_push's "db_sync" not in bat check
    below. This test now guards only the dependency decision itself.
    """
    runtime = (ROOT / "requirements.txt").read_text(encoding="utf-8")
    pipeline = (ROOT / "requirements-pipeline.txt").read_text(encoding="utf-8")
    assert "boto3" in runtime, \
        "requirements.txt should pin boto3 for pdf_store.py"
    assert "moto" not in runtime, \
        "moto is test-only; requirements.txt builds the Render web service"
    assert "moto[s3]" in pipeline, \
        "requirements-pipeline.txt is what CI installs; moto must be there"
    assert not (ROOT / "scripts" / "db_sync.py").exists(), \
        "scripts/db_sync.py should still be gone -- R2 is back as an object " \
        "store, not as the database-sync bus that script implemented"


def test_the_local_recordings_job_does_not_push():
    """The one remaining local job.

    Its git commit/push existed to get catalogue rows to Render. Both now read
    the same Postgres, which is what removed its conflict-avoidance time slot.
    """
    bat = (ROOT / "scripts" / "run_recordings.bat").read_text(encoding="utf-8")
    assert "db_sync" not in bat
    assert "git commit" not in bat
    assert "git push" not in bat


# ---------------------------------------------------------------------------
# Duplicate keys
#
# yaml.safe_load accepts a duplicate mapping key and keeps the last one, with
# no error. GitHub Actions' parser rejects the file outright: the run fails
# instantly and the workflow shows up in the UI as its file path rather than
# its `name:`, which is the only visible symptom.
#
# The cutover inserted a job-level `env:` into eight workflows. daily-digest
# already had one, so it gained a second, and every run from that merge onward
# failed before executing a step — including the 13:00 UTC digest. The
# validation at the time used yaml.safe_load and saw nothing wrong.
# ---------------------------------------------------------------------------

class _DuplicateKeyError(Exception):
    pass


class _StrictLoader(yaml.SafeLoader):
    """SafeLoader that refuses duplicate mapping keys, as Actions does."""


def _no_duplicates(loader, node, deep=False):
    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise _DuplicateKeyError(
                "duplicate key %r at line %d"
                % (key, key_node.start_mark.line + 1))
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_StrictLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _no_duplicates)


@pytest.mark.parametrize(
    "name", sorted(p.name for p in WORKFLOWS.glob("*.yml")))
def test_no_workflow_has_a_duplicate_key(name):
    """Every workflow, not just the DB ones — a parse error breaks any file."""
    try:
        yaml.load((WORKFLOWS / name).read_text(encoding="utf-8"),
                  Loader=_StrictLoader)
    except _DuplicateKeyError as exc:
        pytest.fail("%s: %s — GitHub Actions will refuse to parse this" % (name, exc))


def test_the_strict_loader_actually_rejects_duplicates():
    """Guard on the guard.

    If the loader silently degraded to SafeLoader's behaviour, the test above
    would pass on every file forever while checking nothing.
    """
    doc = "job:\n  env:\n    a: 1\n  env:\n    b: 2\n"
    assert yaml.safe_load(doc) == {"job": {"env": {"b": 2}}}, \
        "safe_load stopped accepting duplicates — this test's premise changed"
    with pytest.raises(_DuplicateKeyError):
        yaml.load(doc, Loader=_StrictLoader)


@pytest.mark.parametrize("name", DB_WORKFLOWS)
def test_each_db_job_declares_the_dsn_exactly_once(name):
    """Two env blocks is the specific shape that broke daily-digest."""
    import re
    text = (WORKFLOWS / name).read_text(encoding="utf-8")
    per_job = len(re.findall(r"^    env:$", text, re.M))
    jobs = len(_load(name).get("jobs", {}))
    assert per_job <= jobs, (
        "%s has %d job-level env blocks across %d job(s)"
        % (name, per_job, jobs))
    assert text.count("DATABASE_URL: ${{ secrets.DATABASE_URL }}") == jobs, name
