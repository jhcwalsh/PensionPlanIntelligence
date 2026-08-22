"""The app has to be able to point at Postgres without a code change.

Both functions are pure and take their environment as an argument. That is
deliberate: database.py builds its engine at import, so the only other way to
test this would be to reload the module -- which orphans the ORM classes and
breaks SQLAlchemy's mapper registry. See tests/conftest.py.
"""

from __future__ import annotations

import pathlib

import database


def test_sqlite_is_still_the_default():
    """Nothing changes until DATABASE_URL is set. This is what makes the
    wiring safe to merge before the cutover."""
    url = database.resolve_database_url({}, db_path="/tmp/x.db")
    assert url == "sqlite:////tmp/x.db"


def test_database_url_wins_when_set():
    url = database.resolve_database_url(
        {"DATABASE_URL": "postgresql://u:p@host/db"}, db_path="/tmp/x.db")
    assert url.startswith("postgresql+psycopg://"), url
    assert "/tmp/x.db" not in url


def test_the_driver_is_pinned():
    """A bare postgresql:// URL maps to psycopg2, which is not installed --
    and if it happens to be present it returns BYTEA as memoryview."""
    assert database.resolve_database_url(
        {"DATABASE_URL": "postgres://u:p@h/d"}).startswith("postgresql+psycopg://")


def test_an_empty_value_is_not_a_url():
    """Render and GitHub Actions both materialise an unset secret as "".
    Treating that as a URL would fail the deploy with an unparseable DSN
    instead of falling back to SQLite."""
    assert database.resolve_database_url(
        {"DATABASE_URL": ""}, db_path="/tmp/x.db") == "sqlite:////tmp/x.db"


def test_whitespace_is_not_a_url():
    """A dashboard field cleared by hand tends to leave a space behind."""
    assert database.resolve_database_url(
        {"DATABASE_URL": "   "}, db_path="/tmp/x.db") == "sqlite:////tmp/x.db"


def test_query_parameters_survive():
    """Neon's URL carries sslmode and channel_binding. Dropping either makes
    the connection fail, or silently downgrades its security."""
    url = database.resolve_database_url({
        "DATABASE_URL":
            "postgresql://u:p@h/d?sslmode=require&channel_binding=require"})
    assert "sslmode=require" in url
    assert "channel_binding=require" in url


def test_postgres_engines_pre_ping():
    """Neon closes idle connections. Without pre-ping the first query after an
    idle period raises OperationalError instead of reconnecting -- which on
    Streamlit means a user sees a stack trace on a page they left open."""
    engine = database.create_app_engine(
        "postgresql+psycopg://u:p@h/d")           # never connected to
    try:
        assert engine.pool._pre_ping is True
    finally:
        engine.dispose()


def test_sqlite_engines_do_not_take_postgres_pool_settings():
    """SQLite's default pool has no pre-ping to set, and pool_recycle is
    meaningless for a local file. Keeping the branch narrow keeps the local
    path exactly as it is today."""
    engine = database.create_app_engine("sqlite://")
    try:
        assert getattr(engine.pool, "_pre_ping", False) is False
    finally:
        engine.dispose()


def test_the_module_engine_uses_the_resolver():
    """A static backstop.

    Every test above passes against a database.py that defines both functions
    and then ignores them -- which is exactly what a careless merge conflict
    resolution produces. This names the two lines that have to survive.
    """
    src = pathlib.Path(database.__file__).read_text(encoding="utf-8")
    assert "engine = create_app_engine(DATABASE_URL)" in src
    assert "DATABASE_URL = resolve_database_url()" in src
    assert 'DATABASE_URL = f"sqlite:///{DB_PATH}"' not in src, \
        "the hardcoded SQLite URL is back -- DATABASE_URL is being ignored"


# ---------------------------------------------------------------------------
# .env must reach database.py whatever the entry point's import order
# ---------------------------------------------------------------------------

def test_dotenv_is_loaded_before_the_url_is_resolved():
    """Ordering, asserted on the source because it only exists at import.

    database.py resolves DATABASE_URL and builds its engine at import time.
    Every entry point that calls load_dotenv() -- app.py, pipeline.py,
    generate_notes.py, the extractors -- does so *after* importing database,
    so before this fix a DATABASE_URL living only in .env was invisible:
    `import app` came up on SQLite with the variable sitting right there, and
    nothing raised. The whole Postgres cutover would have been a local no-op.
    """
    src = pathlib.Path(database.__file__).read_text(encoding="utf-8")
    assert "load_dotenv(" in src, "database.py no longer loads .env itself"
    assert src.index("load_dotenv(") < src.index("DATABASE_URL = resolve_database_url()"), \
        ".env is loaded after the URL is resolved — it cannot take effect"


def test_a_real_environment_variable_beats_the_dotenv_file():
    """override=False, and it matters.

    On GitHub Actions and Render the DSN arrives as a real environment
    variable and there is no .env. If the file could win, a stray .env in a
    checkout would silently redirect a production job.
    """
    src = pathlib.Path(database.__file__).read_text(encoding="utf-8")
    call = src[src.index("load_dotenv("):src.index("from sqlalchemy import (")]
    assert "override=False" in call, (
        "load_dotenv must not override a real environment variable")


def test_entry_points_honour_the_environment_variable(tmp_path):
    """Behavioural backstop, run in a subprocess so imports happen for real.

    An explicit variable is used rather than a .env file because the path
    database.py reads is fixed at the repo root — this still catches an entry
    point that resolves the URL before the environment is in place, or one
    that overwrites it afterwards.
    """
    import os
    import subprocess
    import sys

    target = tmp_path / "entrypoint.db"
    env = dict(os.environ)
    env["DATABASE_URL"] = "sqlite:///%s" % target.as_posix()
    env["ADMIN_PASSWORD"] = "x"

    for module in ("database", "app", "pipeline"):
        out = subprocess.run(
            [sys.executable, "-c",
             "import %s, database; print('URL:', database.DATABASE_URL)" % module],
            capture_output=True, text=True, env=env,
            cwd=str(pathlib.Path(database.__file__).parent))
        line = next((l for l in (out.stdout + out.stderr).splitlines()
                     if l.startswith("URL:")), None)
        assert line is not None, "%s: %s" % (module, out.stderr[-500:])
        assert target.as_posix() in line, (
            "%s did not honour DATABASE_URL: %s" % (module, line))
