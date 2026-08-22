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
