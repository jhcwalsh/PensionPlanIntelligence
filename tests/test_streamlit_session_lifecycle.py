"""The app's shared Session must not outlive a rerun holding a transaction.

Reported from production on 2026-08-27: clicking a subscriber confirmation
link rendered a traceback instead of the app.

    sqlalchemy.exc.InternalError: (psycopg.errors.IdleInTransactionSessionTimeout)
    terminating connection due to idle-in-transaction timeout
    [SQL: SELECT plans.id ... FROM plans ORDER BY plans.name]
      File "/opt/render/project/src/app.py", line ..., in main
      File "/opt/render/project/src/app.py", line ..., in render_sidebar
      File "/opt/render/project/src/app.py", line ..., in load_plans

The mechanism, which is not obvious:

* A SQLAlchemy ``Session`` opens a transaction on its first query and holds
  it -- along with its connection -- until commit or rollback.
* ``app.py`` caches exactly one Session for the life of the process via
  ``st.cache_resource``, shared by 21 call sites, none of which committed
  or rolled back after reading.
* So between reruns the connection sat idle *inside a transaction*. Neon
  terminates those after five minutes.

``pool_pre_ping`` and ``pool_recycle=300`` were already configured and were
powerless here: both act when a connection is checked out of the pool, and a
Session mid-transaction never returns its connection to the pool. Rolling
back is what hands it back -- which is why the fix is a rollback rather than
a bigger pool setting.

This suite is deliberately about transaction *state* rather than about
reproducing Neon's timeout. The timeout is a server-side policy we cannot
provoke on SQLite; the state that invites it is exactly testable, and it is
the thing under our control.
"""

from __future__ import annotations

import pathlib

import pytest

import database

ROOT = pathlib.Path(__file__).resolve().parents[1]


@pytest.fixture()
def app_module(tmp_db, monkeypatch):
    """app.py with a real cache, isolated per test.

    st.cache_resource memoises for the life of the process, which would leak
    one test's Session into the next; a fresh lru_cache per test keeps the
    memoisation the code depends on while isolating the tests from it.
    """
    # The double must memoise *and* expose .clear(), matching
    # st.cache_resource's API. A pass-through would hand every caller its own
    # Session -- and a shared Session is the entire bug -- so the fix would
    # look broken. lru_cache alone spells it cache_clear, and app.py bridges
    # get_db_session.clear onto it at import.
    import functools
    import streamlit as st

    def fake_cache_resource(fn):
        cached = functools.lru_cache(maxsize=None)(fn)
        cached.clear = cached.cache_clear
        return cached

    monkeypatch.setattr(st, "cache_resource", fake_cache_resource)

    import importlib
    import app as app_mod
    reloaded = importlib.reload(app_mod)
    yield reloaded

    # Drop the cached Session before leaving. It is bound to this test's
    # tmp_db engine, and the module object outlives the fixture -- so a later
    # test calling get_db_session() would silently read a database that no
    # longer exists. (test_twin_page_data.py already clears it for exactly
    # this reason; leaving it dirty here made that suite fail on a
    # KeyError: 'twin' that had nothing to do with twins.)
    try:
        reloaded.get_db_session.clear()
    except Exception:                          # noqa: BLE001
        pass


def test_a_read_leaves_the_session_in_a_transaction(tmp_db):
    """The precondition. If this ever stops being true the fix is moot."""
    session = database.get_session()
    try:
        assert not session.in_transaction()
        session.query(database.Plan).all()
        assert session.in_transaction(), (
            "a read no longer opens a transaction — re-derive this fix")
    finally:
        session.close()


def test_release_ends_the_transaction(app_module):
    """The fix itself, at the boundary that matters."""
    session = app_module.get_db_session()
    session.query(database.Plan).all()
    assert session.in_transaction()

    app_module._release_db_session()
    assert not session.in_transaction(), (
        "the transaction outlived the rerun — the connection stays checked "
        "out and idle-in-transaction, which is what Neon terminates")


def test_get_db_session_heals_a_dirty_session(app_module):
    """A rerun that raised mid-render must not poison the cached session."""
    session = app_module.get_db_session()
    session.query(database.Plan).all()
    assert session.in_transaction()          # simulate the aborted rerun

    again = app_module.get_db_session()
    assert again is session, "should be the same cached Session"
    assert not again.in_transaction(), (
        "get_db_session() handed back a session mid-transaction")


def test_reads_still_work_after_a_release(app_module):
    """Releasing must not break the session for the next rerun."""
    session = app_module.get_db_session()
    session.query(database.Plan).all()
    app_module._release_db_session()

    rows = app_module.get_db_session().query(database.Plan).all()
    assert rows == []                        # empty DB, but the query ran


def test_release_is_safe_when_nothing_has_queried(app_module):
    """The finally runs even when main() raised before touching the DB."""
    app_module._release_db_session()
    app_module._release_db_session()          # and is idempotent


def test_release_survives_a_dead_connection(app_module, monkeypatch):
    """Neon terminating the connection must not turn into a second error.

    The real failure arrives as an exception from rollback() itself. If
    _release_db_session let that escape, the finally would replace the
    page's actual error with a confusing one from cleanup.
    """
    session = app_module.get_db_session()
    session.query(database.Plan).all()

    def boom():
        raise RuntimeError("terminating connection due to idle-in-transaction")

    monkeypatch.setattr(session, "rollback", boom)
    app_module._release_db_session()          # must not raise


# ---------------------------------------------------------------------------
# The wiring
# ---------------------------------------------------------------------------

def test_main_is_wrapped_so_the_release_always_runs():
    """Source-level: the finally is the whole mechanism.

    Streamlit re-executes the module per interaction, so this block is the
    per-rerun boundary. A release that only ran on the success path would
    leave the transaction open on exactly the reruns that raised.
    """
    src = (ROOT / "app.py").read_text(encoding="utf-8")
    tail = src[src.index('if __name__ == "__main__":'):]
    assert "finally:" in tail, "main() is not wrapped in try/finally"
    assert "_release_db_session()" in tail, (
        "the rerun boundary does not release the session")
