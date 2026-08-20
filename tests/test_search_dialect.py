"""Search must degrade loudly, not silently.

FTS5 is SQLite-only. Before this, _init_fts and search_summaries each wrapped
their failure in a bare `except Exception` written for "this SQLite build lacks
FTS5" — which would swallow "this is not SQLite" identically. The consequence
on Neon was that init_db() succeeded, nothing raised, no test failed, and
ranked search quietly became an unranked substring scan.

See docs/superpowers/specs/2026-08-19-portal-readiness-design.md §2.1.
"""

from __future__ import annotations

import logging
import types

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import OperationalError

import database


def _engine_with_dialect(name: str):
    """A stand-in exposing just `.dialect.name`.

    A real postgresql engine would need psycopg installed, which it is not on
    a dev machine — and the contract under test is exactly this narrow: the
    dialect is inspected before anything connects. Keeping the stub minimal
    documents that contract rather than hiding it behind a driver.
    """
    return types.SimpleNamespace(dialect=types.SimpleNamespace(name=name))


def test_fts_is_recognised_as_sqlite_only():
    assert database._fts_dialect_supported(_engine_with_dialect("postgresql")) is False
    assert database._fts_dialect_supported(_engine_with_dialect("mysql")) is False
    assert database._fts_dialect_supported(sa.create_engine("sqlite://")) is True


def test_init_fts_declines_on_postgres_without_connecting(caplog):
    """The bug: this used to attempt FTS5, fail, and return False in silence.

    The stub would raise AttributeError if anything tried to open a
    connection, so this also proves the check happens before connecting.
    """
    with caplog.at_level(logging.WARNING, logger="database"):
        result = database._init_fts(_engine_with_dialect("postgresql"))

    assert result is False
    assert "FTS5 is SQLite-only" in caplog.text
    assert "postgresql" in caplog.text, "the warning must name the dialect"


def test_init_fts_still_works_on_sqlite(tmp_db):
    """The supported path is unchanged."""
    assert database._init_fts(database.engine) is True


def test_unexpected_query_errors_are_not_swallowed(tmp_db, monkeypatch):
    """Only OperationalError means "no FTS5". Anything else is a real bug.

    The bare except made a broken query, a mapping error or a corrupt index
    indistinguishable from a missing index — all silently returned substring
    results instead.
    """
    session = database.get_session()
    try:
        def boom(*args, **kwargs):
            raise RuntimeError("something genuinely broken")

        monkeypatch.setattr(session, "execute", boom)
        with pytest.raises(RuntimeError, match="genuinely broken"):
            database.search_summaries(session, "pension")
    finally:
        session.close()


def test_operational_error_still_falls_back(tmp_db, caplog):
    """A SQLite build without FTS5 must still degrade gracefully — but loudly.

    Only the FTS5 statement is made to fail; the ILIKE fallback runs for real,
    which is the behaviour that has to survive.
    """
    session = database.get_session()
    original = session.execute

    def fail_only_fts(statement, *args, **kwargs):
        if "summaries_fts" in str(statement):
            raise OperationalError(
                str(statement), {}, Exception("no such table: summaries_fts"))
        return original(statement, *args, **kwargs)

    try:
        session.execute = fail_only_fts
        with caplog.at_level(logging.WARNING, logger="database"):
            result = database.search_summaries(session, "pension")

        assert result == []  # empty corpus — but it fell back instead of raising
        assert "using substring search" in caplog.text, \
            "the fallback must be logged, not silent"
    finally:
        session.execute = original
        session.close()
