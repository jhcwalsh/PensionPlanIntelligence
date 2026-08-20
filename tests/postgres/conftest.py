"""Postgres-only tests. Skipped unless TEST_POSTGRES_URL is set.

These exist because SQLite cannot verify the things the migration depends on.
It ignores DateTime(timezone=True) entirely, and it has no tsvector. Every
other test in this repo runs on SQLite and is therefore blind to both.

CI supplies the URL via a service container (.github/workflows/test.yml).
Locally they skip, so `pytest tests/` stays fast and Docker-free.

See docs/superpowers/plans/2026-08-19-datetime-audit.md (Finding 2) and
docs/superpowers/specs/2026-08-19-portal-readiness-design.md (§8).
"""

from __future__ import annotations

import os

import pytest
import sqlalchemy as sa

URL = os.environ.get("TEST_POSTGRES_URL")


@pytest.fixture()
def pg_engine():
    """A throwaway Postgres with the real schema created from Base.metadata.

    Skips rather than fails when TEST_POSTGRES_URL is unset. The skip lives
    here rather than in a conftest-level `pytestmark`, because pytestmark in a
    conftest does not propagate to test modules — it silently does nothing.

    Note this deliberately does NOT call database.init_db(): that also runs
    _init_fts, whose SQLite-only CREATE VIRTUAL TABLE is the subject of a
    separate test rather than something to paper over here.
    """
    if not URL:
        pytest.skip("TEST_POSTGRES_URL not set — Postgres tests skipped")

    import database

    engine = sa.create_engine(URL, future=True)
    database.Base.metadata.drop_all(engine)
    database.Base.metadata.create_all(engine)
    yield engine
    database.Base.metadata.drop_all(engine)
    engine.dispose()
