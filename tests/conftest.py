"""Shared pytest fixtures: temp DB, mock LLM mode."""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

import pytest

# Ensure repo root is on sys.path for imports like `from database import ...`
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture(autouse=True)
def _force_mock_llm(monkeypatch):
    """All tests run with the LLM mocked. Real Anthropic calls are forbidden."""
    monkeypatch.setenv("LLM_MODE", "mock")


@pytest.fixture
def tmp_db(tmp_path, monkeypatch):
    """
    Spin up a fresh SQLite at tmp_path and rebind the global engine/session.
    Importantly, this must run before any test code imports `database` for the
    first time within the test, because DATABASE_URL is read at module load.
    We work around that by rebinding the engine on the already-imported module.
    """
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("DB_PATH", str(db_path))

    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    import database

    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    database.engine = engine
    database.SessionLocal = sessionmaker(bind=engine)
    database.Base.metadata.create_all(engine)
    return db_path


@pytest.fixture
def seeded_session(tmp_db):
    """tmp_db plus two seeded plans (calpers, calstrs)."""
    import database
    session = database.get_session()
    try:
        session.add_all([
            database.Plan(id="calpers", name="CalPERS",
                          abbreviation="CalPERS", state="CA", aum_billions=502),
            database.Plan(id="calstrs", name="CalSTRS",
                          abbreviation="CalSTRS", state="CA", aum_billions=350),
        ])
        session.commit()
        yield session
    finally:
        session.close()
