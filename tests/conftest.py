"""Pytest fixtures for the insights test suite.

Each test runs against an isolated SQLite file. We do NOT reload the
``database`` module (that orphans the existing insights/* references
to the ORM classes and breaks SQLAlchemy's mapper registry). Instead
we rebuild ``database.engine`` / ``SessionLocal`` per test so the same
class objects keep working — just bound to a fresh DB file.

Mock mode is forced on so no real Anthropic / Resend / Slack traffic
is generated.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


@pytest.fixture(autouse=True)
def _isolated_environment(tmp_path, monkeypatch):
    db_path = tmp_path / "pension_test.db"
    test_tmp = tmp_path / "ins_tmp"

    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("INSIGHTS_MODE", "mock")
    monkeypatch.setenv("APPROVAL_BASE_URL", "https://test.local")
    monkeypatch.setenv("APPROVAL_TOKEN_TTL_DAYS", "7")
    monkeypatch.setenv("APPROVAL_REMINDER_HOURS", "72")
    monkeypatch.setenv("APPROVAL_EMAIL_RECIPIENT", "test@test.local")
    monkeypatch.setenv("APPROVAL_EMAIL_FROM", "noreply@test.local")
    monkeypatch.setenv("RESEND_API_KEY", "")
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "")

    # Repoint the database module's engine + sessionmaker without
    # reloading the module (which would orphan the ORM classes).
    import database
    new_engine = create_engine(f"sqlite:///{db_path}", echo=False)
    new_sessionmaker = sessionmaker(bind=new_engine)
    monkeypatch.setattr(database, "engine", new_engine)
    monkeypatch.setattr(database, "SessionLocal", new_sessionmaker)
    monkeypatch.setattr(database, "DB_PATH", str(db_path))

    database.init_db()

    # Redirect insights.config paths to per-test tmp dirs.
    import insights.config as ic
    monkeypatch.setattr(ic, "TMP_DIR", test_tmp)
    monkeypatch.setattr(ic, "SENT_EMAILS_DIR", test_tmp / "sent_emails")
    monkeypatch.setattr(ic, "PDF_OUTPUT_DIR", test_tmp / "pdfs")

    # The notify module captured the old TMP_DIR at import — repoint it.
    import insights.notify as _notify
    monkeypatch.setattr(
        _notify, "_MOCK_NOTIFICATIONS_FILE",
        test_tmp / "slack_notifications.jsonl",
    )

    yield

    # tmp_path is purged by pytest; nothing else to clean.


@pytest.fixture()
def session():
    from database import get_session
    s = get_session()
    try:
        yield s
    finally:
        s.close()


@pytest.fixture()
def seed_plan(session):
    from database import Plan
    p = Plan(id="testplan", name="Test Plan", abbreviation="TEST",
            state="CA", aum_billions=100.0)
    session.add(p)
    session.commit()
    return p
