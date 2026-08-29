"""Shared pytest fixtures for the insights and RFP test suites.

Each test runs against an isolated SQLite file. We do NOT reload the
``database`` module (that orphans the existing insights/* references
to the ORM classes and breaks SQLAlchemy's mapper registry). Instead
we rebuild ``database.engine`` / ``SessionLocal`` per test so the same
class objects keep working — just bound to a fresh DB file.

Mock mode is forced on for both Anthropic-via-insights and the
LLM-via-RFP-pipeline so no real outbound traffic is generated.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Ensure repo root is on sys.path for imports like `from database import ...`
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pdf_store


@pytest.fixture(autouse=True)
def _force_mock_llm(monkeypatch):
    """RFP pipeline: all tests run with the LLM mocked."""
    monkeypatch.setenv("LLM_MODE", "mock")
    # IPS verification (fetch_ips.verify_is_ips) honours its own flag — set
    # for the same reason: no test should make a real Anthropic call.
    monkeypatch.setenv("IPS_MODE", "mock")


@pytest.fixture(autouse=True)
def _isolated_environment(tmp_path, monkeypatch):
    """Insights suite: per-test DB, mock mode, isolated tmp dirs."""
    db_path = tmp_path / "pension_test.db"
    test_tmp = tmp_path / "ins_tmp"

    # database.py loads .env at import so DATABASE_URL reaches it whatever the
    # entry point's import order. A developer with a real DSN in .env would
    # otherwise carry it into every test — the engine below is rebound anyway,
    # but anything reading the variable directly, or spawning a subprocess,
    # would reach the live database. Tests must not depend on whose machine
    # they run on.
    monkeypatch.delenv("DATABASE_URL", raising=False)
    # Same reasoning for R2: every test in the pdf-retention plan passes
    # `cfg=` explicitly, so nothing today reaches live R2 -- this just stops
    # a *future* test that forgets and would otherwise write to the real
    # bucket if credentials happened to be sitting in the environment.
    monkeypatch.delenv("R2_ACCOUNT_ID", raising=False)
    monkeypatch.delenv("R2_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("R2_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.delenv("R2_BUCKET", raising=False)
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

    # Redirect insights.config paths to per-test tmp dirs (insights only).
    try:
        import insights.config as ic
        monkeypatch.setattr(ic, "TMP_DIR", test_tmp)
        monkeypatch.setattr(ic, "SENT_EMAILS_DIR", test_tmp / "sent_emails")
        monkeypatch.setattr(ic, "PDF_OUTPUT_DIR", test_tmp / "pdfs")

        import insights.notify as _notify
        monkeypatch.setattr(
            _notify, "_MOCK_NOTIFICATIONS_FILE",
            test_tmp / "slack_notifications.jsonl",
        )
    except ImportError:
        # insights package not present in some test contexts; ignore.
        pass

    yield

    # tmp_path is purged by pytest; nothing else to clean.


@pytest.fixture()
def r2(monkeypatch):
    """A mocked R2 bucket plus matching config.

    moto mocks the S3 API, which is what R2 exposes. Credentials are set to
    dummy values so boto3 never reaches for real ones on the developer's
    machine.

    Shared here (rather than in tests/test_pdf_store.py, where the pdf-
    retention plan's brief originally placed it) because a later task adds
    tests/test_backfill_pdf_store.py, which also needs it -- pytest fixtures
    don't cross files.

    boto3 and moto are imported here rather than at module level on purpose:
    moto[s3] lives in requirements-pipeline.txt (test/CI only), not in
    requirements.txt, so a module-level import would make the whole suite
    fail to *collect* anywhere moto is absent -- not just the R2 tests.
    """
    import boto3
    from moto import mock_aws

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    # pdf_store.client is lru_cached on the (frozen, hashable) config, so a
    # client built inside one test's moto context would otherwise be reused
    # by the next one. Clear on the way in -- not on the way out, where a
    # test that monkeypatched pdf_store.client itself has not been unwound
    # yet and the attribute is a plain function with no cache_clear.
    pdf_store.client.cache_clear()
    with mock_aws():
        cfg = pdf_store.R2Config("acct", "test", "test", "pension-documents")
        # moto only intercepts requests to endpoints it recognizes as AWS;
        # a custom S3-compatible endpoint (R2's account-scoped hostname)
        # must be registered explicitly or boto3 tries a real network call.
        # See moto.core.utils.get_equivalent_url_in_aws_domain /
        # moto.settings.get_s3_custom_endpoints.
        monkeypatch.setenv(
            "MOTO_S3_CUSTOM_ENDPOINTS",
            cfg.endpoint_url.removeprefix("https://"),
        )
        boto3.client("s3").create_bucket(Bucket=cfg.bucket)
        yield cfg


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
    """Insights suite: a single TESTPLAN row."""
    from database import Plan
    p = Plan(id="testplan", name="Test Plan", abbreviation="TEST",
            state="CA", aum_billions=100.0)
    session.add(p)
    session.commit()
    return p


@pytest.fixture
def tmp_db(tmp_path, monkeypatch):
    """RFP suite: standalone fresh SQLite, rebinds database engine.

    Note: ``_isolated_environment`` (autouse) has already done this once.
    Tests that explicitly use ``tmp_db`` get a *second* fresh DB at a
    different path — this preserves backward compatibility with RFP-suite
    fixtures that depend on the path being ``test.db``.
    """
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("DB_PATH", str(db_path))

    import database

    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    database.engine = engine
    database.SessionLocal = sessionmaker(bind=engine)
    database.Base.metadata.create_all(engine)
    return db_path


@pytest.fixture
def seeded_session(tmp_db):
    """RFP suite: tmp_db plus two seeded plans (calpers, calstrs)."""
    import database
    s = database.get_session()
    try:
        s.add_all([
            database.Plan(id="calpers", name="CalPERS",
                          abbreviation="CalPERS", state="CA", aum_billions=502),
            database.Plan(id="calstrs", name="CalSTRS",
                          abbreviation="CalSTRS", state="CA", aum_billions=350),
        ])
        s.commit()
        yield s
    finally:
        s.close()
