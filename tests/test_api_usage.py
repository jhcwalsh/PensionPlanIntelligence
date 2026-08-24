"""Usage rows, and the rule that recording never breaks a job.

A lost usage row is a lost measurement. A summary lost because the measurement
failed is a lost day of work — so every failure here is swallowed and logged,
and each of the three ways it can fail has a test.
"""

from __future__ import annotations

import logging
import types
from decimal import Decimal

import costs
import database


def _usage(i=1000, o=100, cw=0, cr=0):
    return types.SimpleNamespace(
        input_tokens=i, output_tokens=o,
        cache_creation_input_tokens=cw, cache_read_input_tokens=cr)


def test_a_call_is_recorded_with_its_cost(tmp_db):
    row = database.record_api_usage("claude-haiku-4-5-20251001", _usage())
    assert row is not None
    assert row.input_tokens == 1000 and row.output_tokens == 100
    assert Decimal(str(row.cost_usd)) > 0


def test_the_operation_label_is_taken_from_the_context(tmp_db):
    with costs.track("summarize", run_id="run-7"):
        row = database.record_api_usage("claude-haiku-4-5-20251001", _usage())
    assert row.operation == "summarize"
    assert row.run_id == "run-7"


def test_calls_outside_a_track_block_are_still_recorded(tmp_db):
    """Unattributed spend is the spend most worth seeing — it is the call
    nobody remembered to label."""
    row = database.record_api_usage("claude-haiku-4-5-20251001", _usage())
    assert row.operation == "unattributed"


def test_an_unknown_model_does_not_break_the_caller(tmp_db, caplog):
    """A model bump must not take the pipeline down for want of a price."""
    with caplog.at_level(logging.WARNING, logger="database"):
        row = database.record_api_usage("claude-future-9", _usage())
    assert row is None
    assert "no price" in caplog.text.lower()


def test_a_database_failure_does_not_break_the_caller(tmp_db, monkeypatch,
                                                      caplog):
    """The whole point: measurement is subordinate to the work."""
    def boom(*a, **k):
        raise RuntimeError("database on fire")

    monkeypatch.setattr(database, "get_session", boom)
    with caplog.at_level(logging.WARNING, logger="database"):
        assert database.record_api_usage(
            "claude-haiku-4-5-20251001", _usage()) is None
    assert "database on fire" in caplog.text


def test_cache_tokens_are_stored_separately(tmp_db):
    """So a later question — 'is caching paying for itself?' — is answerable."""
    row = database.record_api_usage("claude-sonnet-4-6", _usage(cw=5000, cr=20000))
    assert row.cache_write_tokens == 5000
    assert row.cache_read_tokens == 20000


def test_the_cost_reflects_the_cache_tokens(tmp_db):
    """Storing them but not pricing them would understate every cached call."""
    plain = database.record_api_usage("claude-sonnet-4-6", _usage())
    cached = database.record_api_usage("claude-sonnet-4-6", _usage(cr=100_000))
    assert Decimal(str(cached.cost_usd)) > Decimal(str(plain.cost_usd))


def test_occurred_at_is_declared_timezone_aware():
    """Asserted on the column, not on a value read back.

    record_api_usage refreshes the row, and SQLite strips the offset on write,
    so the value comes back naive here however the column is declared. The
    behaviour is verified against a real Postgres in
    tests/postgres/test_api_usage_semantics.py.
    """
    assert database.ApiUsage.__table__.c.occurred_at.type.timezone is True


def test_the_default_is_aware_before_it_is_stored(tmp_db):
    """The value the code produces is aware; only SQLite's round-trip is not."""
    default = database.ApiUsage.__table__.c.occurred_at.default.arg
    assert default(None).tzinfo is not None


def test_the_table_is_in_the_metadata():
    """init_db() creates it on both backends; there is no migration step."""
    assert "api_usage" in database.Base.metadata.tables
