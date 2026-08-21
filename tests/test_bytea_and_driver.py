"""Two things that only break on Postgres, tested where they can be tested.

Both are consequences of the same trap: `postgresql://` — the scheme every
managed provider hands out — selects psycopg2 in SQLAlchemy 2.0, not the
`psycopg[binary]` this project installs. If psycopg2 is absent that is a loud
ImportError; if it is present it returns BYTEA as `memoryview`, which
GzippedText did not match, so `extracted_text` came back as a memoryview
instead of str and nothing complained.

The URL normaliser and the TypeDecorator are both pure functions of their
input, so neither test needs a Postgres — the whole point is that they are
reachable from the SQLite suite that runs on every push.
"""

from __future__ import annotations

import gzip
import types

import database


def test_memoryview_bytea_is_decompressed_to_str():
    """psycopg2 hands BYTEA back as memoryview; it must still yield str."""
    col = database.GzippedText()
    blob = gzip.compress("pension board investment minutes".encode("utf-8"))

    out = col.process_result_value(memoryview(blob), None)

    assert isinstance(out, str), f"got {type(out).__name__}, not str"
    assert out == "pension board investment minutes"


def test_uncompressed_memoryview_is_still_decoded():
    """Legacy rows predate the gzip wrapper and must survive the same path."""
    col = database.GzippedText()
    out = col.process_result_value(memoryview(b"legacy plain text"), None)
    assert out == "legacy plain text"


def test_bytes_and_str_paths_are_unchanged():
    col = database.GzippedText()
    blob = gzip.compress("hello".encode("utf-8"))
    assert col.process_result_value(blob, None) == "hello"
    assert col.process_result_value(bytearray(blob), None) == "hello"
    assert col.process_result_value("already text", None) == "already text"
    assert col.process_result_value(None, None) is None


def test_bare_postgresql_scheme_is_pinned_to_psycopg3():
    assert database.normalise_pg_url(
        "postgresql://u:p@host/db") == "postgresql+psycopg://u:p@host/db"
    assert database.normalise_pg_url(
        "postgres://u:p@host/db") == "postgresql+psycopg://u:p@host/db"


def test_an_explicit_driver_is_left_alone():
    for url in ("postgresql+psycopg://u@h/db",
                "postgresql+psycopg2://u@h/db",
                "sqlite:///db/pension.db"):
        assert database.normalise_pg_url(url) == url


def test_query_string_survives_normalisation():
    """Neon URLs carry ?sslmode=require; losing it breaks the connection."""
    assert database.normalise_pg_url(
        "postgresql://u:p@h/db?sslmode=require"
    ) == "postgresql+psycopg://u:p@h/db?sslmode=require"


def test_pg_search_index_is_not_attempted_on_sqlite():
    """The GIN DDL is invalid on SQLite, which every other test runs on."""
    fake = types.SimpleNamespace(dialect=types.SimpleNamespace(name="sqlite"))
    assert database._init_pg_search_index(fake) is False
