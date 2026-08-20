"""The one-shot UTC stamp for rows written before the columns went aware.

Audit finding (2026-08-19): all 45 populated DateTime columns hold naive
values, and no writer ever used local time, so a wholesale stamp is correct.
"""

from __future__ import annotations

import sqlite3

from scripts.backfill_utc_datetimes import stamp_utc


def _make_db(tmp_path):
    db = tmp_path / "t.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE pipeline_runs (run_id TEXT PRIMARY KEY, "
                "started_at TEXT, completed_at TEXT, status TEXT)")
    con.execute("INSERT INTO pipeline_runs VALUES "
                "('a', '2026-08-19 11:00:00.000000', NULL, 'done')")
    con.execute("INSERT INTO pipeline_runs VALUES "
                "('b', '2026-08-19 12:00:00.000000', "
                "'2026-08-19 12:30:00.000000', 'done')")
    con.commit()
    con.close()
    return db


def test_stamps_naive_values(tmp_path):
    db = _make_db(tmp_path)
    changed = stamp_utc(str(db))

    assert changed["pipeline_runs.started_at"] == 2
    assert changed["pipeline_runs.completed_at"] == 1

    con = sqlite3.connect(db)
    rows = dict(con.execute("SELECT run_id, started_at FROM pipeline_runs"))
    con.close()
    assert rows["a"] == "2026-08-19 11:00:00.000000+00:00"
    assert rows["b"] == "2026-08-19 12:00:00.000000+00:00"


def test_leaves_nulls_alone(tmp_path):
    db = _make_db(tmp_path)
    stamp_utc(str(db))
    con = sqlite3.connect(db)
    value = con.execute(
        "SELECT completed_at FROM pipeline_runs WHERE run_id='a'").fetchone()[0]
    con.close()
    assert value is None, "a NULL must stay NULL, not become '+00:00'"


def test_is_idempotent(tmp_path):
    """Re-running must be a no-op — the same contract as the gzip migration."""
    db = _make_db(tmp_path)
    stamp_utc(str(db))
    second = stamp_utc(str(db))
    assert second["pipeline_runs.started_at"] == 0
    assert second["pipeline_runs.completed_at"] == 0

    con = sqlite3.connect(db)
    value = con.execute(
        "SELECT started_at FROM pipeline_runs WHERE run_id='a'").fetchone()[0]
    con.close()
    assert value == "2026-08-19 11:00:00.000000+00:00", "double-stamped"


def test_skips_tables_absent_from_the_file(tmp_path):
    """The model set is wider than any given DB file; missing tables are fine."""
    db = _make_db(tmp_path)
    changed = stamp_utc(str(db))
    assert "documents.downloaded_at" not in changed
