"""Datetime discipline: UTC-aware everywhere, enforced by a shrinking list.

SQLite cannot verify timezone semantics (it ignores DateTime(timezone=True)
entirely), so these are static checks. Real round-trip semantics are tested
in tests/postgres/, which only runs when TEST_POSTGRES_URL is set.

See docs/superpowers/plans/2026-08-19-datetime-audit.md for why.
"""

from __future__ import annotations

import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
SKIP_DIRS = {".venv", ".claude", "build", "tests", "node_modules",
             "__pycache__", "tmp", "db"}

# Every file that still calls the banned naive constructor. This list may only
# ever SHRINK. Delete an entry when you convert that file; the ratchet test
# fails if you leave a stale entry behind.
KNOWN_OFFENDERS: set[str] = set()


def _scan_offenders() -> dict[str, list[int]]:
    """Files calling datetime.utcnow(), ignoring comment-only mentions."""
    found: dict[str, list[int]] = {}
    for path in sorted(ROOT.rglob("*.py")):
        rel = path.relative_to(ROOT)
        if any(part in SKIP_DIRS for part in rel.parts):
            continue
        hits = [
            i for i, line in enumerate(
                path.read_text(encoding="utf-8", errors="replace").splitlines(), 1)
            if not line.lstrip().startswith("#") and "datetime.utcnow()" in line
        ]
        if hits:
            found[rel.as_posix()] = hits
    return found


def test_no_new_naive_utcnow_callers():
    """datetime.utcnow() is naive and deprecated in 3.12. No NEW file may use it."""
    new = set(_scan_offenders()) - KNOWN_OFFENDERS
    assert not new, (
        "New file(s) using naive datetime.utcnow(): " + ", ".join(sorted(new))
        + "\nUse database.utcnow() instead — it returns an aware UTC datetime.")


def test_offender_list_has_no_stale_entries():
    """The ratchet only turns one way: a converted file must leave the list."""
    stale = KNOWN_OFFENDERS - set(_scan_offenders())
    assert not stale, (
        "These files no longer call datetime.utcnow() — delete them from "
        "KNOWN_OFFENDERS: " + ", ".join(sorted(stale)))


def test_database_utcnow_is_public_and_aware():
    """The single source of truth for "now" must be aware UTC."""
    import database
    assert database.utcnow().tzinfo is not None, "utcnow() must be aware"
    assert database.utcnow().utcoffset().total_seconds() == 0, "must be UTC"
    assert database._utcnow is database.utcnow, "_utcnow must alias utcnow"


def test_no_module_defines_its_own_utcnow():
    """Four functions named _utcnow with two opposite meanings caused this bug.

    refresh_recordings/notify_new_recordings/download_recordings each defined a
    _utcnow() that STRIPPED the offset, the opposite of database._utcnow.
    """
    offenders = [
        path.relative_to(ROOT).as_posix()
        for path in sorted(ROOT.rglob("*.py"))
        if not any(p in SKIP_DIRS for p in path.relative_to(ROOT).parts)
        and path.name != "database.py"
        and "def _utcnow" in path.read_text(encoding="utf-8", errors="replace")
    ]
    assert not offenders, (
        "Local _utcnow definitions shadow database.utcnow(): " + ", ".join(offenders))


def test_all_datetime_columns_declare_timezone():
    """Every timestamp column must be TIMESTAMPTZ on Postgres.

    SQLite ignores this flag, so this metadata check is only an approximation;
    tests/postgres/test_tz_semantics.py asserts the real column type.
    """
    import sqlalchemy as sa
    import database
    naive = [
        f"{name}.{col.name}"
        for name, table in database.Base.metadata.tables.items()
        for col in table.columns
        if isinstance(col.type, sa.DateTime) and not col.type.timezone
    ]
    assert not naive, f"{len(naive)} naive DateTime column(s): " + ", ".join(naive)


def test_no_column_default_is_naive():
    """A column whose default returns a naive value writes naive values forever.

    Asserted by calling the default rather than comparing it to
    database.utcnow: SQLAlchemy wraps a zero-argument callable in one that
    takes a context, so `default.arg is database.utcnow` is False even when
    the column is correct. The behaviour is what matters anyway.
    """
    import sqlalchemy as sa
    import database
    bad = []
    for name, table in database.Base.metadata.tables.items():
        for col in table.columns:
            if not isinstance(col.type, sa.DateTime):
                continue
            default = col.default
            if default is None or not getattr(default, "is_callable", False):
                continue
            produced = default.arg(None)  # the wrapper takes an ExecutionContext
            if produced.tzinfo is None:
                bad.append(f"{name}.{col.name}")
    assert not bad, (
        f"{len(bad)} column default(s) produce naive datetimes: " + ", ".join(bad))


# ---------------------------------------------------------------------------
# as_utc — the bridge between the two engines
# ---------------------------------------------------------------------------

def test_as_utc_attaches_utc_to_naive_values():
    """SQLite returns naive reads. Every stored value is UTC, so attach it."""
    from datetime import datetime, timezone
    import database
    got = database.as_utc(datetime(2026, 8, 20, 11, 0, 0))
    assert got == datetime(2026, 8, 20, 11, 0, 0, tzinfo=timezone.utc)
    assert got.tzinfo is not None


def test_as_utc_leaves_aware_values_untouched():
    """Postgres returns aware reads. Do not shift them."""
    from datetime import datetime, timezone
    import database
    already = datetime(2026, 8, 20, 11, 0, 0, tzinfo=timezone.utc)
    assert database.as_utc(already) is already


def test_as_utc_passes_none_through():
    """Nullable columns are everywhere; None must not become a datetime."""
    import database
    assert database.as_utc(None) is None


def test_as_utc_makes_a_sqlite_read_comparable_to_utcnow(tmp_db):
    """The exact failure this exists to prevent, exercised through the ORM."""
    import database
    session = database.get_session()
    try:
        run = database.PipelineRun(started_at=database.utcnow(), status="test")
        session.add(run)
        session.commit()
        read_back = session.query(database.PipelineRun.started_at).scalar()

        # SQLite strips the offset, so the raw value cannot be compared.
        if read_back.tzinfo is None:
            with pytest.raises(TypeError):
                database.utcnow() - read_back

        # Normalised, it always can — on either engine.
        assert (database.utcnow() - database.as_utc(read_back)).total_seconds() >= 0
    finally:
        session.close()
