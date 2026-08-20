"""Datetime discipline: UTC-aware everywhere, enforced by a shrinking list.

SQLite cannot verify timezone semantics (it ignores DateTime(timezone=True)
entirely), so these are static checks. Real round-trip semantics are tested
in tests/postgres/, which only runs when TEST_POSTGRES_URL is set.

See docs/superpowers/plans/2026-08-19-datetime-audit.md for why.
"""

from __future__ import annotations

import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
SKIP_DIRS = {".venv", ".claude", "build", "tests", "node_modules",
             "__pycache__", "tmp", "db"}

# Every file that still calls the banned naive constructor. This list may only
# ever SHRINK. Delete an entry when you convert that file; the ratchet test
# fails if you leave a stale entry behind.
KNOWN_OFFENDERS = {
    "app.py", "backfill_downloads.py", "cafr_year_check.py", "database.py",
    "discover_video_sources.py", "export_cafr_summaries.py",
    "extract_cafr_investments.py", "extractor.py", "fetch_cafr.py",
    "fetcher.py", "generate_notes.py", "insights/approval.py",
    "insights/compose.py", "insights/config.py", "insights/cycle_common.py",
    "insights/daily.py", "insights/subscribers.py", "insights/weekly.py",
    "pipeline.py", "publish_notes.py", "queries.py", "refresh_cafrs.py",
    "refresh_ips.py", "retry_asrs.py", "run_report.py",
    "scripts/backfill_april_monthly.py", "scripts/backfill_extraction_details.py",
    "scripts/backfill_pruned_documents.py", "scripts/build_manager_roster.py",
    "scripts/cleanup_video_sources.py", "scripts/hydrate_recording_metadata.py",
    "scripts/notify_failure.py", "scripts/probe_scrape.py",
    "scripts/prune_pre_2026_docs.py", "scripts/prune_pre_2026_failed_docs.py",
    "scripts/send_publication_notice.py", "scripts/send_test_email.py",
    "summarizer.py", "twin_builder.py",
}


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
