"""TwinSnapshot lifecycle: hash-skip writes, changed-facet diffs, pruning."""
import json
from datetime import datetime, timedelta

from database import TwinSnapshot, get_session, get_twin_snapshot, init_db
import twin_builder


def _twin(facets):
    return {"schema_version": "twin_v0", "plan_id": "testplan",
            "facets": facets, "completeness": {}, "freshness": {}}


def test_save_writes_then_skips_identical(tmp_db):
    session = get_session()
    assert twin_builder.save_snapshot(session, "testplan", _twin({"identity": {"name": "T"}})) is True
    assert twin_builder.save_snapshot(session, "testplan", _twin({"identity": {"name": "T"}})) is False
    assert session.query(TwinSnapshot).count() == 1
    session.close()


def test_changed_facets_diff_names_changed_keys(tmp_db):
    session = get_session()
    twin_builder.save_snapshot(session, "testplan",
                               _twin({"identity": {"name": "T"}, "allocation": {"rows": []}}))
    twin_builder.save_snapshot(session, "testplan",
                               _twin({"identity": {"name": "T"}, "allocation": {"rows": [1]}}))
    latest = get_twin_snapshot(session, "testplan")
    assert json.loads(latest.changed_facets) == ["allocation"]
    session.close()


def test_get_twin_snapshot_as_of(tmp_db):
    session = get_session()
    twin_builder.save_snapshot(session, "testplan", _twin({"identity": {"name": "old"}}))
    old = get_twin_snapshot(session, "testplan")
    old.built_at = datetime(2026, 1, 15)
    session.commit()
    twin_builder.save_snapshot(session, "testplan", _twin({"identity": {"name": "new"}}))
    at_feb = get_twin_snapshot(session, "testplan", as_of=datetime(2026, 2, 1))
    assert json.loads(at_feb.facets)["facets"]["identity"]["name"] == "old"
    assert json.loads(get_twin_snapshot(session, "testplan").facets)["facets"]["identity"]["name"] == "new"
    session.close()


def test_prune_keeps_8_plus_month_firsts(tmp_db):
    session = get_session()
    # 14 distinct snapshots, two per month Jan..Jul 2026
    for i in range(14):
        twin_builder.save_snapshot(session, "testplan", _twin({"identity": {"n": i}}))
        row = get_twin_snapshot(session, "testplan")
        row.built_at = datetime(2026, 1 + i // 2, 1 + (i % 2) * 14)
        session.commit()
    twin_builder.save_snapshot(session, "testplan", _twin({"identity": {"n": "final"}}))
    rows = session.query(TwinSnapshot).filter_by(plan_id="testplan").all()
    kept = {r.built_at for r in rows}
    # every month-first survives
    for month in range(1, 8):
        assert datetime(2026, month, 1) in kept
    # eight most recent survive; total is bounded (8 recent + month-firsts, overlapping)
    assert len(rows) <= 8 + 7
    session.close()


def test_get_twin_index_single_query_no_facets(tmp_db):
    """The index helper returns latest-per-plan metadata without touching
    the gzipped facets column, in a bounded number of statements."""
    from datetime import datetime as _dt
    from sqlalchemy import event
    import database
    from database import Plan, get_twin_index

    session = get_session()
    session.add_all([
        Plan(id="p1", name="Plan One", abbreviation="P1", state="CA", aum_billions=10.0),
        Plan(id="p2", name="Plan Two", abbreviation="P2", state="TX", aum_billions=5.0),
    ])
    session.commit()
    twin_builder.save_snapshot(session, "p1", _twin({"identity": {"n": 1}}))
    old = get_twin_snapshot(session, "p1")
    old.built_at = _dt(2026, 1, 1)
    session.commit()
    twin_builder.save_snapshot(session, "p1", _twin({"identity": {"n": 2}}))

    statements = []
    def _count(conn, cursor, stmt, params, context, executemany):
        statements.append(stmt)
    event.listen(database.engine, "before_cursor_execute", _count)
    try:
        rows = get_twin_index(session)
    finally:
        event.remove(database.engine, "before_cursor_execute", _count)

    assert len(rows) == 1  # p2 has no snapshot
    row = rows[0]
    assert row["plan_id"] == "p1" and row["name"] == "Plan One"
    assert row["built_at"].year != 2026 or row["built_at"] != _dt(2026, 1, 1)
    assert "facets" not in row
    assert len(statements) <= 2
    assert not any("facets" in s for s in statements)
    session.close()
