"""Twin API: index + latest + as_of + 404."""

import json
from datetime import datetime

from fastapi.testclient import TestClient

from api.main import app
from database import Plan, get_session
import twin_builder


def _seed_snapshot(plan_id="testplan"):
    session = get_session()
    session.add(Plan(id=plan_id, name="Test Plan", abbreviation="TP",
                     state="CA", aum_billions=10.0))
    session.commit()
    plan = session.get(Plan, plan_id)
    twin_builder.save_snapshot(session, plan_id, twin_builder.build_twin(session, plan))
    session.close()


def test_twin_detail_and_index(tmp_db):
    _seed_snapshot()
    client = TestClient(app)
    r = client.get("/api/v1/twin/testplan")
    assert r.status_code == 200
    body = r.json()
    assert body["plan_id"] == "testplan"
    assert body["schema_version"] == "twin_v1"
    assert "identity" in body["facets"]
    idx = client.get("/api/v1/twins").json()
    assert idx["total"] == 1
    assert idx["results"][0]["plan_id"] == "testplan"


def test_twin_404_when_absent(tmp_db):
    client = TestClient(app)
    assert client.get("/api/v1/twin/nosuch").status_code == 404


def test_twin_as_of_filters(tmp_db):
    _seed_snapshot()
    session = get_session()
    from database import TwinSnapshot
    snap = session.query(TwinSnapshot).one()
    snap.built_at = datetime(2026, 6, 1)
    session.commit()
    session.close()
    client = TestClient(app)
    assert client.get("/api/v1/twin/testplan?as_of=2026-07-01").status_code == 200
    assert client.get("/api/v1/twin/testplan?as_of=2026-05-01").status_code == 404
