"""db_sync: R2-backed pull/push with optimistic concurrency.

Uses moto's S3 mock as a stand-in for R2 (same S3 API surface we use).
The concurrency guard under test is the generation check (re-read
manifest before PUT); the If-Match header is a second, real-R2-only
belt-and-braces verified manually in Task 8.
"""
import json

import boto3
import pytest
from moto import mock_aws

from scripts import db_sync


@pytest.fixture
def r2(monkeypatch, tmp_path):
    with mock_aws():
        # Set env vars so enabled() is True.
        monkeypatch.setenv("R2_ENDPOINT", "moto")
        monkeypatch.setenv("R2_ACCESS_KEY_ID", "test")
        monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "test")
        monkeypatch.setenv("R2_BUCKET", "pension-db")
        db_sync._reset_client_cache()
        # Monkeypatch _s3() to use boto3 directly so moto intercepts.
        monkeypatch.setattr(db_sync, "_s3", lambda: boto3.client("s3"))
        boto3.client("s3").create_bucket(Bucket="pension-db")
        yield tmp_path


def _seed(r2, content=b"gen1-bytes"):
    src = r2 / "seed.db"
    src.write_bytes(content)
    return db_sync.push(src, uploaded_by="test-seed")


def test_disabled_without_env(monkeypatch):
    monkeypatch.delenv("R2_BUCKET", raising=False)
    db_sync._reset_client_cache()
    assert db_sync.enabled() is False
    assert db_sync.pull("/nonexistent/nothing.db") is False


def test_push_then_pull_roundtrip(r2):
    gen = _seed(r2, b"hello database")
    assert gen == 1
    dest = r2 / "local.db"
    assert db_sync.pull(dest) is True
    assert dest.read_bytes() == b"hello database"


def test_pull_noop_when_generation_matches(r2):
    _seed(r2)
    dest = r2 / "local.db"
    db_sync.pull(dest)
    assert db_sync.pull(dest) is False  # second pull: same generation


def test_push_conflict_raises_and_preserves_winner(r2):
    _seed(r2)
    a, b = r2 / "a.db", r2 / "b.db"
    db_sync.pull(a); db_sync.pull(b)
    a.write_bytes(b"writer-A")
    b.write_bytes(b"writer-B")
    assert db_sync.push(a, uploaded_by="A") == 2
    with pytest.raises(db_sync.SyncConflict):
        db_sync.push(b, uploaded_by="B")  # B's sidecar still at gen 1
    fresh = r2 / "fresh.db"
    db_sync.pull(fresh)
    assert fresh.read_bytes() == b"writer-A"  # winner intact


def test_conflict_loser_can_repull_and_push(r2):
    _seed(r2)
    b = r2 / "b.db"
    db_sync.pull(b)
    a = r2 / "a.db"
    db_sync.pull(a); a.write_bytes(b"writer-A"); db_sync.push(a, "A")
    with pytest.raises(db_sync.SyncConflict):
        b.write_bytes(b"writer-B"); db_sync.push(b, "B")
    db_sync.pull(b)              # re-pull: now at gen 2
    b.write_bytes(b"writer-B2")  # re-apply work
    assert db_sync.push(b, "B") == 3


def test_snapshot_copies_current(r2):
    _seed(r2, b"snap-me")
    key = db_sync.snapshot()
    assert key.startswith("snapshots/") and key.endswith(".db")
    body = boto3.client("s3").get_object(Bucket="pension-db", Key=key)["Body"].read()
    assert body == b"snap-me"


def test_push_prunes_old_versions(r2):
    src = r2 / "seed.db"
    for i in range(12):
        src.write_bytes(f"gen{i}".encode())
        if i == 0:
            db_sync.push(src, "seed")
        else:
            db_sync.pull(r2 / "sync.db"); db_sync.push(src, "seed")
    keys = [o["Key"] for o in boto3.client("s3").list_objects_v2(
        Bucket="pension-db", Prefix="versions/")["Contents"]]
    assert len(keys) == 10  # keeps last 10 generations
