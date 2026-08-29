"""R2 document store: hashing, key format, and config resolution.

The store is content-addressed because R2 has no S3-style object
versioning (see the 2026-07-08 db-to-r2 spec, still true). The digest IS
the key, so these tests pin the exact format other code depends on.
"""
from __future__ import annotations

import boto3
import pytest

import pdf_store


def test_sha256_bytes_is_lowercase_hex():
    # Known-answer test: sha256(b"hello") is a fixed, published value.
    digest = pdf_store.sha256_bytes(b"hello")
    assert digest == (
        "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    )
    assert digest == digest.lower()


def test_key_for_uses_documents_prefix():
    assert pdf_store.key_for("abc123") == "documents/abc123.pdf"


def test_config_from_env_returns_none_when_incomplete(monkeypatch):
    for var in ("R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID",
                "R2_SECRET_ACCESS_KEY", "R2_BUCKET"):
        monkeypatch.delenv(var, raising=False)
    assert pdf_store.config_from_env() is None


def test_config_from_env_reads_all_four(monkeypatch):
    monkeypatch.setenv("R2_ACCOUNT_ID", "acct")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "secret")
    monkeypatch.setenv("R2_BUCKET", "pension-documents")
    cfg = pdf_store.config_from_env()
    assert cfg is not None
    assert cfg.account_id == "acct"
    assert cfg.access_key_id == "key"
    assert cfg.secret_access_key == "secret"
    assert cfg.bucket == "pension-documents"


def test_put_returns_digest_and_stores_object(r2):
    sha = pdf_store.put(r2, b"%PDF-1.4 fake")
    assert sha == pdf_store.sha256_bytes(b"%PDF-1.4 fake")
    assert pdf_store.exists(r2, sha)


def test_put_is_idempotent(r2):
    """Same bytes twice -> one object, same key. This is what replaces
    versioning: an identical re-upload must not create a duplicate."""
    first = pdf_store.put(r2, b"same bytes")
    second = pdf_store.put(r2, b"same bytes")
    assert first == second
    listing = boto3.client("s3").list_objects_v2(Bucket=r2.bucket)
    assert listing["KeyCount"] == 1


def test_different_bytes_get_different_keys(r2):
    """A restated document becomes a new object rather than clobbering the
    original -- the property content-addressing buys in place of versioning."""
    a = pdf_store.put(r2, b"version one")
    b = pdf_store.put(r2, b"version two")
    assert a != b
    assert pdf_store.exists(r2, a) and pdf_store.exists(r2, b)


def test_get_round_trips(r2):
    sha = pdf_store.put(r2, b"round trip me")
    assert pdf_store.get(r2, sha) == b"round trip me"


def test_get_raises_on_digest_mismatch(r2):
    """Corrupt bytes must raise, never return silently.

    A silent return would let a corrupted object flow into an extractor and
    be recorded as a real (wrong) extraction.
    """
    sha = pdf_store.sha256_bytes(b"honest bytes")
    boto3.client("s3").put_object(
        Bucket=r2.bucket, Key=pdf_store.key_for(sha), Body=b"tampered")
    with pytest.raises(pdf_store.DigestMismatch):
        pdf_store.get(r2, sha)


def test_exists_is_false_for_absent_key(r2):
    assert not pdf_store.exists(r2, "0" * 64)
