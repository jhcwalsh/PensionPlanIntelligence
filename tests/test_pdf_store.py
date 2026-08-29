"""R2 document store: hashing, key format, and config resolution.

The store is content-addressed because R2 has no S3-style object
versioning (see the 2026-07-08 db-to-r2 spec, still true). The digest IS
the key, so these tests pin the exact format other code depends on.
"""
from __future__ import annotations

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
