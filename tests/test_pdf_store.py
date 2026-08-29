"""R2 document store: hashing, key format, and config resolution.

The store is content-addressed because R2 has no S3-style object
versioning (see the 2026-07-08 db-to-r2 spec, still true). The digest IS
the key, so these tests pin the exact format other code depends on.
"""
from __future__ import annotations

import boto3
import pytest

import database
import pdf_store
from database import Document, Plan, get_session


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


def test_exists_reraises_non_404_client_errors(r2, monkeypatch):
    """A credentials error must not masquerade as "object missing".

    If exists() swallowed a 403 the same way it swallows a 404, put() would
    proceed on a false premise -- treating "access denied" as "not yet
    uploaded" -- and fail later with a misleading error, right at the moment
    (first-time R2 credential setup) an operator most needs the real cause.
    """
    from botocore.exceptions import ClientError

    class _DeniedClient:
        def head_object(self, **kwargs):
            raise ClientError(
                {"Error": {"Code": "403", "Message": "Forbidden"}},
                "HeadObject",
            )

    monkeypatch.setattr(pdf_store, "client", lambda cfg: _DeniedClient())
    with pytest.raises(ClientError):
        pdf_store.exists(r2, "0" * 64)


def _seed_doc(session, url="https://x/a.pdf", local_path=None):
    if session.get(Plan, "p1") is None:
        session.add(Plan(id="p1", name="P", abbreviation="P", state="CA"))
        session.flush()
    doc = Document(plan_id="p1", url=url, filename="a.pdf",
                   doc_type="board_pack", local_path=local_path)
    session.add(doc)
    session.commit()
    return doc


def test_store_document_records_sha_and_timestamp(r2, tmp_db, tmp_path):
    pdf = tmp_path / "a.pdf"
    pdf.write_bytes(b"%PDF-1.4 stored")
    session = get_session()
    try:
        doc = _seed_doc(session, local_path=str(pdf))
        sha = pdf_store.store_document(session, doc, pdf, cfg=r2)
        assert sha == pdf_store.sha256_bytes(b"%PDF-1.4 stored")
        assert doc.content_sha256 == sha
        assert doc.r2_uploaded_at is not None
        assert pdf_store.exists(r2, sha)
    finally:
        session.close()


def test_store_document_is_non_fatal_on_upload_failure(r2, tmp_db, tmp_path,
                                                       monkeypatch):
    """An R2 outage must not fail the caller.

    This is the test that protects the daily pipeline: retention is additive
    to fetching, and must never become a new way for the fetch to break.
    """
    pdf = tmp_path / "a.pdf"
    pdf.write_bytes(b"%PDF-1.4 boom")

    def explode(*a, **k):
        raise RuntimeError("R2 is down")

    monkeypatch.setattr(pdf_store, "put", explode)

    session = get_session()
    try:
        doc = _seed_doc(session, local_path=str(pdf))
        result = pdf_store.store_document(session, doc, pdf, cfg=r2)
        assert result is None
        assert doc.content_sha256 is None      # row still usable
    finally:
        session.close()


def test_store_document_is_non_fatal_on_missing_file(r2, tmp_db, tmp_path):
    """'Never raises' must hold for every statement in the try block, not
    just the R2 call.

    The entire daily pipeline depends on store_document never raising. The
    existing upload-failure test only exercises a failure inside put(); this
    covers a failure earlier in the same try block, before any network call
    is made, by pointing at a path that does not exist so read_bytes()
    raises FileNotFoundError first.
    """
    missing = tmp_path / "does_not_exist.pdf"
    session = get_session()
    try:
        doc = _seed_doc(session, local_path=str(missing))
        result = pdf_store.store_document(session, doc, missing, cfg=r2)
        assert result is None
        assert doc.content_sha256 is None      # row still usable
    finally:
        session.close()


def test_store_document_skips_when_r2_unconfigured(tmp_db, tmp_path,
                                                   monkeypatch):
    """No credentials (local dev) -> skip quietly, don't raise."""
    for var in ("R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID",
                "R2_SECRET_ACCESS_KEY", "R2_BUCKET"):
        monkeypatch.delenv(var, raising=False)
    pdf = tmp_path / "a.pdf"
    pdf.write_bytes(b"%PDF-1.4 x")
    session = get_session()
    try:
        doc = _seed_doc(session, local_path=str(pdf))
        assert pdf_store.store_document(session, doc, pdf) is None
    finally:
        session.close()


def test_open_local_or_remote_prefers_local(r2, tmp_db, tmp_path):
    pdf = tmp_path / "local.pdf"
    pdf.write_bytes(b"%PDF-1.4 local")
    session = get_session()
    try:
        doc = _seed_doc(session, local_path=str(pdf))
        got = pdf_store.open_local_or_remote(doc, cfg=r2)
        assert got == pdf                       # the original, not a copy
    finally:
        session.close()


def test_open_local_or_remote_falls_back_to_r2(r2, tmp_db, tmp_path):
    """The case that matters: the local file is gone (2,633 documents are
    already in this state) but the object is retained."""
    sha = pdf_store.put(r2, b"%PDF-1.4 remote")
    session = get_session()
    try:
        doc = _seed_doc(session, local_path=str(tmp_path / "missing.pdf"))
        doc.content_sha256 = sha
        session.commit()
        got = pdf_store.open_local_or_remote(doc, cfg=r2)
        assert got.read_bytes() == b"%PDF-1.4 remote"
    finally:
        session.close()


def test_open_local_or_remote_raises_when_nowhere(r2, tmp_db, tmp_path):
    session = get_session()
    try:
        doc = _seed_doc(session, local_path=str(tmp_path / "gone.pdf"))
        with pytest.raises(FileNotFoundError):
            pdf_store.open_local_or_remote(doc, cfg=r2)
    finally:
        session.close()
