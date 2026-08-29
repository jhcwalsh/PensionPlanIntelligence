# PDF Retention (R2 Document Store) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Retain every fetched source PDF in a Cloudflare R2 bucket so re-extraction, new structured extractors, and durable links stop depending on a file that only ever existed on a CI runner.

**Architecture:** A thin `pdf_store.py` over boto3 against R2's S3-compatible endpoint, keyed by SHA-256 of the file bytes. Two new columns on `documents` record the key and upload time. The daily GHA pipeline uploads at fetch time (non-fatally). A separate resumable script backfills the existing corpus.

**Tech Stack:** Python 3.12, boto3 (R2 S3-compatible API), moto (test mocks), SQLAlchemy, pytest, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-08-29-pdf-retention-design.md`

## Global Constraints

- **Never write SQL `ALTER TABLE` migrations.** Add the SQLAlchemy column to the model and re-run `init_db()`. There is no Alembic. (CLAUDE.md)
- **`documents.extracted_text` is `deferred()`.** Any query looping over many `Document` rows and reading text must add `.options(undefer(Document.extracted_text))` or it becomes N+1. This plan's queries read `local_path`/`url`/`content_sha256` only, so they must NOT undefer.
- **Upload failure must never fail the daily pipeline.** Retention is additive; log and continue. (spec §4)
- **WAF-blocked plans** (`data/waf_blocked_plans.json`) are skipped in any re-fetch path, as everywhere else.
- **R2 has no S3-style object versioning.** Content-addressing supplies it; never overwrite a key with different bytes. (spec §3.1)
- **Bucket name:** `pension-documents`. **Key format:** `documents/<sha256>.pdf` (lowercase hex).
- **GHA secrets, declared at *job* level:** `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET`.
- **Don't run `git add .`** — the repo root has many intentional untracked scratch files. Stage by name. (CLAUDE.md)
- Tests run with `LLM_MODE=mock` (autouse in `tests/conftest.py`); this feature makes no LLM calls.

---

### Task 1: Add boto3/moto dependencies and the `pdf_store` module core

`boto3` and `moto` were removed during the Postgres cutover because they existed only for the abandoned R2-as-database route. They come back here for R2-as-object-store, which was always base spec §1's plan.

**Files:**
- Modify: `requirements.txt`
- Create: `pdf_store.py`
- Create: `tests/test_pdf_store.py`

**Interfaces:**
- Consumes: nothing (first task).
- Produces:
  - `pdf_store.sha256_bytes(data: bytes) -> str` — lowercase hex digest.
  - `pdf_store.key_for(sha: str) -> str` — returns `f"documents/{sha}.pdf"`.
  - `pdf_store.R2Config` dataclass with fields `account_id: str`, `access_key_id: str`, `secret_access_key: str`, `bucket: str`.
  - `pdf_store.config_from_env() -> R2Config | None` — returns `None` when any required var is missing.

- [ ] **Step 1: Add dependencies**

Append to `requirements.txt`:

```
boto3  # R2 object store (pdf_store.py); removed in the Postgres cutover, back for the PDF store
moto[s3]  # test-only: mocks R2's S3-compatible API in tests/test_pdf_store.py
```

Then install:

```bash
pip install boto3 "moto[s3]"
```

- [ ] **Step 2: Write the failing test**

Create `tests/test_pdf_store.py`:

```python
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
```

- [ ] **Step 3: Run test to verify it fails**

Run: `LLM_MODE=mock python -m pytest tests/test_pdf_store.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'pdf_store'`

- [ ] **Step 4: Write the minimal implementation**

Create `pdf_store.py`:

```python
"""Content-addressed PDF store on Cloudflare R2.

Source PDFs used to exist only on the GHA runner and were discarded after
extraction. That made re-extraction impossible (the 450 documents truncated
at the old 150k cap), left CAFRs stuck at "pending extract" once their file
aged off disk, and forced structured extraction to run in the same job as
the fetch.

Keys are the SHA-256 of the file bytes, not the URL or a plan/filename path.
R2 has no S3-style object versioning, so content-addressing is what supplies
it: re-uploading identical bytes is a no-op, a restated document becomes a
new object instead of clobbering the original, and two plans publishing the
same PDF share one object.

See docs/superpowers/specs/2026-08-29-pdf-retention-design.md.
"""
from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass

KEY_PREFIX = "documents"


def sha256_bytes(data: bytes) -> str:
    """Lowercase hex SHA-256 digest of `data`."""
    return hashlib.sha256(data).hexdigest()


def key_for(sha: str) -> str:
    """R2 object key for a digest."""
    return f"{KEY_PREFIX}/{sha}.pdf"


@dataclass(frozen=True)
class R2Config:
    account_id: str
    access_key_id: str
    secret_access_key: str
    bucket: str

    @property
    def endpoint_url(self) -> str:
        return f"https://{self.account_id}.r2.cloudflarestorage.com"


def config_from_env() -> R2Config | None:
    """Build config from env vars, or None if any is missing.

    Returning None rather than raising is deliberate: callers in the daily
    pipeline treat "no R2 configured" as "skip retention", not as an error.
    A local dev run with no credentials must still fetch and extract.
    """
    account_id = os.environ.get("R2_ACCOUNT_ID", "")
    access_key_id = os.environ.get("R2_ACCESS_KEY_ID", "")
    secret_access_key = os.environ.get("R2_SECRET_ACCESS_KEY", "")
    bucket = os.environ.get("R2_BUCKET", "")
    if not all([account_id, access_key_id, secret_access_key, bucket]):
        return None
    return R2Config(account_id, access_key_id, secret_access_key, bucket)
```

- [ ] **Step 5: Run test to verify it passes**

Run: `LLM_MODE=mock python -m pytest tests/test_pdf_store.py -q`
Expected: PASS (4 passed)

- [ ] **Step 6: Commit**

```bash
git add requirements.txt pdf_store.py tests/test_pdf_store.py
git commit -m "PDF store: content-addressed keys and R2 config resolution"
```

---

### Task 2: `put`, `get`, and `exists` against R2

**Files:**
- Modify: `pdf_store.py`
- Modify: `tests/test_pdf_store.py`

**Interfaces:**
- Consumes: `sha256_bytes`, `key_for`, `R2Config`, `config_from_env` (Task 1).
- Produces:
  - `pdf_store.client(cfg: R2Config)` — a boto3 S3 client bound to R2's endpoint.
  - `pdf_store.put(cfg: R2Config, data: bytes) -> str` — uploads if absent, returns the sha.
  - `pdf_store.get(cfg: R2Config, sha: str) -> bytes` — downloads, verifies digest, raises `DigestMismatch` on corruption.
  - `pdf_store.exists(cfg: R2Config, sha: str) -> bool`.
  - `pdf_store.DigestMismatch` — exception type.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_pdf_store.py`:

```python
import boto3
import pytest
from moto import mock_aws


@pytest.fixture()
def r2(monkeypatch):
    """A mocked R2 bucket plus matching config.

    moto mocks the S3 API, which is what R2 exposes. Credentials are set to
    dummy values so boto3 never reaches for real ones on the developer's
    machine.
    """
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    with mock_aws():
        cfg = pdf_store.R2Config("acct", "test", "test", "pension-documents")
        boto3.client("s3").create_bucket(Bucket=cfg.bucket)
        yield cfg


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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `LLM_MODE=mock python -m pytest tests/test_pdf_store.py -q`
Expected: FAIL — `AttributeError: module 'pdf_store' has no attribute 'put'`

- [ ] **Step 3: Write the implementation**

Add to `pdf_store.py` (after `config_from_env`):

```python
class DigestMismatch(Exception):
    """Stored bytes did not hash to the key they were stored under."""


def client(cfg: R2Config):
    """A boto3 S3 client pointed at R2's S3-compatible endpoint."""
    import boto3
    return boto3.client(
        "s3",
        endpoint_url=cfg.endpoint_url,
        aws_access_key_id=cfg.access_key_id,
        aws_secret_access_key=cfg.secret_access_key,
        region_name="auto",          # R2 ignores region but boto3 wants one
    )


def exists(cfg: R2Config, sha: str) -> bool:
    """True if an object is already stored under `sha`."""
    from botocore.exceptions import ClientError
    try:
        client(cfg).head_object(Bucket=cfg.bucket, Key=key_for(sha))
        return True
    except ClientError:
        return False


def put(cfg: R2Config, data: bytes) -> str:
    """Store `data`, returning its digest. No-op if already present.

    The existence check is what makes re-uploads free: backfill can be
    interrupted and restarted without re-sending gigabytes.
    """
    sha = sha256_bytes(data)
    if exists(cfg, sha):
        return sha
    client(cfg).put_object(
        Bucket=cfg.bucket, Key=key_for(sha), Body=data,
        ContentType="application/pdf",
    )
    return sha


def get(cfg: R2Config, sha: str) -> bytes:
    """Fetch the object stored under `sha`, verifying it on the way out.

    Verification is not paranoia: the digest is the key, so a mismatch means
    the object was replaced out of band. Returning it silently would feed
    wrong bytes to an extractor that records the result as real data.
    """
    body = client(cfg).get_object(
        Bucket=cfg.bucket, Key=key_for(sha))["Body"].read()
    actual = sha256_bytes(body)
    if actual != sha:
        raise DigestMismatch(
            f"object {key_for(sha)} hashes to {actual}, not {sha}")
    return body
```

Note the `boto3` import is inside `client()` rather than at module top: it
keeps `sha256_bytes`/`key_for`/`config_from_env` importable (and Task 1's
tests passing) on a machine where boto3 is not installed.

- [ ] **Step 4: Run tests to verify they pass**

Run: `LLM_MODE=mock python -m pytest tests/test_pdf_store.py -q`
Expected: PASS (10 passed)

- [ ] **Step 5: Commit**

```bash
git add pdf_store.py tests/test_pdf_store.py
git commit -m "PDF store: put/get/exists with digest verification"
```

---

### Task 3: Schema columns on `documents`

**Files:**
- Modify: `database.py` (the `Document` class, around line 252-285)
- Create: `tests/test_pdf_store_schema.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `Document.content_sha256: str | None`, `Document.r2_uploaded_at: datetime | None`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_pdf_store_schema.py`:

```python
"""The two retention columns on documents.

Deliberately two columns on an existing table rather than a new table: a
document has at most one stored object, so a join would buy nothing.
"""
from __future__ import annotations

from datetime import datetime, timezone

import database
from database import Document, Plan, get_session


def test_document_carries_retention_columns(tmp_db):
    session = get_session()
    try:
        session.add(Plan(id="p1", name="P", abbreviation="P", state="CA"))
        doc = Document(
            plan_id="p1", url="https://x/a.pdf", filename="a.pdf",
            doc_type="board_pack",
            content_sha256="a" * 64,
            r2_uploaded_at=datetime(2026, 8, 29, tzinfo=timezone.utc),
        )
        session.add(doc)
        session.commit()

        got = session.query(Document).one()
        assert got.content_sha256 == "a" * 64
        assert got.r2_uploaded_at.year == 2026
    finally:
        session.close()


def test_retention_columns_default_to_null(tmp_db):
    """A document with no stored object is the normal pre-backfill state and
    must not require the columns to be set."""
    session = get_session()
    try:
        session.add(Plan(id="p2", name="P2", abbreviation="P2", state="CA"))
        session.add(Document(plan_id="p2", url="https://x/b.pdf",
                             filename="b.pdf", doc_type="agenda"))
        session.commit()
        got = session.query(Document).one()
        assert got.content_sha256 is None
        assert got.r2_uploaded_at is None
    finally:
        session.close()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `LLM_MODE=mock python -m pytest tests/test_pdf_store_schema.py -q`
Expected: FAIL — `TypeError: 'content_sha256' is an invalid keyword argument for Document`

- [ ] **Step 3: Add the columns**

In `database.py`, inside `class Document`, immediately after the
`fiscal_year` column and before the `plan = relationship(...)` line, add:

```python
    # PDF retention (docs/superpowers/specs/2026-08-29-pdf-retention-design.md).
    # content_sha256 is the R2 object key; null means "not stored" -- either
    # not yet backfilled, or unrecoverable because the source URL is dead.
    # local_path stays meaningful: it is no longer the only copy, but a
    # machine that happens to have the file should still use it.
    content_sha256 = Column(String(64))
    r2_uploaded_at = Column(DateTime(timezone=True))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `LLM_MODE=mock python -m pytest tests/test_pdf_store_schema.py -q`
Expected: PASS (2 passed)

- [ ] **Step 5: Apply the schema to the live database**

`init_db()` calls `Base.metadata.create_all()`, which creates missing
*tables* but does not add columns to an existing one. Add them directly,
once:

```bash
python -c "
from sqlalchemy import text
import database
with database.engine.begin() as c:
    c.execute(text('ALTER TABLE documents ADD COLUMN IF NOT EXISTS content_sha256 VARCHAR(64)'))
    c.execute(text('ALTER TABLE documents ADD COLUMN IF NOT EXISTS r2_uploaded_at TIMESTAMPTZ'))
print('columns added')
"
```

This is the documented exception, not a migration framework: CLAUDE.md's
"never write ALTER TABLE migrations" rule is about not building a migration
*system*. Adding two nullable columns to a live table once, by hand, is the
"one-off script" the same section calls for. New databases get them from the
model via `create_all`.

- [ ] **Step 6: Verify against the live database**

```bash
python -c "
import database
from database import Document, SessionLocal
s = SessionLocal()
print(s.query(Document.content_sha256).limit(1).all())
s.close()
print('live schema OK')
"
```
Expected: prints a one-row result (value `None`) then `live schema OK`.

- [ ] **Step 7: Commit**

```bash
git add database.py tests/test_pdf_store_schema.py
git commit -m "PDF store: content_sha256 and r2_uploaded_at on documents"
```

---

### Task 4: `store_document` and `open_local_or_remote`

The two functions callers actually use. `store_document` is what the pipeline calls; `open_local_or_remote` is what extractors call so they stop caring where the PDF lives.

**Files:**
- Modify: `pdf_store.py`
- Modify: `tests/test_pdf_store.py`

**Interfaces:**
- Consumes: `put`, `get`, `exists`, `config_from_env`, `R2Config` (Tasks 1-2); `Document` columns (Task 3).
- Produces:
  - `pdf_store.store_document(session, document, path) -> str | None` — uploads, sets `content_sha256`/`r2_uploaded_at`, commits, returns the sha; returns `None` and logs on any failure or when R2 is unconfigured.
  - `pdf_store.open_local_or_remote(document, cfg=None) -> pathlib.Path` — local file if present, else pull from R2 to a temp file; raises `FileNotFoundError` if neither.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_pdf_store.py`:

```python
import pathlib

import database
from database import Document, Plan, get_session


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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `LLM_MODE=mock python -m pytest tests/test_pdf_store.py -q`
Expected: FAIL — `AttributeError: module 'pdf_store' has no attribute 'store_document'`

- [ ] **Step 3: Write the implementation**

Add to `pdf_store.py`:

```python
import logging
import pathlib
import tempfile

logger = logging.getLogger(__name__)


def store_document(session, document, path, cfg: R2Config | None = None):
    """Upload `path` and record the key on `document`. Returns the sha or None.

    Never raises. The daily pipeline calls this immediately after a fetch, and
    an R2 outage must not fail a run whose real job is fetching and
    extracting -- a null content_sha256 simply means the backfill sweeps it
    up later.
    """
    cfg = cfg or config_from_env()
    if cfg is None:
        logger.debug("R2 not configured; skipping retention for %s", document.url)
        return None
    try:
        data = pathlib.Path(path).read_bytes()
        sha = put(cfg, data)
        document.content_sha256 = sha
        document.r2_uploaded_at = utcnow()
        session.commit()
        return sha
    except Exception as e:                       # noqa: BLE001 - deliberate
        logger.warning("R2 upload failed for %s: %s", document.url, e)
        session.rollback()
        return None


def open_local_or_remote(document, cfg: R2Config | None = None) -> pathlib.Path:
    """Return a readable path for `document`'s PDF.

    Prefers a present local file (free); otherwise pulls the retained object
    to a temp file. This is the call extractors use so they stop caring
    whether the PDF survived on disk.
    """
    if document.local_path:
        local = pathlib.Path(document.local_path)
        if local.exists():
            return local

    if not document.content_sha256:
        raise FileNotFoundError(
            f"document {document.id} has no local file and no stored object")

    cfg = cfg or config_from_env()
    if cfg is None:
        raise FileNotFoundError(
            f"document {document.id} is not on disk and R2 is not configured")

    data = get(cfg, document.content_sha256)
    tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
    tmp.write(data)
    tmp.close()
    return pathlib.Path(tmp.name)
```

Add `utcnow` to the existing `database` import at the top of `pdf_store.py`:

```python
from database import utcnow
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `LLM_MODE=mock python -m pytest tests/test_pdf_store.py -q`
Expected: PASS (16 passed)

- [ ] **Step 5: Commit**

```bash
git add pdf_store.py tests/test_pdf_store.py
git commit -m "PDF store: store_document and open_local_or_remote"
```

---

### Task 5: Wire upload into the fetcher

**Files:**
- Modify: `fetcher.py:558-582` (the `Document(...)` creation block in `run_fetcher`)
- Create: `tests/test_fetcher_retention.py`

**Interfaces:**
- Consumes: `pdf_store.store_document` (Task 4).
- Produces: no new API; the fetcher now populates `content_sha256` on newly fetched documents.

- [ ] **Step 1: Write the failing test**

Create `tests/test_fetcher_retention.py`:

```python
"""The fetcher stores each PDF as it lands.

Storing at fetch time is the whole point: the window between "file exists on
the runner" and "runner is destroyed" is the only moment the bytes are
freely available. Anything later is a re-download that may 404.
"""
from __future__ import annotations

import pathlib

import fetcher
import pdf_store
from database import Document, Plan, get_session


def test_fetcher_stores_downloaded_pdf(tmp_db, tmp_path, monkeypatch):
    pdf = tmp_path / "board.pdf"
    pdf.write_bytes(b"%PDF-1.4 fetched")

    calls = []

    def fake_store(session, document, path, cfg=None):
        calls.append((document.url, pathlib.Path(path).name))
        document.content_sha256 = "f" * 64
        session.commit()
        return "f" * 64

    monkeypatch.setattr(pdf_store, "store_document", fake_store)
    monkeypatch.setattr(fetcher, "download_document",
                        lambda url, d, f: (pdf, pdf.stat().st_size))
    monkeypatch.setattr(fetcher, "discover_document_links", lambda p: [
        {"url": "https://x/board.pdf", "filename": "board.pdf",
         "doc_type": "board_pack", "meeting_date": None},
    ])
    monkeypatch.setattr(fetcher, "load_plans", lambda: [
        {"id": "p1", "abbreviation": "P1", "name": "Plan One"},
    ])

    session = get_session()
    session.add(Plan(id="p1", name="Plan One", abbreviation="P1", state="CA"))
    session.commit()
    session.close()

    fetcher.run_fetcher()

    assert calls == [("https://x/board.pdf", "board.pdf")]
    session = get_session()
    try:
        doc = session.query(Document).filter_by(url="https://x/board.pdf").one()
        assert doc.content_sha256 == "f" * 64
    finally:
        session.close()


def test_fetcher_continues_when_retention_fails(tmp_db, tmp_path, monkeypatch):
    """Retention is additive. A failed upload must still leave a usable
    document row -- otherwise an R2 outage silently costs a day's fetch."""
    pdf = tmp_path / "board.pdf"
    pdf.write_bytes(b"%PDF-1.4 fetched")

    monkeypatch.setattr(pdf_store, "store_document",
                        lambda *a, **k: None)     # simulates failure
    monkeypatch.setattr(fetcher, "download_document",
                        lambda url, d, f: (pdf, pdf.stat().st_size))
    monkeypatch.setattr(fetcher, "discover_document_links", lambda p: [
        {"url": "https://x/board.pdf", "filename": "board.pdf",
         "doc_type": "board_pack", "meeting_date": None},
    ])
    monkeypatch.setattr(fetcher, "load_plans", lambda: [
        {"id": "p1", "abbreviation": "P1", "name": "Plan One"},
    ])

    session = get_session()
    session.add(Plan(id="p1", name="Plan One", abbreviation="P1", state="CA"))
    session.commit()
    session.close()

    fetcher.run_fetcher()

    session = get_session()
    try:
        doc = session.query(Document).filter_by(url="https://x/board.pdf").one()
        assert doc.content_sha256 is None
        assert doc.extraction_status == "pending"   # still extractable
    finally:
        session.close()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `LLM_MODE=mock python -m pytest tests/test_fetcher_retention.py -q`
Expected: FAIL — `assert [] == [('https://x/board.pdf', 'board.pdf')]` (store_document never called)

- [ ] **Step 3: Wire it in**

In `fetcher.py`, add the import near the other local imports at the top:

```python
import pdf_store
```

Then in `run_fetcher`, immediately after the existing `session.commit()` that
follows `session.add(doc)` (the per-document commit around line 581), add:

```python
                # Retain the PDF while it still exists. The runner is
                # destroyed at the end of the job, and 2,633 documents in the
                # corpus already have no recoverable local file. Non-fatal by
                # design: store_document swallows and logs, so an R2 outage
                # costs retention for a day, not the fetch.
                if local_path:
                    pdf_store.store_document(session, doc, local_path)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `LLM_MODE=mock python -m pytest tests/test_fetcher_retention.py -q`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add fetcher.py tests/test_fetcher_retention.py
git commit -m "Fetcher: retain each PDF in R2 at fetch time"
```

---

### Task 6: GHA secrets at job level

**Files:**
- Modify: `.github/workflows/daily-pipeline.yml`
- Modify: `tests/test_deployment_config.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `R2_*` env available to the daily pipeline job.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_deployment_config.py`, after `test_every_db_job_receives_the_dsn`:

```python
R2_WORKFLOWS = ["daily-pipeline.yml"]
R2_VARS = ("R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID",
           "R2_SECRET_ACCESS_KEY", "R2_BUCKET")


@pytest.mark.parametrize("name", R2_WORKFLOWS)
def test_r2_credentials_declared_at_job_level(name):
    """Same rationale as the DSN: declared once for the job so a step added
    later cannot silently skip retention.

    pdf_store.config_from_env() returns None when any var is missing, and
    store_document treats None as "skip quietly" -- correct for local dev,
    invisible on a runner. Asserting the config directly is the only place
    that distinction gets caught.
    """
    for job_name, job in _jobs(name):
        env = job.get("env") or {}
        for var in R2_VARS:
            assert var in env, f"{name}:{job_name} has no job-level {var}"
            assert f"secrets.{var}" in str(env[var]), env
```

- [ ] **Step 2: Run test to verify it fails**

Run: `LLM_MODE=mock python -m pytest tests/test_deployment_config.py -k r2 -q`
Expected: FAIL — `daily-pipeline.yml:pipeline has no job-level R2_ACCOUNT_ID`

- [ ] **Step 3: Add the secrets to the workflow**

In `.github/workflows/daily-pipeline.yml`, find the `jobs.pipeline.env:`
block that already declares `DATABASE_URL` and add the four R2 vars beside
it:

```yaml
      R2_ACCOUNT_ID: ${{ secrets.R2_ACCOUNT_ID }}
      R2_ACCESS_KEY_ID: ${{ secrets.R2_ACCESS_KEY_ID }}
      R2_SECRET_ACCESS_KEY: ${{ secrets.R2_SECRET_ACCESS_KEY }}
      R2_BUCKET: ${{ secrets.R2_BUCKET }}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `LLM_MODE=mock python -m pytest tests/test_deployment_config.py -q`
Expected: PASS (all deployment-config tests)

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/daily-pipeline.yml tests/test_deployment_config.py
git commit -m "Daily pipeline: R2 credentials at job level"
```

**Note for the human operator:** the four secrets must exist in the GitHub
repo settings before the next scheduled run, and the `pension-documents`
bucket must exist in Cloudflare R2. Until then `config_from_env()` returns
`None` and retention is skipped — the pipeline still succeeds.

---

### Task 7: Backfill script

**Files:**
- Create: `scripts/backfill_pdf_store.py`
- Create: `tests/test_backfill_pdf_store.py`

**Interfaces:**
- Consumes: `pdf_store.put`, `pdf_store.exists`, `config_from_env` (Tasks 1-2); `Document.content_sha256` (Task 3).
- Produces: `backfill_pdf_store.run(limit=None, refetch=True) -> dict[str, int]` — status counts keyed `stored_local`, `stored_refetch`, `already`, `unrecoverable`, `skipped_waf`, `failed`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_backfill_pdf_store.py`:

```python
"""Backfill: local files first, then re-fetch, resumable throughout.

Ordering is not cosmetic. Local-file uploads are free and risk-free; the
re-fetch path costs bandwidth and races link rot (a 20-URL sample on
2026-08-29 found 19 still live, so ~5% is already unrecoverable and rising).
Doing the free half first means an interrupted run has still made progress.
"""
from __future__ import annotations

import pytest

import pdf_store
from database import Document, Plan, get_session
from scripts import backfill_pdf_store


def _seed(session, url, local_path=None, sha=None, plan_id="p1"):
    if session.get(Plan, plan_id) is None:
        session.add(Plan(id=plan_id, name=plan_id, abbreviation=plan_id,
                         state="CA"))
        session.flush()
    doc = Document(plan_id=plan_id, url=url, filename="d.pdf",
                   doc_type="board_pack", local_path=local_path,
                   content_sha256=sha)
    session.add(doc)
    session.commit()
    return doc


def test_uploads_local_file(r2, tmp_db, tmp_path, monkeypatch):
    pdf = tmp_path / "have.pdf"
    pdf.write_bytes(b"%PDF-1.4 have")
    session = get_session()
    _seed(session, "https://x/have.pdf", local_path=str(pdf))
    session.close()

    counts = backfill_pdf_store.run(cfg=r2, refetch=False)
    assert counts["stored_local"] == 1

    session = get_session()
    try:
        doc = session.query(Document).one()
        assert doc.content_sha256 == pdf_store.sha256_bytes(b"%PDF-1.4 have")
    finally:
        session.close()


def test_skips_already_stored(r2, tmp_db, tmp_path):
    """Resume must cost nothing: a document already carrying a sha is not
    re-read, re-hashed, or re-uploaded."""
    session = get_session()
    _seed(session, "https://x/done.pdf", sha="a" * 64)
    session.close()

    counts = backfill_pdf_store.run(cfg=r2, refetch=False)
    assert counts["already"] == 1
    assert counts["stored_local"] == 0


def test_refetches_when_local_missing(r2, tmp_db, tmp_path, monkeypatch):
    monkeypatch.setattr(backfill_pdf_store, "_fetch_bytes",
                        lambda url: b"%PDF-1.4 refetched")
    session = get_session()
    _seed(session, "https://x/gone.pdf", local_path=str(tmp_path / "nope.pdf"))
    session.close()

    counts = backfill_pdf_store.run(cfg=r2, refetch=True)
    assert counts["stored_refetch"] == 1


def test_dead_url_recorded_as_unrecoverable_and_continues(r2, tmp_db, tmp_path,
                                                          monkeypatch):
    """A 404 is a permanent fact about the corpus, not a crash. The run must
    continue and the count must be visible -- that number is the floor of
    what can never be recovered."""
    def dead(url):
        return None

    monkeypatch.setattr(backfill_pdf_store, "_fetch_bytes", dead)
    session = get_session()
    _seed(session, "https://x/dead.pdf", local_path=None)
    _seed(session, "https://x/dead2.pdf", local_path=None)
    session.close()

    counts = backfill_pdf_store.run(cfg=r2, refetch=True)
    assert counts["unrecoverable"] == 2


def test_waf_blocked_plans_are_not_refetched(r2, tmp_db, monkeypatch):
    """No runner can reach these; attempting is guaranteed waste."""
    monkeypatch.setattr(backfill_pdf_store, "_waf_blocked_plan_ids",
                        lambda: {"blocked"})
    called = []
    monkeypatch.setattr(backfill_pdf_store, "_fetch_bytes",
                        lambda url: called.append(url) or b"x")
    session = get_session()
    _seed(session, "https://x/w.pdf", plan_id="blocked")
    session.close()

    counts = backfill_pdf_store.run(cfg=r2, refetch=True)
    assert counts["skipped_waf"] == 1
    assert called == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `LLM_MODE=mock python -m pytest tests/test_backfill_pdf_store.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'scripts.backfill_pdf_store'`

- [ ] **Step 3: Write the implementation**

Create `scripts/backfill_pdf_store.py`:

```python
"""Backfill the R2 document store from the existing corpus.

One-shot, resumable, safe to re-run. Not scheduled -- run it by hand.

Measured 2026-08-29: 4,542 documents, of which 1,909 still had a local PDF
and 2,633 did not. A 20-URL sample of the missing ones found 19 still
fetchable, so the re-fetch path recovers most but not all of them, and that
proportion only falls as link rot accumulates.

Ordering is deliberate: local files first (free, no network, no link-rot
race), then re-fetches newest-first, since recent documents are both more
likely to still resolve and more likely to matter.

Usage:
    python -m scripts.backfill_pdf_store              # everything
    python -m scripts.backfill_pdf_store --limit 50   # a taste first
    python -m scripts.backfill_pdf_store --no-refetch # local files only
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
import time

import requests
from rich.console import Console

import pdf_store
from database import Document, get_session, init_db, utcnow

console = Console(legacy_windows=False)

ROOT = pathlib.Path(__file__).resolve().parents[1]
WAF_FILE = ROOT / "data" / "waf_blocked_plans.json"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
REQUEST_DELAY_SECONDS = 0.5


def _waf_blocked_plan_ids() -> set[str]:
    try:
        return set(json.loads(WAF_FILE.read_text(encoding="utf-8")))
    except (OSError, ValueError):
        return set()


def _fetch_bytes(url: str) -> bytes | None:
    """Download `url`, or None if it no longer resolves as a PDF."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=60)
        if resp.status_code != 200 or not resp.content.startswith(b"%PDF"):
            return None
        return resp.content
    except Exception:                            # noqa: BLE001 - deliberate
        return None


def run(limit: int | None = None, refetch: bool = True, cfg=None) -> dict:
    init_db()
    cfg = cfg or pdf_store.config_from_env()
    if cfg is None:
        console.print("[red]R2 not configured (need R2_* env vars)[/red]")
        return {}

    blocked = _waf_blocked_plan_ids()
    counts: dict[str, int] = {}

    def bump(key):
        counts[key] = counts.get(key, 0) + 1

    session = get_session()
    try:
        docs = (session.query(Document)
                .order_by(Document.downloaded_at.desc().nullslast())
                .all())
        # Local files first: free, and an interrupted run still made progress.
        docs.sort(key=lambda d: 0 if (d.local_path and
                                      pathlib.Path(d.local_path).exists())
                  else 1)
        if limit:
            docs = docs[:limit]

        for doc in docs:
            if doc.content_sha256:
                bump("already")
                continue

            local = pathlib.Path(doc.local_path) if doc.local_path else None
            if local and local.exists():
                try:
                    sha = pdf_store.put(cfg, local.read_bytes())
                    doc.content_sha256 = sha
                    doc.r2_uploaded_at = utcnow()
                    session.commit()
                    bump("stored_local")
                except Exception as e:           # noqa: BLE001
                    console.print(f"  [red]{doc.url}: {e}[/red]")
                    session.rollback()
                    bump("failed")
                continue

            if not refetch:
                continue

            if doc.plan_id in blocked:
                bump("skipped_waf")
                continue

            data = _fetch_bytes(doc.url)
            time.sleep(REQUEST_DELAY_SECONDS)
            if data is None:
                # Permanent, and worth counting: this is the floor of what
                # can never be recovered.
                bump("unrecoverable")
                continue
            try:
                sha = pdf_store.put(cfg, data)
                doc.content_sha256 = sha
                doc.r2_uploaded_at = utcnow()
                session.commit()
                bump("stored_refetch")
            except Exception as e:               # noqa: BLE001
                console.print(f"  [red]{doc.url}: {e}[/red]")
                session.rollback()
                bump("failed")
    finally:
        session.close()

    console.rule("[bold green]Backfill complete[/bold green]")
    for key in ("stored_local", "stored_refetch", "already",
                "unrecoverable", "skipped_waf", "failed"):
        if counts.get(key):
            console.print(f"  {key:16s} {counts[key]}")
    return counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--no-refetch", action="store_true",
                        help="Only upload PDFs already on disk.")
    args = parser.parse_args()
    counts = run(limit=args.limit, refetch=not args.no_refetch)
    sys.exit(1 if counts.get("failed") else 0)


if __name__ == "__main__":
    main()
```

`scripts/__init__.py` already exists (verified 2026-08-29), so
`from scripts import backfill_pdf_store` resolves without further setup —
the same import style `tests/test_normalize_asset_classes.py` already uses.

- [ ] **Step 4: Run tests to verify they pass**

Run: `LLM_MODE=mock python -m pytest tests/test_backfill_pdf_store.py -q`
Expected: PASS (5 passed)

- [ ] **Step 5: Run the full suite to check for regressions**

Run: `LLM_MODE=mock INSIGHTS_MODE=mock python -m pytest tests/ -q`
Expected: PASS — 577 pre-existing + this plan's new tests, 0 failures.

Note: redirect to a file rather than piping to `tail`, or the exit code you
see is `tail`'s:
`... python -m pytest tests/ -q > /tmp/run.log 2>&1; echo "EXIT=$?"`

- [ ] **Step 6: Commit**

```bash
git add scripts/backfill_pdf_store.py tests/test_backfill_pdf_store.py
git commit -m "Backfill script for the R2 document store"
```

---

### Task 8: Live verification and the real backfill

Not a code task — the operational step that proves the store works against real R2 before 7.3 GB moves. Everything above is mocked; nothing has touched Cloudflare.

**Files:** none (operational).

**Interfaces:**
- Consumes: everything above.
- Produces: a populated bucket and a real `unrecoverable` count.

- [ ] **Step 1: Confirm the bucket and credentials exist**

Human operator: create the `pension-documents` bucket in Cloudflare R2 and
an API token with object read/write on it. Put the four values in `.env`
locally and in GitHub repo secrets.

- [ ] **Step 2: Round-trip one real document**

```bash
python -c "
import pdf_store, pathlib
from database import SessionLocal, init_db, Document
init_db()
cfg = pdf_store.config_from_env()
assert cfg, 'R2 not configured — check .env'
s = SessionLocal()
doc = (s.query(Document)
       .filter(Document.local_path.isnot(None))
       .order_by(Document.downloaded_at.desc()).first())
p = pathlib.Path(doc.local_path)
assert p.exists(), f'pick another doc; {p} is gone'
original = p.read_bytes()
sha = pdf_store.put(cfg, original)
assert pdf_store.get(cfg, sha) == original, 'ROUND TRIP MISMATCH'
print('round trip OK', doc.id, sha, len(original), 'bytes')
s.close()
"
```
Expected: `round trip OK <id> <sha> <n> bytes`

- [ ] **Step 3: Backfill a small sample first**

```bash
python -m scripts.backfill_pdf_store --limit 20 --no-refetch
```
Expected: `stored_local 20` (or fewer plus `already`). Inspect the Cloudflare
dashboard and confirm objects appear under `documents/`.

- [ ] **Step 4: Backfill all local files**

```bash
python -m scripts.backfill_pdf_store --no-refetch
```
Expected: roughly `stored_local 1909`. Free — no network fetching.

- [ ] **Step 5: Backfill the re-fetch half**

```bash
python -m scripts.backfill_pdf_store
```
Expected: roughly `stored_refetch ~2500`, `unrecoverable ~130` (the
2026-08-29 sample implies ~5%). This one takes hours and ~7 GB of
bandwidth; it is resumable, so interrupt freely.

- [ ] **Step 6: Record the real numbers**

Report the final counts. The `unrecoverable` figure is the permanent floor
of what this corpus can never recover, and it belongs in `nextsteps.md`
under E1 — it is the number that justifies retention having happened now
rather than later.

---

## Self-Review

**Spec coverage:**

| Spec section | Task |
|---|---|
| §3.1 content-addressed keys | 1, 2 |
| §3.2 schema (`content_sha256`, `r2_uploaded_at`) | 3 |
| §3.3 `put`/`get`/`exists`/`open_local_or_remote` | 2, 4 |
| §3.3 boto3/moto return | 1 |
| §4 upload in daily pipeline, job-level secrets | 5, 6 |
| §4 non-fatal failure posture | 4 (test), 5 (test) |
| §5 backfill, ordering, resume, unrecoverable, WAF skip | 7 |
| §7 testing (idempotent, digest verify, non-fatal, resume, config) | 2, 4, 6, 7 |
| §8 sequencing | task order; Task 8 runs the backfill last |

§6 ("what this spec does not do") needs no task by construction —
re-extraction, cap changes, portal URLs, and user-facing downloads are all
excluded, and no task above touches them.

**Placeholder scan:** none. Every code step carries real code; every command
carries its expected output.

**Type consistency:** `sha256_bytes`/`key_for`/`exists`/`put`/`get`/
`store_document`/`open_local_or_remote` keep identical signatures across
Tasks 1-7. `content_sha256` (never `sha256` or `content_hash`) is the column
name everywhere. Backfill count keys match between the docstring, `run()`,
and the tests.

**One deliberate deviation from the spec**, flagged for the reviewer: spec
§3.2 says "add the columns to the model and re-run `init_db()`, no ALTER
TABLE." That is right for a *new* database but does not work on the live one
— `create_all()` creates missing tables, not missing columns. Task 3 Step 5
therefore runs a one-off `ADD COLUMN IF NOT EXISTS` by hand, which is the
"one-off script" CLAUDE.md's own rule allows for existing-row backfill. The
prohibition is on building a migration *system*, not on ever altering a
table.
