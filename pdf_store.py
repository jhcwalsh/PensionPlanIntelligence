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

import contextlib
import functools
import hashlib
import logging
import os
import pathlib
import tempfile
from dataclasses import dataclass

from database import utcnow

logger = logging.getLogger(__name__)

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


class DigestMismatch(Exception):
    """Stored bytes did not hash to the key they were stored under."""


@functools.lru_cache(maxsize=None)
def client(cfg: R2Config):
    """A boto3 S3 client pointed at R2's S3-compatible endpoint.

    Cached on the config, which is a frozen dataclass and therefore
    hashable. Without the cache every call built a fresh client and a fresh
    TLS handshake: `put()` alone calls `exists()` then `put_object()`, so the
    ~9,000-document backfill would open ~18,000 connections and close none of
    them.
    """
    import boto3
    from botocore.config import Config
    return boto3.client(
        "s3",
        endpoint_url=cfg.endpoint_url,
        aws_access_key_id=cfg.access_key_id,
        aws_secret_access_key=cfg.secret_access_key,
        region_name="auto",          # R2 ignores region but boto3 wants one
        # The corpus's largest document is 89.8 MB and the backfill runs from
        # a home connection: botocore's default 60s read timeout would fail
        # the biggest uploads on upstream speed alone.
        config=Config(
            retries={"max_attempts": 5, "mode": "standard"},
            read_timeout=300,
            connect_timeout=30,
        ),
    )


def preflight(cfg: R2Config) -> None:
    """One real round-trip against R2. Raises if it does not work.

    `exists()` deliberately re-raises a 403, so a wrong secret key or a
    mistyped bucket name turns every single upload into a logged failure
    while the caller keeps going. For the daily pipeline that is the right
    posture (retention is additive), but for the backfill it means an
    operator downloads gigabytes, stores nothing, and finds out hours later.
    Callers that are about to do bulk work should call this first and stop
    on failure.

    `head_bucket` is the clearest check, but an R2 API token scoped to
    "Object Read & Write" on a single bucket can be denied it while still
    being able to do everything the backfill needs. So a 403 there falls
    back to `exists()`, which exercises `head_object` -- the exact call
    `put()` makes. Credentials that pass the fallback are good enough.
    """
    from botocore.exceptions import ClientError
    try:
        client(cfg).head_bucket(Bucket=cfg.bucket)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in ("403", "AccessDenied"):
            raise
        exists(cfg, "0" * 64)


_NOT_FOUND_CODES = {"404", "NoSuchKey", "NotFound"}


def exists(cfg: R2Config, sha: str) -> bool:
    """True if an object is already stored under `sha`.

    Returns `False` only when the object is genuinely absent (S3/R2 report
    this as a 404, NoSuchKey, or NotFound error code depending on the call).
    Any other `ClientError` -- a 403 from bad or misconfigured credentials, a
    500, throttling -- propagates instead of being folded into `False`. That
    matters most for a first-time credentials setup: swallowing a 403 here
    would report it as "object not present", and `put()` would then attempt
    an unguarded upload that fails with a misleading error.
    """
    from botocore.exceptions import ClientError
    try:
        client(cfg).head_object(Bucket=cfg.bucket, Key=key_for(sha))
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in _NOT_FOUND_CODES:
            return False
        raise


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


def store_document(session, document, path, cfg: R2Config | None = None):
    """Upload `path` and record the key on `document`. Returns the sha or None.

    Never raises. The daily pipeline calls this immediately after a fetch, and
    an R2 outage must not fail a run whose real job is fetching and
    extracting -- a null content_sha256 simply means the backfill sweeps it
    up later.

    Ordering inside the try block is load-bearing, not incidental:
    `document.content_sha256` / `r2_uploaded_at` are only assigned AFTER
    `put()` succeeds. The caller (fetcher.py) commits the document row
    before ever calling this function, so the `session.rollback()` in the
    except branch below only ever discards this function's own uncommitted
    mutations -- never the document's insert. If this function were ever
    changed to assign those fields before the upload, a failed upload's
    rollback could discard more than intended, on whatever the caller
    happened to have pending on the same session at that point.
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


def _resolve(document, cfg: R2Config | None = None) -> tuple[pathlib.Path, bool]:
    """(path, is_temporary) for `document`'s PDF.

    Prefers a present local file (free); otherwise pulls the retained object
    to a temp file. The boolean is the part callers cannot work out for
    themselves, and getting it wrong either leaks a 90 MB temp file per
    document or deletes the corpus's only local copy.
    """
    if document.local_path:
        local = pathlib.Path(document.local_path)
        if local.exists():
            return local, False

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
    return pathlib.Path(tmp.name), True


@contextlib.contextmanager
def document_pdf(document, cfg: R2Config | None = None):
    """Yield a readable path to `document`'s PDF, cleaning up after itself.

    **This is the call extractors should use.** ``open_local_or_remote``
    returns a path that is sometimes a permanent local file and sometimes a
    temp file the caller owns, with nothing in the return value saying which
    -- fine for a one-off, a leak of up to 90 MB per document in a loop over
    a corpus. Ownership is decided here, where it is known, instead of by
    each caller comparing paths and hoping.

    Raises FileNotFoundError when the PDF is neither on disk nor retained,
    which is the signal to record `file_missing` rather than to fail loudly.
    """
    path, is_temp = _resolve(document, cfg)
    try:
        yield path
    finally:
        if is_temp:
            try:
                path.unlink()
            except OSError:
                pass


def open_local_or_remote(document, cfg: R2Config | None = None) -> pathlib.Path:
    """Return a readable path for `document`'s PDF.

    Prefer ``document_pdf`` — this leaves any temp file for the caller to
    remove, and callers reliably forget. Kept for one-shot uses where the
    process is about to exit anyway.
    """
    return _resolve(document, cfg)[0]
