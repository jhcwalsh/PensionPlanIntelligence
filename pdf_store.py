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

    The returned `Path` is one of two different things depending on which
    branch was taken, and the return value alone does not tell you which:
    when the local file was present, this is that permanent file and must
    not be deleted; when it fell back to R2, this is a freshly written
    temporary file (`tempfile.NamedTemporaryFile(delete=False)`) that
    nothing else will clean up -- the caller owns removing it. There is no
    flag or wrapper type distinguishing the two cases yet; a caller that
    cares must compare the returned path against `document.local_path`
    itself, or a future revision of this function needs to make the
    distinction explicit.
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
