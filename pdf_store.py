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
