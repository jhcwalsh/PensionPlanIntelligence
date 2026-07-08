"""Sync db/pension.db with the R2 bucket that is its source of truth.

Layout in the bucket:
    versions/<generation>.db   immutable DB bytes, one object per push
    current/manifest.json      the only mutable object:
                               {generation, key, sha256, size,
                                uploaded_by, uploaded_at}
    snapshots/YYYY-MM-DD.db    daily copy, expired by lifecycle rule (30d)

Concurrency: push() re-reads the manifest and refuses (SyncConflict) if
the remote generation is not the one this process pulled (recorded in a
`<path>.r2gen` sidecar). The manifest PUT also sends If-Match with the
ETag captured at that re-read, closing the remaining race window on real
R2 (moto does not enforce it; Task 8 verifies against the real bucket).

Every entry point is a no-op / False when the four R2_* env vars are
absent, so local dev and CI never need credentials.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
import tempfile
import threading
from datetime import datetime, timezone
from pathlib import Path

VERSIONS_PREFIX = "versions/"
MANIFEST_KEY = "current/manifest.json"
SNAPSHOT_PREFIX = "snapshots/"
KEEP_VERSIONS = 10

_client_lock = threading.Lock()
_client = None

# Tracks whether install_auto_push has an armed debounce timer or an
# in-flight push, so callers (app.py's freshness pull) can avoid yanking
# the local DB file out from under a pending local write.
_pending_lock = threading.Lock()
_pending_state = {"armed": False, "in_flight": False}


def auto_push_pending() -> bool:
    """True while a debounced auto-push timer is armed or a push is running."""
    with _pending_lock:
        return _pending_state["armed"] or _pending_state["in_flight"]


class SyncConflict(Exception):
    """Another writer pushed a newer generation; pull and re-run."""


def enabled() -> bool:
    return all(os.environ.get(k) for k in
               ("R2_ENDPOINT", "R2_ACCESS_KEY_ID",
                "R2_SECRET_ACCESS_KEY", "R2_BUCKET"))


def _reset_client_cache() -> None:
    global _client
    with _client_lock:
        _client = None


def _s3():
    global _client
    with _client_lock:
        if _client is None:
            import boto3
            _client = boto3.client(
                "s3",
                endpoint_url=os.environ.get("R2_ENDPOINT") or None,
                aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
                aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
            )
        return _client


def _bucket() -> str:
    return os.environ["R2_BUCKET"]


def _get_manifest():
    """Returns (manifest_dict, etag) or (None, None) when unseeded."""
    try:
        resp = _s3().get_object(Bucket=_bucket(), Key=MANIFEST_KEY)
    except _s3().exceptions.NoSuchKey:
        return None, None
    return json.loads(resp["Body"].read()), resp["ETag"]


def _sidecar(path) -> Path:
    return Path(str(path) + ".r2gen")


def _read_sidecar(path) -> int:
    try:
        return int(_sidecar(path).read_text().strip())
    except (FileNotFoundError, ValueError):
        return 0


def pull(dest, pre_replace=None) -> bool:
    """Download the current DB to dest if remote generation is newer.

    ``pre_replace``, if given, is called after the download is verified
    but immediately BEFORE the atomic ``os.replace`` swap — e.g. to
    dispose SQLAlchemy engine connections and clear cached sessions.
    On Windows a file can't be replaced while another handle has it
    open, so disposal must happen before the swap, not after.

    Returns True when the file was replaced. Atomic via os.replace.
    """
    if not enabled():
        return False
    manifest, _ = _get_manifest()
    if manifest is None:
        raise RuntimeError("R2 bucket not seeded — run push once first")
    dest = Path(dest)
    if _read_sidecar(dest) == manifest["generation"] and dest.exists():
        return False
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(dest.parent), suffix=".r2tmp")
    os.close(fd)
    try:
        _s3().download_file(_bucket(), manifest["key"], tmp)
        digest = hashlib.sha256(Path(tmp).read_bytes()).hexdigest()
        if digest != manifest["sha256"]:
            raise RuntimeError(
                f"sha256 mismatch pulling gen {manifest['generation']}")
        if pre_replace is not None:
            pre_replace()
        os.replace(tmp, dest)
    finally:
        Path(tmp).unlink(missing_ok=True)
    _sidecar(dest).write_text(str(manifest["generation"]))
    return True


def _snapshot_bytes(src: Path) -> bytes:
    """Read src's bytes via the sqlite3 backup API, not a raw file read.

    ``src`` may be the live application DB, with another connection
    writing to it concurrently. A raw ``read_bytes()`` can observe a
    torn/partial write (SQLite writes aren't a single atomic file
    write in WAL/rollback-journal mode). The backup API takes SQLite's
    own locking and streams a transactionally consistent copy into a
    temp file, so we hash/upload that instead.

    Falls back to a raw read when sqlite3 can't open ``src`` as a
    database at all (``sqlite3.DatabaseError``) — this happens for
    test fixtures that write arbitrary bytes as a stand-in DB file
    (and, in principle, for some hypothetical non-SQLite payload). Such
    files aren't subject to the torn-read race in the first place,
    since nothing is concurrently writing to them via SQLite.
    """
    fd, tmp_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        try:
            src_conn = sqlite3.connect(str(src))
            try:
                tmp_conn = sqlite3.connect(tmp_path)
                try:
                    with tmp_conn:
                        src_conn.backup(tmp_conn)
                finally:
                    tmp_conn.close()
            finally:
                src_conn.close()
            return Path(tmp_path).read_bytes()
        except sqlite3.DatabaseError:
            return src.read_bytes()
    finally:
        Path(tmp_path).unlink(missing_ok=True)


def push(src, uploaded_by: str) -> int:
    """Upload src as the next generation. Raises SyncConflict if the
    remote moved past the generation recorded in src's sidecar."""
    if not enabled():
        return 0
    src = Path(src)
    manifest, etag = _get_manifest()
    remote_gen = manifest["generation"] if manifest else 0
    local_gen = _read_sidecar(src)
    if manifest is not None and local_gen != remote_gen:
        raise SyncConflict(
            f"remote at generation {remote_gen}, local sidecar at "
            f"{local_gen} — pull and re-run")
    body = _snapshot_bytes(src)
    digest = hashlib.sha256(body).hexdigest()
    if manifest is not None and manifest.get("sha256") == digest:
        print(f"[db_sync] push: unchanged (generation {remote_gen}); "
              f"skipping upload")
        return remote_gen
    new_gen = remote_gen + 1
    key = f"{VERSIONS_PREFIX}{new_gen}.db"
    _s3().put_object(Bucket=_bucket(), Key=key, Body=body)
    new_manifest = json.dumps({
        "generation": new_gen,
        "key": key,
        "sha256": digest,
        "size": len(body),
        "uploaded_by": uploaded_by,
        "uploaded_at": datetime.now(timezone.utc).isoformat(),
    })
    extra = {}
    if etag is not None:
        # Honoured by real R2 (conditional writes); ignored by moto.
        extra["IfMatch"] = etag.strip('"')
    try:
        _s3().put_object(Bucket=_bucket(), Key=MANIFEST_KEY,
                         Body=new_manifest, **extra)
    except Exception as exc:  # PreconditionFailed from real R2
        if "PreconditionFailed" in type(exc).__name__ or "412" in str(exc):
            raise SyncConflict(str(exc)) from exc
        raise
    _sidecar(src).write_text(str(new_gen))
    _prune(new_gen)
    return new_gen


def _prune(current_gen: int) -> None:
    cutoff = current_gen - KEEP_VERSIONS
    if cutoff <= 0:
        return
    resp = _s3().list_objects_v2(Bucket=_bucket(), Prefix=VERSIONS_PREFIX)
    for obj in resp.get("Contents", []):
        try:
            gen = int(Path(obj["Key"]).stem)
        except ValueError:
            continue
        if gen <= cutoff:
            _s3().delete_object(Bucket=_bucket(), Key=obj["Key"])


def snapshot() -> str:
    """Server-side copy of the current version to snapshots/YYYY-MM-DD.db."""
    if not enabled():
        return ""
    manifest, _ = _get_manifest()
    if manifest is None:
        raise RuntimeError("R2 bucket not seeded")
    key = f"{SNAPSHOT_PREFIX}{datetime.now(timezone.utc):%Y-%m-%d}.db"
    _s3().copy_object(Bucket=_bucket(), Key=key,
                      CopySource={"Bucket": _bucket(), "Key": manifest["key"]})
    return key


def _push_ignore_conflict(db_path, uploaded_by: str) -> None:
    """Push db_path; on SyncConflict, log loudly and leave the file alone.

    Used by ``install_auto_push``'s debounced callback. A conflict means
    another writer's generation raced ours: the OLD behaviour here used
    to pull-and-replace the local file and re-push, which silently threw
    away the just-committed local write (the very thing this push was
    trying to persist) and replaced it with the remote winner's bytes.
    Instead we do nothing to the local file — it will either get pushed
    successfully on the next auto-push cycle (if this process's sidecar
    catches up via a freshness pull first, that pull is itself skipped
    while a push is pending — see ``auto_push_pending``) or be reconciled
    /lost the next time this process pulls a newer generation.
    """
    try:
        push(db_path, uploaded_by=uploaded_by)
    except SyncConflict:
        print(
            "db_sync CONFLICT: local Streamlit write NOT pushed; will be "
            "reconciled or lost at next freshness pull — generation raced "
            "by another writer",
            file=sys.stderr,
        )


def install_auto_push(session_factory, uploaded_by: str,
                      db_path: str, debounce_seconds: float = 5.0) -> None:
    """After any commit through session_factory, push the DB (debounced).

    Used by the Streamlit service so subscriber sign-ups and approval
    clicks survive redeploys. Push failures are logged, never raised
    into the UI. On SyncConflict: log loudly and leave the local file
    alone — see ``_push_ignore_conflict``.
    """
    if not enabled():
        return
    from sqlalchemy import event
    timer_box = {}

    def _do_push():
        with _pending_lock:
            _pending_state["armed"] = False
            _pending_state["in_flight"] = True
        try:
            _push_ignore_conflict(db_path, uploaded_by)
        except Exception as exc:  # noqa: BLE001
            print(f"[db_sync] auto-push failed: {exc}", file=sys.stderr)
        finally:
            with _pending_lock:
                _pending_state["in_flight"] = False

    @event.listens_for(session_factory, "after_commit")
    def _after_commit(session):  # noqa: ARG001
        t = timer_box.get("t")
        if t is not None:
            t.cancel()
        with _pending_lock:
            _pending_state["armed"] = True
        timer_box["t"] = threading.Timer(debounce_seconds, _do_push)
        timer_box["t"].daemon = True
        timer_box["t"].start()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="db_sync")
    parser.add_argument("command",
                        choices=["pull", "push", "snapshot", "verify"])
    parser.add_argument("--by", default="manual",
                        help="uploaded_by label for push")
    parser.add_argument("--path", default=os.environ.get(
        "DB_PATH", str(Path(__file__).parent.parent / "db" / "pension.db")))
    args = parser.parse_args(argv)
    if not enabled():
        print("db_sync: R2_* env vars not set; nothing to do")
        return 0
    if args.command == "pull":
        print("replaced" if pull(args.path) else "already current")
    elif args.command == "push":
        print(f"pushed generation {push(args.path, uploaded_by=args.by)}")
    elif args.command == "snapshot":
        print(f"snapshot -> {snapshot()}")
    elif args.command == "verify":
        manifest, _ = _get_manifest()
        print(f"manifest: {manifest}")
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "verify.db"
            pull(p)
            print(f"pulled {p.stat().st_size:,} bytes OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
