"""Publish step — write approved Markdown to ``notes/`` and push to origin.

In the existing flow (``publish_notes.py``) "publish" means: regenerate
notes, ``git add notes/``, commit, push to the deploy branch, Render
auto-deploys. This adapter does the same thing for an approved
``Publication`` row, except the Markdown is already in the row — no
regeneration step.

Mock mode skips the git operations and just writes the Markdown to
``notes/`` so integration tests can assert the file appeared.
"""

from __future__ import annotations

import logging
import subprocess
from datetime import datetime
from pathlib import Path

from database import Publication
from insights import config

logger = logging.getLogger(__name__)

NOTES_DIR = config.REPO_ROOT / "notes"
DEPLOY_BRANCH = "master"

# Inline identity for the publish commit. Render's Streamlit container
# runs git as ``render@srv-...(none)`` which git refuses to use as an
# author. Setting these via ``git -c user.email=... -c user.name=...``
# on the commit invocation avoids relying on host git config (which the
# container doesn't have) and keeps the audit trail consistent with the
# GHA bot identity used elsewhere in the repo.
_GIT_AUTHOR_EMAIL = "41898282+github-actions[bot]@users.noreply.github.com"
_GIT_AUTHOR_NAME = "github-actions[bot]"


def _filename_for(publication: Publication) -> str:
    """Map a publication to its canonical ``notes/<file>.md`` path.

    Weekly notes already follow ``7day_highlights_YYYY-MM-DD.md`` in the
    existing repo — match that. Monthly and annual files are new.
    """
    period = publication.period_start.isoformat()
    if publication.cadence == "weekly":
        return f"7day_highlights_{period}.md"
    if publication.cadence == "monthly":
        return f"monthly_cio_insights_{period}.md"
    if publication.cadence == "annual":
        return f"annual_cio_insights_{publication.period_start.year}.md"
    if publication.cadence == "daily":
        return f"daily_digest_{period}.md"
    raise ValueError(f"Unknown cadence: {publication.cadence}")


def publish(publication: Publication) -> Path:
    """Write the approved draft to disk and (in live mode) push to origin.

    Returns the path the Markdown was written to.
    """
    if publication.status != "approved":
        raise ValueError(
            f"publish() requires an approved publication; got status='{publication.status}'"
        )
    if not publication.draft_markdown:
        raise ValueError("publication has no draft_markdown")

    NOTES_DIR.mkdir(parents=True, exist_ok=True)
    path = NOTES_DIR / _filename_for(publication)
    path.write_text(publication.draft_markdown, encoding="utf-8")
    logger.info("Wrote %s (%d chars)", path, len(publication.draft_markdown))

    if config.is_mock():
        return path

    _git_commit_and_push(path, publication)
    return path


def _git_commit_and_push(path: Path, publication: Publication) -> None:
    """Stage, commit, and push the approved note to ``DEPLOY_BRANCH``.

    Errors are logged and re-raised so the cycle marks the publication
    failed (rather than silently leaving an un-pushed change).
    """
    msg = (
        f"Publish {publication.cadence} Insights "
        f"({publication.period_start.isoformat()})"
    )

    def run(cmd: list[str]) -> subprocess.CompletedProcess:
        result = subprocess.run(
            cmd, cwd=config.REPO_ROOT, capture_output=True, text=True, timeout=60
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"git command failed: {' '.join(cmd)}\n"
                f"stdout: {result.stdout}\nstderr: {result.stderr}"
            )
        return result

    rel_path = path.relative_to(config.REPO_ROOT).as_posix()
    run(["git", "add", "--", rel_path])

    # Skip commit if nothing actually changed (same content as previous run).
    diff = subprocess.run(
        ["git", "diff", "--cached", "--stat"],
        cwd=config.REPO_ROOT, capture_output=True, text=True, timeout=30,
    )
    if not diff.stdout.strip():
        logger.info("No staged changes after git add; skipping commit/push.")
        return

    run([
        "git",
        "-c", f"user.email={_GIT_AUTHOR_EMAIL}",
        "-c", f"user.name={_GIT_AUTHOR_NAME}",
        "commit", "-m", msg,
    ])
    run(["git", "push", "origin", DEPLOY_BRANCH])
