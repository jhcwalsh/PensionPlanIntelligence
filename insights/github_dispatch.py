"""Trigger a GitHub Actions workflow via the REST API.

Used by the Streamlit approve handler to fire the ``publish-approved``
workflow automatically, so the user gets a complete approve → publish
flow without having to open the Actions tab.

Requires the env var ``GITHUB_DISPATCH_TOKEN`` — a fine-grained PAT
scoped to this repo with ``Actions: Read and write`` permission. If
the token isn't configured, ``dispatch_publish_approved`` returns
False so the caller can fall back to surfacing the manual link.
"""
from __future__ import annotations

import logging
import os
from typing import Optional

import requests

logger = logging.getLogger(__name__)

REPO_OWNER = "jhcwalsh"
REPO_NAME = "PensionPlanIntelligence"
WORKFLOW_FILE = "publish-approved.yml"
REF = "master"

_ENDPOINT = (
    f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}"
    f"/actions/workflows/{WORKFLOW_FILE}/dispatches"
)


class DispatchError(RuntimeError):
    pass


def _token() -> Optional[str]:
    return os.environ.get("GITHUB_DISPATCH_TOKEN")


def is_configured() -> bool:
    return bool(_token())


def dispatch_publish_approved(publication_id: int, *, timeout: float = 10.0) -> None:
    """Fire publish-approved workflow_dispatch for ``publication_id``.

    Raises ``DispatchError`` on missing token, network failure, or
    non-success HTTP status. The caller should catch and surface the
    error rather than letting it bubble up to Streamlit's red banner.
    """
    token = _token()
    if not token:
        raise DispatchError("GITHUB_DISPATCH_TOKEN not set")

    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    payload = {
        "ref": REF,
        "inputs": {"publication_id": str(publication_id)},
    }

    try:
        resp = requests.post(_ENDPOINT, headers=headers, json=payload, timeout=timeout)
    except requests.RequestException as exc:
        raise DispatchError(f"workflow_dispatch request failed: {exc}") from exc

    if resp.status_code != 204:
        raise DispatchError(
            f"workflow_dispatch returned HTTP {resp.status_code}: "
            f"{resp.text[:200]}"
        )

    logger.info(
        "Dispatched publish-approved workflow for publication %s", publication_id,
    )
