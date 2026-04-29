"""Slack webhook alerts for failed runs or low extraction yield."""

from __future__ import annotations

import json
import os

import httpx

from rfp.logging_setup import get_logger

ZERO_EXTRACTION_THRESHOLD = 0.20  # alert if >20% of docs in a run produced zero records
HTTP_TIMEOUT_SECONDS = 5.0


def _webhook_url() -> str | None:
    return os.environ.get("SLACK_WEBHOOK_URL") or None


def _post(text: str) -> None:
    url = _webhook_url()
    if not url:
        return
    try:
        httpx.post(url, json={"text": text}, timeout=HTTP_TIMEOUT_SECONDS)
    except httpx.HTTPError as e:
        # Alerting is best-effort; never let a failed Slack call break a pipeline run.
        get_logger(component="alerting").warning("slack_post_failed", error=str(e))


def maybe_alert_on_run(
    *,
    run_id: str,
    status: str,
    documents_processed: int,
    docs_with_zero_records: int,
    errors: list[str] | None = None,
) -> None:
    """Decide whether the completed run warrants a Slack ping, and send one if so."""
    if status == "failed":
        _post(
            f":rotating_light: RFP pipeline run `{run_id}` failed. "
            f"Errors: {json.dumps(errors or [])[:500]}"
        )
        return

    if documents_processed == 0:
        return
    zero_ratio = docs_with_zero_records / documents_processed
    if zero_ratio > ZERO_EXTRACTION_THRESHOLD:
        _post(
            f":warning: RFP pipeline run `{run_id}` produced 0 records for "
            f"{docs_with_zero_records}/{documents_processed} documents "
            f"({zero_ratio:.0%}). Check stage-1 verdicts."
        )
