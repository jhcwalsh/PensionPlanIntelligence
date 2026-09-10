"""One Message Batch, submitted and collected.

Shared by every job that has no reason to pay the interactive rate: the
summariser and the CAFR actuarial extractor today. Batches bill every token
at half the standard price; the caller records that with
``database.record_api_usage(..., batch=True)`` because the batch path never
goes through the instrumented client's ``create()``.
"""
from __future__ import annotations

import os
import time

#: How long a run waits for its batch before cancelling it. Most batches
#: finish in minutes; the API's own ceiling is 24 hours.
DEFAULT_TIMEOUT_MINUTES = int(os.environ.get("BATCH_TIMEOUT_MIN", "180"))
DEFAULT_POLL_SECONDS = 30


def run_message_batch(client, requests: list[dict], *,
                      poll_seconds: float | None = None,
                      timeout_minutes: float | None = None,
                      log=print) -> list:
    """Submit ``requests`` (each ``{"custom_id", "params"}``) and return results.

    On timeout the batch is cancelled and whatever finished is collected:
    cancellation is not immediate, and requests caught mid-flight come back
    ``canceled`` rather than billed. The caller decides what an unsuccessful
    result means for its document; usually "try again next run".
    """
    if not requests:
        return []
    poll_seconds = DEFAULT_POLL_SECONDS if poll_seconds is None else poll_seconds
    timeout_minutes = DEFAULT_TIMEOUT_MINUTES if timeout_minutes is None else timeout_minutes

    batch = client.messages.batches.create(requests=requests)
    log(f"  Batch {batch.id} submitted: {len(requests)} requests")

    started = time.monotonic()
    cancelled = False
    while True:
        batch = client.messages.batches.retrieve(batch.id)
        if batch.processing_status == "ended":
            break
        elapsed_min = (time.monotonic() - started) / 60
        if not cancelled and elapsed_min >= timeout_minutes:
            log(f"  Batch {batch.id} still {batch.processing_status} after "
                f"{elapsed_min:.0f} min; cancelling and keeping what finished")
            client.messages.batches.cancel(batch.id)
            cancelled = True
        time.sleep(poll_seconds)

    return list(client.messages.batches.results(batch.id))


def result_detail(result) -> str:
    """Why a batch request did not succeed, as the API phrased it.

    An errored result carries ``error`` (an ErrorResponse) whose own
    ``error`` holds the message; canceled and expired results carry nothing
    beyond their type.
    """
    response = getattr(result, "error", None)
    inner = getattr(response, "error", None)
    return getattr(inner, "message", None) or result.type
