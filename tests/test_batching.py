"""One Message Batch runner shared by every extractor that batches."""
import types

import batching


class FakeBatches:
    def __init__(self, statuses=("ended",)):
        self.statuses = list(statuses)
        self.created = []
        self.cancelled = []

    def create(self, requests):
        self.created.append(requests)
        return types.SimpleNamespace(id="b1", processing_status="in_progress")

    def retrieve(self, batch_id):
        status = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
        return types.SimpleNamespace(id=batch_id, processing_status=status)

    def cancel(self, batch_id):
        self.cancelled.append(batch_id)

    def results(self, batch_id):
        for req in self.created[-1]:
            yield types.SimpleNamespace(custom_id=req["custom_id"],
                                        result=types.SimpleNamespace(type="succeeded"))


def _client(batches):
    return types.SimpleNamespace(messages=types.SimpleNamespace(batches=batches))


def test_submits_the_requests_and_returns_results_keyed_by_custom_id():
    fb = FakeBatches()
    out = batching.run_message_batch(
        _client(fb), [{"custom_id": "7", "params": {"model": "m"}}], poll_seconds=0)
    assert fb.created == [[{"custom_id": "7", "params": {"model": "m"}}]]
    assert [r.custom_id for r in out] == ["7"]


def test_timeout_cancels_and_still_collects():
    fb = FakeBatches(statuses=["in_progress", "canceling", "ended"])
    out = batching.run_message_batch(
        _client(fb), [{"custom_id": "1", "params": {}}], poll_seconds=0, timeout_minutes=0)
    assert fb.cancelled == ["b1"]
    assert len(out) == 1


def test_no_requests_means_no_batch():
    fb = FakeBatches()
    assert batching.run_message_batch(_client(fb), [], poll_seconds=0) == []
    assert fb.created == []


def test_result_detail_reads_the_nested_error_message():
    errored = types.SimpleNamespace(
        type="errored",
        error=types.SimpleNamespace(error=types.SimpleNamespace(message="overloaded")))
    assert batching.result_detail(errored) == "overloaded"
    assert batching.result_detail(types.SimpleNamespace(type="expired")) == "expired"


def test_resuming_an_existing_batch_skips_create():
    fb = FakeBatches()
    fb.created.append([{"custom_id": "9", "params": {}}])   # what the earlier run submitted
    out = batching.run_message_batch(_client(fb), [], poll_seconds=0, batch_id="b1")
    assert len(fb.created) == 1
    assert [r.custom_id for r in out] == ["9"]
