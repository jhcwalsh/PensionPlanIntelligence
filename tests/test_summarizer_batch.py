"""The summariser submits its calls as one Message Batch.

Nothing waits on a summary: the pipeline runs once a day and the digest
reads whatever is there. Batches bill at half the standard rate, so this
is the single largest lever on the bill. The sync path survives behind
SUMMARIZE_MODE=sync for local one-offs.
"""

import json
import types
from decimal import Decimal

import pytest

import costs
import database
import summarizer
from database import ApiUsage, Document, DocumentSkip, Plan, Summary


SUMMARY_JSON = json.dumps({
    "summary": "The board met.",
    "key_topics": ["returns"],
    "investment_actions": [],
    "decisions": [],
    "performance_data": [],
})


def _usage(**kw):
    base = dict(input_tokens=1_000, output_tokens=100,
                cache_creation_input_tokens=0, cache_read_input_tokens=0)
    base.update(kw)
    return types.SimpleNamespace(**base)


def _message(text=SUMMARY_JSON, stop_reason="end_turn"):
    content = [types.SimpleNamespace(text=text)] if text is not None else []
    return types.SimpleNamespace(content=content, stop_reason=stop_reason,
                                 usage=_usage())


def succeeded(text=SUMMARY_JSON):
    return types.SimpleNamespace(type="succeeded", message=_message(text))


def refused():
    return types.SimpleNamespace(type="succeeded",
                                 message=_message(None, stop_reason="refusal"))


def errored():
    # Shape of MessageBatchErroredResult: error is an ErrorResponse whose
    # own .error carries the message.
    return types.SimpleNamespace(
        type="errored",
        error=types.SimpleNamespace(
            type="error",
            error=types.SimpleNamespace(type="api_error", message="overloaded")))


class FakeBatches:
    """Enough of client.messages.batches to drive the summariser.

    ``outcome`` maps a document id to the result its request gets; anything
    unmapped succeeds. ``statuses`` is what successive retrieve() calls
    report; the last value repeats.
    """

    def __init__(self, outcome=None, statuses=("ended",)):
        self.outcome = outcome or {}
        self.statuses = list(statuses)
        self.created = []
        self.cancelled = []

    def create(self, requests):
        self.created.append(requests)
        return types.SimpleNamespace(id="batch_1", processing_status="in_progress")

    def retrieve(self, batch_id):
        status = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
        return types.SimpleNamespace(id=batch_id, processing_status=status)

    def cancel(self, batch_id):
        self.cancelled.append(batch_id)
        return types.SimpleNamespace(id=batch_id, processing_status="canceling")

    def results(self, batch_id):
        for req in self.created[-1]:
            doc_id = int(req["custom_id"])
            yield types.SimpleNamespace(custom_id=req["custom_id"],
                                        result=self.outcome.get(doc_id, succeeded()))


@pytest.fixture
def client(monkeypatch):
    batches = FakeBatches()
    sync_calls = []

    def create(**kwargs):
        sync_calls.append(kwargs)
        return _message()

    fake = types.SimpleNamespace(
        messages=types.SimpleNamespace(create=create, batches=batches))
    fake.sync_calls = sync_calls
    monkeypatch.setattr(summarizer, "_get_client", lambda: fake)
    return fake


@pytest.fixture
def session():
    s = database.get_session()
    s.add(Plan(id="testplan", name="Test Plan", abbreviation="TP",
               state="CA", aum_billions=1))
    s.commit()
    yield s
    s.close()


def _doc(session, filename, text, doc_type="agenda"):
    d = Document(plan_id="testplan", url=f"https://x/{filename}",
                 filename=filename, doc_type=doc_type,
                 extraction_status="done", extracted_text=text)
    session.add(d)
    session.commit()
    return d.id


SHORT = "Agenda for the regular meeting of the board of retirement. " * 5
LONG = ("Investment committee report on portfolio returns and manager "
        "performance. " * 400)


def test_batch_submits_one_request_per_document_with_routed_model(client, session):
    short_id = _doc(session, "agenda.pdf", SHORT)
    long_id = _doc(session, "pack.pdf", LONG, doc_type="board_pack")

    summarizer.run_summarizer(poll_seconds=0)

    (requests,) = client.messages.batches.created
    by_id = {int(r["custom_id"]): r["params"] for r in requests}
    assert set(by_id) == {short_id, long_id}
    assert by_id[short_id]["model"] == summarizer.MODEL_HAIKU
    assert by_id[long_id]["model"] == summarizer.MODEL_SONNET
    assert by_id[long_id]["system"] == summarizer.SYSTEM_PROMPT
    assert client.sync_calls == []

    rows = {s.document_id: s for s in session.query(Summary).all()}
    assert rows[short_id].summary_text == "The board met."
    assert rows[short_id].model_used == summarizer.MODEL_HAIKU
    assert rows[long_id].model_used == summarizer.MODEL_SONNET


def test_batch_usage_is_recorded_at_half_price(client, session, monkeypatch):
    monkeypatch.delenv("LLM_MODE", raising=False)
    monkeypatch.delenv("INSIGHTS_MODE", raising=False)
    _doc(session, "agenda.pdf", SHORT)

    with costs.track("summarize", run_id="7"):
        summarizer.run_summarizer(poll_seconds=0)

    (row,) = session.query(ApiUsage).all()
    assert row.operation == "summarize"
    assert row.run_id == "7"
    assert row.input_tokens == 1_000
    full = costs.cost_usd(summarizer.MODEL_HAIKU, _usage())
    assert Decimal(row.cost_usd) == full / 2


def test_refusal_in_batch_result_records_a_permanent_skip(client, session):
    doc_id = _doc(session, "agenda.pdf", SHORT)
    client.messages.batches.outcome[doc_id] = refused()

    summarizer.run_summarizer(poll_seconds=0)

    assert session.query(Summary).count() == 0
    (skip,) = session.query(DocumentSkip).all()
    assert skip.document_id == doc_id
    assert skip.reason == "refusal"


def test_errored_result_leaves_document_for_the_next_run(client, session, capsys):
    doc_id = _doc(session, "agenda.pdf", SHORT)
    client.messages.batches.outcome[doc_id] = errored()

    summarizer.run_summarizer(poll_seconds=0)

    assert session.query(Summary).count() == 0
    assert session.query(DocumentSkip).count() == 0
    assert "overloaded" in capsys.readouterr().out


def test_timeout_cancels_the_batch_and_keeps_what_finished(client, session):
    doc_id = _doc(session, "agenda.pdf", SHORT)
    client.messages.batches.statuses = ["in_progress", "canceling", "ended"]

    summarizer.run_summarizer(poll_seconds=0, timeout_minutes=0)

    assert client.messages.batches.cancelled == ["batch_1"]
    (row,) = session.query(Summary).all()
    assert row.document_id == doc_id


def test_dedup_and_skips_never_enter_the_batch(client, session):
    original = _doc(session, "a.pdf", SHORT)
    duplicate = _doc(session, "b.pdf", SHORT)
    tiny = _doc(session, "c.pdf", "too short")

    summarizer.run_summarizer(poll_seconds=0)

    (requests,) = client.messages.batches.created
    assert [int(r["custom_id"]) for r in requests] == [original]
    rows = {s.document_id: s.model_used for s in session.query(Summary).all()}
    assert rows[duplicate] == f"dedup:{summarizer.MODEL_HAIKU}"
    assert tiny not in rows


def test_nothing_pending_means_no_batch_is_created(client, session):
    _doc(session, "c.pdf", "too short")

    summarizer.run_summarizer(poll_seconds=0)

    assert client.messages.batches.created == []


def test_sync_mode_calls_messages_create_directly(client, session, monkeypatch):
    monkeypatch.setenv("SUMMARIZE_MODE", "sync")
    doc_id = _doc(session, "agenda.pdf", SHORT)

    summarizer.run_summarizer()

    assert client.messages.batches.created == []
    assert [c["model"] for c in client.sync_calls] == [summarizer.MODEL_HAIKU]
    (row,) = session.query(Summary).all()
    assert row.document_id == doc_id
