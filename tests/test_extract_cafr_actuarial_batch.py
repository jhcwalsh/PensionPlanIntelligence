"""Actuarial extraction: PDFs from R2 when not on disk, and one batch per run.

The monthly runner only holds the CAFRs it downloaded that same run, so
on 2026-09-01 97 of 137 came back "no_section" that were really "no
file". Every retained PDF is in R2; the extractor now reads from there.
And the job is monthly, so nothing waits on it: batch rate.
"""
import types
from decimal import Decimal

import pytest

import costs
import extract_cafr_actuarial as eca
import pdf_store
from database import ApiUsage, CafrActuarial, Document, Plan, get_session


@pytest.fixture
def live(monkeypatch):
    """Leave extract_one's mock branch; stub PDF handling to a fixed section."""
    monkeypatch.setenv("LLM_MODE", "live")
    monkeypatch.setattr(eca, "locate_actuarial_section", lambda path: (1, 1))
    monkeypatch.setattr(eca, "extract_section_text",
                        lambda path, s, e: "Funded ratio 75.0% " * 50)


def _seed(local_path=None, sha=None, n=1):
    s = get_session()
    s.add(Plan(id="p1", name="P", abbreviation="P", state="CA"))
    ids = []
    for i in range(n):
        d = Document(plan_id="p1", url=f"https://x/cafr{i}.pdf", filename=f"cafr{i}.pdf",
                     doc_type="cafr", extraction_status="done", fiscal_year=2020 + i,
                     local_path=local_path, content_sha256=sha)
        s.add(d); s.commit(); ids.append(d.id)
    s.close()
    return ids


def _tool_result(payload=None):
    block = types.SimpleNamespace(type="tool_use", name="record_actuarial_data",
                                  input=payload or eca.MOCK_PAYLOAD)
    msg = types.SimpleNamespace(content=[block], stop_reason="tool_use",
                                usage=types.SimpleNamespace(
                                    input_tokens=1000, output_tokens=100,
                                    cache_creation_input_tokens=0, cache_read_input_tokens=0))
    return types.SimpleNamespace(type="succeeded", message=msg)


class FakeBatches:
    def __init__(self):
        self.created = []
        self.outcome = {}

    def create(self, requests):
        self.created.append(requests)
        return types.SimpleNamespace(id="b1", processing_status="in_progress")

    def retrieve(self, batch_id):
        return types.SimpleNamespace(id=batch_id, processing_status="ended")

    def results(self, batch_id):
        for req in self.created[-1]:
            yield types.SimpleNamespace(
                custom_id=req["custom_id"],
                result=self.outcome.get(int(req["custom_id"]), _tool_result()))


@pytest.fixture
def client(monkeypatch):
    fb = FakeBatches()
    sync = []

    def create(**kw):
        sync.append(kw)
        return _tool_result().message

    fake = types.SimpleNamespace(messages=types.SimpleNamespace(create=create, batches=fb))
    fake.sync = sync
    monkeypatch.setattr(eca, "_get_client", lambda: fake)
    return fake


# ---------------------------------------------------------------------------
# R2 fallback
# ---------------------------------------------------------------------------

def test_missing_local_pdf_is_fetched_from_r2(tmp_db, live, client, monkeypatch):
    for k in ("R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET"):
        monkeypatch.setenv(k, "x")
    fetched = []
    monkeypatch.setattr(pdf_store, "get", lambda cfg, sha: fetched.append(sha) or b"%PDF-1.4 stub")
    _seed(local_path="/nowhere/cafr.pdf", sha="abc123")

    counts = eca.run_extraction(["p1"], poll_seconds=0)

    assert fetched == ["abc123"]
    assert counts["saved"] == 1


def test_pdf_missing_everywhere_is_reported_as_no_file(tmp_db, live, client, monkeypatch):
    monkeypatch.delenv("R2_ACCOUNT_ID", raising=False)
    _seed(local_path="/nowhere/cafr.pdf", sha=None)

    counts = eca.run_extraction(["p1"], poll_seconds=0)

    assert counts["no_file"] == 1
    assert client.messages.batches.created == []


# ---------------------------------------------------------------------------
# Batch
# ---------------------------------------------------------------------------

def test_batch_submits_one_request_per_cafr_and_saves_each_result(tmp_db, live, client, tmp_path):
    pdf = tmp_path / "cafr.pdf"; pdf.write_bytes(b"%PDF-1.4 stub")
    ids = _seed(local_path=str(pdf), n=2)

    counts = eca.run_extraction(["p1"], poll_seconds=0)

    (requests,) = client.messages.batches.created
    assert sorted(int(r["custom_id"]) for r in requests) == sorted(ids)
    params = requests[0]["params"]
    assert params["model"] == eca.MODEL
    assert params["tool_choice"] == {"type": "tool", "name": "record_actuarial_data"}
    assert params["thinking"] == {"type": "disabled"}
    assert client.sync == []
    assert counts["saved"] == 2
    s = get_session()
    assert s.query(CafrActuarial).count() == 2
    s.close()


def test_batch_usage_is_recorded_at_half_price(tmp_db, live, client, tmp_path, monkeypatch):
    monkeypatch.delenv("LLM_MODE", raising=False)
    monkeypatch.setenv("LLM_MODE", "live")
    monkeypatch.delenv("INSIGHTS_MODE", raising=False)
    pdf = tmp_path / "cafr.pdf"; pdf.write_bytes(b"%PDF-1.4 stub")
    _seed(local_path=str(pdf))

    with costs.track("cafr_extract", run_id="t"):
        eca.run_extraction(["p1"], poll_seconds=0)

    (row,) = get_session().query(ApiUsage).all()
    assert row.operation == "cafr_extract"
    full = costs.cost_usd(eca.MODEL, _tool_result().message.usage)
    assert Decimal(row.cost_usd) == full / 2


def test_errored_result_counts_as_failed_and_saves_nothing(tmp_db, live, client, tmp_path):
    pdf = tmp_path / "cafr.pdf"; pdf.write_bytes(b"%PDF-1.4 stub")
    (doc_id,) = _seed(local_path=str(pdf))
    client.messages.batches.outcome[doc_id] = types.SimpleNamespace(
        type="errored", error=types.SimpleNamespace(error=types.SimpleNamespace(message="boom")))

    counts = eca.run_extraction(["p1"], poll_seconds=0)

    assert counts["failed"] == 1
    assert get_session().query(CafrActuarial).count() == 0


def test_already_extracted_cafrs_stay_out_of_the_batch(tmp_db, live, client, tmp_path):
    pdf = tmp_path / "cafr.pdf"; pdf.write_bytes(b"%PDF-1.4 stub")
    _seed(local_path=str(pdf))
    eca.run_extraction(["p1"], poll_seconds=0)

    counts = eca.run_extraction(["p1"], poll_seconds=0)

    assert counts["already_have"] == 1
    assert len(client.messages.batches.created) == 1


def test_sync_mode_calls_create_directly(tmp_db, live, client, tmp_path):
    pdf = tmp_path / "cafr.pdf"; pdf.write_bytes(b"%PDF-1.4 stub")
    _seed(local_path=str(pdf))

    counts = eca.run_extraction(["p1"], mode="sync")

    assert counts["saved"] == 1
    assert len(client.sync) == 1
    assert client.messages.batches.created == []
