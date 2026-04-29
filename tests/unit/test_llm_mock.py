"""rfp/llm.py mock mode reads from a fixture dir and validates records."""

from __future__ import annotations

import json

from lib.pipeline_diagnostic import TASK_PROFILES
from rfp.ids import compute_rfp_id
from rfp.llm import cache_key, extract_rfps
from rfp.relevance import Chunk, PageText


def _chunk():
    return Chunk(pages=(
        PageText(1, "RFP issued for general consulting services on 2024-03-15."),
    ))


def _good_record():
    return {
        "rfp_id": compute_rfp_id("calpers", "Consultant", "2024-03-15", "Test RFP"),
        "plan_id": "calpers",
        "rfp_type": "Consultant",
        "title": "Test RFP",
        "status": "Issued",
        "release_date": "2024-03-15",
        "response_due_date": None,
        "award_date": None,
        "mandate_size_usd_millions": None,
        "asset_class": None,
        "incumbent_manager": None,
        "incumbent_manager_id": None,
        "shortlisted_managers": [],
        "awarded_manager": None,
        "source_document": {
            "url": "https://example.com/doc.pdf",
            "page_number": 1,
            "document_id": 99,
        },
        "source_quote": "RFP issued for general consulting services on 2024-03-15.",
        "extraction_confidence": 0.9,
    }


def test_cache_key_is_stable():
    a = cache_key("prompt", "chunk", "calpers", 1)
    b = cache_key("prompt", "chunk", "calpers", 1)
    assert a == b
    assert len(a) == 16


def test_cache_key_changes_with_inputs():
    base = cache_key("prompt", "chunk", "calpers", 1)
    assert cache_key("prompt2", "chunk", "calpers", 1) != base
    assert cache_key("prompt", "chunk2", "calpers", 1) != base
    assert cache_key("prompt", "chunk", "calstrs", 1) != base
    assert cache_key("prompt", "chunk", "calpers", 2) != base


def test_mock_mode_returns_empty_when_no_fixture(monkeypatch, tmp_path):
    monkeypatch.setenv("LLM_FIXTURE_DIR", str(tmp_path))
    result = extract_rfps(
        chunk=_chunk(), plan_id="calpers",
        document_id=99, document_url="https://example.com/doc.pdf",
    )
    assert result.records == []
    assert result.model == "mock"


def test_mock_mode_loads_fixture(monkeypatch, tmp_path):
    monkeypatch.setenv("LLM_FIXTURE_DIR", str(tmp_path))
    chunk = _chunk()
    # Compute the same key the implementation will compute
    from rfp.llm import _load_prompt
    key = cache_key(_load_prompt(), chunk.text, "calpers", 99)
    (tmp_path / f"{key}.json").write_text(json.dumps({"rfps": [_good_record()]}))

    result = extract_rfps(
        chunk=chunk, plan_id="calpers",
        document_id=99, document_url="https://example.com/doc.pdf",
    )
    assert len(result.records) == 1
    assert result.records[0]["rfp_type"] == "Consultant"


def test_mock_mode_drops_invalid_records(monkeypatch, tmp_path):
    monkeypatch.setenv("LLM_FIXTURE_DIR", str(tmp_path))
    chunk = _chunk()
    bad = _good_record()
    bad["rfp_type"] = "NotAValidType"
    from rfp.llm import _load_prompt
    key = cache_key(_load_prompt(), chunk.text, "calpers", 99)
    (tmp_path / f"{key}.json").write_text(json.dumps({"rfps": [bad, _good_record()]}))

    result = extract_rfps(
        chunk=chunk, plan_id="calpers",
        document_id=99, document_url="https://example.com/doc.pdf",
    )
    assert len(result.records) == 1
