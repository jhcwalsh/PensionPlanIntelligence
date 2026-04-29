"""
Claude tool-use wrapper for RFP extraction, with a deterministic mock mode.

The same cache key (sha256 of prompt+chunk_text) is used in both modes so
fixture responses keyed off the key drop in seamlessly. Tests run with
LLM_MODE=mock and never touch the network.

Real mode forces tool use so the model returns schema-valid JSON or
nothing.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from lib.schema_validator import load_schema, validate_record
from rfp.relevance import Chunk

PROMPT_PATH = Path(__file__).parent / "prompts" / "rfp_v1.md"
DEFAULT_MODEL = "claude-sonnet-4-6"
DEFAULT_MAX_TOKENS = 4096
DEFAULT_TIMEOUT_SECONDS = 60.0

# Pulled in via env on first call so import-time has no side effects.
_FIXTURE_DIR_ENV = "LLM_FIXTURE_DIR"
DEFAULT_FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "llm_responses"


@dataclass(frozen=True)
class LLMResult:
    """Result of one Claude call: a list of raw record dicts plus the cache key."""
    records: list[dict[str, Any]]
    cache_key: str
    model: str


def _load_prompt() -> str:
    return PROMPT_PATH.read_text(encoding="utf-8")


def _build_tool_input_schema() -> dict[str, Any]:
    """Wrap the rfp_schema as items in an array, which the model returns."""
    return {
        "type": "object",
        "properties": {
            "rfps": {
                "type": "array",
                "items": load_schema(),
            }
        },
        "required": ["rfps"],
    }


def cache_key(prompt: str, chunk_text: str, plan_id: str, document_id: int) -> str:
    """
    Deterministic 16-hex key over (prompt, chunk text, plan, document).

    plan_id and document_id are included so identical chunk text from two
    different documents doesn't collide (real PDFs sometimes copy boilerplate
    between board packets).
    """
    h = hashlib.sha256()
    h.update(prompt.encode("utf-8"))
    h.update(b"|")
    h.update(plan_id.encode("utf-8"))
    h.update(b"|")
    h.update(str(document_id).encode("utf-8"))
    h.update(b"|")
    h.update(chunk_text.encode("utf-8"))
    return h.hexdigest()[:16]


def _fixture_dir() -> Path:
    return Path(os.environ.get(_FIXTURE_DIR_ENV, str(DEFAULT_FIXTURE_DIR)))


def _read_fixture(key: str) -> dict[str, Any]:
    path = _fixture_dir() / f"{key}.json"
    if not path.exists():
        # No matching fixture → no records (lets us add new test docs without
        # writing a "no RFPs" fixture for every chunk).
        return {"rfps": []}
    with path.open() as f:
        return json.load(f)


def _filter_valid_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop records that don't conform to the schema; log silently for now."""
    out = []
    for rec in records:
        if not validate_record(rec):
            out.append(rec)
    return out


def _client():
    """Reuse the existing summarizer client bootstrap (env var → OAuth fallback)."""
    from summarizer import _get_client
    return _get_client()


def _real_extract(
    *,
    chunk: Chunk,
    plan_id: str,
    document_id: int,
    document_url: str,
    model: str,
) -> list[dict[str, Any]]:
    import anthropic  # noqa: F401  — surface a clear import error if missing

    prompt = _load_prompt()
    system = (
        "You are extracting RFP records. The source document URL is "
        f"{document_url} and document_id is {document_id}. "
        "Use these exact values for source_document.url and "
        "source_document.document_id in every record you emit."
    )
    user = (
        f"{prompt}\n\n---\n\nDocument excerpt for plan_id={plan_id}, "
        f"document_id={document_id}:\n\n{chunk.text}"
    )

    tool = {
        "name": "report_rfps",
        "description": "Report all RFP records found in the excerpt.",
        "input_schema": _build_tool_input_schema(),
    }

    msg = _client().messages.create(
        model=model,
        max_tokens=DEFAULT_MAX_TOKENS,
        system=system,
        messages=[{"role": "user", "content": user}],
        tools=[tool],
        tool_choice={"type": "tool", "name": "report_rfps"},
        timeout=DEFAULT_TIMEOUT_SECONDS,
    )

    for block in msg.content:
        if getattr(block, "type", None) == "tool_use" and block.name == "report_rfps":
            return list(block.input.get("rfps", []))
    return []


def extract_rfps(
    *,
    chunk: Chunk,
    plan_id: str,
    document_id: int,
    document_url: str,
    model: str = DEFAULT_MODEL,
) -> LLMResult:
    """
    Run the RFP extraction prompt against one chunk. Returns validated records.

    LLM_MODE=mock reads from fixtures/llm_responses/{cache_key}.json and
    returns its contents (or [] if no fixture). LLM_MODE unset (or any other
    value) calls Claude with tool-use forced.
    """
    prompt = _load_prompt()
    key = cache_key(prompt, chunk.text, plan_id, document_id)

    if os.environ.get("LLM_MODE", "").lower() == "mock":
        payload = _read_fixture(key)
        records = list(payload.get("rfps", []))
    else:
        records = _real_extract(
            chunk=chunk,
            plan_id=plan_id,
            document_id=document_id,
            document_url=document_url,
            model=model,
        )

    return LLMResult(
        records=_filter_valid_records(records),
        cache_key=key,
        model=model if os.environ.get("LLM_MODE", "").lower() != "mock" else "mock",
    )
