"""Thin wrapper around lib/rfp_schema.json for runtime validation."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

import jsonschema

_SCHEMA_PATH = Path(__file__).parent / "rfp_schema.json"


@lru_cache(maxsize=1)
def load_schema() -> dict[str, Any]:
    with _SCHEMA_PATH.open() as f:
        return json.load(f)


@lru_cache(maxsize=1)
def _validator() -> jsonschema.Draft7Validator:
    return jsonschema.Draft7Validator(load_schema())


def validate_record(record: dict[str, Any]) -> list[str]:
    """Return a list of human-readable error strings; empty list = valid."""
    return [
        f"{'/'.join(str(p) for p in e.absolute_path) or '<root>'}: {e.message}"
        for e in _validator().iter_errors(record)
    ]


def is_valid(record: dict[str, Any]) -> bool:
    return not validate_record(record)
