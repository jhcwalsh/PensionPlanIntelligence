"""Deterministic rfp_id derivation."""

from __future__ import annotations

import hashlib
import re

_WHITESPACE_RE = re.compile(r"\s+")
_PUNCT_RE = re.compile(r"[^\w\s]")


def normalize_title(title: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace."""
    cleaned = _PUNCT_RE.sub(" ", title.lower())
    return _WHITESPACE_RE.sub(" ", cleaned).strip()


def compute_rfp_id(
    plan_id: str,
    rfp_type: str,
    anchor_date: str | None,
    title: str,
) -> str:
    """
    Deterministic 16-char hex id.

    Same (plan_id, rfp_type, anchor_date, normalized_title) always produce the
    same id, so re-running the pipeline against the same document upserts
    rather than duplicates. anchor_date is the first non-null of
    (release_date, response_due_date, award_date) — pick that ordering at the
    call site so partial RFPs (Planned → Issued → Awarded) keep the same id
    across status transitions when at least one date is stable.
    """
    payload = "|".join([
        plan_id,
        rfp_type,
        anchor_date or "",
        normalize_title(title),
    ])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
