"""Split board investment actions into one item per named fund (Haiku 4.5).

Input is the action JSON the summariser already produced — never the
document's extracted text. One action can bundle several funds ("KKR GII V
($258M), ASF IX ($248M), Actis 6 ...") and the fund name usually lives only in
the free-text description, so a cheap model call does the splitting and
labels each fund's status. Identity matching happens later and
deterministically, in funds/identity.py.
"""
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass

from summarizer import MODEL_HAIKU, message_text

MODEL = MODEL_HAIKU
BATCH_SIZE = 40
STATUSES = ("approved", "proposed", "pipeline", "declined", "unknown")

_SYSTEM = """\
You read investment actions extracted from U.S. public pension board meeting
documents and list the specific funds each one commits to.

For each action, output a list with one item per DISTINCT fund named:
  fund_name      the fund exactly as written (e.g. "GTCR Fund XV, L.P."), or
                 null if no specific fund is named. Never invent or complete a
                 fund name; "a real estate partnership with Almanac" is null.
  manager        the firm that manages that fund, or null
  amount_millions  that fund's own commitment in USD millions, or null. When
                 one total covers several funds, do not copy the total onto
                 each fund.
  status         one of:
                 approved  - the board voted to approve it, or a closing is recorded
                 proposed  - on the agenda for approval, or a staff/consultant
                             recommendation put to this board for a vote
                 pipeline  - a consultant pipeline, due-diligence list, or fund
                             "under consideration" with no vote sought yet
                 declined  - rejected, withdrawn or tabled
                 unknown   - none of the above can be told
  asset_class    e.g. Private Equity, Private Credit, Real Estate, Infrastructure, or null

Minutes usually record approvals; agendas and board packs usually carry
proposals. An action that is not a commitment to any fund gets an empty list.

Output ONLY a JSON object mapping each input index (as a string) to its list.
No prose, no code fences."""


@dataclass
class ActionInput:
    action_hash: str
    action: dict
    doc_type: str | None
    meeting_date: str | None


def action_hash(action: dict) -> str:
    return hashlib.sha1(
        json.dumps(action, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()


def _mock_items(action: dict) -> list[dict]:
    return [{
        "fund_name": action.get("fund_name") or action.get("description"),
        "manager": action.get("manager"),
        "amount_millions": action.get("amount_millions"),
        "status": action.get("_mock_status", "approved"),
        "asset_class": action.get("asset_class"),
    }]


def _clean_item(item) -> dict | None:
    if not isinstance(item, dict):
        return None
    status = item.get("status")
    amount = item.get("amount_millions")
    if not isinstance(amount, (int, float)) or isinstance(amount, bool):
        amount = None
    return {
        "fund_name": item.get("fund_name") or None,
        "manager": item.get("manager") or None,
        "amount_millions": amount,
        "status": status if status in STATUSES else "unknown",
        "asset_class": item.get("asset_class") or None,
    }


def extract_batch(client, batch: list[ActionInput]) -> dict[str, list[dict]]:
    """Per-fund items keyed by action hash. Missing indices are left out, so
    the caller retries them next run rather than caching an empty answer."""
    if os.environ.get("LLM_MODE") == "mock":
        return {a.action_hash: _mock_items(a.action) for a in batch}

    payload = {
        str(i): {
            "action": a.action.get("action"),
            "description": a.action.get("description"),
            "manager": a.action.get("manager"),
            "fund_name": a.action.get("fund_name"),
            "amount_millions": a.action.get("amount_millions"),
            "asset_class": a.action.get("asset_class"),
            "doc_type": a.doc_type,
            "meeting_date": a.meeting_date,
        }
        for i, a in enumerate(batch)
    }
    msg = client.messages.create(
        model=MODEL,
        max_tokens=8000,
        temperature=0,
        system=[{"type": "text", "text": _SYSTEM, "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": json.dumps(payload, ensure_ascii=False)}],
    )
    text = message_text(msg).strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
    data = json.loads(text)

    out: dict[str, list[dict]] = {}
    for idx, items in data.items():
        if not idx.isdigit() or int(idx) >= len(batch) or not isinstance(items, list):
            continue
        out[batch[int(idx)].action_hash] = [c for c in map(_clean_item, items) if c]
    return out
