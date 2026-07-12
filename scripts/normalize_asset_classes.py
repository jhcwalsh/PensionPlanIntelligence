"""LLM-assisted asset-class normalization.

Pulls every distinct asset-class label from CAFRs, IPS documents, summaries, and
RFP records, sends them to Claude in batches, and writes canonical mappings to
``data/asset_class_mappings.json``.

The mapping format is a flat dict keyed by raw label:

    {
      "Global Equity": {"canonical": "public_equity_global", "confidence": "high"},
      "Equity":        {"canonical": "public_equity_us", "confidence": "medium"},
      "Private Debt":  {"canonical": "unmapped", "confidence": "low"}
    }

Idempotent: re-running picks up any new raw labels that aren't in the existing
mapping; entries already present are left untouched. To re-process a label,
delete its key from the JSON first.

Usage:
    python -m scripts.normalize_asset_classes              # process all unmapped labels
    python -m scripts.normalize_asset_classes --batch 60   # batch size (default 60)
    python -m scripts.normalize_asset_classes --dry-run    # print what would happen, no LLM calls
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MAPPINGS_PATH = REPO_ROOT / "data" / "asset_class_mappings.json"

_PROMPT_INSTRUCTIONS = """\
You are normalizing asset-class labels extracted from pension fund investment
documents (CAFRs, IPSs, meeting minutes, RFPs). The input is a list of raw
strings; output one JSON object per input, keyed by the raw string exactly as
given.

For each raw label, decide:

1. ``canonical`` (string) — map to EXACTLY ONE of these canonical asset classes:
   - "public_equity_us" — U.S. common stock, equity indices
   - "public_equity_non_us" — non-U.S. common stock, international equity
   - "public_equity_global" — global/world equity baskets, multi-region
   - "fixed_income_core" — investment-grade bonds, bond indices
   - "fixed_income_credit" — high-yield, credit-focused bonds
   - "private_equity" — private equity, leveraged buyout, venture capital
   - "private_credit" — private debt, direct lending, private credit funds
   - "real_estate" — real estate, REITs, property
   - "real_assets_infrastructure" — infrastructure, commodities, real assets
   - "hedge_funds_absolute_return" — hedge funds, alternatives, absolute return
   - "cash_short_term" — cash, short-term instruments, money market
   - "opportunistic_other" — opportunistic, other, multi-strategy
   - "total" — total fund, blended allocation, composite
   - "unmapped" — genuinely ambiguous, unknown, or not an asset class (e.g. "Staff", "Performance")

   If a label is ambiguous or doesn't fit (e.g. "Other", "TBD", "Staff", "YTD Return"),
   classify as "unmapped".

2. ``confidence`` (string: "high", "medium", or "low") — your confidence in the mapping:
   - "high" — unambiguous match (e.g. "US Equities" → "public_equity_us")
   - "medium" — reasonable but non-obvious mapping (e.g. "Long-Only" → "hedge_funds_absolute_return")
   - "low" — speculative, generic, or marginal fit (e.g. "Alternative Strategies" → "opportunistic_other")

Output: a single JSON object. Keys are the raw input strings exactly. No
prose, no code fences, no commentary.

Example input:
  ["Global Equities", "US Stocks", "Private Debt", "Staff", "Total Fund"]

Example output:
  {
    "Global Equities": {"canonical": "public_equity_global", "confidence": "high"},
    "US Stocks": {"canonical": "public_equity_us", "confidence": "high"},
    "Private Debt": {"canonical": "private_credit", "confidence": "medium"},
    "Staff": {"canonical": "unmapped", "confidence": "high"},
    "Total Fund": {"canonical": "total", "confidence": "high"}
  }
"""


def _load_existing() -> dict:
    if MAPPINGS_PATH.exists():
        return json.loads(MAPPINGS_PATH.read_text(encoding="utf-8"))
    return {}


def _save(mapping: dict) -> None:
    MAPPINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    MAPPINGS_PATH.write_text(
        json.dumps(mapping, indent=2, sort_keys=True, ensure_ascii=False)
    )


def _classify_batch(client, batch: list[str]) -> dict:
    """Send one batch to Claude and parse the JSON response."""
    # Mock mode: return all labels as unmapped with low confidence
    if os.environ.get("LLM_MODE") == "mock":
        return {label: {"canonical": "unmapped", "confidence": "low"} for label in batch}

    user_msg = (
        "Normalize these asset-class labels. Output the JSON object described "
        "in the system prompt — no prose, no code fences.\n\n"
        + json.dumps(batch, ensure_ascii=False)
    )
    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=8192,
        temperature=0,
        system=[{
            "type": "text",
            "text": _PROMPT_INSTRUCTIONS,
            "cache_control": {"type": "ephemeral"},
        }],
        messages=[{"role": "user", "content": user_msg}],
    )
    text = msg.content[0].text.strip()
    # Tolerate a leading code fence even though the prompt forbids it
    if text.startswith("```"):
        text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
    return json.loads(text)


def collect_distinct_labels(session) -> list[str]:
    """Gather all distinct asset-class labels from the database.

    Sources:
    - CafrAllocation.asset_class
    - CafrPerformance.scope (excluding "total_fund")
    - Summary.investment_actions[].asset_class (JSON list)
    - RFPRecord.record asset_class (from JSON)
    - IpsAllocation.asset_class

    Returns a sorted list of stripped, non-empty labels (deduplicated).
    """
    labels = set()

    # CafrAllocation.asset_class
    from database import CafrAllocation
    for row in session.query(CafrAllocation.asset_class).all():
        if row.asset_class:
            stripped = row.asset_class.strip()
            if stripped:
                labels.add(stripped)

    # CafrPerformance.scope (excluding "total_fund")
    from database import CafrPerformance
    for row in session.query(CafrPerformance.scope).distinct().all():
        if row.scope and row.scope.strip() != "total_fund":
            stripped = row.scope.strip()
            if stripped:
                labels.add(stripped)

    # Summary.investment_actions
    from database import Summary
    import json as json_lib
    for row in session.query(Summary.investment_actions).all():
        if row.investment_actions:
            try:
                actions = json_lib.loads(row.investment_actions)
                if isinstance(actions, list):
                    for act in actions:
                        if isinstance(act, dict) and act.get("asset_class"):
                            stripped = str(act["asset_class"]).strip()
                            if stripped:
                                labels.add(stripped)
            except (json_lib.JSONDecodeError, TypeError):
                pass

    # RFPRecord.record asset_class
    from database import RFPRecord
    for row in session.query(RFPRecord.record).all():
        if row.record:
            try:
                rec = json_lib.loads(row.record)
                if isinstance(rec, dict) and rec.get("asset_class"):
                    stripped = str(rec["asset_class"]).strip()
                    if stripped:
                        labels.add(stripped)
            except (json_lib.JSONDecodeError, TypeError):
                pass

    # IpsAllocation.asset_class
    from database import IpsAllocation
    for row in session.query(IpsAllocation.asset_class).all():
        if row.asset_class:
            stripped = row.asset_class.strip()
            if stripped:
                labels.add(stripped)

    return sorted(labels)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="scripts.normalize_asset_classes")
    parser.add_argument("--batch", type=int, default=60,
                        help="Labels per LLM call (default 60)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would happen but don't call the LLM")
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap on total new labels processed in this run")
    args = parser.parse_args(argv)

    sys.path.insert(0, str(REPO_ROOT))
    from database import get_session

    session = get_session()
    all_raw = collect_distinct_labels(session)

    existing = _load_existing()
    unmapped = [n for n in all_raw if n not in existing]
    if args.limit:
        unmapped = unmapped[: args.limit]

    print(f"Distinct raw labels in DB:       {len(all_raw):,}")
    print(f"Already mapped in {MAPPINGS_PATH.name}: {len(existing):,}")
    print(f"To process this run:             {len(unmapped):,}")
    if not unmapped:
        print("Nothing to do.")
        return 0

    if args.dry_run:
        print("First 10 unmapped labels:")
        for n in unmapped[:10]:
            print(f"  - {n!r}")
        return 0

    from summarizer import _get_client
    client = _get_client()

    new_entries = 0
    failed_batches = 0
    for i in range(0, len(unmapped), args.batch):
        batch = unmapped[i : i + args.batch]
        print(f"  batch {i // args.batch + 1}: classifying {len(batch)} labels...",
              end=" ", flush=True)
        try:
            result = _classify_batch(client, batch)
        except Exception as exc:  # noqa: BLE001
            print(f"FAILED: {type(exc).__name__}: {exc}")
            failed_batches += 1
            continue
        # Merge — only accept entries whose key was actually in the batch
        accepted = 0
        for raw in batch:
            if raw in result:
                existing[raw] = result[raw]
                accepted += 1
        _save(existing)  # save after every batch so partial runs are durable
        new_entries += accepted
        print(f"got {accepted}/{len(batch)} (saved)")

    print()
    print(f"Done. Added {new_entries} mappings; {failed_batches} batches failed.")
    print(f"Total mapped: {len(existing):,} of {len(all_raw):,}")
    session.close()
    return 0 if failed_batches == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
