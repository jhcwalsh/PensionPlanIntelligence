"""LLM-assisted manager-name normalization.

Pulls every distinct manager string from Summary.investment_actions, sends
them to Claude in batches, and writes canonical mappings to
``data/manager_mappings.json``.

The mapping format is a flat dict keyed by raw name:

    {
      "BlackRock":      {"canonical": "BlackRock, Inc.", "is_manager": true},
      "BlackRock Inc.": {"canonical": "BlackRock, Inc.", "is_manager": true},
      "Various":        {"canonical": null, "is_manager": false,
                         "reason": "placeholder, not a real entity"}
    }

Idempotent: re-running picks up any new raw names that aren't in the
existing mapping; entries already present are left untouched. To
re-process a name, delete its key from the JSON first.

Usage:
    python -m scripts.normalize_managers              # process all unmapped names
    python -m scripts.normalize_managers --batch 100  # batch size (default 60)
    python -m scripts.normalize_managers --dry-run    # print what would happen, no LLM calls
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MAPPINGS_PATH = REPO_ROOT / "data" / "manager_mappings.json"

_PROMPT_INSTRUCTIONS = """\
You are normalizing pension-fund manager / consultant / vendor names extracted
from board-meeting summaries. The input is a list of raw strings; output one
JSON object per input, keyed by the raw string exactly as given.

For each raw name, decide:

1. ``is_manager`` (bool) — true if it names an investment manager, asset
   manager, consultant, custodian, advisor, fund-of-funds, or similar
   service provider that a pension plan would hire. False for things that
   slipped through extraction by mistake: generic placeholders ("Various",
   "TBD", "N/A"), pension plan names themselves ("Illinois State Board of
   Investment"), asset class names ("Private Equity"), descriptive phrases
   ("the firm").

2. ``canonical`` (string or null) — if is_manager, the canonical form
   (preferring the most complete brand name). Drop trailing legal suffixes
   like ", Inc.", ", LLC", ", L.P." from the canonical UNLESS the suffix
   disambiguates two distinct entities. Example: prefer "BlackRock" over
   "BlackRock, Inc." but keep "Apollo Global Management" (not "Apollo")
   because "Apollo" is ambiguous in finance.

   Cluster variants to the same canonical: "BlackRock", "BlackRock Inc.",
   "BlackRock Asset Management" → all canonical = "BlackRock".

   If is_manager is false, canonical = null.

3. ``reason`` (optional string) — only for is_manager=false, brief reason.

Output: a single JSON object. Keys are the raw input strings exactly. No
prose, no code fences, no commentary.

Example input:
  ["BlackRock", "BlackRock, Inc.", "Various", "Illinois State Board of Investment"]

Example output:
  {
    "BlackRock": {"canonical": "BlackRock", "is_manager": true},
    "BlackRock, Inc.": {"canonical": "BlackRock", "is_manager": true},
    "Various": {"canonical": null, "is_manager": false, "reason": "placeholder"},
    "Illinois State Board of Investment": {"canonical": null, "is_manager": false, "reason": "pension plan, not a manager"}
  }
"""


def _load_existing() -> dict:
    if MAPPINGS_PATH.exists():
        return json.loads(MAPPINGS_PATH.read_text())
    return {}


def _save(mapping: dict) -> None:
    MAPPINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    MAPPINGS_PATH.write_text(
        json.dumps(mapping, indent=2, sort_keys=True, ensure_ascii=False)
    )


def _classify_batch(client, batch: list[str]) -> dict:
    """Send one batch to Claude and parse the JSON response."""
    user_msg = (
        "Normalize these manager names. Output the JSON object described "
        "in the system prompt — no prose, no code fences.\n\n"
        + json.dumps(batch, ensure_ascii=False)
    )
    msg = client.messages.create(
        model="claude-sonnet-4-6",
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="scripts.normalize_managers")
    parser.add_argument("--batch", type=int, default=60,
                        help="Names per LLM call (default 60)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would happen but don't call the LLM")
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap on total new names processed in this run")
    args = parser.parse_args(argv)

    sys.path.insert(0, str(REPO_ROOT))
    from database import get_session, aggregate_managers

    session = get_session()
    agg = aggregate_managers(session)
    all_raw = sorted({r["raw_name"] for r in agg})

    existing = _load_existing()
    unmapped = [n for n in all_raw if n not in existing]
    if args.limit:
        unmapped = unmapped[: args.limit]

    print(f"Distinct raw names in DB:        {len(all_raw):,}")
    print(f"Already mapped in {MAPPINGS_PATH.name}: {len(existing):,}")
    print(f"To process this run:             {len(unmapped):,}")
    if not unmapped:
        print("Nothing to do.")
        return 0

    if args.dry_run:
        print("First 10 unmapped names:")
        for n in unmapped[:10]:
            print(f"  - {n!r}")
        return 0

    from summarizer import _get_client
    client = _get_client()

    new_entries = 0
    failed_batches = 0
    for i in range(0, len(unmapped), args.batch):
        batch = unmapped[i : i + args.batch]
        print(f"  batch {i // args.batch + 1}: classifying {len(batch)} names...",
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
    return 0 if failed_batches == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
