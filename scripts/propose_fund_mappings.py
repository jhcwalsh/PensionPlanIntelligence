"""Propose fuzzy fund merges and new manager mappings for James to review.

The nightly build merges only on an exact key. This script is the only path
for anything looser, and it changes nothing but two JSON files in git:

1. Manager strings Haiku extracted that ``data/manager_mappings.json`` has
   never seen are classified by the existing manager normaliser.
2. Fund keys with the same manager and number whose series differ only by
   extra words ("bcp capital partners" vs "capital partners") are put to Haiku
   one pair at a time. Only a confident "same fund" becomes an alias in
   ``data/fund_mappings.json``; everything else is recorded under ``separate``
   so it is not asked again.

Review the diff, commit, then ``python -m scripts.build_fund_commitments
--rebuild-only`` to apply.

Usage:
    python -m scripts.propose_fund_mappings [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict

from database import Fund, FundActionExtract, get_session
from funds import build
from summarizer import MODEL_HAIKU, message_text

_PAIR_PROMPT = """\
Two U.S. pension board documents name these private-markets funds. Are they
the same fund (the same pool of capital; parallel vehicles count as the
same)? Different fund numbers, strategies or sleeves mean different funds.

Fund A: {a}
Fund B: {b}

Answer ONLY with JSON: {{"same": true|false, "confidence": "high"|"low"}}"""


def candidate_pairs(keys: list[str], mappings: dict) -> list[tuple[str, str]]:
    aliases = mappings.get("aliases", {})
    separate = {frozenset(p) for p in mappings.get("separate", [])}
    groups = defaultdict(list)
    for k in keys:
        manager, series, number = k.split("|")
        if number and k not in aliases:
            groups[(manager, number)].append((k, set(series.split())))
    pairs = []
    for members in groups.values():
        for i, (ka, sa) in enumerate(members):
            for kb, sb in members[i + 1:]:
                if sa == sb or not (sa <= sb or sb <= sa):
                    continue
                longer, shorter = (ka, kb) if len(sa) > len(sb) else (kb, ka)
                if frozenset((longer, shorter)) not in separate:
                    pairs.append((longer, shorter))
    return sorted(pairs)


def _ask_same(client, a: str, b: str) -> bool:
    msg = client.messages.create(
        model=MODEL_HAIKU, max_tokens=50, temperature=0,
        messages=[{"role": "user", "content": _PAIR_PROMPT.format(a=a, b=b)}],
    )
    text = message_text(msg).strip().strip("`").removeprefix("json").strip()
    verdict = json.loads(text)
    return verdict.get("same") is True and verdict.get("confidence") == "high"


def _new_manager_strings(session, known: dict) -> list[str]:
    names = set()
    for (items_json,) in session.query(FundActionExtract.items_json):
        for item in json.loads(items_json):
            m = (item.get("manager") or "").strip()
            if m and m not in known:
                names.add(m)
    return sorted(names)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="scripts.propose_fund_mappings")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    mock = os.environ.get("LLM_MODE") == "mock"

    session = get_session()
    try:
        from scripts import normalize_managers as nm
        known = nm._load_existing()
        new_managers = _new_manager_strings(session, known)
        names = {k: n for k, n in session.query(Fund.fund_key, Fund.canonical_name)}
    finally:
        session.close()

    mappings = build.load_fund_mappings()
    pairs = candidate_pairs(list(names), mappings)
    print(f"New manager strings: {len(new_managers):,}   candidate fund pairs: {len(pairs):,}")
    if args.dry_run:
        return 0

    client = None
    if not mock:
        from summarizer import _get_client
        client = _get_client()

    if new_managers and client is not None:
        for i in range(0, len(new_managers), 60):
            batch = new_managers[i:i + 60]
            result = nm._classify_batch(client, batch)
            known.update({raw: result[raw] for raw in batch if raw in result})
            nm._save(known)

    aliased = 0
    for longer, shorter in pairs:
        same = (client is not None
                and _ask_same(client, names.get(longer) or longer, names.get(shorter) or shorter))
        if same:
            mappings["aliases"][longer] = shorter
            aliased += 1
        else:
            mappings["separate"].append([longer, shorter])
    build.FUND_MAPPINGS_PATH.write_text(
        json.dumps(mappings, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8")
    print(f"Proposed {aliased} aliases, {len(pairs) - aliased} kept separate. Review the git diff.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
