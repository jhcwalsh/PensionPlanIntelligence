"""Build the plan-to-fund table that answers "who else is in this fund?".

Extracts any new commitment/hire actions with Haiku (cached by content hash,
so a nightly run only pays for new ones), then rebuilds ``funds`` and
``fund_commitments`` from the cache. See funds/build.py.

Usage:
    python -m scripts.build_fund_commitments                 # extract new + rebuild
    python -m scripts.build_fund_commitments --limit 80      # cap actions extracted
    python -m scripts.build_fund_commitments --rebuild-only  # no model calls (after a mappings edit)
    python -m scripts.build_fund_commitments --dry-run       # count pending, change nothing
"""
from __future__ import annotations

import argparse
import os
import sys

from database import FundActionExtract, get_session, init_db
from funds.build import collect_actions, extract_pending, rebuild


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="scripts.build_fund_commitments")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--rebuild-only", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    init_db()
    session = get_session()
    try:
        if args.dry_run:
            done = {h for (h,) in session.query(FundActionExtract.action_hash)}
            pending = {inp.action_hash for inp, _ in collect_actions(session)} - done
            print(f"Actions pending extraction: {len(pending):,}")
            return 0

        failed = 0
        if not args.rebuild_only:
            client = None
            if os.environ.get("LLM_MODE") != "mock":
                from summarizer import _get_client
                client = _get_client()
            extracted, failed = extract_pending(session, client, args.limit)
            print(f"Extracted {extracted:,} actions ({failed} failed batches)")

        counts = rebuild(session)
        print("Rebuilt: " + ", ".join(f"{k}={v:,}" for k, v in counts.items()))
        return 1 if failed else 0
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
