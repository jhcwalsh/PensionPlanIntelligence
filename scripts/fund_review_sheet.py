"""Write a precision-review sheet: a random sample of funds held by 2+ plans,
each holder with the raw name and board-document quote it was matched on.

A reviewer reads each fund's rows and asks: is every one of these the same
fund? Any "no" is a false merge to fix before the tool is trusted.

Usage:
    python -m scripts.fund_review_sheet --n 50 --seed 1 --out <path.md>
"""
from __future__ import annotations

import argparse
import json
import random
import sys
from collections import defaultdict

from database import Fund, FundCommitment, Plan, get_session


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="scripts.fund_review_sheet")
    parser.add_argument("--n", type=int, default=50)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)

    s = get_session()
    try:
        plans = {p.id: p.name for p in s.query(Plan)}
        by_fund = defaultdict(list)
        for c in s.query(FundCommitment).filter(FundCommitment.status == "approved",
                                                FundCommitment.fund_id.isnot(None)):
            by_fund[c.fund_id].append(c)
        multi = sorted(fid for fid, cs in by_fund.items() if len({c.plan_id for c in cs}) >= 2)
        sample = random.Random(args.seed).sample(multi, min(args.n, len(multi)))
        funds = {f.id: f for f in s.query(Fund).filter(Fund.id.in_(sample))}

        lines = [f"# Fund-crowding precision review (seed {args.seed})", "",
                 f"{len(multi)} funds have 2+ approved holders; {len(sample)} sampled.", ""]
        for fid in sample:
            f = funds[fid]
            lines += [f"## {f.canonical_name}", f"`{f.fund_key}`", ""]
            for c in sorted(by_fund[fid], key=lambda c: c.plan_id):
                ev = json.loads(c.evidence or "[]")
                quote = ev[0]["quote"] if ev else ""
                amt = f"${c.amount_millions:g}M" if c.amount_millions is not None else "n/a"
                lines.append(f"- **{plans.get(c.plan_id, c.plan_id)}**: \"{c.fund_name_raw}\", "
                             f"{amt}, {c.last_seen or 'undated'}, {c.match_confidence}. {quote}")
            lines.append("")
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines))
        print(f"Wrote {len(sample)} funds to {args.out}")
        return 0
    finally:
        s.close()


if __name__ == "__main__":
    sys.exit(main())
