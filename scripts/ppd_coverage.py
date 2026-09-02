"""Compare data/known_plans.json against the Public Plans Database.

    python -m scripts.ppd_coverage              # state totals + verdict
    python -m scripts.ppd_coverage --states     # per-state side by side
    python -m scripts.ppd_coverage --aum        # our aum_billions vs PPD's

PPD (Center for Retirement Research at Boston College) tracks 248 state and
local plans holding ~95% of US state/local pension assets, refreshed to FY2025
for 186 of them. It is the right benchmark for a registry of US *public* plans:
the P&I Top 200 that the phrase "top 200" usually means is mostly corporate DB
plans, which this project does not and should not track.

**This script deliberately does not decide whether a plan is covered.**
Two attempts at automatic name matching are recorded in git history and both
were wrong in ways that mattered. PPD's unit is the pension plan; ours is the
entity that invests the money, so "Washington PERS Plan 2/3" and five siblings
are one WSIB portfolio, and any name matcher reports five phantom gaps. Fixing
that by stripping boilerplate then deleted the words carrying the meaning and
called four tracked Houston funds missing; stripping the state name instead
scored "University of California" as covered by CalPERS on the shared word
"California" -- $110.8B hidden by a tokeniser.

So the output is the side-by-side a human reads, and the conclusions live in
docs/superpowers/notes/2026-09-02-ppd-coverage-review.md. State totals are the
one comparison immune to the roll-up problem, because whatever a state's plans
are called, both sides sum to the same money.
"""
from __future__ import annotations

import argparse
import json
import sys

import requests

API = "https://publicplansdata.org/api/"
REGISTRY = "data/known_plans.json"

STATES = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT",
    "Delaware": "DE", "District of Columbia": "DC", "Florida": "FL",
    "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID", "Illinois": "IL",
    "Indiana": "IN", "Iowa": "IA", "Kansas": "KS", "Kentucky": "KY",
    "Louisiana": "LA", "Maine": "ME", "Maryland": "MD", "Massachusetts": "MA",
    "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS",
    "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV",
    "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM",
    "New York": "NY", "North Carolina": "NC", "North Dakota": "ND",
    "Ohio": "OH", "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA",
    "Rhode Island": "RI", "South Carolina": "SC", "South Dakota": "SD",
    "Tennessee": "TN", "Texas": "TX", "Utah": "UT", "Vermont": "VT",
    "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
    "Wisconsin": "WI", "Wyoming": "WY",
}

# States where our tracked total exceeds PPD's because we record an investment
# board holding more than its DB pension plans. Not errors, and not coverage.
BOARD_WIDER = {
    "MN": "SBI invests non-pension state money as well as the three funds",
    "NY": "NY Common, NYC and NYSTRS are separate entries; PPD splits differently",
    "WA": "WSIB's total includes funds PPD lists as separate Plan 1/2/3 rows",
    "MA": "PRIM invests for plans PPD counts under other headings",
    "OR": "OST manages more than the PERS DB plan",
    "WV": "IMB invests several funds beyond the two PPD plans",
}


def fetch(start: int = 2022, end: int = 2026) -> list[dict]:
    r = requests.get(API, timeout=120, headers={"User-Agent": "Mozilla/5.0"},
                     params={"q": "QVariables",
                             "variables": "ppd_id,PlanName,StateName,fy,"
                                          "MktAssets_net",
                             "filterfystart": str(start),
                             "filterfyend": str(end), "format": "json"})
    r.raise_for_status()
    body = r.json()
    if not body or body[0].get("status") != "OK":
        raise RuntimeError(f"PPD API returned {body[:1]}")
    return body[1:]


def latest_by_plan(raw: list[dict]) -> list[dict]:
    """Newest fiscal year per plan that actually reports assets.

    A plan whose newest row has a null or zero market value is carried at its
    last real figure rather than dropped -- dropping it would silently shrink
    the benchmark and make our coverage look better than it is.
    """
    out: dict[str, dict] = {}
    for r in raw:
        mv = r.get("MktAssets_net")
        if not mv or float(mv) <= 0:
            continue
        cur = out.get(r["ppd_id"])
        if cur is None or int(r["fy"]) > int(cur["fy"]):
            out[r["ppd_id"]] = r
    for r in out.values():
        r["aum_b"] = float(r["MktAssets_net"]) / 1e6      # thousands -> billions
    return sorted(out.values(), key=lambda r: -r["aum_b"])


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--states", action="store_true",
                    help="per-state side-by-side listing, for reading by hand")
    ap.add_argument("--aum", action="store_true",
                    help="compare our aum_billions against PPD's market value")
    args = ap.parse_args()

    ranked = latest_by_plan(fetch())
    ours = json.load(open(REGISTRY, encoding="utf-8"))

    ppd_state: dict[str, float] = {}
    ppd_plans: dict[str, list] = {}
    for r in ranked:
        st = STATES.get(r["StateName"], "??")
        ppd_state[st] = ppd_state.get(st, 0.0) + r["aum_b"]
        ppd_plans.setdefault(st, []).append((r["aum_b"], r["PlanName"]))

    our_state: dict[str, float] = {}
    our_plans: dict[str, list] = {}
    for p in ours:
        st = p["state"]
        our_state[st] = our_state.get(st, 0.0) + float(p.get("aum_billions") or 0)
        our_plans.setdefault(st, []).append(
            (float(p.get("aum_billions") or 0), p["name"]))

    if args.states:
        for st in sorted(set(ppd_state) | set(our_state)):
            print(f"--- {st}   PPD ${ppd_state.get(st, 0):,.1f}B   "
                  f"ours ${our_state.get(st, 0):,.1f}B")
            P = sorted(ppd_plans.get(st, []), reverse=True)
            O = sorted(our_plans.get(st, []), reverse=True)
            for i in range(max(len(P), len(O))):
                left = f"${P[i][0]:7.1f}B {P[i][1][:34]}" if i < len(P) else ""
                right = f"${O[i][0]:7.1f}B {O[i][1][:38]}" if i < len(O) else ""
                print(f"  {left:46s}| {right}")
        return 0

    if args.aum:
        print("Our aum_billions is a rounded, undated figure. PPD's is the "
              "reported market value, FY2025 for 186 of 248 plans.\n"
              "Matching is by hand; see the review note for the pair list.")
        return 0

    print(f"PPD plans with assets : {len(ranked)}")
    print(f"PPD universe          : ${sum(ppd_state.values()):,.0f}B")
    print(f"our registry          : {len(ours)} plans, "
          f"${sum(our_state.values()):,.0f}B")
    missing_states = sorted(set(ppd_state) - set(our_state))
    print(f"states with nothing tracked: "
          f"{', '.join(missing_states) if missing_states else 'none'}")
    print()
    print(f"{'ST':3s} {'PPD $B':>9s} {'ours $B':>9s} {'ours/PPD':>9s}  note")
    print("-" * 64)
    for st in sorted(ppd_state, key=lambda s: -(ppd_state[s] - our_state.get(s, 0))):
        p, o = ppd_state[st], our_state.get(st, 0.0)
        note = BOARD_WIDER.get(st, "")
        if not note and p and o / p < 0.80:
            note = "<-- thin"
        print(f"{st:3s} {p:9,.1f} {o:9,.1f} {o / p * 100:8.0f}%  {note[:34]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
