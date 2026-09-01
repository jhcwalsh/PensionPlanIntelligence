"""The WAF-blocked plan IDs, in one place, for the Mac Mini job.

`pipeline.py` and `refresh_cafrs.py` each load their own block list and
subtract it from the registry on every run. The Mini needs the same two lists
in the opposite direction -- to name those plans explicitly on the CLI, which
is the documented bypass:

    "Explicit CLI args win and bypass the block list -- that is how you run a
     blocked plan by hand from a residential IP."
        -- pipeline.py::_resolve_plan_ids

Not every blocked plan is blocked for a reason the Mini fixes. The 2026-09-01
probe (Task 1 of the Stage 1 plan) split the lists three ways, recorded as
``blocked_by`` on each entry:

- ``datacentre_ip`` -- a residential host gets through. **These are the Mini's.**
- ``other`` -- 403 from a residential IP too, so no host we have reaches it.
- ``scraper`` -- not blocked at all; the discovery selector is stale.

The default here is the *residential* subset, because naming an ``other`` or
``scraper`` plan on the Mini's CLI produces a job that fails those plans every
night for a reason the Mini can never fix. ``--all-blocked`` gives the raw
list for anything that needs to know what the cloud skips.

Usage:
    python -m scripts.waf_blocked_ids               # residential union
    python -m scripts.waf_blocked_ids --materials   # for pipeline.py
    python -m scripts.waf_blocked_ids --cafr        # for refresh_cafrs.py
    python -m scripts.waf_blocked_ids --all-blocked # everything the cloud skips

Shell consumers word-split the single output line, so the format is one line
of space-separated ids and nothing else -- no header, no trailing commentary.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
MATERIALS_FILE = REPO_ROOT / "data" / "waf_blocked_plans.json"
CAFR_FILE = REPO_ROOT / "data" / "waf_blocked_cafr_plans.json"


# The only value that means "a residential host can fetch this". Anything
# else -- including a missing field on a hand-edited entry -- is excluded, so
# the failure mode of forgetting to classify a new plan is that the Mini
# doesn't try it, rather than that it fails nightly.
RESIDENTIAL = "datacentre_ip"


def _load(path: Path, residential_only: bool = False) -> list[str]:
    with open(path, encoding="utf-8") as f:
        plans = json.load(f)["plans"]
    if residential_only:
        plans = [p for p in plans if p.get("blocked_by") == RESIDENTIAL]
    return sorted(p["id"] for p in plans)


def materials_ids() -> list[str]:
    """Board-materials plans the Mini should fetch (fed to pipeline.py).

    The residential subset, not the whole block list -- see the module
    docstring. Use ``all_blocked_materials_ids()`` for what the cloud skips.
    """
    return _load(MATERIALS_FILE, residential_only=True)


def cafr_ids() -> list[str]:
    """CAFR plans the Mini should fetch (fed to refresh_cafrs.py)."""
    return _load(CAFR_FILE, residential_only=True)


def all_ids() -> list[str]:
    """The deduplicated union. asrs and strs_ohio appear on both lists."""
    return sorted(set(materials_ids()) | set(cafr_ids()))


def all_blocked_materials_ids() -> list[str]:
    """Every id pipeline.py skips, whatever the reason."""
    return _load(MATERIALS_FILE)


def all_blocked_cafr_ids() -> list[str]:
    """Every id refresh_cafrs.py skips, whatever the reason."""
    return _load(CAFR_FILE)


def unreachable_ids() -> list[str]:
    """Blocked plans no host we have can fetch -- the residue for A4.

    Kept as a named function rather than left implicit: these are the only
    plans a proxy would still buy, and that is the whole remaining case for
    paying for one.
    """
    return sorted(set(all_blocked_materials_ids() + all_blocked_cafr_ids())
                  - set(all_ids()))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="scripts.waf_blocked_ids")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--materials", action="store_true",
                       help="Only the board-materials block list")
    group.add_argument("--cafr", action="store_true",
                       help="Only the CAFR block list")
    group.add_argument("--all-blocked", action="store_true",
                       help="Every id the cloud skips, including the ones no "
                            "residential host can fetch either")
    args = parser.parse_args(argv)

    if args.materials:
        ids = materials_ids()
    elif args.cafr:
        ids = cafr_ids()
    elif args.all_blocked:
        ids = sorted(set(all_blocked_materials_ids())
                     | set(all_blocked_cafr_ids()))
    else:
        ids = all_ids()

    print(" ".join(ids))
    return 0


if __name__ == "__main__":
    sys.exit(main())
