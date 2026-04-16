"""
Update a plan's materials_url (and optional settings) in known_plans.json + DB.

Workflow: after running test_plan_urls.py, manually find the correct URL for
each BAD/FAIL/EMPTY plan, then use this tool to update both the JSON seed file
and the live DB in one step. Keeps the two sources in sync.

Single-plan mode:
    python update_plan_url.py ucrp --url "https://..."
    python update_plan_url.py lafpp --url "https://..." --type playwright
    python update_plan_url.py ohsers --url "https://..." --dry-run

Batch mode — reads a CSV with columns: plan_id, materials_url, materials_type (optional)
    python update_plan_url.py --batch fixes.csv
    python update_plan_url.py --batch fixes.csv --dry-run

Show current config for a plan (no changes):
    python update_plan_url.py ucrp --show
"""

import argparse
import csv
import json
import sys
from pathlib import Path

from database import Plan, get_session, init_db

PLANS_FILE = Path(__file__).parent / "data" / "known_plans.json"
VALID_MATERIALS_TYPES = {"html_links", "playwright"}


def load_plans() -> list[dict]:
    with open(PLANS_FILE, encoding="utf-8") as f:
        return json.load(f)


def save_plans(plans: list[dict]) -> None:
    """Write known_plans.json with the same pretty format as the original."""
    with open(PLANS_FILE, "w", encoding="utf-8") as f:
        json.dump(plans, f, indent=2, ensure_ascii=False)
        f.write("\n")


def apply_update(plans: list[dict], plan_id: str, new_url: str | None,
                  new_type: str | None) -> tuple[dict | None, dict]:
    """
    Mutates `plans` in place. Returns (updated_entry, diff_dict). If plan_id
    is not found, returns (None, {}).
    """
    for entry in plans:
        if entry["id"] == plan_id:
            diff = {}
            if new_url is not None and entry.get("materials_url") != new_url:
                diff["materials_url"] = (entry.get("materials_url"), new_url)
                entry["materials_url"] = new_url
            if new_type is not None and entry.get("materials_type") != new_type:
                diff["materials_type"] = (entry.get("materials_type"), new_type)
                entry["materials_type"] = new_type
            return entry, diff
    return None, {}


def sync_db(session, entry: dict) -> None:
    """Mirror the JSON entry into the DB Plan row."""
    plan = session.get(Plan, entry["id"])
    if plan is None:
        # Create if missing (shouldn't normally happen, but be defensive)
        plan = Plan(
            id=entry["id"], name=entry["name"],
            abbreviation=entry.get("abbreviation"),
            state=entry.get("state"),
            aum_billions=entry.get("aum_billions"),
            website=entry.get("website"),
            materials_url=entry.get("materials_url"),
        )
        session.add(plan)
    else:
        plan.materials_url = entry.get("materials_url")
    session.commit()


def print_diff(plan_id: str, diff: dict) -> None:
    if not diff:
        print(f"  {plan_id}: no changes")
        return
    for key, (old, new) in diff.items():
        print(f"  {plan_id}.{key}:")
        print(f"    - {old}")
        print(f"    + {new}")


def show_plan(plan_id: str) -> None:
    for entry in load_plans():
        if entry["id"] == plan_id:
            print(json.dumps(entry, indent=2, ensure_ascii=False))
            return
    print(f"No plan with id={plan_id!r}", file=sys.stderr)
    sys.exit(1)


def run_single(plan_id: str, url: str | None, type_: str | None, dry_run: bool) -> None:
    if type_ is not None and type_ not in VALID_MATERIALS_TYPES:
        print(f"Invalid --type {type_!r}. Must be one of: {sorted(VALID_MATERIALS_TYPES)}",
              file=sys.stderr)
        sys.exit(2)
    if url is None and type_ is None:
        print("Nothing to update. Pass --url and/or --type.", file=sys.stderr)
        sys.exit(2)

    plans = load_plans()
    entry, diff = apply_update(plans, plan_id, url, type_)
    if entry is None:
        print(f"No plan with id={plan_id!r}", file=sys.stderr)
        sys.exit(1)

    print_diff(plan_id, diff)
    if not diff:
        return
    if dry_run:
        print("\n(dry run — no files or DB changed)")
        return

    save_plans(plans)
    init_db()
    session = get_session()
    try:
        sync_db(session, entry)
    finally:
        session.close()
    print("\nUpdated known_plans.json and DB.")


def run_batch(csv_path: str, dry_run: bool) -> None:
    rows = list(csv.DictReader(open(csv_path, encoding="utf-8")))
    if not rows:
        print(f"No rows in {csv_path}", file=sys.stderr)
        sys.exit(1)

    required = {"plan_id", "materials_url"}
    missing = required - set(rows[0].keys())
    if missing:
        print(f"CSV missing required columns: {sorted(missing)}", file=sys.stderr)
        print(f"Found columns: {sorted(rows[0].keys())}", file=sys.stderr)
        sys.exit(2)

    plans = load_plans()
    updated_entries: list[dict] = []
    skipped: list[str] = []
    unchanged: list[str] = []

    for row in rows:
        pid = (row.get("plan_id") or "").strip()
        new_url = (row.get("materials_url") or "").strip() or None
        new_type = (row.get("materials_type") or "").strip() or None
        if new_type and new_type not in VALID_MATERIALS_TYPES:
            print(f"{pid}: invalid materials_type={new_type!r}, skipping")
            skipped.append(pid)
            continue
        if not pid or not new_url:
            skipped.append(pid or "<blank>")
            continue

        entry, diff = apply_update(plans, pid, new_url, new_type)
        if entry is None:
            print(f"{pid}: plan not found, skipping")
            skipped.append(pid)
            continue
        if not diff:
            unchanged.append(pid)
            continue
        print_diff(pid, diff)
        updated_entries.append(entry)

    print()
    print(f"To update: {len(updated_entries)}")
    print(f"Unchanged: {len(unchanged)}")
    print(f"Skipped:   {len(skipped)}")

    if not updated_entries:
        print("\nNothing to do.")
        return
    if dry_run:
        print("\n(dry run — no files or DB changed)")
        return

    save_plans(plans)
    init_db()
    session = get_session()
    try:
        for entry in updated_entries:
            sync_db(session, entry)
    finally:
        session.close()
    print(f"\nUpdated known_plans.json and DB ({len(updated_entries)} plans).")


def main():
    parser = argparse.ArgumentParser(
        description="Update a plan's materials_url in known_plans.json + DB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("plan_id", nargs="?", help="Plan id (e.g. ucrp)")
    parser.add_argument("--url", help="New materials_url")
    parser.add_argument("--type", dest="type_", choices=sorted(VALID_MATERIALS_TYPES),
                        help="New materials_type (html_links or playwright)")
    parser.add_argument("--batch", metavar="CSV",
                        help="Apply batch updates from CSV "
                             "(columns: plan_id, materials_url, materials_type)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would change without writing")
    parser.add_argument("--show", action="store_true",
                        help="Print the current config for the given plan_id")
    args = parser.parse_args()

    if args.batch:
        if args.plan_id or args.url or args.type_:
            parser.error("--batch is exclusive with positional plan_id / --url / --type")
        run_batch(args.batch, args.dry_run)
        return

    if not args.plan_id:
        parser.error("plan_id is required (or use --batch)")

    if args.show:
        show_plan(args.plan_id)
        return

    run_single(args.plan_id, args.url, args.type_, args.dry_run)


if __name__ == "__main__":
    main()
