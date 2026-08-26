"""Repoint already-published briefing links at the canonical domain.

Briefings bake absolute links in at compose time -- insights/daily.py builds
``{base_url}/?document={id}`` -- so every briefing composed before the domain
change carries the Render-assigned subdomain forever. Changing the code fixes
new briefings; this fixes the back-catalogue.

Two stores hold them:

* ``notes/*.md`` -- the canonical published files, committed to git and served
  by the Streamlit app.
* ``publications.draft_markdown`` -- the row the app renders from, and the
  source monthly/quarterly/annual compose from.

Deliberately NOT touched: the PDFs under ``notes/pdfs/``. They were rendered
at publish time and would need regenerating, which changes bytes a reader may
already have downloaded. Their links still resolve while the old subdomain
answers; if that stops, regenerate them separately.

This is a plain host-for-host substitution, so it reverses exactly by running
with --from and --to swapped.

    python scripts/rewrite_published_links.py            # dry run, default
    python scripts/rewrite_published_links.py --apply
"""

from __future__ import annotations

import argparse
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import database  # noqa: E402

OLD_DEFAULT = "https://pensionplanintelligence.onrender.com"
NEW_DEFAULT = "https://pensiongraph.com"


def rewrite_notes(old: str, new: str, apply: bool) -> tuple[int, int]:
    """Returns (files touched, occurrences replaced)."""
    files = occurrences = 0
    for path in sorted((ROOT / "notes").rglob("*.md")):
        text = path.read_text(encoding="utf-8")
        n = text.count(old)
        if not n:
            continue
        files += 1
        occurrences += n
        print("  %-58s %d" % (path.relative_to(ROOT).as_posix(), n))
        if apply:
            path.write_text(text.replace(old, new), encoding="utf-8")
    return files, occurrences


def rewrite_publications(old: str, new: str, apply: bool) -> tuple[int, int]:
    """Returns (rows touched, occurrences replaced)."""
    session = database.get_session()
    rows = occurrences = 0
    try:
        P = database.Publication
        for pub in (session.query(P)
                    .filter(P.draft_markdown.ilike("%" + old.split("//")[1] + "%"))
                    .order_by(P.id)):
            n = pub.draft_markdown.count(old)
            if not n:
                continue
            rows += 1
            occurrences += n
            print("  publication %-4d %-10s %s  %d"
                  % (pub.id, pub.cadence, pub.period_start, n))
            if apply:
                pub.draft_markdown = pub.draft_markdown.replace(old, new)
        if apply:
            session.commit()
    finally:
        session.close()
    return rows, occurrences


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--from", dest="old", default=OLD_DEFAULT)
    ap.add_argument("--to", dest="new", default=NEW_DEFAULT)
    ap.add_argument("--apply", action="store_true",
                    help="actually write; without it this is a dry run")
    args = ap.parse_args()

    mode = "APPLYING" if args.apply else "DRY RUN (pass --apply to write)"
    print("%s\n  %s\n  -> %s\n" % (mode, args.old, args.new))
    print("backend:", database.engine.dialect.name, "\n")

    print("notes/ files:")
    nf, no = rewrite_notes(args.old, args.new, args.apply)
    print("  %d files, %d occurrences\n" % (nf, no))

    print("publications.draft_markdown:")
    pr, po = rewrite_publications(args.old, args.new, args.apply)
    print("  %d rows, %d occurrences\n" % (pr, po))

    if args.apply:
        print("Done. To reverse: rerun with --from %s --to %s --apply"
              % (args.new, args.old))
    else:
        print("Nothing written. Rerun with --apply to make these changes.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
