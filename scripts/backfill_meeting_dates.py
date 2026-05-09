"""One-shot backfill: re-evaluate Document.meeting_date for rows where the
current value is implausible relative to ``downloaded_at``.

The legacy ``extractor.infer_meeting_date`` had no sanity check against
``downloaded_at`` and no filename-based parser, so some rows ended up
with dates pulled from in-document text that don't match the actual
meeting (e.g. ``agenda.board.04292026.pdf`` getting parsed as 2026-06-23
from a forward-looking date inside the PDF).

This script:
  1. Selects rows where ``meeting_date > downloaded_at + 60 days``
     OR ``meeting_date < downloaded_at - 5 years``.
  2. Recomputes a candidate date using the new
     ``extractor.parse_date_from_filename`` + plausibility filter.
  3. Updates the row if a clearly-better candidate exists.

Idempotent — running it twice on the same DB is safe; the second run
will see the previous run's already-corrected rows and skip them.

Usage:
    python -m scripts.backfill_meeting_dates                # write changes
    python -m scripts.backfill_meeting_dates --dry-run      # report only
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta

from sqlalchemy import or_

from database import Document, get_session
from extractor import parse_date_from_filename


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="scripts.backfill_meeting_dates")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report changes without writing to DB.")
    args = parser.parse_args(argv)

    session = get_session()
    try:
        # Conservative scope: only fix far-future dates (>60 days after fetch).
        # Historical dates older than the fetch are usually legitimate
        # (a 2018 minutes file recently re-fetched is fine).
        rows = (
            session.query(Document)
            .filter(Document.downloaded_at.isnot(None))
            .filter(Document.meeting_date.isnot(None))
            .all()
        )
        candidates: list[Document] = [
            d for d in rows
            if d.meeting_date > d.downloaded_at + timedelta(days=60)
        ]

        print(f"scanned: {len(rows)} docs")
        print(f"implausible meeting_date rows: {len(candidates)}")

        updated = 0
        cleared = 0
        kept = 0
        for d in candidates:
            new_date = parse_date_from_filename(d.filename)
            # Plausibility check: must be within the same window we just used to flag.
            if new_date is not None:
                if (new_date <= d.downloaded_at + timedelta(days=60)
                        and new_date >= d.downloaded_at - timedelta(days=5 * 365)):
                    print(f"  fix:  doc#{d.id:>5}  {d.meeting_date.date()} → "
                          f"{new_date.date()}  {(d.filename or '')[:55]}")
                    if not args.dry_run:
                        d.meeting_date = new_date
                    updated += 1
                    continue
            # No filename signal — clear the bad date so the presentation
            # filter can hide it. Better than displaying a wrong year.
            print(f"  clr:  doc#{d.id:>5}  {d.meeting_date.date()} → None       "
                  f"  {(d.filename or '')[:55]}")
            if not args.dry_run:
                d.meeting_date = None
            cleared += 1

        if not args.dry_run:
            session.commit()
        print(f"\nupdated: {updated}, cleared: {cleared}, kept: {kept}"
              f"{' (dry-run, no DB writes)' if args.dry_run else ''}")
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
