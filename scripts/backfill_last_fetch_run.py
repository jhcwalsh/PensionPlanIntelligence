"""Seed one FetchRun row representing the most recent local pipeline run.

The FetchRun table starts empty when this code lands. Without a backfill,
the Admin tab would be blank until the next GHA cron fires (roughly 24
hours). This script identifies the most recent cluster of
documents.downloaded_at values (treating any gap > 1 hour as a different
run) and creates a single row tagged source='local' to represent it.

Idempotent: bails out if any FetchRun rows already exist, so re-running
the script is safe.

Run once after the FetchRun model lands:
    python -m scripts.backfill_last_fetch_run
"""

from __future__ import annotations

import json
import sys
from datetime import timedelta

from sqlalchemy import desc

import database
from database import Document, FetchRun, get_session

# Anything older than this gap from the latest doc is treated as a
# different (older) run.
RUN_GAP = timedelta(hours=1)


def main() -> int:
    database.init_db()
    session = get_session()
    try:
        if session.query(FetchRun).count() > 0:
            print("FetchRun table already populated; nothing to backfill.")
            return 0

        latest = (
            session.query(Document)
            .filter(Document.downloaded_at.isnot(None))
            .order_by(desc(Document.downloaded_at))
            .first()
        )
        if latest is None:
            print("No documents with downloaded_at; nothing to backfill.")
            return 0

        cutoff = latest.downloaded_at - RUN_GAP
        batch = (
            session.query(Document.id, Document.downloaded_at)
            .filter(Document.downloaded_at >= cutoff)
            .order_by(Document.downloaded_at)
            .all()
        )
        started_at = batch[0].downloaded_at
        completed_at = batch[-1].downloaded_at
        new_doc_ids = [row.id for row in batch]

        run = FetchRun(
            source="local",
            started_at=started_at,
            completed_at=completed_at,
            status="success",
            new_document_ids=json.dumps(new_doc_ids),
        )
        session.add(run)
        session.commit()

        print(f"Backfilled FetchRun id={run.id}: source=local, "
              f"{len(new_doc_ids)} documents, "
              f"window {started_at} → {completed_at}")
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
