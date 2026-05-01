"""
Production entry point for the RFP extraction pipeline.

Invoked by the Render cron job. Runs against the live database (DB_PATH)
and the local PDFs already on disk (Document.local_path). Honors LLM_MODE
to support mock-mode dry runs.

Usage:
    python -m scripts.run_rfp_extraction               # all plans
    python -m scripts.run_rfp_extraction calpers       # one plan
"""

from __future__ import annotations

import argparse
import sys

from rfp.logging_setup import configure_logging
from rfp.orchestrator import run_rfp_extraction


def main() -> int:
    parser = argparse.ArgumentParser(description="Run RFP extraction pipeline")
    parser.add_argument("plan_ids", nargs="*", help="Optional list of plan ids")
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Cap on docs processed in this run (useful for bounded "
        "daily backfill; idempotency makes subsequent runs pick up "
        "where this one left off).",
    )
    args = parser.parse_args()

    configure_logging()
    plan_ids = args.plan_ids or None
    run_id = run_rfp_extraction(plan_ids=plan_ids, limit=args.limit)
    print(f"run_id={run_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
