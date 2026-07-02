"""CLI entry point for the insights pipeline.

Cron-invoked:
    python -m insights.scheduler weekly
    python -m insights.scheduler rfp_weekly
    python -m insights.scheduler monthly
    python -m insights.scheduler annual
    python -m insights.scheduler reminders

Manual / backfill:
    python -m insights.scheduler weekly --period 2026-04-19
    python -m insights.scheduler rfp_weekly --period 2026-04-19
    python -m insights.scheduler monthly --period 2026-03
    python -m insights.scheduler annual --year 2025
    python -m insights.scheduler weekly --period 2026-04-19 --force

Exits 0 on success, 1 on any handled error (after Slack alert).
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime

from database import init_db


def _parse_period_date(s: str) -> date:
    return date.fromisoformat(s)


def _parse_month(s: str) -> date:
    """Accept ``YYYY-MM`` and return the 1st of that month."""
    return date.fromisoformat(s + "-01") if len(s) == 7 else date.fromisoformat(s)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="insights.scheduler")
    sub = parser.add_subparsers(dest="cycle", required=True)

    p_weekly = sub.add_parser("weekly", help="Run the weekly cycle")
    p_weekly.add_argument("--period", type=_parse_period_date,
                          help="Period start date (Sunday) — default = most recent completed week")
    p_weekly.add_argument("--force", action="store_true",
                          help="Expire any awaiting_approval row for this period and re-compose")
    p_weekly.add_argument("--skip-scrape", action="store_true",
                          help="Don't refresh documents — compose from current DB state only")

    p_rfp = sub.add_parser("rfp_weekly", help="Run the weekly consultant RFP brief")
    p_rfp.add_argument("--period", type=_parse_period_date,
                       help="Period start date (Sunday) — default = most recent completed week")
    p_rfp.add_argument("--force", action="store_true",
                       help="Expire any awaiting_approval row for this period and re-compose")

    p_monthly = sub.add_parser("monthly", help="Run the monthly cycle")
    p_monthly.add_argument("--period", type=_parse_month,
                           help="Target month as YYYY-MM — default = prior month")
    p_monthly.add_argument("--force", action="store_true")

    p_quarterly = sub.add_parser("quarterly", help="Run the quarterly cycle")
    p_quarterly.add_argument("--period", type=_parse_month,
                             help="Any month in the target quarter as YYYY-MM — "
                                  "default = quarter that just ended")
    p_quarterly.add_argument("--force", action="store_true")

    p_annual = sub.add_parser("annual", help="Run the annual cycle")
    p_annual.add_argument("--year", type=int, help="Target year — default = prior calendar year")
    p_annual.add_argument("--force", action="store_true")

    sub.add_parser("reminders", help="Send 72h reminders and expire stale drafts")

    p_daily = sub.add_parser("daily", help="Run the daily pension digest")
    p_daily.add_argument("--force", action="store_true",
                         help="Expire today's existing publication and re-send")

    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    init_db()

    try:
        if args.cycle == "weekly":
            from insights.weekly import run_weekly_cycle
            pub = run_weekly_cycle(
                period_start=args.period,
                force=args.force,
                skip_scrape=args.skip_scrape,
            )
            print(f"weekly cycle complete: publication_id={pub.id} status={pub.status}")
        elif args.cycle == "rfp_weekly":
            from insights.rfp_weekly import run_rfp_weekly_cycle
            pub = run_rfp_weekly_cycle(
                period_start=args.period,
                force=args.force,
            )
            print(f"rfp_weekly cycle complete: publication_id={pub.id} status={pub.status}")
        elif args.cycle == "monthly":
            from insights.monthly import run_monthly_cycle
            pub = run_monthly_cycle(period_start=args.period, force=args.force)
            print(f"monthly cycle complete: publication_id={pub.id} status={pub.status}")
        elif args.cycle == "quarterly":
            from insights.quarterly import run_quarterly_cycle
            pub = run_quarterly_cycle(period_start=args.period, force=args.force)
            print(f"quarterly cycle complete: publication_id={pub.id} status={pub.status}")
        elif args.cycle == "annual":
            from insights.annual import run_annual_cycle
            pub = run_annual_cycle(year=args.year, force=args.force)
            print(f"annual cycle complete: publication_id={pub.id} status={pub.status}")
        elif args.cycle == "reminders":
            from insights.reminders import run_reminders
            stats = run_reminders(datetime.utcnow())
            print(f"reminders complete: {stats}")
        elif args.cycle == "daily":
            from insights.daily import run_daily_cycle
            pub = run_daily_cycle(force=args.force)
            print(f"daily cycle complete: publication_id={pub.id} status={pub.status}")
        else:
            parser.error(f"unknown cycle: {args.cycle}")
        return 0
    except Exception as exc:  # noqa: BLE001
        logging.exception("scheduler failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
