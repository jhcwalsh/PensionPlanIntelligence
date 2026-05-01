"""One-shot backfill: produce an April 2026 monthly insights publication.

Context (May 1, 2026): the scheduled tasks were registered April 30, so
no April weeklies were ever produced. The May 1 monthly cycle therefore
aborted with "No approved weeklies found for 2026-04". Approval emails
aren't wired up yet either, so the normal magic-link flow can't be used
to approve anything.

This script:
  1. Stubs insights.approval.send_email so the cycle's email step is a
     no-op (publication still ends in awaiting_approval, with tokens
     issued and PDF rendered).
  2. Composes ONE weekly publication for period_start=2026-04-19.
     April 19 is the latest Sunday whose Sun→Sat week fits fully within
     April (Apr 19→25); April 26's week spills into May 2 and the
     monthly's period_end<=2026-04-30 filter would reject it.
     compose_weekly's data gather uses now()-7d as its window, so the
     content reflects the last 7 days of corpus activity rather than the
     historic April 19 week — accepted limitation, flagged in the draft.
  3. Auto-approves that weekly (status="approved" written directly in DB).
  4. Runs the monthly cycle for 2026-04, which synthesizes the approved
     weekly into a monthly publication.
  5. Auto-approves the monthly.

Idempotent: the cycles' find_or_create_publication + force=True path
re-uses or re-composes existing rows for the same period. Safe to re-run.
"""

from __future__ import annotations

import logging
import sys
from datetime import date, datetime

from database import Publication, get_session
from insights import approval, weekly, monthly


def _stub_send_email(email_message) -> str:
    logging.getLogger(__name__).info(
        "send_email stubbed for backfill (to=%s subject=%s)",
        getattr(email_message, "to", "?"),
        getattr(email_message, "subject", "?"),
    )
    return "stubbed-no-email"


def _auto_approve(publication_id: int, label: str) -> None:
    session = get_session()
    try:
        pub = session.get(Publication, publication_id)
        if pub is None:
            raise RuntimeError(f"{label}: publication {publication_id} missing")
        if pub.status == "approved":
            print(f"  {label} pub {publication_id} already approved")
            return
        if pub.status != "awaiting_approval":
            raise RuntimeError(
                f"{label}: pub {publication_id} in unexpected status "
                f"'{pub.status}' — refusing to auto-approve"
            )
        pub.status = "approved"
        pub.approved_at = datetime.utcnow()
        session.commit()
        print(f"  {label} pub {publication_id}: approved")
    finally:
        session.close()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Stub the email send so cycles can finalize without Resend.
    approval.send_email = _stub_send_email  # type: ignore[assignment]

    # 1. One representative weekly for April (period_start=2026-04-19).
    # Latest April Sunday whose Sun→Sat period fits within April.
    week_start = date(2026, 4, 19)
    print(f"composing weekly for {week_start}...")
    week_pub = weekly.run_weekly_cycle(
        period_start=week_start, force=True, skip_scrape=True,
    )
    print(f"  weekly pub {week_pub.id}: status={week_pub.status}")
    if week_pub.status != "awaiting_approval":
        print(f"  unexpected weekly status '{week_pub.status}', aborting")
        return 1

    # 2. Auto-approve it.
    _auto_approve(week_pub.id, "weekly")

    # 3. Compose monthly synthesizing the approved weekly.
    month_start = date(2026, 4, 1)
    print(f"composing monthly for {month_start}...")
    month_pub = monthly.run_monthly_cycle(period_start=month_start, force=True)
    print(f"  monthly pub {month_pub.id}: status={month_pub.status}")
    if month_pub.status != "awaiting_approval":
        print(f"  unexpected monthly status '{month_pub.status}', aborting")
        return 1

    # 4. Auto-approve monthly.
    _auto_approve(month_pub.id, "monthly")

    print("backfill complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
