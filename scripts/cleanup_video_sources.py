"""One-shot cleanup of auto-discovered plan_video_sources.

Phase-1 noise filter for the meeting-video subsystem. The site-crawl
discovery layer cannot tell a plan's social-media footer link
(facebook.com/MyPlan, x.com/MyPlan) apart from a real video archive,
and in practice ~95% of Facebook hits are footer links rather than
FB-Live archives. We deactivate them by default — a human can flip
status back to 'active' if a plan does archive video to FB.

Idempotent: re-running only changes rows that haven't already been
deactivated. Manual edits (discovery_method='manual') are never
touched.

Usage:
  python -m scripts.cleanup_video_sources           # apply
  python -m scripts.cleanup_video_sources --dry-run # report only
"""
from __future__ import annotations

import argparse
from datetime import datetime

from database import PlanVideoSource, SessionLocal, init_db


# Platforms whose auto-discovered hits are predominantly social-media
# footer links rather than meeting-video archives. Adding "facebook" only
# for now; revisit if Twitter/X or LinkedIn start showing up.
NOISY_PLATFORMS = ("facebook",)


def run(dry_run: bool) -> None:
    init_db()
    session = SessionLocal()
    try:
        rows = (
            session.query(PlanVideoSource)
            .filter(PlanVideoSource.platform.in_(NOISY_PLATFORMS))
            .filter(PlanVideoSource.discovery_method.in_(("mined", "site_crawl", "deep_crawl")))
            .filter(PlanVideoSource.status == "active")
            .all()
        )
        print(f"Found {len(rows)} auto-discovered active rows on noisy platforms "
              f"({', '.join(NOISY_PLATFORMS)}).")
        if not rows:
            return

        for row in rows:
            print(f"  [{row.plan_id}] {row.platform:10} {row.source_url}")
            if dry_run:
                continue
            row.status = "inactive"
            existing_note = (row.notes or "").strip()
            cleanup_note = ("auto-deactivated by cleanup_video_sources: "
                            "social-media footer links are noise. "
                            "Flip status='active' manually if this plan archives meeting video here.")
            row.notes = (cleanup_note + ("\n\n" + existing_note if existing_note else ""))
            row.updated_at = datetime.utcnow()

        if dry_run:
            print("\n(dry run — no changes committed)")
        else:
            session.commit()
            print(f"\nDeactivated {len(rows)} row(s).")
    finally:
        session.close()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    run(args.dry_run)


if __name__ == "__main__":
    main()
