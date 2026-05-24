"""Publish a Publication that's been approved (or is awaiting approval).

Designed to run from GitHub Actions, where ``git push`` works correctly
— unlike Render's Streamlit container, which has no ``origin`` remote
configured for pushing back.

Picks up a single publication by id, walks it through the lifecycle to
``published`` (acquiring 'approved' first if needed), writes the
canonical notes file via the existing ``insights.publish`` helper, and
returns 0 on success.

Usage:
    python -m scripts.publish_pending --id 5
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime

from database import Publication, get_session
from insights import cycle_common, publish

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="scripts.publish_pending")
    parser.add_argument(
        "--id", type=int, required=True,
        help="Publication id to publish",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    session = get_session()
    try:
        pub = session.get(Publication, args.id)
        if pub is None:
            logger.error("Publication id=%s not found", args.id)
            return 1

        logger.info(
            "Publication %s (cadence=%s, period=%s) status=%s",
            pub.id, pub.cadence, pub.period_start, pub.status,
        )

        if pub.status == "published":
            logger.info("Already published; nothing to do.")
            return 0

        if pub.status not in ("awaiting_approval", "approved"):
            logger.error(
                "Cannot publish from status=%r; expected awaiting_approval or approved.",
                pub.status,
            )
            return 1

        now = datetime.utcnow()

        if pub.status == "awaiting_approval":
            cycle_common.transition_status(pub, "approved")
            pub.approved_at = now
            session.flush()
            logger.info("Transitioned to approved.")

        path = publish.publish(pub)
        logger.info("Wrote notes file: %s", path)

        cycle_common.transition_status(pub, "published")
        pub.published_at = now
        session.commit()
        logger.info("Transitioned to published.")

        if pub.cadence == "weekly":
            try:
                from insights import notice
                session.refresh(pub)
                notice.send_publication_notice(pub)
                logger.info("Publication notice email sent.")
            except Exception as exc:
                logger.warning("Notice email failed (non-fatal): %s", exc)

        return 0
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
