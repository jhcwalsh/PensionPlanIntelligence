"""One-off publication-notice email send.

Normally the notice fires automatically inside ``app.py``'s
``page_approval_action`` after a magic-link approval transitions a
Publication to ``status='published'``. This script lets you re-send
the notice for any existing Publication — useful for:

  - Live-testing the new template without re-running a full compose cycle.
  - Re-sending after a transient Resend failure (the auto-send swallows
    exceptions to avoid blocking the publish itself).
  - Sending the notice for a publication that was approved before this
    feature shipped.

Honors ``INSIGHTS_MODE=mock`` (via ``send_email`` in insights.approval),
so this can run in CI / offline tests too.

Usage:
    python -m scripts.send_publication_notice 2                        # default recipient
    python -m scripts.send_publication_notice 2 jhcwalsh@me.com        # one-off override
    python -m scripts.send_publication_notice 2 --force                # bypass status gate
    INSIGHTS_MODE=mock python -m scripts.send_publication_notice 2     # dry-run to disk
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime

from database import Publication, get_session
from insights import config, notice


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="scripts.send_publication_notice")
    parser.add_argument(
        "publication_id", type=int,
        help="Publication.id to send the notice for.",
    )
    parser.add_argument(
        "recipient", nargs="?",
        help="Override APPROVAL_EMAIL_RECIPIENT for this send only.",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Send even if Publication.status is not 'published'. The "
             "status gate is skipped by promoting the row in-memory only "
             "(no DB write).",
    )
    args = parser.parse_args(argv)

    session = get_session()
    try:
        pub = session.query(Publication).filter_by(id=args.publication_id).one_or_none()
        if pub is None:
            print(f"ERROR: Publication #{args.publication_id} not found.", file=sys.stderr)
            return 2

        original_status = pub.status
        if pub.status != "published":
            if not args.force:
                print(
                    f"ERROR: Publication #{pub.id} has status='{pub.status}'; "
                    "send_publication_notice() requires 'published'. "
                    "Pass --force to bypass for testing.",
                    file=sys.stderr,
                )
                return 3
            print(f"NOTE: --force in effect — promoting status '{pub.status}' → 'published' (in-memory only).")
            pub.status = "published"
            if pub.published_at is None:
                pub.published_at = pub.approved_at or datetime.utcnow()

        recipient = args.recipient or config.APPROVAL_EMAIL_RECIPIENT
        print("Publication notice send")
        print(f"  mode:      {'mock' if config.is_mock() else 'live'}")
        print(f"  pub:       #{pub.id} ({pub.cadence}, {pub.period_start}..{pub.period_end})")
        print(f"  status:    {original_status}{' (forced→published)' if args.force else ''}")
        print(f"  from:      {config.APPROVAL_EMAIL_FROM}")
        print(f"  to:        {recipient}")

        delivery_id = notice.send_publication_notice(pub, to=recipient)
        print(f"  delivery:  {delivery_id}")
        if config.is_mock():
            print(f"  (mock mode — see {config.SENT_EMAILS_DIR})")
        return 0
    finally:
        # Don't persist any in-memory status flip from --force
        session.rollback()
        session.close()


if __name__ == "__main__":
    sys.exit(main())
