"""One-shot Resend plumbing test.

Sends a minimal "infra check" email through the same code path the weekly
/ monthly insights cycles use, but doesn't touch the publications table,
the DB, or the notes/ directory. Useful right after Resend domain
verification + GitHub secret setup, and any time the email path needs
re-validating.

Honors INSIGHTS_MODE: in mock mode it writes to tmp/sent_emails/ instead
of calling Resend, so this can run safely in tests or local dev too.

Usage:
    python -m scripts.send_test_email                       # to APPROVAL_EMAIL_RECIPIENT
    python -m scripts.send_test_email user@example.com      # one-off override
    INSIGHTS_MODE=mock python -m scripts.send_test_email    # dry-run to disk
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime

from insights import config
from insights.approval import ApprovalEmail, send_email


SUBJECT = "[PensionGraph] Email plumbing test"

HTML_TEMPLATE = """\
<!doctype html>
<html><body style="font-family: -apple-system, sans-serif; max-width: 600px; margin: 2em auto; line-height: 1.5;">
  <h2 style="color: #2c5282;">Email plumbing test</h2>
  <p>This is an end-to-end check of the Resend → inbox path used by the
     weekly / monthly / quarterly Insights approval emails.</p>
  <p>If you can read this, the following are working:</p>
  <ul>
    <li><code>RESEND_API_KEY</code> secret is valid</li>
    <li><code>APPROVAL_EMAIL_FROM</code> domain is verified in Resend</li>
    <li>DNS (SPF + DKIM) is propagated</li>
    <li><code>APPROVAL_EMAIL_RECIPIENT</code> reaches your inbox</li>
  </ul>
  <p style="color: #666; font-size: 0.9em; margin-top: 2em;">
    Sent {sent_at} from <code>{from_addr}</code>.<br>
    Source: <code>scripts/send_test_email.py</code> — does not affect any
    Publication rows or send any approval token.
  </p>
</body></html>
"""

TEXT_TEMPLATE = """\
Email plumbing test
===================

This is an end-to-end check of the Resend -> inbox path used by the
weekly / monthly / quarterly Insights approval emails.

If you can read this, the following are working:
  - RESEND_API_KEY secret is valid
  - APPROVAL_EMAIL_FROM domain is verified in Resend
  - DNS (SPF + DKIM) is propagated
  - APPROVAL_EMAIL_RECIPIENT reaches your inbox

Sent {sent_at} from {from_addr}.
Source: scripts/send_test_email.py — does not affect any Publication
rows or send any approval token.
"""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="scripts.send_test_email")
    parser.add_argument(
        "recipient", nargs="?",
        help="Override APPROVAL_EMAIL_RECIPIENT for this send only.",
    )
    args = parser.parse_args(argv)

    sent_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    email = ApprovalEmail(
        subject=SUBJECT,
        html=HTML_TEMPLATE.format(
            sent_at=sent_at, from_addr=config.APPROVAL_EMAIL_FROM
        ),
        text=TEXT_TEMPLATE.format(
            sent_at=sent_at, from_addr=config.APPROVAL_EMAIL_FROM
        ),
        pdf_attachment=None,
        pdf_filename=None,
    )

    recipient = args.recipient or config.APPROVAL_EMAIL_RECIPIENT
    print(f"Sending plumbing test email")
    print(f"  mode:      {'mock' if config.is_mock() else 'live'}")
    print(f"  from:      {config.APPROVAL_EMAIL_FROM}")
    print(f"  to:        {recipient}")
    print(f"  subject:   {SUBJECT}")

    delivery_id = send_email(email, to=recipient)
    print(f"\n  delivery id / path: {delivery_id}")
    if config.is_mock():
        print(f"  (mock mode — see {config.SENT_EMAILS_DIR})")
    else:
        print("  Live send via Resend — check the recipient inbox.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
