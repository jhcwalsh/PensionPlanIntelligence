"""Daily reminder + expiry sweep.

Runs daily at 09:00 ET via cron:
* For each ``awaiting_approval`` publication composed >= 72h ago without
  ``reminder_sent_at`` set, re-send the approval email with a more
  urgent subject and stamp ``reminder_sent_at``.
* For each ``awaiting_approval`` publication older than the TTL, transition
  to ``expired`` and send a one-shot expiry notice.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from database import ApprovalToken, Publication, get_session
from insights import approval, config, cycle_common, notify

logger = logging.getLogger(__name__)


def _active_tokens_for(session, publication_id: int) -> tuple[Optional[ApprovalToken],
                                                              Optional[ApprovalToken]]:
    """Return (approve_token_row, reject_token_row), each unconsumed if available."""
    rows = (
        session.query(ApprovalToken)
        .filter(ApprovalToken.publication_id == publication_id)
        .filter(ApprovalToken.consumed_at.is_(None))
        .filter(ApprovalToken.expires_at > datetime.utcnow())
        .all()
    )
    by_action = {r.action: r for r in rows}
    return by_action.get("approve"), by_action.get("reject")


def run_reminders(now: Optional[datetime] = None) -> dict:
    """Run the daily nudge + expiry sweep. Returns counts for logging."""
    now = now or datetime.utcnow()
    session = get_session()

    sent_reminders = 0
    expired_count = 0

    try:
        # 1. Reminders for stale-but-not-expired drafts.
        reminder_cutoff = config.reminder_threshold(now)
        candidates = (
            session.query(Publication)
            .filter(Publication.status == "awaiting_approval")
            .filter(Publication.composed_at <= reminder_cutoff)
            .filter(Publication.reminder_sent_at.is_(None))
            .all()
        )
        for pub in candidates:
            # We can't recover the raw token from the hash — but the
            # original tokens are still active in the email the founder
            # already received. The reminder re-sends the SAME content,
            # using the same plaintext tokens. To do that we have to
            # mint fresh tokens (since plaintext was discarded) and
            # invalidate the old ones — single-use semantics are
            # preserved either way.
            old_tokens = (
                session.query(ApprovalToken)
                .filter_by(publication_id=pub.id)
                .filter(ApprovalToken.consumed_at.is_(None))
                .all()
            )
            for t in old_tokens:
                t.consumed_at = now  # invalidate (so re-clicked old links fail cleanly)
            session.flush()

            approve_tok, reject_tok = approval.issue_tokens(session, pub)
            pdf_bytes = Path(pub.pdf_path).read_bytes() if pub.pdf_path else None
            email = approval.render_approval_email(
                pub, approve_tok, reject_tok, pdf_bytes, is_reminder=True
            )
            approval.send_email(email)
            pub.reminder_sent_at = now
            sent_reminders += 1
            session.commit()

        # 2. Expire drafts past TTL. Send a one-line expiry notice.
        expired_ids = cycle_common.expire_stale_drafts(session, now=now)
        for pub_id in expired_ids:
            pub = session.get(Publication, pub_id)
            if pub is None:
                continue
            email = approval.render_approval_email(
                pub,
                approve=approval.IssuedToken(raw="", action="approve"),
                reject=approval.IssuedToken(raw="", action="reject"),
                pdf_bytes=None,
                is_expiry=True,
            )
            approval.send_email(email)
        expired_count = len(expired_ids)

        return {"reminders_sent": sent_reminders, "expired": expired_count}

    except Exception as exc:
        session.rollback()
        notify.alert_failure("reminders", now.date().isoformat(), exc)
        raise
    finally:
        session.close()
