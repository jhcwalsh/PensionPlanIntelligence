"""Shared helpers used by every cadence (weekly/monthly/annual).

Centralises Publication CRUD, status transitions, and the
"compose → render → email" tail that every cycle ends with.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Optional

from sqlalchemy.exc import IntegrityError

from database import Publication, get_session
from insights import approval, config, render

logger = logging.getLogger(__name__)


# Allowed status transitions. Anything else raises ValueError.
_ALLOWED_TRANSITIONS = {
    "generating": {"awaiting_approval", "failed", "expired"},
    "awaiting_approval": {"approved", "rejected", "expired", "failed"},
    "approved": {"published", "failed"},
    "rejected": set(),
    "published": set(),
    "expired": set(),
    "failed": {"generating"},  # explicit re-run via --force
}


def transition_status(publication: Publication, new_status: str) -> None:
    """Apply a status transition, raising on invalid moves.

    Status timestamps (``approved_at``, ``rejected_at``, etc.) must be
    set by the caller — this only validates and assigns ``status``.
    """
    allowed = _ALLOWED_TRANSITIONS.get(publication.status, set())
    if new_status not in allowed:
        raise ValueError(
            f"Invalid transition: {publication.status} -> {new_status}"
        )
    publication.status = new_status


def find_or_create_publication(session, *, cadence: str, period_start: date,
                                period_end: date,
                                source_publication_ids: Optional[list[int]] = None
                                ) -> Publication:
    """Idempotent publication row creation.

    Returns the existing row for ``(cadence, period_start)`` if one is
    present, else inserts a fresh ``generating`` row. The
    ``UniqueConstraint`` on the table is the durable guarantee — this
    function just hides the IntegrityError dance.
    """
    existing = (
        session.query(Publication)
        .filter_by(cadence=cadence, period_start=period_start)
        .one_or_none()
    )
    if existing is not None:
        return existing

    pub = Publication(
        cadence=cadence,
        period_start=period_start,
        period_end=period_end,
        status="generating",
        source_publication_ids=source_publication_ids,
    )
    session.add(pub)
    try:
        session.flush()
    except IntegrityError:
        # Lost the race with a concurrent run — read back the winner.
        session.rollback()
        return (
            session.query(Publication)
            .filter_by(cadence=cadence, period_start=period_start)
            .one()
        )
    return pub


def finalize_for_approval(session, publication: Publication,
                           draft_markdown: str, *, title_for_pdf: str) -> None:
    """Common tail: store draft, render PDF, send email, transition to awaiting.

    Idempotent against re-runs: if the publication is already
    ``awaiting_approval`` we leave it alone.
    """
    if publication.status == "awaiting_approval":
        logger.info(
            "Publication %s already awaiting approval — skipping re-send.",
            publication.id
        )
        return

    if publication.status != "generating":
        raise ValueError(
            f"finalize_for_approval requires status='generating'; got '{publication.status}'"
        )

    now = datetime.utcnow()
    publication.draft_markdown = draft_markdown
    publication.composed_at = now
    publication.expires_at = config.expires_at_default(now)

    # Render PDF and persist its path before we send anything.
    pdf_path = render.write_pdf(
        publication_id=publication.id,
        title=title_for_pdf,
        date_str=now.strftime("%B %d, %Y"),
        markdown_text=draft_markdown,
    )
    publication.pdf_path = str(pdf_path)
    pdf_bytes = pdf_path.read_bytes()

    # Issue tokens; flush so they're visible if the email send fails.
    approve_tok, reject_tok = approval.issue_tokens(session, publication)
    transition_status(publication, "awaiting_approval")
    session.flush()

    email = approval.render_approval_email(publication, approve_tok, reject_tok, pdf_bytes)
    delivery_id = approval.send_email(email)
    logger.info(
        "Approval email sent for publication %s (delivery_id=%s)",
        publication.id, delivery_id
    )

    session.commit()


def detach_for_caller(session, publication: Publication) -> Publication:
    """Refresh + expunge so callers can read attrs after session.close().

    The cycle entry points return the Publication so callers (CLI,
    tests) can inspect ``pub.status`` etc. SQLAlchemy expires attrs on
    commit; without a refresh+expunge they raise DetachedInstanceError
    once the session closes.
    """
    session.refresh(publication)
    session.expunge(publication)
    return publication


def expire_stale_drafts(session, *, now: Optional[datetime] = None) -> list[int]:
    """Move any ``awaiting_approval`` rows older than the TTL to ``expired``.

    Returns the list of publication ids that were expired.
    """
    cutoff = config.expiry_threshold(now)
    rows = (
        session.query(Publication)
        .filter(Publication.status == "awaiting_approval")
        .filter(Publication.composed_at < cutoff)
        .all()
    )
    expired_ids = []
    for pub in rows:
        transition_status(pub, "expired")
        expired_ids.append(pub.id)
    if rows:
        session.commit()
    return expired_ids
