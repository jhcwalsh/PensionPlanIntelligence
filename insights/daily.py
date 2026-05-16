"""Daily Pension Digest — selector, triggers, composer, orchestrator.

Slots into the existing ``insights/`` package as a fifth cadence
alongside weekly / rfp_weekly / monthly / annual. Runs from a GitHub
Actions cron, not Windows Task Scheduler — the lookback window
(``daily_runs.sent_at``) makes the cycle resilient to skipped days.

Unlike weekly/monthly, most days auto-send (no approval gate). The
approval flow is invoked only when ``apply_triggers`` returns reasons
(volume / keyword / reappearing-plan).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from database import Document
from insights import config

logger = logging.getLogger(__name__)


def select_new_docs(
    *,
    since: Optional[datetime],
    now_utc: datetime,
    session: Session,
) -> list[Document]:
    """Return documents whose ``downloaded_at`` is strictly after ``since``.

    If ``since`` is ``None`` (no prior digest) we fall back to a 24-hour
    window ending at ``now_utc``. Future-dated rows (clock skew) and
    rows with ``downloaded_at IS NULL`` (discovered but not yet
    downloaded) are excluded. Ordering matches the digest layout:
    ``(plan_id, meeting_date DESC)`` with null meeting_dates last.
    """
    cutoff = since if since is not None else (now_utc - timedelta(hours=24))
    q = (
        session.query(Document)
        .filter(Document.downloaded_at.isnot(None))
        .filter(Document.downloaded_at > cutoff)
        .filter(Document.downloaded_at < now_utc)
        .order_by(
            Document.plan_id.asc(),
            Document.meeting_date.desc().nullslast(),
        )
    )
    return q.all()


def apply_triggers(
    docs: list[Document],
    *,
    now_utc: datetime,
    session: Session,
) -> list[str]:
    """Return a list of trigger reasons; empty list means auto-send.

    Three rules, ORed:
        1. Volume:   len(docs) > DAILY_APPROVAL_DOC_THRESHOLD
        2. Keyword:  any doc title matches a DAILY_APPROVAL_KEYWORDS entry
        3. Reappear: plan's most-recent *prior* document is older than
                     DAILY_REAPPEAR_DAYS days. A brand-new plan (no prior
                     docs) does NOT trigger — otherwise the trigger would
                     fire on every plan's first appearance.
    """
    reasons: list[str] = []
    if not docs:
        return reasons

    if len(docs) > config.DAILY_APPROVAL_DOC_THRESHOLD:
        reasons.append(f"volume:{len(docs)}")

    keywords_lower = [k.lower() for k in config.DAILY_APPROVAL_KEYWORDS]
    for d in docs:
        title = (d.filename or "").lower()
        matched = next((k for k in keywords_lower if k in title), None)
        if matched:
            reasons.append(f"keyword:{matched}")
            break  # one keyword reason is enough — avoid spam

    reappear_cutoff = now_utc - timedelta(days=config.DAILY_REAPPEAR_DAYS)
    plan_ids = sorted({d.plan_id for d in docs})
    today_min = min(d.downloaded_at for d in docs)
    for plan_id in plan_ids:
        prior_max = (
            session.query(func.max(Document.downloaded_at))
            .filter(Document.plan_id == plan_id)
            .filter(Document.downloaded_at.isnot(None))
            .filter(Document.downloaded_at < today_min)
            .scalar()
        )
        # Brand-new plans (prior_max is None) do NOT trigger reappear.
        if prior_max is not None and prior_max < reappear_cutoff:
            reasons.append(f"reappear:{plan_id}")

    return reasons
