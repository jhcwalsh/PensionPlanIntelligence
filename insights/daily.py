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

from sqlalchemy.orm import Session

from database import Document

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
