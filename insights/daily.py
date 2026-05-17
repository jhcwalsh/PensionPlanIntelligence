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
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from database import DailyRun, Document, Plan, Publication, get_session
from insights import config, cycle_common

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
        2. Keyword:  any doc filename matches a DAILY_APPROVAL_KEYWORDS entry
        3. Reappear: plan's most-recent *prior* document is older than
                     DAILY_REAPPEAR_DAYS days. A brand-new plan (no prior
                     docs) does NOT trigger — otherwise the trigger would
                     fire on every plan's first appearance.

    Precondition: every doc must have a non-null ``downloaded_at``. The
    only call site (``run_daily_cycle``) sources docs from
    ``select_new_docs``, which filters nulls, so this is satisfied. A
    null ``downloaded_at`` would raise ``TypeError`` from the ``min()``
    over the reappear-lookback timestamp.
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


def compose_daily(
    docs: list[Document],
    *,
    triggers: list[str],
    digest_date: datetime,
    session: Optional[Session] = None,
) -> str:
    """Render the daily digest markdown.

    Quiet days return a one-line "nothing today" string with no LLM
    call. Non-quiet days group docs by plan, call ``_synthesize_plan_paragraph``
    once per plan, and append a bulleted doc list under each section.

    If ``session`` is provided, it is used for the plan-name lookup (so the
    orchestrator can own the DB transaction). Otherwise a fresh session is
    opened and closed locally.
    """
    date_str = digest_date.strftime("%Y-%m-%d")
    if not docs:
        return (
            f"# Pension Plans — Daily Digest — {date_str}\n\n"
            f"No new documents fetched in the last 24 hours.\n"
        )

    parts: list[str] = [f"# Pension Plans — Daily Digest — {date_str}\n"]
    if triggers:
        parts.append(f"\nTriggers: {', '.join(triggers)}\n")

    grouped: dict[str, list[Document]] = defaultdict(list)
    for d in docs:
        grouped[d.plan_id].append(d)

    # Resolve plan names in one query.
    owns_session = session is None
    if owns_session:
        session = get_session()
    try:
        plan_map = {
            p.id: p.name
            for p in session.query(Plan).filter(Plan.id.in_(list(grouped.keys()))).all()
        }
    finally:
        if owns_session:
            session.close()

    base_url = config.APPROVAL_BASE_URL

    any_llm_failed = False
    for plan_id in sorted(grouped.keys()):
        plan_name = plan_map.get(plan_id, plan_id)
        plan_docs = grouped[plan_id]
        parts.append(f"\n## {plan_name}\n")

        paragraph, ok = _synthesize_plan_paragraph(plan_name, plan_docs)
        parts.append(f"{paragraph}\n")
        if not ok:
            any_llm_failed = True

        for d in plan_docs:
            title = d.filename or "(untitled document)"
            date_label = (
                d.meeting_date.strftime("%b %d, %Y")
                if d.meeting_date else "no date"
            )
            link = f"{base_url}/?document={d.id}"
            parts.append(f"- [{title} — {date_label}]({link})\n")

    if any_llm_failed:
        parts.insert(
            1,
            "\n_LLM synthesis unavailable for one or more sections — "
            "showing document list only._\n",
        )

    return "".join(parts)


def _synthesize_plan_paragraph(
    plan_name: str,
    docs: list[Document],
) -> tuple[str, bool]:
    """Return ``(paragraph, ok)``. Falls back deterministically on failure.

    Mock mode (``INSIGHTS_MODE=mock``) returns a canned paragraph and
    never calls Anthropic. Live mode is wired in a separate helper so
    the test suite can stub it.
    """
    if config.is_mock():
        return (
            f"{plan_name} posted {len(docs)} document(s) in the last day: "
            + ", ".join(d.doc_type or "document" for d in docs) + ".",
            True,
        )

    try:
        return (_synthesize_via_anthropic(plan_name, docs), True)
    except Exception as exc:  # noqa: BLE001
        logger.warning("LLM synthesis failed for %s: %s", plan_name, exc)
        return (
            f"{len(docs)} document(s) fetched today; LLM synthesis failed.",
            False,
        )


def _synthesize_via_anthropic(plan_name: str, docs: list[Document]) -> str:
    """Live-mode Anthropic call. Imported lazily so mock tests stay light."""
    from anthropic import Anthropic
    from summarizer import MODEL_SONNET

    client = Anthropic()
    doc_lines = "\n".join(
        f"- {d.filename or '(untitled)'} "
        f"({d.doc_type or 'document'}, "
        f"{d.meeting_date.isoformat() if d.meeting_date else 'no date'})"
        for d in docs
    )
    system = (
        "Produce one factual paragraph describing what these documents are. "
        "Do not editorialize. Do not infer significance. Do not recommend. "
        "State only what the documents are and what they cover. Maximum 3 sentences."
    )
    user = f"Plan: {plan_name}\nNew documents today:\n{doc_lines}"
    resp = client.messages.create(
        model=MODEL_SONNET,
        max_tokens=500,
        temperature=0,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    return resp.content[0].text.strip()


def last_sent_at(session: Session) -> Optional[datetime]:
    """Return ``MAX(sent_at)`` over ``daily_runs`` or ``None`` if empty."""
    return session.query(func.max(DailyRun.sent_at)).scalar()


def record_daily_run(
    session: Session,
    *,
    sent_at: datetime,
    publication_id: int,
    docs_count: int,
    triggers: list[str],
    approval_gated: bool,
) -> DailyRun:
    """Insert one ``DailyRun`` row. Caller commits."""
    row = DailyRun(
        sent_at=sent_at,
        publication_id=publication_id,
        docs_count=docs_count,
        triggers=list(triggers),
        approval_gated=approval_gated,
    )
    session.add(row)
    session.flush()
    return row


def run_daily_cycle(
    *,
    now: Optional[datetime] = None,
    force: bool = False,
) -> Publication:
    """Run one daily-digest cycle.

    Steps:
        1. find/create today's Publication (cadence='daily').
        2. select_new_docs since last_sent_at.
        3. apply_triggers → reasons.
        4. compose_daily(docs, reasons).
        5. if reasons: finalize_for_approval; else: finalize_and_send.
        6. record_daily_run.

    Returns the Publication for the CLI to print. ``--force`` expires any
    existing publication for today (including auto-sent ones) and starts
    over.
    """
    now_utc = now if now is not None else datetime.utcnow()
    today = now_utc.date()

    session = get_session()
    publication: Optional[Publication] = None
    try:
        publication = cycle_common.find_or_create_publication(
            session,
            cadence="daily",
            period_start=today,
            period_end=today,
        )

        if force and publication.status in (
            "awaiting_approval", "approved", "published"
        ):
            cycle_common.transition_status(publication, "expired")
            session.flush()
            # Re-create — the unique constraint returns the just-expired
            # row, so we bump it back to generating to refill it.
            publication = cycle_common.find_or_create_publication(
                session,
                cadence="daily",
                period_start=today,
                period_end=today,
            )
            publication.status = "generating"
            publication.draft_markdown = None
            publication.composed_at = None
            publication.pdf_path = None
            session.flush()

        if publication.status != "generating":
            logger.info(
                "Daily publication %s already at status '%s' — skipping.",
                publication.id, publication.status,
            )
            return cycle_common.detach_for_caller(session, publication)

        since = last_sent_at(session)
        docs = select_new_docs(since=since, now_utc=now_utc, session=session)
        triggers = apply_triggers(docs, now_utc=now_utc, session=session)
        draft = compose_daily(docs, triggers=triggers, digest_date=now_utc, session=session)

        approval_gated = bool(triggers)
        title_for_pdf = f"Daily Pension Digest — {today.isoformat()}"

        if approval_gated:
            cycle_common.finalize_for_approval(
                session, publication, draft, title_for_pdf=title_for_pdf,
            )
        else:
            cycle_common.finalize_and_send(
                session, publication, draft, title_for_pdf=title_for_pdf,
            )

        record_daily_run(
            session,
            sent_at=now_utc,
            publication_id=publication.id,
            docs_count=len(docs),
            triggers=triggers,
            approval_gated=approval_gated,
        )
        session.commit()
        return cycle_common.detach_for_caller(session, publication)

    except Exception:
        session.rollback()
        if publication is not None and publication.status == "generating":
            try:
                cycle_common.transition_status(publication, "failed")
                session.commit()
            except Exception:  # noqa: BLE001
                session.rollback()
        raise
    finally:
        session.close()
