"""Weekly admin report card — data layer.

Pure SQLAlchemy. Builds the payload rendered by the Admin tab's
'Report Card' sub-tab. Kept out of ``app.py`` so it can be unit-tested
without pulling in Streamlit.

Two audiences for the same data:

* **Human** (the founder) — at-a-glance view of weekly + cumulative
  coverage and a flagged list of issues.
* **AI assistant** — the same ``issues`` list carries a deterministic
  ``fix_hint`` per item so a Claude session reading this report can act
  without round-tripping through the UI. ``build_report`` returns a
  ``dict`` that's safe to JSON-serialize.

Weeks follow the insights cadence: Sunday→Saturday, indexed by the
Sunday (mirrors ``insights.compose.weekly_period_for``). That makes the
report-card week align 1:1 with ``Publication.period_start`` and
``WeeklyRun.period_start`` rows the insights cycle writes.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from typing import Optional

from sqlalchemy import distinct, func
from sqlalchemy.orm import Session

from database import (
    Document,
    FetchRun,
    IpsDocument,
    Plan,
    Publication,
    Summary,
    WeeklyRun,
    get_session,
)


REPORT_CARD_WEEKS = 13
DAILY_PIPELINE_STALE_HOURS = 36     # GHA cron fires daily ~11:00 UTC
PLAN_STALE_DAYS = 30                # plans untouched longer flag a warning
APPROVAL_PENDING_HOURS = 72         # weekly publication awaiting approval


# ---------------------------------------------------------------------------
# Period helpers
# ---------------------------------------------------------------------------

def weekly_period(today: date) -> tuple[date, date]:
    """Most recent fully-completed Sun→Sat week relative to ``today``."""
    days_back_to_saturday = (today.weekday() + 2) % 7   # Sat=0, Sun=1, ...
    period_end = today - timedelta(days=days_back_to_saturday or 7)
    period_start = period_end - timedelta(days=6)
    return period_start, period_end


def _sunday_of(d: date) -> date:
    """Return the Sunday that begins ``d``'s Sun→Sat week."""
    days_back = (d.weekday() + 1) % 7   # Sun=0, Mon=1, ..., Sat=6
    return d - timedelta(days=days_back)


# ---------------------------------------------------------------------------
# Output shapes
# ---------------------------------------------------------------------------

@dataclass
class WeekRow:
    week_start: date
    week_end: date
    unique_plans: int = 0
    new_documents: int = 0
    new_summaries: int = 0
    gha_success: int = 0
    gha_failed: int = 0
    local_success: int = 0
    local_failed: int = 0
    publication_status: Optional[str] = None
    weekly_run_status: Optional[str] = None
    cumulative_unique_plans: int = 0


@dataclass
class Issue:
    severity: str          # 'error' | 'warning' | 'info'
    category: str
    message: str
    fix_hint: str = ""
    details: list = field(default_factory=list)


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

def build_report(
    weeks_back: int = REPORT_CARD_WEEKS,
    *,
    now: Optional[datetime] = None,
    session: Optional[Session] = None,
) -> dict:
    """Compose the full report-card payload.

    ``now`` is injectable for deterministic tests. ``session`` is too — if
    omitted, opens (and closes) one via :func:`database.get_session`.
    """
    own_session = session is None
    if own_session:
        session = get_session()
    if now is None:
        now = datetime.utcnow()

    try:
        latest_week_start, latest_week_end = weekly_period(now.date())

        weeks: list[WeekRow] = []
        for i in range(weeks_back):
            ws = latest_week_start - timedelta(days=7 * i)
            weeks.append(WeekRow(week_start=ws, week_end=ws + timedelta(days=6)))
        weeks_by_start = {w.week_start: w for w in weeks}
        earliest = min(weeks_by_start)
        earliest_dt = datetime.combine(earliest, datetime.min.time())
        latest_dt = datetime.combine(
            latest_week_end + timedelta(days=1), datetime.min.time()
        )

        cumulative = _cumulative(session)
        _fill_documents(session, weeks_by_start, earliest_dt, latest_dt)
        _fill_summaries(session, weeks_by_start, earliest_dt, latest_dt)
        _fill_fetch_runs(session, weeks_by_start, earliest_dt, latest_dt)
        _fill_publications(session, weeks_by_start)
        _fill_weekly_runs(session, weeks_by_start)
        _fill_cumulative_plans(session, weeks)

        issues = _detect_issues(session, now, latest_week_start, latest_week_end)

        return {
            "generated_at": now.replace(microsecond=0).isoformat() + "Z",
            "latest_week": {
                "start": latest_week_start.isoformat(),
                "end": latest_week_end.isoformat(),
            },
            "cumulative": cumulative,
            "weeks": [
                {
                    "week_start": w.week_start.isoformat(),
                    "week_end": w.week_end.isoformat(),
                    "unique_plans": w.unique_plans,
                    "new_documents": w.new_documents,
                    "new_summaries": w.new_summaries,
                    "gha_success": w.gha_success,
                    "gha_failed": w.gha_failed,
                    "local_success": w.local_success,
                    "local_failed": w.local_failed,
                    "publication_status": w.publication_status,
                    "weekly_run_status": w.weekly_run_status,
                    "cumulative_unique_plans": w.cumulative_unique_plans,
                }
                for w in weeks
            ],
            "issues": [asdict(i) for i in issues],
        }
    finally:
        if own_session:
            session.close()


# ---------------------------------------------------------------------------
# Section builders
# ---------------------------------------------------------------------------

def _cumulative(session: Session) -> dict:
    total_plans = session.query(func.count(Plan.id)).scalar() or 0
    plans_with_doc = (
        session.query(func.count(distinct(Document.plan_id))).scalar() or 0
    )
    plans_with_summary = (
        session.query(func.count(distinct(Document.plan_id)))
        .join(Summary, Summary.document_id == Document.id)
        .scalar() or 0
    )
    plans_with_cafr = (
        session.query(func.count(distinct(Document.plan_id)))
        .filter(Document.doc_type == "cafr")
        .scalar() or 0
    )
    plans_with_ips = (
        session.query(func.count(distinct(IpsDocument.plan_id))).scalar() or 0
    )
    return {
        "total_plans": int(total_plans),
        "plans_with_document": int(plans_with_doc),
        "plans_with_summary": int(plans_with_summary),
        "plans_with_cafr": int(plans_with_cafr),
        "plans_with_ips": int(plans_with_ips),
    }


def _fill_documents(session, weeks_by_start, earliest_dt, latest_dt):
    rows = (
        session.query(Document.plan_id, Document.downloaded_at)
        .filter(Document.downloaded_at.isnot(None))
        .filter(Document.downloaded_at >= earliest_dt)
        .filter(Document.downloaded_at < latest_dt)
        .all()
    )
    plans_per_week: dict[date, set[str]] = {ws: set() for ws in weeks_by_start}
    for plan_id, downloaded_at in rows:
        ws = _sunday_of(downloaded_at.date())
        if ws in weeks_by_start:
            weeks_by_start[ws].new_documents += 1
            plans_per_week[ws].add(plan_id)
    for ws, plans in plans_per_week.items():
        weeks_by_start[ws].unique_plans = len(plans)


def _fill_summaries(session, weeks_by_start, earliest_dt, latest_dt):
    rows = (
        session.query(Summary.generated_at)
        .filter(Summary.generated_at.isnot(None))
        .filter(Summary.generated_at >= earliest_dt)
        .filter(Summary.generated_at < latest_dt)
        .all()
    )
    for (generated_at,) in rows:
        ws = _sunday_of(generated_at.date())
        if ws in weeks_by_start:
            weeks_by_start[ws].new_summaries += 1


def _fill_fetch_runs(session, weeks_by_start, earliest_dt, latest_dt):
    rows = (
        session.query(FetchRun.source, FetchRun.status, FetchRun.started_at)
        .filter(FetchRun.started_at >= earliest_dt)
        .filter(FetchRun.started_at < latest_dt)
        .all()
    )
    for source, status, started_at in rows:
        ws = _sunday_of(started_at.date())
        if ws not in weeks_by_start:
            continue
        row = weeks_by_start[ws]
        ok = status == "success"
        if source == "gha":
            if ok:
                row.gha_success += 1
            else:
                row.gha_failed += 1
        elif source == "local":
            if ok:
                row.local_success += 1
            else:
                row.local_failed += 1


def _fill_publications(session, weeks_by_start):
    pubs = (
        session.query(Publication.period_start, Publication.status)
        .filter(Publication.cadence == "weekly")
        .filter(Publication.period_start.in_(list(weeks_by_start.keys())))
        .all()
    )
    for period_start, status in pubs:
        weeks_by_start[period_start].publication_status = status


def _fill_weekly_runs(session, weeks_by_start):
    runs = (
        session.query(WeeklyRun.period_start, WeeklyRun.status)
        .filter(WeeklyRun.period_start.in_(list(weeks_by_start.keys())))
        .all()
    )
    for period_start, status in runs:
        weeks_by_start[period_start].weekly_run_status = status


def _fill_cumulative_plans(session, weeks: list[WeekRow]):
    """For each week_end, the count of distinct plans we have ever fetched a
    document for through that date."""
    first_seen = (
        session.query(Document.plan_id, func.min(Document.downloaded_at))
        .filter(Document.downloaded_at.isnot(None))
        .group_by(Document.plan_id)
        .all()
    )
    first_dates = sorted(d.date() for _, d in first_seen if d is not None)
    # weeks is latest-first; walk oldest-first for the running count.
    for w in sorted(weeks, key=lambda x: x.week_start):
        w.cumulative_unique_plans = sum(
            1 for fd in first_dates if fd <= w.week_end
        )


# ---------------------------------------------------------------------------
# Issue detection
# ---------------------------------------------------------------------------

def _detect_issues(
    session: Session,
    now: datetime,
    latest_week_start: date,
    latest_week_end: date,
) -> list[Issue]:
    issues: list[Issue] = []
    issues.extend(_check_fetch_pipeline(session, now, "gha", "GHA daily-pipeline",
                                        ".github/workflows/daily-pipeline.yml",
                                        "error"))
    issues.extend(_check_fetch_pipeline(session, now, "local", "Local Task Scheduler",
                                        "scripts/run_daily.bat",
                                        "warning"))
    issues.extend(_check_weekly_publication(session, now, latest_week_start,
                                            latest_week_end))
    issues.extend(_check_weekly_run(session, latest_week_start))
    issues.extend(_check_plan_freshness(session, now))
    issues.extend(_check_extraction_failures(session))
    return issues


def _check_fetch_pipeline(session, now, source, label, workflow, severity):
    issues: list[Issue] = []
    last_success = (
        session.query(FetchRun)
        .filter(FetchRun.source == source)
        .filter(FetchRun.status == "success")
        .order_by(FetchRun.started_at.desc())
        .first()
    )
    if last_success is None:
        issues.append(Issue(
            severity=severity,
            category="fetch_pipeline",
            message=f"No successful {label} run has ever been recorded.",
            fix_hint=f"Verify {workflow} is wired up and required secrets/env vars exist.",
        ))
    else:
        hours = (now - last_success.started_at).total_seconds() / 3600
        if hours > DAILY_PIPELINE_STALE_HOURS:
            issues.append(Issue(
                severity=severity,
                category="fetch_pipeline",
                message=(
                    f"{label} last succeeded {hours:.0f}h ago "
                    f"({last_success.started_at:%Y-%m-%d %H:%M UTC})."
                ),
                fix_hint=(
                    f"Inspect recent runs of {workflow}; common causes: "
                    "expired ANTHROPIC_API_KEY, Playwright/browser dep change, "
                    "Windows host offline (local only)."
                ),
            ))
    last_any = (
        session.query(FetchRun)
        .filter(FetchRun.source == source)
        .order_by(FetchRun.started_at.desc())
        .first()
    )
    if last_any and last_any.status == "failed":
        issues.append(Issue(
            severity="error",
            category="fetch_pipeline",
            message=(
                f"Most recent {label} run failed at "
                f"{last_any.started_at:%Y-%m-%d %H:%M UTC}."
            ),
            fix_hint=(
                f"Error message: {last_any.error_message or '(none captured)'}. "
                f"Re-run after addressing the cause."
            ),
            details=[last_any.error_message] if last_any.error_message else [],
        ))
    return issues


def _check_weekly_publication(session, now, latest_week_start, latest_week_end):
    issues: list[Issue] = []
    pub = (
        session.query(Publication)
        .filter(Publication.cadence == "weekly")
        .filter(Publication.period_start == latest_week_start)
        .first()
    )
    days_since_week_end = (now.date() - latest_week_end).days
    if pub is None:
        # Cron fires Sunday 11:00 UTC for the prior Sun→Sat. Allow 2 days
        # grace before flagging absence.
        if days_since_week_end >= 2:
            issues.append(Issue(
                severity="error",
                category="weekly_insights",
                message=(
                    f"No weekly Publication exists for {latest_week_start.isoformat()} "
                    f"(period ended {days_since_week_end}d ago)."
                ),
                fix_hint=(
                    "Trigger .github/workflows/weekly-insights.yml manually, "
                    "or run locally: "
                    f"`python -m insights.scheduler weekly --period {latest_week_start.isoformat()} --skip-scrape`"
                ),
            ))
        return issues
    if pub.status == "failed":
        issues.append(Issue(
            severity="error",
            category="weekly_insights",
            message=(
                f"Weekly Publication for {latest_week_start.isoformat()} is "
                "in 'failed' status."
            ),
            fix_hint=(
                f"Error: {pub.error_message or '(none captured)'}. Re-run with "
                f"`python -m insights.scheduler weekly --period {latest_week_start.isoformat()} --force`."
            ),
            details=[pub.error_message] if pub.error_message else [],
        ))
    elif pub.status == "awaiting_approval" and pub.composed_at:
        hours = (now - pub.composed_at).total_seconds() / 3600
        if hours > APPROVAL_PENDING_HOURS:
            issues.append(Issue(
                severity="warning",
                category="weekly_insights",
                message=(
                    f"Weekly Publication for {latest_week_start.isoformat()} "
                    f"has been awaiting approval for {hours:.0f}h."
                ),
                fix_hint=(
                    "Open the approval email and click approve/reject — "
                    "the token expires after APPROVAL_TOKEN_TTL_DAYS (default 7)."
                ),
            ))
    return issues


def _check_weekly_run(session, latest_week_start):
    run = (
        session.query(WeeklyRun)
        .filter(WeeklyRun.period_start == latest_week_start)
        .first()
    )
    if not run or run.status not in ("failed", "partial"):
        return []
    return [Issue(
        severity="warning",
        category="weekly_run",
        message=(
            f"WeeklyRun for {latest_week_start.isoformat()} status='{run.status}' "
            f"({run.plans_completed or 0}/{run.plans_total or 0} plans completed)."
        ),
        fix_hint=(
            f"Re-run is idempotent (resumes failed plans only): "
            f"`python -m insights.scheduler weekly --period {latest_week_start.isoformat()}`."
        ),
        details=[run.error_message] if run.error_message else [],
    )]


def _check_plan_freshness(session, now):
    cutoff = now - timedelta(days=PLAN_STALE_DAYS)
    last_doc_per_plan = dict(
        session.query(Document.plan_id, func.max(Document.downloaded_at))
        .filter(Document.downloaded_at.isnot(None))
        .group_by(Document.plan_id)
        .all()
    )
    plans = session.query(Plan.id, Plan.name).all()
    stale: list[tuple[str, datetime]] = []
    never: list[str] = []
    for pid, pname in plans:
        last = last_doc_per_plan.get(pid)
        if last is None:
            never.append(pname)
        elif last < cutoff:
            stale.append((pname, last))
    issues: list[Issue] = []
    if stale:
        stale.sort(key=lambda x: x[1])
        issues.append(Issue(
            severity="warning",
            category="stale_plans",
            message=(
                f"{len(stale)} plan(s) have had no new document in "
                f"{PLAN_STALE_DAYS}+ days."
            ),
            fix_hint=(
                "Likely site redesign breaking the fetcher. Inspect the plan's "
                "materials_url / cafr_url_template in data/known_plans.json "
                "(or _cafr_overrides.json for one-off overrides), then re-run "
                "`python pipeline.py <plan_id>`."
            ),
            details=[f"{n} (last: {d:%Y-%m-%d})" for n, d in stale],
        ))
    if never:
        issues.append(Issue(
            severity="info",
            category="no_documents",
            message=f"{len(never)} plan(s) have zero documents downloaded.",
            fix_hint=(
                "Either newly added plans pending first run, or the registry "
                "is missing materials_url. Inspect data/known_plans.json."
            ),
            details=sorted(never),
        ))
    return issues


def _check_extraction_failures(session):
    failed = (
        session.query(func.count(Document.id))
        .filter(Document.extraction_status == "failed")
        .scalar() or 0
    )
    if not failed:
        return []
    return [Issue(
        severity="info",
        category="extraction_failures",
        message=f"{int(failed)} document(s) have extraction_status='failed'.",
        fix_hint=(
            "Re-run with OCR fallback locally: `python pipeline.py "
            "--retry-failed` (requires Tesseract installed)."
        ),
    )]
