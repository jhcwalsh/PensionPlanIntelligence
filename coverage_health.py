"""Coverage over time, and the gaps in it, for the admin Coverage tab.

Two kinds of gap, because they need different responses:

- **Temporary**: something that normally arrives has stopped. Pipeline-wide,
  that is a run of complete weeks with far fewer downloads than usual (the
  13 Sep - 11 Oct 2026 cron outage went unnoticed for a month). Per plan, it
  is a plan overdue against its own observed meeting cadence.
- **Structural**: a plan with no board material in twelve months, usually
  blocked, publishing nothing fetchable, or needing a fetcher pattern.

Board material only, as in ``queries.aum_coverage``: CAFRs, IPS documents
and actuarial valuations are annual and would hide a plan whose board packs
have stopped. Reads plan ids and dates only, never document text.
"""
from __future__ import annotations

import json
import statistics
from collections import Counter, defaultdict
from datetime import date, timedelta
from pathlib import Path

from sqlalchemy import func, or_

from database import (CafrExtract, Document, FundCommitment, IpsDocument, MeetingRecording,
                      Plan, Summary)

EXCLUDED_TYPES = ("cafr", "ips", "actuarial_valuation")
# Months allowed since the last document before a plan counts as overdue.
# Monthly and quarterly match queries.plan_reference_rows; a sparse board
# (one or two meeting months a year) gets six, or it would alarm all year.
ALLOWANCE = {"monthly": 2, "quarterly": 4, "sparse": 6}
LOW_WEEK_RATIO = 0.25
_DATA = Path(__file__).resolve().parent / "data"


def _month(d) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def _month_index(key: str) -> int:
    y, m = map(int, key.split("-"))
    return y * 12 + m - 1


def _last_months(today: date, n: int) -> list[str]:
    end = today.year * 12 + today.month - 1
    return [f"{i // 12:04d}-{i % 12 + 1:02d}" for i in range(end - n + 1, end + 1)]


def _board_docs(session) -> list[tuple]:
    """(document id, plan id, meeting date, download date) for board material."""
    return (session.query(Document.id, Document.plan_id, Document.meeting_date,
                          Document.downloaded_at)
            .filter(or_(Document.doc_type.is_(None), ~Document.doc_type.in_(EXCLUDED_TYPES)))
            .all())


def _doc_month(meeting, downloaded) -> str | None:
    when = meeting or downloaded
    return _month(when) if when else None


def _blocked_reasons() -> dict[str, str]:
    out = {}
    for name in ("waf_blocked_plans.json", "waf_blocked_cafr_plans.json"):
        try:
            plans = json.loads((_DATA / name).read_text(encoding="utf-8"))["plans"]
        except (OSError, ValueError, KeyError):
            continue
        for p in plans:
            if name.startswith("waf_blocked_plans") or p["id"] not in out:
                reason = (p.get("reason") or "").strip()
                out[p["id"]] = f"{p.get('blocked_by') or 'blocked'}: {reason}"[:160]
    return out


def weekly_intake(session, weeks: int = 26, today: date | None = None,
                  docs: list[tuple] | None = None) -> list[dict]:
    """Board documents downloaded, plans touched and summaries written, per
    Monday-starting week. The current week is marked ``partial``."""
    today = today or date.today()
    this_monday = today - timedelta(days=today.weekday())
    start = this_monday - timedelta(weeks=weeks - 1)
    docs = docs if docs is not None else _board_docs(session)
    downloads, plans = Counter(), defaultdict(set)
    for _id, pid, _meeting, downloaded in docs:
        if downloaded is None or downloaded.date() < start:
            continue
        wk = downloaded.date() - timedelta(days=downloaded.weekday())
        downloads[wk] += 1
        plans[wk].add(pid)
    summaries = Counter()
    for (generated,) in session.query(Summary.generated_at).filter(Summary.generated_at.isnot(None)):
        if generated.date() >= start:
            summaries[generated.date() - timedelta(days=generated.weekday())] += 1
    return [{"week": wk, "downloads": downloads[wk], "plans": len(plans[wk]),
             "summaries": summaries[wk], "partial": wk == this_monday}
            for wk in (start + timedelta(weeks=i) for i in range(weeks))]


def pipeline_gaps(weekly: list[dict], ratio: float = LOW_WEEK_RATIO,
                  min_typical: int = 10) -> list[dict]:
    """Runs of complete weeks whose downloads fell below ``ratio`` of the
    window's median week."""
    complete = [w for w in weekly if not w["partial"]]
    if not complete:
        return []
    typical = statistics.median(w["downloads"] for w in complete)
    if typical < min_typical:
        return []
    gaps, run = [], []
    for w in complete + [None]:
        if w is not None and w["downloads"] < ratio * typical:
            run.append(w)
            continue
        if run:
            gaps.append({"start": run[0]["week"], "end": run[-1]["week"] + timedelta(days=6),
                         "weeks": len(run), "downloads": sum(r["downloads"] for r in run),
                         "typical": int(typical)})
            run = []
    return gaps


def plan_months(session, months: int = 24, today: date | None = None,
                docs: list[tuple] | None = None) -> dict:
    """Board documents per plan per month (meeting month, else download month)."""
    keys = _last_months(today or date.today(), months)
    window = set(keys)
    rows: dict[str, dict[str, int]] = defaultdict(dict)
    for _id, pid, meeting, downloaded in (docs if docs is not None else _board_docs(session)):
        key = _doc_month(meeting, downloaded)
        if key in window:
            rows[pid][key] = rows[pid].get(key, 0) + 1
    return {"months": keys, "rows": dict(rows)}


def plan_status(session, today: date | None = None, docs: list[tuple] | None = None) -> list[dict]:
    """One row per plan: board material in the last twelve months, observed
    cadence, how overdue it is, the other datasets, and a status of
    ``ok``, ``overdue`` (temporary gap) or ``structural``."""
    today = today or date.today()
    docs = docs if docs is not None else _board_docs(session)
    keys = _last_months(today, 12)
    window, now = set(keys), _month_index(keys[-1])
    since_90 = today - timedelta(days=90)
    summarised = {d for (d,) in session.query(Summary.document_id)}

    months_seen, last_month = defaultdict(set), {}
    docs_12m, docs_90d, unsummarised = Counter(), Counter(), Counter()
    for doc_id, pid, meeting, downloaded in docs:
        key = _doc_month(meeting, downloaded)
        if key is None or _month_index(key) > now:
            continue
        if key > last_month.get(pid, ""):
            last_month[pid] = key
        if key in window:
            months_seen[pid].add(key)
            docs_12m[pid] += 1
            if doc_id not in summarised:
                unsummarised[pid] += 1
        if downloaded is not None and downloaded.date() >= since_90:
            docs_90d[pid] += 1

    cafr = dict(session.query(CafrExtract.plan_id, func.max(CafrExtract.fiscal_year))
                .group_by(CafrExtract.plan_id))
    ips = {p for (p,) in session.query(IpsDocument.plan_id).distinct()}
    commitments = dict(session.query(FundCommitment.plan_id, func.count(FundCommitment.id))
                       .group_by(FundCommitment.plan_id))
    year_ago = today - timedelta(days=365)
    recordings = Counter()
    for pid, inferred, published in session.query(MeetingRecording.plan_id,
                                                  MeetingRecording.meeting_date_inferred,
                                                  MeetingRecording.published_at):
        when = inferred or published
        if when is not None and when.date() >= year_ago:
            recordings[pid] += 1
    blocked = _blocked_reasons()

    rows = []
    for plan in session.query(Plan).order_by(Plan.aum_billions.desc().nullslast(), Plan.name):
        pid = plan.id
        n_months = len(months_seen[pid])
        cadence = ("monthly" if n_months >= 8 else "quarterly" if n_months >= 3
                   else "sparse" if n_months else "none")
        last = last_month.get(pid)
        since = now - _month_index(last) if last else None
        if not docs_12m[pid]:
            status = "structural"
            reason = blocked.get(pid) or (f"Nothing since {last}" if last
                                          else "No board documents ever")
        elif since > ALLOWANCE[cadence]:
            status = "overdue"
            reason = (f"{cadence.capitalize()} board, nothing for {since} months "
                      f"(allowed {ALLOWANCE[cadence]})")
        else:
            status, reason = "ok", ""
        rows.append({"plan_id": pid, "name": plan.name, "aum": plan.aum_billions,
                     "state": plan.state, "docs_12m": docs_12m[pid], "docs_90d": docs_90d[pid],
                     "unsummarised": unsummarised[pid], "cadence": cadence, "last_month": last,
                     "months_since": since, "status": status, "reason": reason,
                     "latest_cafr_fy": cafr.get(pid), "has_ips": pid in ips,
                     "recordings_12m": recordings[pid], "fund_commitments": commitments.get(pid, 0)})
    return rows


def balance(rows: list[dict], top: int = 10) -> dict:
    """How evenly board material is spread: the top plans' share of the last
    twelve months, and the share of tracked AUM with any material."""
    vols = sorted((r["docs_12m"] for r in rows), reverse=True)
    total_docs = sum(vols)
    total_aum = sum(r["aum"] or 0 for r in rows)

    def aum_share(field):
        return (sum(r["aum"] or 0 for r in rows if r[field]) / total_aum) if total_aum else 0.0

    return {"top": top, "top_share": (sum(vols[:top]) / total_docs) if total_docs else 0.0,
            "aum_share_12m": aum_share("docs_12m"), "aum_share_90d": aum_share("docs_90d"),
            "plans_12m": sum(1 for r in rows if r["docs_12m"]), "plans_total": len(rows),
            "docs_12m": total_docs}


def report(session, today: date | None = None, weeks: int = 26, months: int = 24) -> dict:
    """Everything the Coverage tab draws, from one read of the documents."""
    today = today or date.today()
    docs = _board_docs(session)
    weekly = weekly_intake(session, weeks, today, docs)
    status = plan_status(session, today, docs)
    return {"today": today, "weekly": weekly, "gaps": pipeline_gaps(weekly),
            "status": status, "months": plan_months(session, months, today, docs),
            "balance": balance(status)}
