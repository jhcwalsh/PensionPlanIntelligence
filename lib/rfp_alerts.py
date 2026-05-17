"""RFP / consultant alert detection and Haiku polish layer.

Used by both the Streamlit app (Insights → RFP Alerts tab) and the
daily-digest email (scripts/send_daily_digest.py) so they share the
same regex pre-filter, Haiku polish prompt, and headline tl;dr.

Public API:
    find_alerts(session, hours)     -> list[dict]   # regex candidates
    polish_alerts(raw, today, cutoff) -> (list[dict], headline_str)

Both stages honor LLM_MODE=mock / INSIGHTS_MODE=mock for tests by
returning the raw context unchanged + a deterministic headline.
"""
from __future__ import annotations

import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

from database import Document, Plan

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Regex pre-filter
# ---------------------------------------------------------------------------

_RFP_PATTERN = re.compile(
    r"\b(RFPs?|RFQs?|Requests? for Proposals?|Requests? for Qualifications?)\b",
    re.IGNORECASE,
)
_CONSULTANT_PATTERN = re.compile(r"\bconsultants?\b", re.IGNORECASE)
_CONSULTANT_ACTIONS = frozenset([
    "search", "searches", "searching",
    "select", "selects", "selection", "selecting", "selected",
    "rebid", "rebidding",
    "procurement", "procure", "procuring",
    "shortlist", "shortlisted",
    "award", "awards", "awarded", "awarding",
    "incumbent",
    "replace", "replaces", "replacing", "replaced",
    "renew", "renews", "renewing", "renewed", "renewal",
    "expire", "expires", "expiring", "expired", "expiration",
    "interview", "interviews", "interviewing",
    "finalist", "finalists",
    "issued", "issuing", "issue",
    "rfp", "rfps", "rfq", "rfqs",
    "proposal", "proposals",
    "responses", "respondents", "respondent",
])
_CONSULTANT_CONTEXT_WINDOW = 8  # words on either side of the consultant token


def _find_consultant_with_context(text: str):
    """First 'consultant' mention paired with an RFP-context action word
    within ±``_CONSULTANT_CONTEXT_WINDOW`` words. Returns the re.Match or None.
    """
    word_matches = list(re.finditer(r"\b\w+\b", text))
    words_lower = [m.group(0).lower() for m in word_matches]
    for i, w in enumerate(words_lower):
        if w not in {"consultant", "consultants"}:
            continue
        lo = max(0, i - _CONSULTANT_CONTEXT_WINDOW)
        hi = min(len(words_lower), i + _CONSULTANT_CONTEXT_WINDOW + 1)
        if any(words_lower[j] in _CONSULTANT_ACTIONS
               for j in range(lo, hi) if j != i):
            return word_matches[i]
    return None


def _window_around(text: str, start: int, total_words: int) -> str:
    """Return ~``total_words`` of text centered on the char position ``start``."""
    half = total_words // 2
    pre_words = text[: start].split()
    post_words = text[start:].split()
    pre = pre_words[-half:] if len(pre_words) > half else pre_words
    post_budget = total_words - len(pre)
    post = post_words[:post_budget]
    snippet = " ".join(pre + post).strip()
    snippet = re.sub(r"\s+", " ", snippet)
    prefix = "… " if len(pre_words) > len(pre) else ""
    suffix = " …" if len(post_words) > len(post) else ""
    return f"{prefix}{snippet}{suffix}"


def extract_rfp_snippet(text: str, max_words: int = 25) -> tuple[str, str, str] | None:
    """Find the first qualifying RFP/consultant mention.

    Returns (keyword, ~25-word snippet, ~150-word polish-context) or None.
    """
    if not text:
        return None
    rfp_match = _RFP_PATTERN.search(text)
    consultant_match = _find_consultant_with_context(text)
    if rfp_match and consultant_match:
        match = rfp_match if rfp_match.start() <= consultant_match.start() else consultant_match
    else:
        match = rfp_match or consultant_match
    if match is None:
        return None
    return (
        match.group(0),
        _window_around(text, match.start(), max_words),
        _window_around(text, match.start(), 150),
    )


def find_alerts(session, hours: int = 24) -> list[dict]:
    """Documents fetched in the last ``hours`` that mention RFPs or consultants.

    One alert per document (the first match). Pulls only documents whose
    ``extraction_status='done'`` so empty-text rows aren't scanned.
    """
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    rows = (
        session.query(Document, Plan)
        .join(Plan, Plan.id == Document.plan_id)
        .filter(Document.downloaded_at >= cutoff)
        .filter(Document.extraction_status == "done")
        .order_by(Document.downloaded_at.desc())
        .all()
    )
    alerts: list[dict] = []
    for doc, plan in rows:
        result = extract_rfp_snippet(doc.extracted_text or "")
        if result is None:
            continue
        keyword, snippet, polish_context = result
        alerts.append({
            "doc_id": doc.id,
            "plan_id": plan.id,
            "plan_name": plan.name or plan.id,
            "plan_abbrev": plan.abbreviation or plan.id,
            "filename": doc.filename or f"Document {doc.id}",
            "doc_type": doc.doc_type or "",
            "downloaded_at": doc.downloaded_at,
            "meeting_date": doc.meeting_date,
            "keyword": keyword,
            "snippet": snippet,
            "polish_context": polish_context,
        })
    return alerts


# ---------------------------------------------------------------------------
# Haiku polish layer
# ---------------------------------------------------------------------------

_ALERT_POLISH_SYSTEM = """\
You are an analyst tracking RFP and consultant procurement events at U.S. \
public pension plans.

Given a context window from a board document, write a single-sentence summary \
(~25 words) that captures:
- WHO is procuring (plan name)
- WHAT they're procuring (asset class, service, mandate type)
- STATUS (planning, issued, responses received, awarded, expiring, renewing)
- A SPECIFIC DATE anchoring when the action happens — release date, response \
due date, award date, contract expiration, or board-action / meeting date
- Any named vendors or dollar figures that appear

Rules:
- Use ONLY information present in the context. Do not infer.
- The summary MUST include at least one specific date. Year alone is fine \
when only year is available; prefer month/day when present. If the context \
has no date at all, fall back to the source document's meeting date \
(provided in the user message). If still no date, append " (date not \
specified)" so the reader knows.
- HISTORICAL FILTER: the user message includes today's date and a one-month \
cutoff. If the only dates referenced in the context are before that cutoff \
AND the action is described as completed historical background (e.g. "in \
2023, we awarded …"), respond with EXACTLY: NOT_RFP. Keep the alert if any \
mentioned date is on/after the cutoff, or if the action is described as \
ongoing, upcoming, or under board review now.
- INCIDENTAL FILTER: if the consultant/RFP mention is incidental (routine \
consultant attendance, a CAFR or financial-statement line item, a listing \
of professional consultants without procurement action, a generic agenda \
item like "consultant due diligence education session", a code-of-conduct \
or governance boilerplate), respond with EXACTLY: NOT_RFP.
- No preamble, no quotes, no markdown — just the one-sentence summary or NOT_RFP.
"""

_ALERT_HEADLINE_SYSTEM = """\
You are summarizing today's RFP / consultant alert feed for a busy CIO.

Given the list of alerts below, write a single sentence (max 30 words) that \
captures: the most consequential events (new RFPs issued, awards announced, \
incumbent transitions, deadlines), and any pattern (e.g. multiple consultant \
searches in one asset class).

Rules:
- Be concrete: name plans and stages.
- Use ONLY information in the alerts. No preamble, no quotes, no markdown.
- If only one alert, summarize that one item.
"""


def _llm_mock() -> bool:
    return os.environ.get("LLM_MODE", "").lower() == "mock" or \
           os.environ.get("INSIGHTS_MODE", "").lower() == "mock"


def polish_alert_snippet(plan_abbrev: str, plan_name: str, filename: str,
                          keyword: str, polish_context: str,
                          meeting_date_str: str, today_iso: str,
                          cutoff_iso: str) -> str | None:
    """Send the wide context to Haiku for a clean ~25-word summary.

    Returns the polished string, or None if Haiku flagged the match as
    historical / incidental. Falls back to None on any error so the
    caller can drop the alert rather than show a half-broken card.
    """
    if _llm_mock():
        return polish_context  # tests / offline dev — bypass Haiku

    try:
        from summarizer import MODEL_HAIKU, _get_client
        meeting_line = (
            f"Source document meeting date: {meeting_date_str}\n"
            if meeting_date_str else ""
        )
        msg = _get_client().messages.create(
            model=MODEL_HAIKU,
            max_tokens=160,
            temperature=0.1,
            system=_ALERT_POLISH_SYSTEM,
            messages=[{
                "role": "user",
                "content": (
                    f"Today: {today_iso}\n"
                    f"One-month cutoff (drop if all referenced dates are "
                    f"before this AND no current/upcoming action): {cutoff_iso}\n"
                    f"Plan: {plan_abbrev} ({plan_name})\n"
                    f"Source document filename: {filename}\n"
                    f"{meeting_line}"
                    f"Matched keyword: {keyword}\n"
                    f"Context (~150 words):\n{polish_context}"
                ),
            }],
        )
        if not msg.content:
            return None
        text = msg.content[0].text.strip()
        if text.startswith("NOT_RFP"):
            return None
        return text
    except Exception:
        logger.exception("polish_alert_snippet failed for %s/%s", plan_abbrev, filename)
        return None


def build_alert_headline(polished: list[dict]) -> str:
    """One-sentence (~30 word) tl;dr across the polished alert list."""
    n = len(polished)
    if n == 0:
        return ""
    if _llm_mock():
        plans = sorted({a["plan_abbrev"] for a in polished})
        return f"{n} RFP / consultant event(s) across {len(plans)} plan(s): {', '.join(plans[:6])}."

    try:
        from summarizer import MODEL_HAIKU, _get_client
        body = "\n".join(
            f"- {a['plan_abbrev']}: {a['snippet']}" for a in polished
        )
        msg = _get_client().messages.create(
            model=MODEL_HAIKU,
            max_tokens=160,
            temperature=0.1,
            system=_ALERT_HEADLINE_SYSTEM,
            messages=[{"role": "user", "content": f"Alerts:\n{body}"}],
        )
        if not msg.content:
            return f"{n} RFP / consultant event(s) in the window."
        return msg.content[0].text.strip()
    except Exception:
        logger.exception("build_alert_headline failed")
        return f"{n} RFP / consultant event(s) in the window."


def polish_alerts(raw_alerts: list[dict],
                   today_iso: str | None = None,
                   cutoff_iso: str | None = None
                   ) -> tuple[list[dict], str]:
    """Run Haiku polish + headline pass over ``raw_alerts``.

    NOT_RFP responses are dropped. Returns (polished_alerts, headline).
    Defaults today_iso/cutoff_iso from datetime.utcnow() with a 30-day
    cutoff when not provided.
    """
    if not raw_alerts:
        return [], ""
    if today_iso is None:
        today_iso = datetime.utcnow().date().isoformat()
    if cutoff_iso is None:
        cutoff_iso = (datetime.utcnow().date() - timedelta(days=30)).isoformat()

    def _meeting_date_str(a):
        if a.get("meeting_date_str"):
            return a["meeting_date_str"]
        md = a.get("meeting_date")
        if md is None:
            return ""
        return md.date().isoformat() if hasattr(md, "date") else str(md)

    payload = [
        (a["doc_id"], a["plan_abbrev"], a["plan_name"], a["filename"],
         a["keyword"], a["polish_context"], _meeting_date_str(a))
        for a in raw_alerts
    ]

    def polish_one(row):
        (doc_id, plan_abbrev, plan_name, filename, keyword,
         polish_context, meeting_date_str) = row
        polished = polish_alert_snippet(
            plan_abbrev, plan_name, filename, keyword, polish_context,
            meeting_date_str, today_iso, cutoff_iso,
        )
        return doc_id, polished

    with ThreadPoolExecutor(max_workers=6) as pool:
        polished_map = dict(pool.map(polish_one, payload))

    raw_by_id = {a["doc_id"]: a for a in raw_alerts}
    out: list[dict] = []
    for row in payload:
        doc_id = row[0]
        polished = polished_map.get(doc_id)
        if polished is None:
            continue
        a = raw_by_id[doc_id]
        out.append({
            "doc_id": a["doc_id"],
            "plan_id": a["plan_id"],
            "plan_abbrev": a["plan_abbrev"],
            "plan_name": a["plan_name"],
            "filename": a["filename"],
            "doc_type": a.get("doc_type", ""),
            "downloaded_at": a.get("downloaded_at"),
            "keyword": a["keyword"],
            "snippet": polished,
        })

    headline = build_alert_headline(out)
    return out, headline
