"""Weekly Watch: Albourne mentions, consultant RFPs, senior hires and departures.

A private cadence. It goes to the approval recipient only — never to
subscribers, never to ``notes/``, never to the public Weekly tab (which
lists the ``weekly`` cadence by name). Runs Sundays from GitHub Actions,
after the silent weekly compose.

Two stages, so the model reads only what matters:

1. A deterministic scan over every document downloaded in the last seven
   days — the full extracted text plus the summary fields — with three
   matchers. Each hit yields a snippet of a few hundred characters,
   capped per document.
2. One Sonnet call over the snippets, grouped by plan, writing the
   newsletter in three sections. It is told to drop false positives and
   to say "Nothing this week" for an empty section.

A week with no hits at all still sends, without calling the model, so
silence never means broken.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional

from sqlalchemy.orm import Session, undefer

from database import Document, Plan, Publication, Summary, as_utc, get_session, utcnow
from insights import config, cycle_common

logger = logging.getLogger(__name__)

CADENCE = "watch"
WINDOW_DAYS = 7
SNIPPET_CHARS = 300          # each side of the match
MAX_HITS_PER_DOC = 6
PROXIMITY = 200              # chars between the two halves of a paired matcher

_ALBOURNE = re.compile(r"\balbourne\b", re.IGNORECASE)

# Consultant RFPs. "advisor" is left out on purpose: "consult your tax
# advisor" boilerplate sits in every custodian statement, and "search" /
# "selection" alone are manager-search language. A consultant search is
# caught by the explicit phrase instead.
_CONSULTANT = re.compile(r"\b(?:consultants?|consulting)\b", re.IGNORECASE)
_RFP = re.compile(
    r"\bRFPs?\b|request for proposals?|\bRFI\b|\bRFQ\b|solicitation|procurement",
    re.IGNORECASE)
_CONSULTANT_SEARCH = re.compile(
    r"consultant search|search for (?:an? |the )?(?:\w+ ){0,3}consultant",
    re.IGNORECASE)

# Senior moves. Chief-level and executive-director titles only: "managing
# director" is every manager bio in every consultant report. The verbs are
# the ones that describe a move, not an attendance list — "named" and
# "appointed" alone match named fiduciaries and appointed trustees on
# every roll call, so they need "as" or an office after them.
_SENIOR_TITLE = re.compile(
    r"\b(?:chief investment officer|CIO|deputy chief investment officer|deputy CIO"
    r"|executive director|chief executive officer|CEO|general counsel"
    r"|chief operating officer|COO|chief financial officer|CFO)\b",
    re.IGNORECASE)
_MOVE = re.compile(
    r"\b(?:hired|hiring|hire of|appointed as|appointment of|appointment as"
    # "retirement" only as someone's: bare, it is "Retirement System" on
    # every page of every packet.
    r"|named as|resign(?:ed|ation|s)?|retir(?:e|es|ing)|retired(?! (?:member|state|employee|annuitant|teacher))|(?:his|her|their|announced) retirement"
    r"|departure|departing"
    r"|step(?:ping|ped|s)? down|interim|vacan(?:cy|t)|succession|search)\b",
    re.IGNORECASE)
SENIOR_PROXIMITY = 120

CATEGORY_LABELS = {
    "albourne": "Albourne",
    "consultant_rfp": "Consultant RFPs and searches",
    "senior_move": "Senior hires and departures",
}


@dataclass
class Hit:
    category: str
    doc: Document
    plan_name: str
    snippet: str


@dataclass
class _Span:
    category: str
    start: int
    end: int


def _paired(text: str, anchor: re.Pattern, partner: re.Pattern,
            category: str, proximity: int = PROXIMITY) -> list[_Span]:
    """Anchor matches with a partner match within ``proximity`` chars."""
    spans = []
    for m in anchor.finditer(text):
        lo = max(0, m.start() - proximity)
        hi = min(len(text), m.end() + proximity)
        if partner.search(text, lo, hi):
            spans.append(_Span(category, m.start(), m.end()))
    return spans


def _snippet(text: str, start: int, end: int) -> str:
    lo = max(0, start - SNIPPET_CHARS)
    hi = min(len(text), end + SNIPPET_CHARS)
    out = " ".join(text[lo:hi].split())
    return ("…" if lo > 0 else "") + out + ("…" if hi < len(text) else "")


def find_hits(text: str, doc: Optional[Document] = None,
              plan_name: str = "") -> list[Hit]:
    """Every match in ``text``, in document order, one Hit per match."""
    spans = [_Span("albourne", m.start(), m.end()) for m in _ALBOURNE.finditer(text)]
    spans += _paired(text, _CONSULTANT, _RFP, "consultant_rfp")
    spans += [_Span("consultant_rfp", m.start(), m.end())
              for m in _CONSULTANT_SEARCH.finditer(text)]
    spans += _paired(text, _SENIOR_TITLE, _MOVE, "senior_move", SENIOR_PROXIMITY)
    spans.sort(key=lambda s: (s.start, s.category))

    hits: list[Hit] = []
    last_end: dict[str, int] = {}
    for s in spans:
        # Two matches of one category inside the same snippet window say
        # the same thing twice.
        if s.start < last_end.get(s.category, -1):
            continue
        last_end[s.category] = s.end + SNIPPET_CHARS
        hits.append(Hit(s.category, doc, plan_name, _snippet(text, s.start, s.end)))
    return hits


def scan_document(doc: Document, summary: Optional[Summary],
                  plan_name: str = "") -> list[Hit]:
    """Hits across the document's text and its summary, capped."""
    parts = [doc.extracted_text or ""]
    if summary is not None:
        parts += [summary.summary_text or "", summary.investment_actions or "",
                  summary.decisions or "", summary.key_topics or ""]
    hits = find_hits("\n\n".join(p for p in parts if p), doc, plan_name)
    return hits[:MAX_HITS_PER_DOC]


def select_window_docs(session: Session, *, now_utc: datetime) -> list[Document]:
    """Documents downloaded in the last WINDOW_DAYS, text loaded in one query."""
    cutoff = now_utc - timedelta(days=WINDOW_DAYS)
    return (
        session.query(Document)
        .options(undefer(Document.extracted_text))
        .filter(Document.downloaded_at.isnot(None))
        .filter(Document.downloaded_at > cutoff)
        .filter(Document.downloaded_at < now_utc)
        .order_by(Document.plan_id.asc(), Document.meeting_date.desc().nullslast())
        .all()
    )


def scan_window(session: Session, *, now_utc: datetime) -> tuple[list[Hit], int, int]:
    """(hits, documents scanned, plans scanned) for the current window."""
    docs = select_window_docs(session, now_utc=now_utc)
    if not docs:
        return [], 0, 0
    plan_names = {p.id: p.name for p in session.query(Plan).all()}
    summaries = {
        s.document_id: s for s in
        session.query(Summary).filter(Summary.document_id.in_([d.id for d in docs]))
    }
    hits: list[Hit] = []
    for doc in docs:
        hits += scan_document(doc, summaries.get(doc.id),
                              plan_names.get(doc.plan_id, doc.plan_id))
    return hits, len(docs), len({d.plan_id for d in docs})


# ---------------------------------------------------------------------------
# Compose
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
You write a short private weekly watch note for one reader, an investment \
professional tracking U.S. public pension plans. You are given snippets that \
a keyword scan pulled from board documents published this week, grouped by \
plan. Each snippet is tagged with the category the scan matched and carries \
the document's URL.

Write clean markdown with exactly these three H2 sections, in this order:

## Albourne
## Consultant RFPs and searches
## Senior hires and departures

Under each section, one bullet per distinct item: **Plan name** — what the \
document says, in one or two plain sentences, then the meeting date if \
known and a markdown link to the source document. Merge snippets that \
describe the same item. Report only what the snippet supports; never infer \
outcomes, motives or timing that are not stated.

Drop false positives. A "search" for an investment manager is not a \
consultant search. A consultant *presenting* is not an RFP. A CIO \
*presenting* a report is not a hire or departure.

Consultant RFPs and searches means investment, actuarial, custodian or \
similar advisory consultants engaged by the plan. RFPs for legal counsel, \
auditors, software or other vendors do not belong in that section.

Senior hires and departures means the plan's own senior staff and trustees: \
CIO, deputy CIO, executive director, CEO, general counsel, COO, CFO, board \
chair. Personnel changes at external investment managers, consultants or \
their parent firms do not belong, however senior; nor do farewells to \
support staff.

If a category has nothing real in it, write exactly: Nothing this week.

Start with the H1 given to you. No preamble, no closing remarks, no code \
fences."""


def _hits_block(hits: list[Hit]) -> str:
    by_plan: dict[str, list[Hit]] = {}
    for h in hits:
        by_plan.setdefault(h.plan_name or h.doc.plan_id, []).append(h)
    lines = []
    for plan_name in sorted(by_plan):
        lines.append(f"## {plan_name}")
        for h in by_plan[plan_name]:
            when = h.doc.meeting_date.date().isoformat() if h.doc.meeting_date else "date unknown"
            lines.append(f"- [{h.category}] {h.doc.filename or 'document'} — {when} — {h.doc.url}")
            lines.append(f"  {h.snippet}")
        lines.append("")
    return "\n".join(lines)


def _title(period_start: date, period_end: date) -> str:
    return f"# Weekly Watch — {period_start.isoformat()} to {period_end.isoformat()}"


def compose_watch(hits: list[Hit], *, docs_scanned: int, plans_scanned: int,
                  period_start: date, period_end: date) -> str:
    title = _title(period_start, period_end)
    footer = (f"\n\n---\n*Scanned {docs_scanned} documents from {plans_scanned} plans "
              f"downloaded {period_start.isoformat()} to {period_end.isoformat()}.*\n")

    if not hits:
        sections = "\n\n".join(f"## {label}\n\nNothing this week."
                               for label in CATEGORY_LABELS.values())
        return (f"{title}\n\nNothing matched this week.\n\n{sections}{footer}")

    if config.is_mock():
        sections = "\n\n".join(
            f"## {label}\n\n- **Mock** — {sum(1 for h in hits if h.category == cat)} "
            f"snippet(s); INSIGHTS_MODE=mock is set."
            for cat, label in CATEGORY_LABELS.items())
        return f"{title}\n\n{sections}{footer}"

    from summarizer import MODEL_SONNET, _get_client, message_text

    user = (f"Use this H1 verbatim as the first line:\n{title}\n\n"
            f"SNIPPETS BY PLAN:\n\n{_hits_block(hits)}")
    message = _get_client().messages.create(
        model=MODEL_SONNET,
        max_tokens=6000,
        # Restating snippets it has in front of it; thinking adds nothing.
        thinking={"type": "disabled"},
        system=_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user}],
    )
    body = message_text(message).strip()
    if not body.startswith("# "):
        body = f"{title}\n\n{body}"
    return body + footer


# ---------------------------------------------------------------------------
# Cycle
# ---------------------------------------------------------------------------

def run_watch_cycle(*, now: Optional[datetime] = None, force: bool = False) -> Publication:
    """One weekly watch: scan the window, compose, email. Idempotent per period."""
    now_utc = as_utc(now) if now is not None else utcnow()
    period_end = now_utc.date()
    period_start = period_end - timedelta(days=WINDOW_DAYS)

    session = get_session()
    publication: Optional[Publication] = None
    try:
        publication = cycle_common.find_or_create_publication(
            session, cadence=CADENCE, period_start=period_start, period_end=period_end)

        if force and publication.status in ("awaiting_approval", "approved", "published"):
            cycle_common.transition_status(publication, "expired")
            session.flush()
            publication = cycle_common.find_or_create_publication(
                session, cadence=CADENCE, period_start=period_start, period_end=period_end)
            publication.status = "generating"
            publication.draft_markdown = None
            publication.composed_at = None
            publication.pdf_path = None
            session.flush()

        if publication.status != "generating":
            logger.info("Watch publication %s already at status '%s' — skipping.",
                        publication.id, publication.status)
            return cycle_common.detach_for_caller(session, publication)

        hits, docs_scanned, plans_scanned = scan_window(session, now_utc=now_utc)
        draft = compose_watch(hits, docs_scanned=docs_scanned, plans_scanned=plans_scanned,
                              period_start=period_start, period_end=period_end)

        # notify=True, archive=False: emailed to the approval recipient, no
        # notes/ file. Subscribers never see it — CADENCES does not include it.
        cycle_common.finalize_and_send(
            session, publication, draft,
            title_for_pdf=f"Weekly Watch: {period_start.isoformat()} – {period_end.isoformat()}",
            notify=True, archive=False)
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
