"""
Streamlit UI — search and browse pension plan meeting documents and summaries.

Run with: streamlit run app.py
"""

import io
import json
import os
import re
import textwrap
from datetime import datetime, timedelta
from pathlib import Path

import streamlit as st
from dotenv import load_dotenv

from database import (
    ApprovalToken,
    CafrAllocation,
    CafrExtract,
    CafrPerformance,
    CafrRefreshLog,
    Document,
    DocumentHealth,
    DocumentSkip,
    FetchRun,
    IpsDocument,
    PipelineRun,
    Plan,
    Publication,
    RFPRecord,
    Summary,
    WeeklyRun,
    aggregate_managers,
    count_search_summaries,
    get_new_meetings,
    get_session,
    init_db,
    search_summaries,
)

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Pension Plan Intelligence",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .summary-card {
        background: #f8f9fa;
        border-left: 4px solid #0066cc;
        padding: 1rem;
        margin-bottom: 1rem;
        border-radius: 4px;
    }
    .tag {
        display: inline-block;
        background: #e3f2fd;
        color: #1565c0;
        padding: 2px 8px;
        border-radius: 12px;
        font-size: 0.8em;
        margin: 2px;
    }
    .action-tag {
        background: #fce4ec;
        color: #880e4f;
    }
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------
#
# Privacy-friendly Plausible tracker. st.html renders inline (not in an
# iframe), so the script runs in the host page context and Plausible sees
# the real app URL and query params (?doc=N, ?cafr_plan=…). The dedupe
# flag prevents Streamlit reruns from stacking duplicate <script> tags.

st.html(
    """
    <script>
    if (!window.__plausible_injected__) {
        window.__plausible_injected__ = true;
        const tracker = document.createElement('script');
        tracker.async = true;
        tracker.src = 'https://plausible.io/js/pa-v_mYvog2AxtbRj85Cu_EP.js';
        document.head.appendChild(tracker);
        window.plausible = window.plausible || function() {
            (plausible.q = plausible.q || []).push(arguments);
        };
        plausible.init = plausible.init || function(i) { plausible.o = i || {}; };
        plausible.init();
    }
    </script>
    """,
    unsafe_allow_javascript=True,
)


# ---------------------------------------------------------------------------
# Session / DB helpers
# ---------------------------------------------------------------------------

@st.cache_resource
def get_db_session():
    init_db()
    return get_session()


def load_plans():
    session = get_db_session()
    return session.query(Plan).order_by(Plan.name).all()


def load_recent_summaries(plan_id=None, limit=20):
    session = get_db_session()
    q = (
        session.query(Document, Summary)
        .join(Summary, Document.id == Summary.document_id)
    )
    if plan_id and plan_id != "All":
        q = q.filter(Document.plan_id == plan_id)
    return q.order_by(Document.meeting_date.desc()).limit(limit).all()


def get_stats():
    session = get_db_session()
    plans = session.query(Plan).count()
    docs = session.query(Document).count()
    summarized = session.query(Summary).count()
    downloaded = session.query(Document).filter(
        Document.extraction_status == "done").count()
    return plans, docs, downloaded, summarized


def parse_json_field(val):
    if not val:
        return []
    try:
        return json.loads(val) if isinstance(val, str) else val
    except Exception:
        return []


def _safe_md(text: str) -> str:
    """Escape $ so Streamlit doesn't treat them as LaTeX delimiters."""
    return text.replace("$", r"\$")


def _highlight(text: str, query: str | None) -> str:
    """Wrap case-insensitive matches of ``query`` in <mark> tags.

    Preserves the original casing of the matched substring. Caller must
    render with ``unsafe_allow_html=True`` for the tags to take effect.
    Safe against regex-special characters in the query via re.escape.
    """
    if not query or not text:
        return text
    pattern = re.compile(re.escape(query), re.IGNORECASE)
    return pattern.sub(lambda m: f"<mark>{m.group(0)}</mark>", text)


DOWNLOADS_DIR = Path(os.environ.get("DOWNLOADS_DIR") or (Path(__file__).parent / "downloads"))


def _retrieve_source_file(url: str, plan_id: str, filename: str) -> tuple[Path | None, int, str]:
    """Lazily fetch a source document from its original URL and cache it on disk.

    Returns (path, size_bytes, error_message). On success, error_message is "".
    """
    import requests

    dest_dir = DOWNLOADS_DIR / plan_id
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / filename

    if dest.exists():
        return dest, dest.stat().st_size, ""

    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; PensionPlanIntelligence/1.0)"}
        resp = requests.get(url, headers=headers, timeout=60, stream=True)
        resp.raise_for_status()

        cd = resp.headers.get("Content-Disposition", "")
        cd_match = re.search(r'filename="?([^";\n]+)"?', cd)
        if cd_match:
            dest = dest_dir / cd_match.group(1).strip()

        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        return dest, dest.stat().st_size, ""
    except Exception as exc:
        return None, 0, str(exc)


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

def render_sidebar():
    st.sidebar.title("🏛️ Pension Intelligence")
    st.sidebar.markdown("---")

    plans = load_plans()
    plan_options = ["All"] + [p.abbreviation or p.name for p in plans]
    plan_map = {"All": None}
    plan_map.update({(p.abbreviation or p.name): p.id for p in plans})

    selected_label = st.sidebar.selectbox("Filter by Plan", plan_options)
    selected_plan_id = plan_map.get(selected_label)

    st.sidebar.markdown("---")
    plans_count, docs_count, extracted_count, summarized_count = get_stats()
    st.sidebar.metric("Plans tracked", plans_count)
    st.sidebar.metric("Documents", docs_count)
    st.sidebar.metric("Extracted", extracted_count)
    st.sidebar.metric("Summarized", summarized_count)

    return selected_plan_id, selected_label


# ---------------------------------------------------------------------------
# Result card rendering
# ---------------------------------------------------------------------------

def render_summary_card(doc: Document, summary: Summary, highlight: str = None):
    plan_name = doc.plan_id.upper()
    date_str = doc.meeting_date.strftime("%b %d, %Y") if doc.meeting_date else "Date unknown"
    doc_type = (doc.doc_type or "document").replace("_", " ").title()

    key_topics = parse_json_field(summary.key_topics)
    investment_actions = parse_json_field(summary.investment_actions)
    decisions = parse_json_field(summary.decisions)
    performance = parse_json_field(summary.performance_data)

    with st.expander(f"**{plan_name}** — {doc_type} — {date_str}", expanded=False):
        summary_md = _highlight(_safe_md(summary.summary_text or ""), highlight)
        st.markdown(f"**Summary**\n\n{summary_md}", unsafe_allow_html=True)

        if key_topics:
            tags_html = " ".join(
                f'<span class="tag">{_highlight(t, highlight)}</span>'
                for t in key_topics[:8]
            )
            st.markdown(f"**Topics:** {tags_html}", unsafe_allow_html=True)

        col1, col2 = st.columns(2)

        with col1:
            if investment_actions:
                st.markdown("**Investment Actions**")
                for action in investment_actions[:5]:
                    desc = action.get("description", "")
                    amt = action.get("amount_millions")
                    amt_str = f" (${amt:,.0f}M)" if amt else ""
                    ac = action.get("asset_class", "")
                    line = (
                        f"- {_highlight(_safe_md(desc), highlight)}{amt_str}"
                        + (f" — *{_highlight(ac, highlight)}*" if ac else "")
                    )
                    st.markdown(line, unsafe_allow_html=True)

            if decisions:
                st.markdown("**Decisions**")
                for d in decisions[:5]:
                    vote = d.get("vote", "")
                    vote_str = f" [{vote}]" if vote else ""
                    desc = d.get("description", "")
                    st.markdown(
                        f"- {_highlight(_safe_md(desc), highlight)}{vote_str}",
                        unsafe_allow_html=True,
                    )

        with col2:
            if performance:
                st.markdown("**Performance Data**")
                for p in performance[:5]:
                    ret = p.get("return_pct")
                    bench = p.get("benchmark_pct")
                    period = p.get("period", "")
                    ac = p.get("asset_class", "")
                    if ret is not None:
                        vs = f" vs {bench:.1f}% benchmark" if bench is not None else ""
                        st.markdown(f"- {ac} ({period}): **{ret:.1f}%**{vs}")

        st.caption(f"Source: [{doc.url}]({doc.url}) | "
                   f"Summarized: {summary.generated_at.strftime('%Y-%m-%d') if summary.generated_at else 'unknown'}")


# ---------------------------------------------------------------------------
# Main pages
# ---------------------------------------------------------------------------

_SEARCH_PAGE_SIZE = 30


def page_search(plan_id, plan_label):
    st.title("Search Meeting Documents")

    # Form gate: search runs on Enter / Search button, not per-keystroke.
    # Each ILIKE scan is 100–1100 ms on this corpus; debouncing via the form
    # avoids running it on every typed character.
    with st.form("search_form", clear_on_submit=False):
        query = st.text_input(
            "Search summaries, topics, investment actions...",
            placeholder='e.g. "infrastructure" or "private equity mandate" or "BlackRock"',
        )
        st.form_submit_button("Search")

    if not query:
        st.info("Enter a search term above and press Enter (or click Search).")
        return

    session = get_db_session()
    pid = plan_id if plan_id and plan_id != "All" else None

    # Per-query growing limit. The key includes plan scope so switching plans
    # resets the pagination state for the same query.
    limit_key = f"_search_limit::{query}::{pid or 'all'}"
    limit = st.session_state.setdefault(limit_key, _SEARCH_PAGE_SIZE)

    total = count_search_summaries(session, query, plan_id=pid)
    plan_suffix = f" in {plan_label}" if plan_label != "All" else ""

    if total == 0:
        st.info(f"No results for **{query}**{plan_suffix}. Try different search terms.")
        return

    results = search_summaries(session, query, plan_id=pid, limit=limit)
    st.caption(
        f"Showing **{len(results)}** of **{total:,}** results for "
        f"**{query}**{plan_suffix}"
    )

    for doc, summary in results:
        render_summary_card(doc, summary, highlight=query)

    if len(results) < total:
        remaining = total - len(results)
        next_batch = min(_SEARCH_PAGE_SIZE, remaining)
        if st.button(f"Show {next_batch} more ({remaining:,} remaining)"):
            st.session_state[limit_key] = limit + _SEARCH_PAGE_SIZE
            st.rerun()


def page_browse(plan_id, plan_label):
    st.title("Recent Meetings")

    limit = st.slider("Show last N documents", 5, 100, 20)
    results = load_recent_summaries(plan_id=plan_id if plan_label != "All" else None,
                                    limit=limit)

    if not results:
        st.warning("No summarized documents yet. Run the pipeline to fetch and process documents.")
        st.code("python pipeline.py calpers  # fetch CalPERS documents")
        return

    st.caption(f"Showing {len(results)} most recent documents"
               + (f" for {plan_label}" if plan_label != "All" else " across all plans"))

    for doc, summary in results:
        render_summary_card(doc, summary)


def page_plans():
    st.title("Tracked Plans")
    session = get_db_session()
    plans = session.query(Plan).order_by(Plan.name).all()

    if not plans:
        st.warning("No plans loaded yet. Run the pipeline to initialize.")
        return

    for plan in plans:
        doc_count = session.query(Document).filter_by(plan_id=plan.id).count()
        summary_count = (session.query(Summary)
                         .join(Document)
                         .filter(Document.plan_id == plan.id).count())

        with st.expander(f"**{plan.abbreviation}** — {plan.name} ({plan.state})"):
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("AUM", f"${plan.aum_billions:.0f}B" if plan.aum_billions else "—")
            col2.metric("Documents", doc_count)
            col3.metric("Summarized", summary_count)
            col4.metric("State", plan.state or "—")
            if plan.materials_url:
                st.markdown(f"Materials page: [{plan.materials_url}]({plan.materials_url})")


def _truncate_words(text: str, max_words: int) -> tuple[str, bool]:
    """Return (truncated_text, was_truncated)."""
    words = text.split()
    if len(words) <= max_words:
        return text, False
    return " ".join(words[:max_words]) + "…", True


def page_summary_updates(plan_id, plan_label):
    st.title("Summary of Updates")
    st.caption("One snapshot per plan — up to 100 words. Expand a plan for the full detail.")

    days = st.slider("Look back (days)", 1, 90, 14, key="summary_days")
    session = get_db_session()
    meetings = get_new_meetings(session, days=days)

    if plan_id:
        meetings = [m for m in meetings if m["plan"] and m["plan"].id == plan_id]

    if not meetings:
        st.info(f"No new meetings found in the last {days} days. Run the pipeline to fetch updates.")
        return

    # Group by plan — keep the most recent meeting per plan as the headline
    from collections import defaultdict
    by_plan: dict[str, list] = defaultdict(list)
    for m in meetings:
        pid = m["plan"].id if m["plan"] else "unknown"
        by_plan[pid].append(m)

    st.caption(f"**{len(by_plan)} plan(s)** with activity in the last {days} days"
               + (f" for {plan_label}" if plan_label != "All" else ""))

    for pid, plan_meetings in sorted(by_plan.items(),
                                     key=lambda kv: kv[1][0]["meeting_date"] or datetime.min,
                                     reverse=True):
        plan = plan_meetings[0]["plan"]
        plan_label_str = (plan.abbreviation or plan.name) if plan else pid.upper()
        latest_date = plan_meetings[0]["meeting_date"]
        date_str = latest_date.strftime("%B %d, %Y") if latest_date else "Date unknown"
        n_meetings = len(plan_meetings)

        # Find the best summary across all meetings for this plan
        headline_summary = None
        for m in plan_meetings:
            if m["agenda_summary"] and m["agenda_summary"].summary_text:
                headline_summary = m["agenda_summary"].summary_text
                break

        col1, col2 = st.columns([6, 1])
        with col1:
            st.markdown(f"### {plan_label_str} — {date_str}")
        with col2:
            st.caption(f"{n_meetings} meeting{'s' if n_meetings > 1 else ''}")

        if headline_summary:
            short, was_truncated = _truncate_words(headline_summary, 100)
            st.markdown(_safe_md(short))
        else:
            st.caption("No summary yet — run Summarize to process.")
            was_truncated = False

        if was_truncated or n_meetings > 1:
            with st.expander("Full details"):
                for m in plan_meetings:
                    m_date = m["meeting_date"].strftime("%B %d, %Y") if m["meeting_date"] else "Date unknown"
                    st.markdown(f"**{m_date}**")
                    if m["agenda_summary"]:
                        st.markdown(_safe_md(m["agenda_summary"].summary_text))
                    else:
                        st.caption("No summary available.")
                    st.markdown("**Materials:**")
                    for d in m["all_docs"]:
                        doc_type = (d.doc_type or "document").replace("_", " ").title()
                        st.markdown(f"- [{doc_type} — {d.filename}]({d.url})")
                    st.divider()
        else:
            # Still show materials even without truncation
            with st.expander("Materials"):
                for m in plan_meetings:
                    for d in m["all_docs"]:
                        doc_type = (d.doc_type or "document").replace("_", " ").title()
                        st.markdown(f"- [{doc_type} — {d.filename}]({d.url})")

        st.divider()


def page_updates(plan_id, plan_label):
    st.title("Meeting Updates")
    st.caption("New meetings detected since last pipeline run, with agenda summaries and links to materials.")

    days = st.slider("Look back (days)", 1, 90, 14)
    session = get_db_session()
    meetings = get_new_meetings(session, days=days)

    if plan_id:
        meetings = [m for m in meetings if m["plan"] and m["plan"].id == plan_id]

    if not meetings:
        st.info(f"No new meetings found in the last {days} days. Run the pipeline to fetch updates.")
        return

    st.caption(f"**{len(meetings)} new meeting(s)** in the last {days} days"
               + (f" for {plan_label}" if plan_label != "All" else ""))

    for m in meetings:
        plan = m["plan"]
        plan_label_str = (plan.abbreviation or plan.name) if plan else "Unknown"
        date_str = m["meeting_date"].strftime("%B %d, %Y") if m["meeting_date"] else "Date unknown"
        doc = m["agenda_doc"]
        summary = m["agenda_summary"]

        header = f"**{plan_label_str}** — {date_str}"
        with st.expander(header, expanded=True):
            if summary:
                st.markdown(_safe_md(summary.summary_text))
            elif doc:
                st.caption("No summary yet — run Summarize to process this document.")
            else:
                st.caption("No agenda document found for this meeting.")

            # Links to all materials for this meeting
            st.markdown("**Materials:**")
            for d in m["all_docs"]:
                doc_type = (d.doc_type or "document").replace("_", " ").title()
                st.markdown(f"- [{doc_type} — {d.filename}]({d.url})")


# ---------------------------------------------------------------------------
# PDF generation helper
# ---------------------------------------------------------------------------

NOTES_DIR = Path(__file__).parent / "notes"


def _find_all_highlights() -> list[tuple[Path, str, str]]:
    """Find all 7-day highlights files, sorted newest first by Generated date.

    Returns list of (path, title, generated_date) tuples. Sorting by the
    ``*Generated: ...*`` line rather than the filename matters because
    filenames embed ``period_start`` while the actual content window is
    set by ``compose_weekly``'s now()-7d gather, so a filename-sorted
    list can put a stale note ahead of the most recently generated one.
    """
    results = []
    for path in NOTES_DIR.glob("7day_highlights_*.md"):
        content = path.read_text(encoding="utf-8")
        # Extract title from first H1 line
        first_line = content.split("\n")[0] if content else ""
        title = first_line[2:].strip() if first_line.startswith("# ") else path.stem

        # Extract generated date from *Generated: ...* line
        gen_match = re.search(r"\*Generated:\s*(.+?)\*", content)
        if gen_match:
            generated_date = gen_match.group(1).strip()
        else:
            # Fall back to date in filename
            date_match = re.search(r"(\d{4}-\d{2}-\d{2})", path.name)
            if date_match:
                dt = datetime.strptime(date_match.group(1), "%Y-%m-%d")
                generated_date = dt.strftime("%B %d, %Y")
            else:
                generated_date = "Unknown"

        try:
            sort_key = datetime.strptime(generated_date, "%B %d, %Y")
        except ValueError:
            sort_key = datetime.min

        results.append((sort_key, path, title, generated_date))

    # Newest Generated date first; filename as deterministic tie-break.
    results.sort(key=lambda r: (r[0], r[1].name), reverse=True)
    return [(path, title, gen) for _, path, title, gen in results]


def _find_latest_insights() -> tuple[Path, str, str] | None:
    """Find the YTD CIO Insights note and extract its generated date."""
    path = NOTES_DIR / "2026_cio_insights.md"
    if not path.exists():
        return None
    content = path.read_text(encoding="utf-8")
    gen_match = re.search(r"\*Generated:\s*(.+?)\*", content)
    generated_date = gen_match.group(1).strip() if gen_match else "Unknown"
    return (path, "CIO Insights: 2026 Institutional Trends", generated_date)


def _find_latest_insights_recent() -> tuple[Path, str, str] | None:
    """Find the latest Monthly CIO Insights note for the Notes tab.

    Prefers the new approval-flow output (``monthly_cio_insights_<date>.md``
    written by ``insights.publish.publish``) sorted by period date.
    Falls back to the legacy rolling-window file (``cio_insights_*day.md``)
    if no approved monthly has been published yet.
    """
    monthly = sorted(
        NOTES_DIR.glob("monthly_cio_insights_*.md"),
        reverse=True,  # filenames embed YYYY-MM-DD; lexical sort = chronological
    )
    if monthly:
        path = monthly[0]
        content = path.read_text(encoding="utf-8")
        gen_match = re.search(r"\*Generated:\s*(.+?)\*", content)
        generated_date = gen_match.group(1).strip() if gen_match else "Unknown"
        m = re.match(r"monthly_cio_insights_(\d{4}-\d{2})", path.name)
        if m:
            month = datetime.strptime(m.group(1) + "-01", "%Y-%m-%d").strftime("%B %Y")
            title = f"Monthly CIO Insights: {month}"
        else:
            title = "Monthly CIO Insights"
        return (path, title, generated_date)

    legacy = sorted(
        NOTES_DIR.glob("cio_insights_*day.md"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not legacy:
        return None
    path = legacy[0]
    content = path.read_text(encoding="utf-8")
    gen_match = re.search(r"\*Generated:\s*(.+?)\*", content)
    generated_date = gen_match.group(1).strip() if gen_match else "Unknown"
    m = re.match(r"cio_insights_(\d+)day\.md", path.name)
    days = m.group(1) if m else "?"
    return (path, f"CIO Insights: Past {days} Days", generated_date)



from insights.render import markdown_to_pdf_bytes as _markdown_to_pdf_bytes


def _notes_md_to_html(content: str) -> str:
    """Convert notes markdown to HTML with inline styles, bypassing Streamlit's renderer."""
    def inline(text: str) -> str:
        # Links: [text](url) → <a>
        text = re.sub(
            r'\[([^\]]+)\]\(([^)]+)\)',
            r'<a href="\2" style="color:#4A90D9;text-decoration:underline;">\1</a>',
            text,
        )
        text = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', text)
        text = re.sub(r'\*(.+?)\*', r'<em>\1</em>', text)
        return text

    lines = content.splitlines()
    parts: list[str] = []
    para: list[str] = []

    def flush():
        if para:
            parts.append(
                f'<p style="margin:0 0 14px;line-height:1.65;">{inline(" ".join(para))}</p>'
            )
            para.clear()

    for line in lines:
        s = line.strip()
        if s.startswith("## "):
            flush()
            parts.append(
                f'<h2 style="margin:28px 0 8px;font-size:1.25em;font-weight:600;">'
                f'{inline(s[3:])}</h2>'
            )
        elif s == "---":
            flush()
            parts.append('<hr style="margin:16px 0;border:none;border-top:1px solid #555;">')
        elif s.startswith("# "):
            continue  # skip H1 — shown via st.title
        elif s.startswith("- ") or s.startswith("* "):
            flush()
            parts.append(
                f'<p style="margin:0 0 6px;line-height:1.65;padding-left:16px;">'
                f'&bull; {inline(s[2:])}</p>'
            )
        elif s == "":
            flush()
        else:
            para.append(s)

    flush()
    return "\n".join(parts)


def _render_note_page(md_path: Path, title: str, generated_date: str, pdf_filename: str):
    """Render a markdown note with a date stamp and PDF download button."""
    if not md_path.exists():
        st.warning(f"Note file not found: {md_path}")
        return

    content = md_path.read_text(encoding="utf-8")

    col1, col2 = st.columns([5, 1])
    with col1:
        st.caption(f"Generated: {generated_date}")
    with col2:
        pdf_bytes = _markdown_to_pdf_bytes(title, generated_date, content)
        st.download_button(
            "Download PDF",
            data=pdf_bytes,
            file_name=pdf_filename,
            mime="application/pdf",
            use_container_width=True,
        )

    st.divider()
    html = _notes_md_to_html(content)
    st.markdown(
        f'<div style="font-family:-apple-system,BlinkMacSystemFont,\'Segoe UI\','
        f'Arial,sans-serif;font-size:15px;color:inherit;">{html}</div>',
        unsafe_allow_html=True,
    )


def _find_latest_consultant_rfps() -> tuple[Path, str, str] | None:
    """Find the latest Monthly Consultant RFP brief.

    Picks the newest ``monthly_consultant_rfps_<YYYY-MM-DD>.md`` by
    filename (lexical sort = chronological since the date is embedded).
    """
    candidates = sorted(
        NOTES_DIR.glob("monthly_consultant_rfps_*.md"),
        reverse=True,
    )
    if not candidates:
        return None
    path = candidates[0]
    content = path.read_text(encoding="utf-8")
    gen_match = re.search(r"\*Generated:\s*(.+?)\*", content)
    generated_date = gen_match.group(1).strip() if gen_match else "Unknown"
    m = re.match(r"monthly_consultant_rfps_(\d{4}-\d{2})", path.name)
    if m:
        month = datetime.strptime(m.group(1) + "-01", "%Y-%m-%d").strftime("%B %Y")
        title = f"Monthly Consultant RFP Brief: {month}"
    else:
        title = "Monthly Consultant RFP Brief"
    return (path, title, generated_date)


def page_notes():
    tab_week, tab_insights_monthly, tab_rfps, tab_insights_year = st.tabs([
        "7-Day Highlights",
        "Monthly CIO Insights",
        "Consultant RFPs",
        "2026 CIO Insights",
    ])

    with tab_rfps:
        st.title("Monthly Consultant RFP Brief")
        result = _find_latest_consultant_rfps()
        if result:
            path, title, gen_date = result
            _render_note_page(
                md_path=path,
                title=title,
                generated_date=gen_date,
                pdf_filename=path.stem + ".pdf",
            )
        else:
            st.info(
                "No consultant RFP brief found yet. "
                "Run `python -m scripts.compose_rfp_monthly` to generate."
            )

    with tab_insights_monthly:
        st.title("Monthly CIO Insights")
        result = _find_latest_insights_recent()
        if result:
            path, title, gen_date = result
            _render_note_page(
                md_path=path,
                title=title,
                generated_date=gen_date,
                pdf_filename=path.stem + ".pdf",
            )
        else:
            st.info(
                "No monthly insights document found. "
                "Run `python generate_notes.py --insights-30day-only` to generate."
            )

    with tab_insights_year:
        st.title("2026 CIO Insights")
        result = _find_latest_insights()
        if result:
            path, title, gen_date = result
            _render_note_page(
                md_path=path,
                title=title,
                generated_date=gen_date,
                pdf_filename="2026_cio_insights.pdf",
            )
        else:
            st.info("No 2026 insights document found. Run `python generate_notes.py --insights-ytd-only` to generate.")

    with tab_week:
        st.title("7-Day Highlights")
        all_highlights = _find_all_highlights()
        if not all_highlights:
            st.info("No highlights found. Run `python generate_notes.py` to generate.")
        elif len(all_highlights) == 1:
            path, title, gen_date = all_highlights[0]
            _render_note_page(
                md_path=path,
                title=title,
                generated_date=gen_date,
                pdf_filename=path.stem + ".pdf",
            )
        else:
            labels = [title for _, title, _ in all_highlights]
            selected_idx = st.selectbox(
                "Select week", range(len(labels)),
                format_func=lambda i: labels[i],
            )
            path, title, gen_date = all_highlights[selected_idx]
            _render_note_page(
                md_path=path,
                title=title,
                generated_date=gen_date,
                pdf_filename=path.stem + ".pdf",
            )


def page_investment_actions(plan_id, plan_label):
    st.title("Investment Actions")
    st.caption("Manager hires/fires, allocation changes, and new commitments extracted from board packs.")

    session = get_db_session()
    q = (
        session.query(Document, Summary)
        .join(Summary, Document.id == Summary.document_id)
        .filter(Summary.investment_actions != "[]")
        .filter(Summary.investment_actions.isnot(None))
    )
    if plan_id:
        q = q.filter(Document.plan_id == plan_id)
    results = q.order_by(Document.meeting_date.desc()).limit(100).all()

    if not results:
        st.info("No investment actions found yet.")
        return

    action_filter = st.multiselect(
        "Filter by action type",
        ["hire", "fire", "rebalance", "allocation_change", "commitment", "other"],
        default=[]
    )

    rows = []
    for doc, summary in results:
        actions = parse_json_field(summary.investment_actions)
        for a in actions:
            if action_filter and a.get("action") not in action_filter:
                continue
            rows.append({
                "Plan": doc.plan_id.upper(),
                "Date": doc.meeting_date.strftime("%Y-%m-%d") if doc.meeting_date else "",
                "Action": a.get("action", ""),
                "Description": a.get("description", ""),
                "Manager": a.get("manager", ""),
                "Asset Class": a.get("asset_class", ""),
                "Amount ($M)": a.get("amount_millions", ""),
            })

    if rows:
        import pandas as pd
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)
        csv = df.to_csv(index=False)
        st.download_button("Download CSV", csv, "investment_actions.csv", "text/csv")
    else:
        st.info("No actions match the current filter.")


# ---------------------------------------------------------------------------
# Managers tab
# ---------------------------------------------------------------------------

MANAGER_MAPPINGS_PATH = Path(__file__).parent / "data" / "manager_mappings.json"


@st.cache_data(ttl=300)
def _load_manager_mappings() -> dict:
    """Load the LLM-classified manager-name mappings.

    Returns ``{}`` if the file is missing — page falls back to raw names.
    Refreshed via ``python -m scripts.normalize_managers``.
    """
    if not MANAGER_MAPPINGS_PATH.exists():
        return {}
    return json.loads(MANAGER_MAPPINGS_PATH.read_text())


@st.cache_data(ttl=300)
def _aggregate_canonical_managers() -> tuple[list[dict], int]:
    """Aggregate raw mentions, then collapse by canonical name from the mapping.

    Each output row represents one canonical manager and merges every raw
    variant's mention count, plan count, doc IDs, and date range. Rows
    classified as ``is_manager: false`` (placeholders, plan names, etc.)
    are excluded from the output but counted in the returned excluded
    count for transparency.
    """
    session = get_db_session()
    raw = aggregate_managers(session)
    mappings = _load_manager_mappings()

    canonical: dict[str, dict] = {}
    excluded = 0
    for row in raw:
        m = mappings.get(row["raw_name"])
        if m is None:
            # Unmapped → fall back to using the raw name as canonical
            canon = row["raw_name"]
            is_mgr = True
        elif not m.get("is_manager"):
            excluded += row["mention_count"]
            continue
        else:
            canon = m.get("canonical") or row["raw_name"]
            is_mgr = True

        existing = canonical.setdefault(canon, {
            "canonical": canon,
            "variants": [],
            "mention_count": 0,
            "plan_ids": set(),
            "doc_ids": set(),
            "first_meeting": None,
            "latest_meeting": None,
        })
        existing["variants"].append(row["raw_name"])
        existing["mention_count"] += row["mention_count"]
        existing["plan_ids"].update(row["plan_ids"])
        existing["doc_ids"].update(row["doc_ids"])
        for fld in ("first_meeting", "latest_meeting"):
            v = row[fld]
            if v is None:
                continue
            if existing[fld] is None or (fld == "first_meeting" and v < existing[fld]) \
                    or (fld == "latest_meeting" and v > existing[fld]):
                existing[fld] = v

    out = []
    for entry in canonical.values():
        out.append({
            "canonical": entry["canonical"],
            "variants": sorted(entry["variants"]),
            "mention_count": entry["mention_count"],
            "plan_count": len(entry["plan_ids"]),
            "doc_ids": sorted(entry["doc_ids"]),
            "first_meeting": entry["first_meeting"],
            "latest_meeting": entry["latest_meeting"],
        })
    out.sort(key=lambda r: -r["mention_count"])
    return out, excluded


def page_managers():
    st.title("Managers")
    st.caption(
        "Investment managers, consultants, custodians, and advisors mentioned in "
        "the structured `investment_actions` extracted from board materials. "
        "Canonical names produced by `scripts/normalize_managers.py`."
    )

    rows, excluded_mentions = _aggregate_canonical_managers()
    if not rows:
        st.info("No manager mentions found yet. Run the pipeline to summarize documents.")
        return

    total_mentions = sum(r["mention_count"] for r in rows)
    st.markdown(
        f"**{len(rows):,} distinct managers** across **{total_mentions:,} mentions**"
        + (f" — {excluded_mentions:,} additional mentions excluded "
           "(placeholders, plan names, compound listings)" if excluded_mentions else "")
    )

    col_a, col_b = st.columns([2, 1])
    with col_a:
        query = st.text_input(
            "Filter by name", placeholder="e.g. BlackRock, Meketa, Albourne",
            key="manager_filter",
        )
    with col_b:
        min_mentions = st.number_input(
            "Min. mentions", min_value=1, max_value=100, value=1, step=1,
            key="manager_min_mentions",
        )

    filtered = [
        r for r in rows
        if r["mention_count"] >= min_mentions
        and (not query or query.lower() in r["canonical"].lower()
             or any(query.lower() in v.lower() for v in r["variants"]))
    ]
    st.caption(f"Showing {len(filtered):,} of {len(rows):,} managers")

    for r in filtered[:200]:
        first = r["first_meeting"].strftime("%Y-%m-%d") if r["first_meeting"] else "—"
        latest = r["latest_meeting"].strftime("%Y-%m-%d") if r["latest_meeting"] else "—"
        header = (
            f"**{r['canonical']}** — {r['mention_count']} mentions, "
            f"{r['plan_count']} plans · {first} → {latest}"
        )
        with st.expander(header):
            if len(r["variants"]) > 1:
                variants_str = " · ".join(f"`{v}`" for v in r["variants"])
                st.markdown(f"**Raw variants ({len(r['variants'])}):** {variants_str}")

            session = get_db_session()
            docs = (
                session.query(Document)
                .filter(Document.id.in_(r["doc_ids"]))
                .order_by(Document.meeting_date.desc())
                .limit(20)
                .all()
            )
            st.markdown(f"**Recent documents ({min(len(docs), 20)} of {len(r['doc_ids'])}):**")
            for d in docs:
                date_str = d.meeting_date.strftime("%Y-%m-%d") if d.meeting_date else "—"
                doc_type = (d.doc_type or "document").replace("_", " ").title()
                st.markdown(
                    f"- {date_str} · **{d.plan_id.upper()}** · {doc_type} "
                    f"[→ open](?doc={d.id})"
                )

    if len(filtered) > 200:
        st.caption(f"… {len(filtered) - 200:,} more matches not shown — refine the filter to narrow.")


# ---------------------------------------------------------------------------
# App entry
# ---------------------------------------------------------------------------

def page_document_detail(doc_id: int):
    """Display a single document's summary when accessed via ?doc=ID."""
    session = get_session()
    try:
        doc = session.query(Document).get(doc_id)
        if not doc:
            st.error(f"Document #{doc_id} not found.")
            return

        plan = session.query(Plan).get(doc.plan_id) if doc.plan_id else None
        summary = session.query(Summary).filter_by(document_id=doc.id).first()

        plan_name = (plan.abbreviation or plan.name) if plan else doc.plan_id
        date_str = doc.meeting_date.strftime("%B %d, %Y") if doc.meeting_date else "Unknown"
        doc_type = (doc.doc_type or "document").replace("_", " ").title()

        st.title(f"{plan_name} — {doc_type}")
        st.caption(f"Meeting date: {date_str}")

        if st.button("Back to dashboard"):
            st.query_params.clear()
            st.rerun()

        # Source file access. The file lives on the persistent disk at
        # /data/downloads on Render. If it's missing (e.g. the doc was
        # fetched before the persistent-disk migration), we lazily re-fetch
        # from the original URL on demand so the copy gets cached on disk.
        local_file = Path(doc.local_path) if doc.local_path else None
        file_present = bool(local_file and local_file.exists())

        if file_present:
            try:
                file_bytes = local_file.read_bytes()
                mime = "application/pdf" if local_file.suffix.lower() == ".pdf" else "application/octet-stream"
                st.download_button(
                    label=f"Download source file ({local_file.name})",
                    data=file_bytes,
                    file_name=local_file.name,
                    mime=mime,
                )
            except OSError as exc:
                st.caption(f"Source file unavailable: {exc}")
        elif doc.url:
            if st.button("Retrieve source file"):
                with st.spinner("Fetching from original source..."):
                    path, size, err = _retrieve_source_file(
                        doc.url, doc.plan_id, doc.filename or f"doc_{doc.id}.pdf"
                    )
                    if path:
                        doc.local_path = str(path)
                        doc.file_size_bytes = size
                        session.commit()
                        st.success(f"Retrieved {path.name} ({size:,} bytes). Reloading...")
                        st.rerun()
                    else:
                        st.error(
                            f"Couldn't retrieve the file — {err}. "
                            "The full extracted text is still available below."
                        )

        st.divider()

        if summary:
            render_summary_card(doc, summary)
        else:
            st.info("This document has not been summarized yet.")

        # Full extracted text — always available from the DB, even if the
        # original URL breaks or the source file is missing. Rendered via
        # st.code so Streamlit supplies its built-in copy-to-clipboard icon
        # in the top-right of the block.
        if doc.extracted_text:
            with st.expander("Full extracted text", expanded=False):
                st.code(doc.extracted_text, language=None)

        st.caption(f"Original source (may break over time): {doc.url}")
    finally:
        session.close()


RECENT_RUNS_LIMIT = 14   # ~1 week of GHA + local entries


def _render_recent_runs():
    """Render the 'Recent Runs' Admin sub-tab: last N FetchRun entries,
    each expandable to show plan → filename of every new document."""
    import json
    from collections import defaultdict
    from sqlalchemy import desc

    st.caption(
        f"The last {RECENT_RUNS_LIMIT} pipeline runs (GHA cron and local "
        "Task Scheduler combined). Each row expands to show the new "
        "documents fetched in that run, grouped by plan."
    )

    session = get_session()
    try:
        runs = (
            session.query(FetchRun)
            .order_by(desc(FetchRun.started_at))
            .limit(RECENT_RUNS_LIMIT)
            .all()
        )
        if not runs:
            st.info("No pipeline runs recorded yet. The next GHA cron or "
                    "local Task Scheduler invocation will populate this.")
            return

        for run in runs:
            doc_ids = json.loads(run.new_document_ids or "[]")
            if run.status == "failed":
                marker = "✗"
                summary = f"failed: {run.error_message or 'no message recorded'}"
            elif run.status == "running":
                marker = "⋯"
                summary = "still running…"
            else:
                summary = (
                    f"{len(doc_ids)} new document{'s' if len(doc_ids) != 1 else ''}"
                )
                marker = "✓"

            elapsed_str = ""
            if run.completed_at and run.started_at:
                secs = int((run.completed_at - run.started_at).total_seconds())
                if secs >= 60:
                    elapsed_str = f" ({secs // 60}m {secs % 60}s)"
                else:
                    elapsed_str = f" ({secs}s)"

            label = (
                f"{marker} {run.started_at.strftime('%Y-%m-%d %H:%M UTC')} "
                f"· {run.source} · {summary}{elapsed_str}"
            )

            with st.expander(label, expanded=(run is runs[0] and bool(doc_ids))):
                if run.status == "failed":
                    st.error(run.error_message or "No error message captured.")
                if not doc_ids:
                    st.write("No new documents in this run.")
                    continue

                # Group filenames under their plan name
                rows = (
                    session.query(Plan.name, Document.filename, Document.downloaded_at)
                    .join(Document, Document.plan_id == Plan.id)
                    .filter(Document.id.in_(doc_ids))
                    .order_by(Document.downloaded_at)
                    .all()
                )
                grouped = defaultdict(list)
                for plan_name, filename, downloaded_at in rows:
                    grouped[plan_name].append((filename, downloaded_at))

                for plan_name in sorted(grouped):
                    st.markdown(f"**{plan_name}**")
                    for filename, _ in grouped[plan_name]:
                        st.markdown(f"&nbsp;&nbsp;• {filename}", unsafe_allow_html=True)
    finally:
        session.close()


def _render_failed_docs():
    """Render the 'Failed Docs' Admin sub-tab: per-plan list of documents
    that won't get summarised — either because text extraction failed, or
    because they're recorded in the DocumentSkip table (Claude refusals)."""
    from collections import defaultdict
    from sqlalchemy import desc

    st.caption(
        "Plans with at least one document the pipeline cannot process. "
        "Two failure modes: PDF text extraction failed (PyMuPDF couldn't "
        "read the file), or the summariser was permanently skipped "
        "(currently only Claude content-policy refusals)."
    )

    session = get_session()
    try:
        # Two queries, then merge by plan
        ext_rows = (
            session.query(Plan.id, Plan.name, Document.id, Document.filename)
            .join(Document, Document.plan_id == Plan.id)
            .filter(Document.extraction_status == "failed")
            .all()
        )
        skip_rows = (
            session.query(Plan.id, Plan.name, Document.id, Document.filename,
                          DocumentSkip.reason, DocumentSkip.error_message)
            .join(Document, Document.plan_id == Plan.id)
            .join(DocumentSkip, DocumentSkip.document_id == Document.id)
            .all()
        )

        if not ext_rows and not skip_rows:
            st.success(
                "No failed documents. Every downloaded PDF has been "
                "extracted, and the summariser has nothing flagged for "
                "permanent skip."
            )
            return

        # Group failures per plan
        by_plan: dict[str, dict] = defaultdict(
            lambda: {"name": "", "extraction": [], "skip": []}
        )
        for plan_id, plan_name, _doc_id, filename in ext_rows:
            by_plan[plan_id]["name"] = plan_name
            by_plan[plan_id]["extraction"].append(filename)
        for plan_id, plan_name, _doc_id, filename, reason, err in skip_rows:
            by_plan[plan_id]["name"] = plan_name
            by_plan[plan_id]["skip"].append((filename, reason, err))

        total_ext = len(ext_rows)
        total_skip = len(skip_rows)

        c1, c2, c3 = st.columns(3)
        c1.metric("Plans with failures", len(by_plan))
        c2.metric("Extraction failures", total_ext)
        c3.metric("Permanent skips", total_skip)

        # Sort plans by total failure count descending
        plans_sorted = sorted(
            by_plan.items(),
            key=lambda kv: -(len(kv[1]["extraction"]) + len(kv[1]["skip"])),
        )

        for _plan_id, info in plans_sorted:
            n = len(info["extraction"]) + len(info["skip"])
            label = f"{info['name']}  ·  {n} failed"
            with st.expander(label):
                if info["extraction"]:
                    st.markdown(
                        f"**Extraction failed** ({len(info['extraction'])})"
                    )
                    for fn in sorted(info["extraction"]):
                        st.markdown(
                            f"&nbsp;&nbsp;• {fn}", unsafe_allow_html=True
                        )
                if info["skip"]:
                    st.markdown(
                        f"**Permanently skipped** ({len(info['skip'])})"
                    )
                    for fn, reason, err in sorted(info["skip"]):
                        detail = f"{reason}" + (f" — {err}" if err else "")
                        st.markdown(
                            f"&nbsp;&nbsp;• {fn} *({detail})*",
                            unsafe_allow_html=True,
                        )

        st.info(
            "**Retry options.**\n\n"
            "Extraction failures are usually image-only PDFs (no text "
            "layer). Re-run locally with OCR fallback:\n\n"
            "`python pipeline.py --retry-failed`\n\n"
            "Requires Tesseract installed (Windows: see "
            "github.com/UB-Mannheim/tesseract/wiki). Permanent skips can "
            "be cleared with `DELETE FROM document_skips WHERE "
            "document_id = ?` if you want the summariser to retry them."
        )
    finally:
        session.close()


def _render_cafr_coverage():
    """Render the 'CAFR Coverage' Admin sub-tab: how many plans have a
    recent ACFR/CAFR in the DB, broken down by latest fiscal year."""
    from sqlalchemy import func

    st.caption(
        "Latest CAFR/ACFR fiscal year held per plan. 'FY2024+' is the "
        "current freshness target — anything older is a backfill gap. "
        "Plans with no CAFR at all need URL hygiene in known_plans.json."
    )

    session = get_session()
    try:
        # Latest CAFR FY per plan (one row per plan that has any CAFR)
        latest_rows = (
            session.query(
                Document.plan_id,
                func.max(Document.fiscal_year).label("latest_fy"),
            )
            .filter(Document.doc_type == "cafr")
            .filter(Document.fiscal_year.isnot(None))
            .group_by(Document.plan_id)
            .all()
        )
        latest_by_plan = {pid: fy for pid, fy in latest_rows}

        # All plans (so we can count "none")
        plans = session.query(Plan).order_by(Plan.id).all()
        total_plans = len(plans)

        # CAFR documents by fiscal year (across all plans, not just latest)
        by_fy_rows = (
            session.query(
                Document.fiscal_year,
                func.count(Document.id).label("n"),
            )
            .filter(Document.doc_type == "cafr")
            .filter(Document.fiscal_year.isnot(None))
            .group_by(Document.fiscal_year)
            .order_by(Document.fiscal_year.desc())
            .all()
        )
    finally:
        session.close()

    if total_plans == 0:
        st.warning("No plans tracked yet.")
        return

    # ---------- headline metrics
    n_2024_plus = sum(1 for fy in latest_by_plan.values() if fy and fy >= 2024)
    n_2025_plus = sum(1 for fy in latest_by_plan.values() if fy and fy >= 2025)
    n_gap = total_plans - n_2024_plus
    pct = round(100 * n_2024_plus / total_plans) if total_plans else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Plans tracked", total_plans)
    c2.metric("FY2024+ coverage", f"{n_2024_plus} ({pct}%)")
    c3.metric("FY2025+ coverage", n_2025_plus)
    c4.metric("Backfill gap", n_gap)

    # ---------- documents by fiscal year
    st.markdown("##### CAFR documents by fiscal year")
    if not by_fy_rows:
        st.info("No CAFR documents in the database yet.")
    else:
        import pandas as pd
        fy_df = pd.DataFrame(
            [(f"FY{int(fy)}", int(n)) for fy, n in by_fy_rows],
            columns=["Fiscal year", "CAFRs"],
        )
        st.dataframe(fy_df, hide_index=True, use_container_width=False)

    # ---------- plans by latest CAFR (bucketed)
    st.markdown("##### Plans by latest CAFR fiscal year")
    from collections import Counter
    bucket = Counter()
    for plan in plans:
        fy = latest_by_plan.get(plan.id)
        if fy is None:
            bucket["none"] += 1
        elif fy >= 2025:
            bucket["FY2025+"] += 1
        elif fy == 2024:
            bucket["FY2024 (only)"] += 1
        else:
            bucket[f"FY{int(fy)} (stale)"] += 1
    # Stable display order
    order = ["FY2025+", "FY2024 (only)", "FY2023 (stale)", "FY2022 (stale)",
             "FY2021 (stale)", "FY2020 (stale)", "none"]
    import pandas as pd
    bucket_df = pd.DataFrame(
        [(k, bucket[k]) for k in order if k in bucket],
        columns=["Bucket", "Plans"],
    )
    st.dataframe(bucket_df, hide_index=True, use_container_width=False)

    # ---------- detail: every plan with its latest FY
    st.markdown("##### Per-plan detail")
    rows = []
    for plan in plans:
        fy = latest_by_plan.get(plan.id)
        rows.append({
            "Plan": plan.name or plan.id,
            "Abbrev": plan.abbreviation or "",
            "State": plan.state or "",
            "Latest CAFR FY": int(fy) if fy else None,
            "Status": (
                "current" if fy and fy >= 2024
                else "stale" if fy
                else "none"
            ),
        })
    detail_df = pd.DataFrame(rows)
    # pandas would coerce int+None to float64 (displays "2024.0"); use the
    # nullable integer dtype so "Latest CAFR FY" renders as plain integers.
    detail_df["Latest CAFR FY"] = detail_df["Latest CAFR FY"].astype("Int64")
    # Sort gaps-first: none → stale → current; within each, oldest FY first.
    status_rank = {"none": 0, "stale": 1, "current": 2}
    detail_df = (
        detail_df
        .assign(_rank=detail_df["Status"].map(status_rank))
        .sort_values(by=["_rank", "Latest CAFR FY", "Plan"], na_position="first")
        .drop(columns=["_rank"])
        .reset_index(drop=True)
    )
    st.dataframe(detail_df, hide_index=True, use_container_width=True)


CAFR_REFRESH_LIMIT = 10  # how many distinct refresh runs to surface


def _render_cafr_refreshes():
    """Render the 'CAFR Refreshes' Admin sub-tab: per-run breakdown of
    refresh_cafrs.py outcomes (saved / already_have / no_strategy / etc.),
    last N runs, each expandable to per-plan status."""
    from collections import defaultdict, Counter
    from sqlalchemy import desc

    st.caption(
        f"Last {CAFR_REFRESH_LIMIT} CAFR refresh runs (GHA monthly + local "
        "Task Scheduler). Each run produces one row per CAFR-having plan in "
        "the cafr_refresh_log table. Status legend: saved = new CAFR saved; "
        "already_have = nothing to do; no_strategy = no URL produced a "
        "candidate (template/landing/static all returned None); url_failed "
        "= download failed; validation_failed = file < min size or magic "
        "header / cover-year mismatch."
    )

    session = get_session()
    try:
        # Distinct run timestamps, newest first
        recent_run_ats = [
            row[0] for row in
            session.query(CafrRefreshLog.run_at)
            .distinct()
            .order_by(desc(CafrRefreshLog.run_at))
            .limit(CAFR_REFRESH_LIMIT)
            .all()
        ]
        if not recent_run_ats:
            st.info(
                "No CAFR refresh runs recorded yet. The next monthly cron "
                "(GHA + local Task Scheduler) will populate this tab."
            )
            return

        # Pull every row for those run_ats in one query
        rows = (
            session.query(
                CafrRefreshLog.run_at,
                CafrRefreshLog.plan_id,
                CafrRefreshLog.expected_year,
                CafrRefreshLog.status,
                CafrRefreshLog.url_tried,
                CafrRefreshLog.notes,
            )
            .filter(CafrRefreshLog.run_at.in_(recent_run_ats))
            .order_by(desc(CafrRefreshLog.run_at), CafrRefreshLog.plan_id)
            .all()
        )
        # Plan abbreviations for display
        plans = {p.id: (p.abbreviation or p.id, p.name or p.id)
                 for p in session.query(Plan).all()}
    finally:
        session.close()

    # Group rows by run_at
    by_run: dict = defaultdict(list)
    for r in rows:
        by_run[r.run_at].append(r)

    # ---------- headline metrics across the most recent run
    latest = recent_run_ats[0]
    latest_rows = by_run[latest]
    latest_counts = Counter(r.status for r in latest_rows)
    n_total = len(latest_rows)
    n_saved = latest_counts.get("saved", 0)
    n_already = latest_counts.get("already_have", 0)
    n_failed = (latest_counts.get("no_strategy", 0)
                + latest_counts.get("url_failed", 0)
                + latest_counts.get("validation_failed", 0)
                + latest_counts.get("error", 0))

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Latest run", latest.strftime("%Y-%m-%d %H:%M"))
    c2.metric("Plans processed", n_total)
    c3.metric("Saved + already-have",
              f"{n_saved + n_already} ({(n_saved + n_already) * 100 // n_total}%)"
              if n_total else "0")
    c4.metric("Failed", n_failed)

    # ---------- per-run expanders, newest first
    for run_at in recent_run_ats:
        run_rows = by_run[run_at]
        counts = Counter(r.status for r in run_rows)
        # Build a one-line summary from the count breakdown
        order = ["saved", "already_have", "no_strategy",
                 "url_failed", "validation_failed", "error"]
        summary_parts = [f"{counts[s]} {s}" for s in order if counts.get(s)]
        label = (f"{run_at.strftime('%Y-%m-%d %H:%M UTC')}  ·  "
                 f"{len(run_rows)} plans  ·  " + ", ".join(summary_parts))
        # Expand only the most-recent run by default
        with st.expander(label, expanded=(run_at == latest)):
            # Group plans within the run by status for readability
            by_status: dict = defaultdict(list)
            for r in run_rows:
                abbrev, name = plans.get(r.plan_id, (r.plan_id, r.plan_id))
                by_status[r.status].append({
                    "plan_id": r.plan_id,
                    "abbrev": abbrev,
                    "name": name,
                    "expected_year": r.expected_year,
                    "url_tried": r.url_tried,
                    "notes": r.notes,
                })
            for status in order:
                items = by_status.get(status, [])
                if not items:
                    continue
                st.markdown(f"**{status}** ({len(items)})")
                # 'saved' and 'already_have' are routine — show as one-liners
                if status in ("saved", "already_have"):
                    for it in items:
                        line = f"&nbsp;&nbsp;• {it['abbrev']}"
                        if it['expected_year']:
                            line += f" — FY{it['expected_year']}"
                        if it['notes']:
                            line += f"  *({it['notes']})*"
                        st.markdown(line, unsafe_allow_html=True)
                else:
                    # Failures get a richer view: include url + notes
                    for it in items:
                        line = (f"&nbsp;&nbsp;• **{it['abbrev']}** "
                                f"(target FY{it['expected_year']})")
                        if it['notes']:
                            line += f"  — {it['notes']}"
                        if it['url_tried']:
                            line += (f"  [`{it['url_tried'][:80]}`"
                                     f"{'…' if len(it['url_tried']) > 80 else ''}`]")
                        st.markdown(line, unsafe_allow_html=True)


def _admin_plan_coverage_df():
    """Build the per-plan coverage table used by the Admin page.

    Returns a pandas DataFrame with one row per tracked plan, summarising
    how many documents have been downloaded, extracted and summarised,
    plus the timestamp of the most recent download.
    """
    import pandas as pd
    from sqlalchemy import case, distinct, func

    session = get_db_session()
    rows = (
        session.query(
            Plan.name.label("plan"),
            Plan.abbreviation.label("abbrev"),
            Plan.state.label("state"),
            func.count(distinct(Document.id)).label("downloaded"),
            func.sum(
                case((Document.extraction_status == "done", 1), else_=0)
            ).label("extracted"),
            func.count(distinct(Summary.id)).label("summarized"),
            func.max(Document.downloaded_at).label("last_download"),
        )
        .outerjoin(Document, Document.plan_id == Plan.id)
        .outerjoin(Summary, Summary.document_id == Document.id)
        .group_by(Plan.id)
        .order_by(Plan.name)
        .all()
    )

    df = pd.DataFrame(
        [
            {
                "Plan": r.plan,
                "Abbrev": r.abbrev or "",
                "State": r.state or "",
                "Downloaded": int(r.downloaded or 0),
                "Extracted": int(r.extracted or 0),
                "Summarized": int(r.summarized or 0),
                "Last download": (
                    r.last_download.strftime("%Y-%m-%d %H:%M")
                    if r.last_download else "—"
                ),
            }
            for r in rows
        ]
    )
    return df


_SEVERITY_BADGE = {"error": "🔴", "warning": "🟡", "info": "🔵"}


def _render_admin_report_card():
    """Render the 'Report Card' Admin sub-tab.

    Single-pane progress + health view. Sourced from admin_report.build_report
    so the same payload is consumable by tests and AI assistants reading the
    page (the structured JSON is also exposed in an expander at the bottom).
    """
    import json
    import pandas as pd

    from admin_report import build_report

    st.caption(
        "Weekly progress and health summary. Two purposes: a human-readable "
        "snapshot of how many plans we covered each week (and cumulatively), "
        "and a structured issue list — every problem includes a fix hint so "
        "an AI assistant pasted into a session can investigate without "
        "round-tripping through the UI."
    )

    report = build_report()
    cum = report["cumulative"]
    issues = report["issues"]

    # ---- Cumulative headline ------------------------------------------
    st.subheader("Cumulative coverage")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Plans tracked", cum["total_plans"])
    c2.metric("Plans w/ documents", cum["plans_with_document"])
    c3.metric("Plans w/ summaries", cum["plans_with_summary"])
    c4.metric("Plans w/ CAFR", cum["plans_with_cafr"])
    c5.metric("Plans w/ IPS", cum["plans_with_ips"])

    # ---- This week --------------------------------------------------------
    st.subheader(
        f"Latest reportable week — "
        f"{report['latest_week']['start']} → {report['latest_week']['end']}"
    )
    latest = report["weeks"][0]
    w1, w2, w3, w4 = st.columns(4)
    w1.metric("Unique plans this week", latest["unique_plans"])
    w2.metric("New documents", latest["new_documents"])
    w3.metric("New summaries", latest["new_summaries"])
    w4.metric(
        "Weekly Insights",
        latest["publication_status"] or "missing",
    )

    # ---- Issues -----------------------------------------------------------
    st.subheader("Issues")
    if not issues:
        st.success("No issues detected.")
    else:
        # Severity-sorted: errors first, then warnings, then info
        order = {"error": 0, "warning": 1, "info": 2}
        for issue in sorted(issues, key=lambda i: order.get(i["severity"], 9)):
            badge = _SEVERITY_BADGE.get(issue["severity"], "•")
            label = (
                f"{badge} **{issue['severity'].upper()}** · "
                f"{issue['category']} — {issue['message']}"
            )
            with st.expander(label, expanded=(issue["severity"] == "error")):
                if issue.get("fix_hint"):
                    st.markdown(f"**Fix:** {issue['fix_hint']}")
                details = issue.get("details") or []
                if details:
                    st.markdown(f"**Details ({len(details)}):**")
                    # Cap at 50 to avoid runaway expanders; truth is in the JSON below.
                    for line in details[:50]:
                        st.markdown(f"&nbsp;&nbsp;• {line}", unsafe_allow_html=True)
                    if len(details) > 50:
                        st.caption(f"… and {len(details) - 50} more (see JSON below).")

    # ---- Weekly trend table ----------------------------------------------
    st.subheader(f"Last {len(report['weeks'])} weeks (Sun→Sat)")

    def _fmt_runs(ok, failed):
        if ok == 0 and failed == 0:
            return "—"
        if failed == 0:
            return f"{ok}"
        return f"{ok} ✓ / {failed} ✗"

    df = pd.DataFrame(
        [
            {
                "Week": w["week_start"],
                "Plans w/ new docs": w["unique_plans"],
                "New docs": w["new_documents"],
                "New summaries": w["new_summaries"],
                "GHA runs": _fmt_runs(w["gha_success"], w["gha_failed"]),
                "Local runs": _fmt_runs(w["local_success"], w["local_failed"]),
                "Insights": w["publication_status"] or "—",
                "Weekly run": w["weekly_run_status"] or "—",
                "Cumulative plans": w["cumulative_unique_plans"],
            }
            for w in report["weeks"]
        ]
    )
    st.dataframe(df, use_container_width=True, hide_index=True)

    # ---- AI / debug payload ----------------------------------------------
    with st.expander("Diagnostics JSON (for AI assistants)", expanded=False):
        st.caption(
            "Same payload as above, structured. Paste into a Claude session "
            "with `admin_report.build_report()` to act on the issue list."
        )
        st.code(json.dumps(report, indent=2, default=str), language="json")


@st.cache_data(ttl=300)
def _cafr_coverage_df():
    """Per-plan CAFR + extraction coverage table.

    For each tracked plan, picks the most recent CAFR document (by
    fiscal_year, then downloaded_at) and joins to its CafrExtract row if
    one exists, plus counts of allocation and performance rows.

    Cached for 5 minutes — the underlying tables are write-rare (CAFR
    refresh is monthly; extraction is one-shot per plan).
    """
    import pandas as pd
    from sqlalchemy import func

    session = get_db_session()

    cafr_rows = (
        session.query(Document)
        .filter(Document.doc_type == "cafr")
        .order_by(Document.plan_id)
        .all()
    )
    # Reduce to the most recent CAFR per plan (highest fiscal_year, then
    # most recent downloaded_at). Done in Python to avoid SQL nullslast
    # portability concerns.
    latest_cafr: dict[str, Document] = {}
    cafr_count: dict[str, int] = {}
    for d in cafr_rows:
        cafr_count[d.plan_id] = cafr_count.get(d.plan_id, 0) + 1
        prev = latest_cafr.get(d.plan_id)
        if prev is None:
            latest_cafr[d.plan_id] = d
            continue
        prev_key = (prev.fiscal_year or 0, prev.downloaded_at or datetime.min)
        d_key = (d.fiscal_year or 0, d.downloaded_at or datetime.min)
        if d_key > prev_key:
            latest_cafr[d.plan_id] = d

    extracts: dict[int, CafrExtract] = {
        e.document_id: e for e in session.query(CafrExtract).all()
    }
    alloc_counts = dict(
        session.query(
            CafrAllocation.cafr_extract_id,
            func.count(CafrAllocation.id),
        ).group_by(CafrAllocation.cafr_extract_id).all()
    )
    perf_counts = dict(
        session.query(
            CafrPerformance.cafr_extract_id,
            func.count(CafrPerformance.id),
        ).group_by(CafrPerformance.cafr_extract_id).all()
    )

    plans = session.query(Plan).order_by(Plan.name).all()
    rows = []
    for p in plans:
        doc = latest_cafr.get(p.id)
        if doc is None:
            status = "Missing CAFR"
            cafr_fy = ""
            downloaded = ""
            url = ""
            extracted = "No"
            extract_fy = ""
            alloc = 0
            perf = 0
        else:
            cafr_fy = str(doc.fiscal_year) if doc.fiscal_year else ""
            downloaded = doc.downloaded_at.strftime("%Y-%m-%d") if doc.downloaded_at else ""
            url = doc.url or ""
            ext = extracts.get(doc.id)
            if ext is None:
                status = "Pending extract"
                extracted = "No"
                extract_fy = ""
                alloc = 0
                perf = 0
            else:
                status = "Extracted"
                extracted = "Yes"
                extract_fy = str(ext.fiscal_year) if ext.fiscal_year else ""
                alloc = int(alloc_counts.get(ext.id, 0))
                perf = int(perf_counts.get(ext.id, 0))

        rows.append({
            "plan_id": p.id,
            "Plan": p.abbreviation or p.name,
            "Name": p.name,
            "State": p.state or "",
            "FYE": p.fiscal_year_end or "",
            "Status": status,
            "CAFR FY": cafr_fy,
            "Source": url or None,
            "Extract FY": extract_fy,
            "# Asset classes": alloc,
            "# Perf rows": perf,
            "Downloaded": downloaded,
        })

    return pd.DataFrame(rows)


@st.cache_data(ttl=300)
def _cafr_plan_detail_data(plan_id: str) -> dict:
    """Fetch the latest CAFR extract for a plan, with allocations + performance."""
    session = get_db_session()
    plan = session.query(Plan).filter_by(id=plan_id).first()
    if plan is None:
        return {}

    extract = (
        session.query(CafrExtract)
        .filter(CafrExtract.plan_id == plan_id)
        .order_by(CafrExtract.fiscal_year.desc(), CafrExtract.id.desc())
        .first()
    )
    if extract is None:
        return {"plan": {
            "name": plan.name,
            "abbreviation": plan.abbreviation,
            "state": plan.state,
        }}

    allocations = (
        session.query(CafrAllocation)
        .filter(CafrAllocation.cafr_extract_id == extract.id)
        .order_by(CafrAllocation.id)
        .all()
    )
    performance = (
        session.query(CafrPerformance)
        .filter(CafrPerformance.cafr_extract_id == extract.id)
        .order_by(CafrPerformance.scope, CafrPerformance.period)
        .all()
    )
    document = session.query(Document).filter_by(id=extract.document_id).first()

    return {
        "plan": {
            "name": plan.name,
            "abbreviation": plan.abbreviation,
            "state": plan.state,
        },
        "extract": {
            "id": extract.id,
            "fiscal_year": extract.fiscal_year,
            "extracted_at": extract.extracted_at,
            "model_used": extract.model_used,
            "pages_used": extract.pages_used,
            "investment_policy_text": extract.investment_policy_text,
            "notes": extract.notes,
        },
        "document": {
            "id": document.id if document else None,
            "url": document.url if document else None,
            "filename": document.filename if document else None,
        } if document else None,
        "allocations": [
            {
                "Asset class": a.asset_class,
                "Target %": a.target_pct,
                "Actual %": a.actual_pct,
                "Range low %": a.target_range_low,
                "Range high %": a.target_range_high,
                "Notes": a.notes or "",
            }
            for a in allocations
        ],
        "performance": [
            {
                "Scope": p.scope,
                "Period": p.period,
                "Return %": p.return_pct,
                "Benchmark %": p.benchmark_return_pct,
                "Benchmark": p.benchmark_name or "",
                "Notes": p.notes or "",
            }
            for p in performance
        ],
    }


def page_cafr_plan_detail(plan_id: str) -> None:
    """Standalone detail page for one plan's CAFR extraction.

    Reached via ``?cafr_plan=<plan_id>``. Lives outside the tab system,
    which sidesteps the React reconciler issues the in-tab variant hit.
    """
    import pandas as pd

    data = _cafr_plan_detail_data(plan_id)
    if not data or not data.get("plan"):
        st.error(f"Plan '{plan_id}' not found.")
        if st.button("Back to dashboard"):
            st.query_params.clear()
            st.rerun()
        return

    plan = data["plan"]
    extract = data.get("extract") or {}
    doc = data.get("document") or {}
    allocations = data.get("allocations") or []
    performance = data.get("performance") or []

    plan_label = plan.get("abbreviation") or plan.get("name") or plan_id
    st.title(f"{plan_label} — CAFR detail")
    st.caption(plan["name"])

    if st.button("← Back to CAFR coverage"):
        st.query_params.clear()
        st.rerun()

    if not extract:
        st.info(
            "No CAFR extraction yet for this plan. "
            f"Run `python extract_cafr_investments.py {plan_id}` to populate."
        )
        return

    extracted_at = extract.get("extracted_at")
    fy = extract.get("fiscal_year")
    cols = st.columns(4)
    cols[0].metric("Fiscal Year", str(fy) if fy else "—")
    cols[1].metric("# Asset classes", len(allocations))
    cols[2].metric("# Performance rows", len(performance))
    cols[3].metric(
        "Extracted",
        extracted_at.strftime("%Y-%m-%d") if extracted_at else "—",
    )

    if doc.get("url"):
        st.markdown(
            f"**Source:** [{doc.get('filename') or doc['url']}]({doc['url']})"
        )
    pages_used = extract.get("pages_used")
    model_used = extract.get("model_used")
    if pages_used or model_used:
        st.caption(
            f"Pages used: {pages_used or '—'} · "
            f"Model: {model_used or '—'}"
        )

    # ---- Asset Allocation ----
    st.markdown("## Asset Allocation")
    if allocations:
        df_alloc = pd.DataFrame(allocations)
        df_alloc["Drift %"] = df_alloc.apply(
            lambda r: (r["Actual %"] - r["Target %"])
            if pd.notna(r["Actual %"]) and pd.notna(r["Target %"]) else None,
            axis=1,
        )
        st.dataframe(
            df_alloc,
            use_container_width=True,
            hide_index=True,
            height=min(60 + 35 * len(df_alloc), 700),
            column_config={
                "Target %": st.column_config.NumberColumn(format="%.2f"),
                "Actual %": st.column_config.NumberColumn(format="%.2f"),
                "Range low %": st.column_config.NumberColumn(format="%.2f"),
                "Range high %": st.column_config.NumberColumn(format="%.2f"),
                "Drift %": st.column_config.NumberColumn(
                    format="%+.2f",
                    help="Actual − Target (positive = overweight).",
                ),
            },
        )

        targets = df_alloc["Target %"].dropna()
        if len(targets) > 0:
            total = float(targets.sum())
            if abs(total - 100.0) > 1.0:
                st.warning(
                    f"Targets sum to {total:.2f}% — extraction may be "
                    "missing or duplicating an asset class."
                )
    else:
        st.info("No asset-allocation rows extracted.")

    # ---- Performance ----
    st.markdown("## Performance")
    if performance:
        df_perf = pd.DataFrame(performance)
        df_perf["vs Benchmark"] = df_perf.apply(
            lambda r: (r["Return %"] - r["Benchmark %"])
            if pd.notna(r["Return %"]) and pd.notna(r["Benchmark %"]) else None,
            axis=1,
        )
        st.dataframe(
            df_perf,
            use_container_width=True,
            hide_index=True,
            height=min(60 + 35 * len(df_perf), 700),
            column_config={
                "Return %": st.column_config.NumberColumn(format="%.2f"),
                "Benchmark %": st.column_config.NumberColumn(format="%.2f"),
                "vs Benchmark": st.column_config.NumberColumn(
                    format="%+.2f",
                    help="Return − Benchmark (positive = outperform).",
                ),
            },
        )

        # Pivot view: scope × period for total-fund-style overview
        pivot = df_perf.pivot_table(
            index="Scope", columns="Period",
            values="Return %", aggfunc="first",
        )
        if not pivot.empty and pivot.shape[0] > 1:
            with st.expander("Pivot: scope × period", expanded=False):
                st.dataframe(pivot, use_container_width=True)
    else:
        st.info("No performance rows extracted.")

    if extract.get("investment_policy_text"):
        with st.expander("Investment policy text", expanded=False):
            st.markdown(_safe_md(extract["investment_policy_text"]))


ASSET_ALLOCATION_VIEWS = (
    {
        "tab_name": "Private Equity",
        "match_patterns": ("%private equit%",),
        "exclude_patterns": ("%total other%",),
        "exact_label": "private equity",
    },
    {
        "tab_name": "Private Credit",
        "match_patterns": ("%private credit%", "%private debt%"),
        "exclude_patterns": ("%total other%",),
        "exact_label": "private credit",
    },
    {
        "tab_name": "Real Estate",
        "match_patterns": ("%real estate%",),
        "exclude_patterns": (),
        "exact_label": "real estate",
    },
    {
        "tab_name": "Real Assets",
        "match_patterns": ("%real asset%",),
        "exclude_patterns": (),
        "exact_label": "real assets",
    },
)


@st.cache_data(ttl=300)
def _allocation_df(match_patterns: tuple, exclude_patterns: tuple, exact_label: str):
    """Plans with both target and actual weights for a given asset class.

    Pulls the latest CAFR extract per plan, filters allocation rows whose
    asset_class matches any of `match_patterns` (case-insensitive LIKE)
    and matches none of `exclude_patterns`, and keeps only rows where both
    target_pct and actual_pct are populated. When a plan has multiple
    matching rows, the one whose asset_class equals `exact_label` is
    preferred; otherwise the first row is kept.
    """
    import pandas as pd
    from sqlalchemy import func, or_

    session = get_db_session()

    latest_extract_id = (
        session.query(func.max(CafrExtract.id))
        .filter(CafrExtract.plan_id == Plan.id)
        .correlate(Plan)
        .scalar_subquery()
    )

    asset_class_lower = func.lower(CafrAllocation.asset_class)
    match_clause = or_(*[asset_class_lower.like(p) for p in match_patterns])

    query = (
        session.query(
            Plan.id,
            Plan.name,
            Plan.abbreviation,
            Plan.state,
            CafrExtract.fiscal_year,
            CafrAllocation.asset_class,
            CafrAllocation.target_pct,
            CafrAllocation.actual_pct,
        )
        .join(CafrExtract, CafrExtract.id == latest_extract_id)
        .join(CafrAllocation, CafrAllocation.cafr_extract_id == CafrExtract.id)
        .filter(match_clause)
        .filter(CafrAllocation.target_pct.isnot(None))
        .filter(CafrAllocation.actual_pct.isnot(None))
    )
    for pat in exclude_patterns:
        query = query.filter(~asset_class_lower.like(pat))
    rows = query.all()

    df = pd.DataFrame(rows, columns=[
        "plan_id", "plan_name", "abbreviation", "state",
        "fiscal_year", "asset_class", "target_pct", "actual_pct",
    ])
    if df.empty:
        return df

    df["label"] = df["abbreviation"].fillna("").where(
        df["abbreviation"].astype(bool), df["plan_id"]
    )
    df["_priority"] = df["asset_class"].str.lower().eq(exact_label).astype(int)
    df = (
        df.sort_values(["plan_id", "_priority"], ascending=[True, False])
          .drop_duplicates(subset=["plan_id"], keep="first")
          .drop(columns="_priority")
          .reset_index(drop=True)
    )
    return df


def _render_allocation_view(df, asset_label: str) -> None:
    """Render the scatter chart + table for one asset-class view."""
    import altair as alt
    import pandas as pd

    if df.empty:
        st.info(f"No plans have both target and actual {asset_label.lower()} weights.")
        return

    st.subheader(f"{asset_label} — target vs actual weight ({len(df)} plans)")

    max_val = float(max(df["target_pct"].max(), df["actual_pct"].max()))
    domain_max = max(5.0, max_val * 1.10)
    domain = [0.0, domain_max]

    line_df = pd.DataFrame({"x": domain, "y": domain})

    base = alt.Chart(df).encode(
        x=alt.X(
            "target_pct:Q",
            title="Target weight (%)",
            scale=alt.Scale(domain=domain, nice=False),
        ),
        y=alt.Y(
            "actual_pct:Q",
            title="Actual weight (%)",
            scale=alt.Scale(domain=domain, nice=False),
        ),
    )

    points = base.mark_circle(size=120, opacity=0.75, color="#0066cc").encode(
        tooltip=[
            alt.Tooltip("plan_name:N", title="Plan"),
            alt.Tooltip("state:N", title="State"),
            alt.Tooltip("fiscal_year:Q", title="FY"),
            alt.Tooltip("asset_class:N", title="Asset class"),
            alt.Tooltip("target_pct:Q", title="Target %", format=".2f"),
            alt.Tooltip("actual_pct:Q", title="Actual %", format=".2f"),
        ],
    )

    labels = base.mark_text(
        align="left",
        baseline="middle",
        dx=7,
        fontSize=10,
        color="white",
    ).encode(text="label:N")

    diagonal = alt.Chart(line_df).mark_line(
        color="grey", strokeDash=[4, 4], strokeWidth=1,
    ).encode(x="x:Q", y="y:Q")

    chart = (diagonal + points + labels).properties(height=600)
    st.altair_chart(chart, use_container_width=True)

    table = df[[
        "plan_name", "plan_id", "state", "fiscal_year", "asset_class",
        "target_pct", "actual_pct",
    ]].copy()
    table["over_under_pct"] = (table["actual_pct"] - table["target_pct"]).round(2)
    table = table.rename(columns={
        "plan_name": "Plan",
        "plan_id": "Plan ID",
        "state": "State",
        "fiscal_year": "FY",
        "asset_class": "Asset Class",
        "target_pct": "Target %",
        "actual_pct": "Actual %",
        "over_under_pct": "Actual − Target",
    })
    sorted_table = table.sort_values("Actual − Target", ascending=False)
    centered_cols = ["FY", "Target %", "Actual %", "Actual − Target"]
    styled = (
        sorted_table.style
        .format({
            "FY": "{:.0f}",
            "Target %": "{:.2f}",
            "Actual %": "{:.2f}",
            "Actual − Target": "{:+.2f}",
        }, na_rep="")
        .set_properties(subset=centered_cols, **{"text-align": "center"})
    )
    st.dataframe(
        styled,
        use_container_width=True,
        hide_index=True,
    )


def page_asset_allocation():
    """Asset allocation tab — target vs actual weights from latest CAFR extracts."""
    st.title("Asset Allocation")
    st.caption(
        "Each point is one plan's policy target weight versus its actual "
        "weight, taken from the latest CAFR extract. The dashed 45° line is "
        "target = actual; points above are overweight, points below "
        "underweight."
    )

    sub_tabs = st.tabs([v["tab_name"] for v in ASSET_ALLOCATION_VIEWS])
    for tab, view in zip(sub_tabs, ASSET_ALLOCATION_VIEWS):
        with tab:
            df = _allocation_df(
                view["match_patterns"],
                view["exclude_patterns"],
                view["exact_label"],
            )
            _render_allocation_view(df, view["tab_name"])


def page_cafr():
    """CAFR coverage tab: per-plan view of CAFR + extraction status."""
    st.title("CAFR Coverage")
    st.caption(
        "One row per tracked plan — does it have a CAFR/ACFR in the DB, "
        "what fiscal year does the latest one cover, and have we extracted "
        "the Investment Section into structured asset-allocation and "
        "performance rows."
    )

    df = _cafr_coverage_df()
    if df.empty:
        st.warning("No plans in the database yet.")
        return

    total = len(df)
    extracted = int((df["Status"] == "Extracted").sum())
    pending = int((df["Status"] == "Pending extract").sum())
    missing = int((df["Status"] == "Missing CAFR").sum())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Plans tracked", total)
    c2.metric("Extracted", extracted)
    c3.metric("Pending extract", pending)
    c4.metric("Missing CAFR", missing)

    status_filter = st.multiselect(
        "Filter by status",
        ["Extracted", "Pending extract", "Missing CAFR"],
        default=[],
        key="cafr_status_filter",
    )
    view = df[df["Status"].isin(status_filter)] if status_filter else df

    st.dataframe(
        view.drop(columns=["plan_id"]),
        use_container_width=True,
        hide_index=True,
        column_config={
            "Source": st.column_config.LinkColumn(
                "Source",
                display_text="open PDF",
                help="Direct link to the latest CAFR PDF for this plan.",
            ),
        },
    )

    st.caption(
        "Click any plan's row in the table to view its CAFR detail. Or use "
        "the deep-link below."
    )

    # A small selectbox→link affordance to drill into the detail page,
    # since stDataFrame's native row selection is fiddly.
    extracted_view = df[df["Status"] == "Extracted"].copy()
    if not extracted_view.empty:
        labels = ["—"] + [
            f"{r['Plan']} — {r['Name']} (FY{r['Extract FY']})"
            for _, r in extracted_view.iterrows()
        ]
        ids_by_label = {
            "—": None,
            **{
                f"{r['Plan']} — {r['Name']} (FY{r['Extract FY']})": r["plan_id"]
                for _, r in extracted_view.iterrows()
            },
        }
        choice = st.selectbox(
            "Open detail page for plan",
            labels,
            key="cafr_plan_detail_choice",
        )
        plan_id = ids_by_label.get(choice)
        if plan_id:
            st.markdown(
                f"[Open detail for {choice} →](?cafr_plan={plan_id})"
            )

    st.download_button(
        "Download CSV",
        df.drop(columns=["plan_id"]).to_csv(index=False),
        "cafr_coverage.csv",
        "text/csv",
    )


@st.cache_data(ttl=300)
def _rfp_records_df():
    """Build a flat DataFrame from RFPRecord rows + their JSON payloads."""
    import pandas as pd

    session = get_db_session()
    rows = (
        session.query(RFPRecord, Document, Plan)
        .join(Document, RFPRecord.document_id == Document.id)
        .outerjoin(Plan, RFPRecord.plan_id == Plan.id)
        .order_by(RFPRecord.extracted_at.desc())
        .all()
    )
    out = []
    for r, doc, plan in rows:
        try:
            payload = json.loads(r.record)
        except Exception:
            payload = {}
        src = payload.get("source_document") or {}
        shortlist = payload.get("shortlisted_managers") or []
        plan_name = (plan.name if plan else "") or r.plan_id
        plan_abbr = (plan.abbreviation if plan else "") or r.plan_id
        out.append({
            "plan_id": r.plan_id,
            "Plan": plan_name,
            "Abbrev": plan_abbr,
            "Type": payload.get("rfp_type", ""),
            "Title": payload.get("title", ""),
            "Status": payload.get("status", ""),
            "Asset class": payload.get("asset_class") or "",
            "Mandate $M": payload.get("mandate_size_usd_millions"),
            "Released": payload.get("release_date") or "",
            "Due": payload.get("response_due_date") or "",
            "Awarded": payload.get("award_date") or "",
            "Incumbent": payload.get("incumbent_manager") or "",
            "Shortlist": ", ".join(shortlist) if shortlist else "",
            "Awarded mgr": payload.get("awarded_manager") or "",
            "Confidence": r.extraction_confidence,
            "Needs review": "Yes" if r.needs_review else "",
            "Source": src.get("url") or doc.url or "",
            "Source page": src.get("page_number"),
            "Doc": f"?doc={r.document_id}",
            "Extracted": r.extracted_at.strftime("%Y-%m-%d") if r.extracted_at else "",
            "rfp_id": r.rfp_id,
        })
    return pd.DataFrame(out)


@st.cache_data(ttl=300)
def _rfp_health_summary():
    """Aggregate counts from document_health + pipeline_runs for the header."""
    session = get_db_session()
    from sqlalchemy import func
    verdict_counts = dict(
        session.query(DocumentHealth.stage1_verdict,
                      func.count(DocumentHealth.document_id))
        .group_by(DocumentHealth.stage1_verdict).all()
    )
    last_run = (
        session.query(PipelineRun)
        .order_by(PipelineRun.started_at.desc())
        .first()
    )
    last_run_started_at = last_run.started_at if last_run else None
    return {
        "verdicts": verdict_counts,
        "last_run_started_at": last_run_started_at,
    }


def page_rfp(plan_id, plan_label):
    """RFP records tab: extracted RFP/Manager/Consultant searches."""
    st.title("RFPs and Manager Searches")
    st.caption(
        "Structured RFP records extracted from board materials by the "
        "rfp/orchestrator pipeline. Each row links back to the source "
        "document and page where the RFP was found."
    )

    df = _rfp_records_df()
    health = _rfp_health_summary()

    if plan_id and not df.empty:
        df = df[df["plan_id"] == plan_id]

    total = len(df)
    needs_review = int((df["Needs review"] == "Yes").sum()) if total else 0
    distinct_plans = df["plan_id"].nunique() if total else 0
    last_run_started_at = health.get("last_run_started_at")
    last_run_str = (
        last_run_started_at.strftime("%Y-%m-%d %H:%M")
        if last_run_started_at else "—"
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("RFP records", total)
    c2.metric("Plans with RFPs", distinct_plans)
    c3.metric("Needs review", needs_review)
    c4.metric("Last extraction", last_run_str)

    verdicts = health.get("verdicts") or {}
    if verdicts:
        st.caption(
            "Document diagnostic verdicts across the corpus: "
            + " · ".join(f"**{k}** {v}" for k, v in sorted(verdicts.items()))
        )

    if df.empty:
        st.info(
            "No RFP records yet. Run a backfill: "
            "`python -m scripts.run_rfp_extraction --limit 100`."
        )
        return

    # Filters
    col_a, col_b, col_c = st.columns(3)
    with col_a:
        type_filter = st.multiselect(
            "Type",
            sorted([t for t in df["Type"].dropna().unique() if t]),
            default=[],
            key="rfp_type_filter",
        )
    with col_b:
        status_filter = st.multiselect(
            "Status",
            sorted([s for s in df["Status"].dropna().unique() if s]),
            default=[],
            key="rfp_status_filter",
        )
    with col_c:
        review_only = st.checkbox(
            "Show only 'needs review'",
            value=False, key="rfp_needs_review_only",
        )

    view = df.copy()
    if type_filter:
        view = view[view["Type"].isin(type_filter)]
    if status_filter:
        view = view[view["Status"].isin(status_filter)]
    if review_only:
        view = view[view["Needs review"] == "Yes"]

    st.dataframe(
        view.drop(columns=["rfp_id", "plan_id"]),
        use_container_width=True,
        hide_index=True,
        column_config={
            "Mandate $M": st.column_config.NumberColumn(format="%.1f"),
            "Confidence": st.column_config.NumberColumn(format="%.2f"),
            "Source": st.column_config.LinkColumn(
                "Source", display_text="open PDF",
                help="Direct link to the source document.",
            ),
            "Doc": st.column_config.LinkColumn(
                "Doc", display_text="view summary",
                help="Open this document's summary in the app.",
            ),
        },
    )

    st.download_button(
        "Download CSV",
        view.to_csv(index=False),
        "rfp_records.csv",
        "text/csv",
    )


def page_admin():
    """Admin views: pipeline / data-quality diagnostics for the site owner."""
    st.title("Admin")
    (tab_report, tab_runs, tab_coverage, tab_backlog, tab_failed,
     tab_cafr, tab_cafr_refreshes) = st.tabs(
        ["Report Card", "Recent Runs", "Plan Coverage", "Pipeline Backlog",
         "Failed Docs", "CAFR Coverage", "CAFR Refreshes"]
    )

    with tab_report:
        _render_admin_report_card()

    with tab_runs:
        _render_recent_runs()

    with tab_coverage:
        st.caption(
            "One row per tracked plan — counts of documents downloaded, "
            "text extracted, Claude summaries generated, plus the timestamp "
            "of the most recent document download for that plan."
        )
        df = _admin_plan_coverage_df()

        if df.empty:
            st.warning("No plans in the database yet.")
            return

        # Headline totals at the top
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Plans tracked", len(df))
        c2.metric("Documents downloaded", int(df["Downloaded"].sum()))
        c3.metric("Documents extracted", int(df["Extracted"].sum()))
        c4.metric("Documents summarized", int(df["Summarized"].sum()))

        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
        )

    with tab_backlog:
        st.caption(
            "Plans where the pipeline has work outstanding — either "
            "downloaded documents that haven't been extracted, or "
            "extracted text that hasn't been summarised. Rows where "
            "Downloaded == Extracted == Summarized are hidden."
        )
        df = _admin_plan_coverage_df()
        if df.empty:
            st.warning("No plans in the database yet.")
        else:
            # Compute the two backlog deltas and filter
            df = df.copy()
            df["Extract pending"] = df["Downloaded"] - df["Extracted"]
            df["Summarize pending"] = df["Extracted"] - df["Summarized"]
            backlog = df[
                (df["Extract pending"] > 0) | (df["Summarize pending"] > 0)
            ].copy()

            if backlog.empty:
                st.success(
                    "Pipeline is fully caught up — every downloaded document "
                    "has been extracted and summarised."
                )
            else:
                c1, c2, c3 = st.columns(3)
                c1.metric("Plans with backlog", len(backlog))
                c2.metric(
                    "Extract pending", int(backlog["Extract pending"].sum())
                )
                c3.metric(
                    "Summarize pending", int(backlog["Summarize pending"].sum())
                )

                # Reorder columns to put the backlog deltas next to the counts
                cols = [
                    "Plan", "Abbrev", "State",
                    "Downloaded", "Extracted", "Summarized",
                    "Extract pending", "Summarize pending",
                    "Last download",
                ]
                st.dataframe(
                    backlog[cols].sort_values(
                        ["Summarize pending", "Extract pending"],
                        ascending=False,
                    ),
                    use_container_width=True,
                    hide_index=True,
                )

                st.info(
                    "Close the backlog by running the pipeline with the step "
                    "that applies:\n\n"
                    "`python pipeline.py --extract-only` — fills both Extract "
                    "and Summarize gaps (extractor runs, then summariser).\n\n"
                    "`python pipeline.py --summarize-only` — summarises any "
                    "documents that have already been extracted but not yet "
                    "processed by Claude."
                )

    with tab_failed:
        _render_failed_docs()

    with tab_cafr:
        _render_cafr_coverage()

    with tab_cafr_refreshes:
        _render_cafr_refreshes()


def page_approval_action(raw_token: str, action: str):
    """Handle ?approve=<token> / ?reject=<token>.

    Looks up the token, applies the action atomically, and renders a
    confirmation page. On approve, also triggers the publish step.
    """
    from insights import approval as _approval, publish as _publish

    if action not in ("approve", "reject"):
        st.error(f"Invalid action: {action}")
        return

    try:
        publication = _approval.consume_token(raw_token, expected_action=action)
    except _approval.TokenError as exc:
        st.title("Token error")
        st.error(str(exc))
        st.caption(
            "If you believe this is a mistake, the publication may already "
            "have been actioned. Check the Drafts tab."
        )
        if st.button("Go to dashboard"):
            st.query_params.clear()
            st.rerun()
        return

    if action == "approve":
        try:
            _publish.publish(publication)
            session = get_session()
            try:
                pub = session.get(Publication, publication.id)
                pub.status = "published"
                pub.published_at = datetime.utcnow()
                session.commit()
                publication = pub
            finally:
                session.close()
        except Exception as exc:
            st.title("Approve succeeded — publish failed")
            st.error(f"The draft was approved but publishing failed: {exc}")
            st.caption(
                "The publication is in 'approved' status. You can retry "
                "the publish step manually."
            )
            return

    st.title(f"{action.title()}d")
    st.success(
        f"Publication #{publication.id} ({publication.cadence}, "
        f"{publication.period_start.isoformat()}) is now "
        f"**{publication.status}**."
    )
    if action == "approve":
        st.caption("Render auto-deploy will pick up the push within a few minutes.")
    if st.button("Back to dashboard"):
        st.query_params.clear()
        st.rerun()


def page_drafts():
    """List publications awaiting founder approval."""
    st.title("Drafts awaiting approval")
    st.caption(
        "CIO Insights publications generated by the scheduler that haven't "
        "yet been approved or rejected. The approval link in the email is "
        "the canonical way to act on these — this view is for visibility."
    )

    session = get_db_session()
    rows = (
        session.query(Publication)
        .filter(Publication.status == "awaiting_approval")
        .order_by(Publication.composed_at.desc())
        .all()
    )

    if not rows:
        st.info("No drafts awaiting approval right now.")
        return

    for pub in rows:
        composed = pub.composed_at.strftime("%Y-%m-%d %H:%M") if pub.composed_at else "—"
        expires = pub.expires_at.strftime("%Y-%m-%d %H:%M") if pub.expires_at else "—"
        period = f"{pub.period_start.isoformat()} – {pub.period_end.isoformat()}"
        with st.expander(
            f"**{pub.cadence.title()}** — {period} (composed {composed})",
            expanded=False,
        ):
            c1, c2, c3 = st.columns(3)
            c1.metric("Composed", composed)
            c2.metric("Expires", expires)
            c3.metric("Sources",
                      len(pub.source_publication_ids or [])
                      if pub.source_publication_ids else "—")
            if pub.draft_markdown:
                st.markdown("**Preview**")
                st.markdown(_safe_md(pub.draft_markdown[:2000]))
                if len(pub.draft_markdown) > 2000:
                    st.caption(f"… {len(pub.draft_markdown) - 2000:,} more chars in full draft")
            if pub.pdf_path and Path(pub.pdf_path).exists():
                st.download_button(
                    "Download draft PDF",
                    data=Path(pub.pdf_path).read_bytes(),
                    file_name=Path(pub.pdf_path).name,
                    mime="application/pdf",
                )


def page_insights():
    """List approved/published CIO Insights publications with PDF downloads."""
    st.title("Published CIO Insights")
    st.caption(
        "Every CIO Insights publication that has cleared the approval flow. "
        "The 'Notes' tab still serves the live versions; this view is the "
        "audit trail."
    )

    session = get_db_session()
    rows = (
        session.query(Publication)
        .filter(Publication.status.in_(("approved", "published")))
        .order_by(Publication.period_start.desc())
        .all()
    )
    if not rows:
        st.info("No approved publications yet.")
        return

    for pub in rows:
        period = f"{pub.period_start.isoformat()} – {pub.period_end.isoformat()}"
        when = pub.published_at or pub.approved_at
        when_str = when.strftime("%Y-%m-%d %H:%M") if when else "—"
        with st.expander(
            f"**{pub.cadence.title()}** — {period} ({pub.status}, {when_str})",
            expanded=False,
        ):
            if pub.draft_markdown:
                st.markdown(_safe_md(pub.draft_markdown))
            if pub.pdf_path and Path(pub.pdf_path).exists():
                st.download_button(
                    "Download PDF",
                    data=Path(pub.pdf_path).read_bytes(),
                    file_name=Path(pub.pdf_path).name,
                    mime="application/pdf",
                    key=f"insights_pdf_{pub.id}",
                )


def main():
    plan_id, plan_label = render_sidebar()

    # Approval routing — handled before tabs so the magic-link click
    # lands on the action page rather than the dashboard.
    approve_param = st.query_params.get("approve")
    reject_param = st.query_params.get("reject")
    if approve_param:
        page_approval_action(approve_param, "approve")
        return
    if reject_param:
        page_approval_action(reject_param, "reject")
        return

    # Handle deep-link to a specific document
    doc_param = st.query_params.get("doc")
    if doc_param:
        try:
            page_document_detail(int(doc_param))
        except (ValueError, TypeError):
            st.error(f"Invalid document ID: {doc_param}")
        return

    cafr_plan_param = st.query_params.get("cafr_plan")
    if cafr_plan_param:
        page_cafr_plan_detail(cafr_plan_param)
        return

    tabs = st.tabs([
        "Notes", "Summary", "Updates", "Search", "Browse Recent",
        "Investment Actions", "Managers", "RFPs", "CAFR", "Asset Allocation",
        "Plans", "Drafts", "Insights", "Admin",
    ])

    with tabs[0]:
        page_notes()
    with tabs[1]:
        page_summary_updates(plan_id, plan_label)
    with tabs[2]:
        page_updates(plan_id, plan_label)
    with tabs[3]:
        page_search(plan_id, plan_label)
    with tabs[4]:
        page_browse(plan_id, plan_label)
    with tabs[5]:
        page_investment_actions(plan_id, plan_label)
    with tabs[6]:
        page_managers()
    with tabs[7]:
        page_rfp(plan_id, plan_label)
    with tabs[8]:
        page_cafr()
    with tabs[9]:
        page_asset_allocation()
    with tabs[10]:
        page_plans()
    with tabs[11]:
        page_drafts()
    with tabs[12]:
        page_insights()
    with tabs[13]:
        page_admin()


if __name__ == "__main__":
    main()
