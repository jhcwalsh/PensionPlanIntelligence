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
    Document,
    DocumentHealth,
    PipelineRun,
    Plan,
    Publication,
    RFPRecord,
    Summary,
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


def do_search(query, plan_id=None):
    session = get_db_session()
    pid = plan_id if plan_id and plan_id != "All" else None
    return search_summaries(session, query, plan_id=pid, limit=30)


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
        st.markdown(f"**Summary**\n\n{_safe_md(summary.summary_text)}")

        if key_topics:
            tags_html = " ".join(f'<span class="tag">{t}</span>' for t in key_topics[:8])
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
                    st.markdown(f"- {desc}{amt_str}" + (f" — *{ac}*" if ac else ""))

            if decisions:
                st.markdown("**Decisions**")
                for d in decisions[:5]:
                    vote = d.get("vote", "")
                    vote_str = f" [{vote}]" if vote else ""
                    st.markdown(f"- {d.get('description', '')}{vote_str}")

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

def page_search(plan_id, plan_label):
    st.title("Search Meeting Documents")

    query = st.text_input("Search summaries, topics, investment actions...",
                          placeholder='e.g. "infrastructure" or "private equity mandate" or "BlackRock"')

    if query:
        results = do_search(query, plan_id=plan_id)
        st.caption(f"{len(results)} results for **{query}**"
                   + (f" in {plan_label}" if plan_label != "All" else ""))

        if not results:
            st.info("No results found. Try different search terms.")
        for doc, summary in results:
            render_summary_card(doc, summary, highlight=query)
    else:
        st.info("Enter a search term above to find relevant meeting content.")


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
    tab_coverage, tab_backlog = st.tabs(["Plan Coverage", "Pipeline Backlog"])

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
        "Investment Actions", "RFPs", "CAFR", "Asset Allocation",
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
        page_rfp(plan_id, plan_label)
    with tabs[7]:
        page_cafr()
    with tabs[8]:
        page_asset_allocation()
    with tabs[9]:
        page_plans()
    with tabs[10]:
        page_drafts()
    with tabs[11]:
        page_insights()
    with tabs[12]:
        page_admin()


if __name__ == "__main__":
    main()
