"""
Streamlit UI — search and browse pension plan meeting documents and summaries.

Run with: streamlit run app.py
"""

import io
import json
import os
import re
import textwrap
from datetime import datetime
from pathlib import Path

import streamlit as st
from dotenv import load_dotenv

from database import Document, Plan, Summary, get_session, init_db, search_summaries, get_new_meetings

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
    plans = session.query(Plan).order_by(Plan.aum_billions.desc()).all()

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


def _markdown_to_pdf_bytes(title: str, date_str: str, markdown_text: str) -> bytes:
    """Convert a markdown note to a PDF using reportlab."""
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, HRFlowable
    from reportlab.lib import colors

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=20 * mm, rightMargin=20 * mm,
        topMargin=20 * mm, bottomMargin=20 * mm,
    )

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "NoteTitle", parent=styles["Heading1"],
        fontSize=16, spaceAfter=4, textColor=colors.HexColor("#003366"),
    )
    date_style = ParagraphStyle(
        "NoteDate", parent=styles["Normal"],
        fontSize=9, textColor=colors.grey, spaceAfter=10,
    )
    h2_style = ParagraphStyle(
        "NoteH2", parent=styles["Heading2"],
        fontSize=12, spaceBefore=10, spaceAfter=4,
        textColor=colors.HexColor("#003366"),
    )
    body_style = ParagraphStyle(
        "NoteBody", parent=styles["Normal"],
        fontSize=10, leading=14, spaceAfter=6,
    )
    bullet_style = ParagraphStyle(
        "NoteBullet", parent=body_style,
        leftIndent=12, bulletIndent=0,
    )

    story = [
        Paragraph(title, title_style),
        Paragraph(date_str, date_style),
        HRFlowable(width="100%", thickness=1, color=colors.HexColor("#003366"), spaceAfter=8),
    ]

    for line in markdown_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("---") or stripped.startswith("*Generated"):
            continue
        if stripped.startswith("## "):
            story.append(Spacer(1, 4))
            story.append(Paragraph(stripped[3:], h2_style))
        elif stripped.startswith("# "):
            pass  # already in title
        elif stripped.startswith("- ") or stripped.startswith("* "):
            text = stripped[2:].replace("**", "<b>", 1).replace("**", "</b>", 1)
            story.append(Paragraph(f"• {text}", bullet_style))
        else:
            # Bold markers
            text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", stripped)
            text = re.sub(r"\*(.+?)\*", r"<i>\1</i>", text)
            story.append(Paragraph(text, body_style))

    doc.build(story)
    return buf.getvalue()


def _notes_md_to_html(content: str) -> str:
    """Convert notes markdown to HTML with inline styles, bypassing Streamlit's renderer."""
    def inline(text: str) -> str:
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


def page_notes():
    tab_trends, tab_week = st.tabs(["2026 Agenda Trends", "7-Day Highlights"])

    with tab_trends:
        st.title("2026 Meeting Agenda Trends")
        _render_note_page(
            md_path=NOTES_DIR / "2026_meeting_trends_summary.md",
            title="2026 Meeting Agenda Trends",
            generated_date="April 7, 2026",
            pdf_filename="2026_meeting_agenda_trends.pdf",
        )

    with tab_week:
        st.title("7-Day Highlights")
        _render_note_page(
            md_path=NOTES_DIR / "7day_highlights_2026-04-07.md",
            title="7-Day Highlights: April 1–7, 2026",
            generated_date="April 7, 2026",
            pdf_filename="7day_highlights_2026-04-07.pdf",
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

def main():
    plan_id, plan_label = render_sidebar()

    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
        "Notes", "Summary", "Updates", "Search", "Browse Recent", "Investment Actions", "Plans"
    ])

    with tab1:
        page_notes()
    with tab2:
        page_summary_updates(plan_id, plan_label)
    with tab3:
        page_updates(plan_id, plan_label)
    with tab4:
        page_search(plan_id, plan_label)
    with tab5:
        page_browse(plan_id, plan_label)
    with tab6:
        page_investment_actions(plan_id, plan_label)
    with tab7:
        page_plans()


if __name__ == "__main__":
    main()
