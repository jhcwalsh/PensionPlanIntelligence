"""
Streamlit UI — search and browse pension plan meeting documents and summaries.

Run with: streamlit run app.py
"""

import json
import os
from datetime import datetime
from pathlib import Path

import streamlit as st
from dotenv import load_dotenv

from database import Document, Plan, Summary, get_session, init_db, search_summaries

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
    st.sidebar.subheader("Pipeline")
    if st.sidebar.button("Run Pipeline (all plans)", use_container_width=True):
        with st.spinner("Running pipeline..."):
            from pipeline import run_pipeline
            run_pipeline()
        st.success("Pipeline complete!")
        st.rerun()

    col1, col2 = st.sidebar.columns(2)
    with col1:
        if st.button("Fetch Only", use_container_width=True):
            with st.spinner("Fetching..."):
                from fetcher import run_fetcher
                run_fetcher(plan_ids=[selected_plan_id] if selected_plan_id else None)
            st.success("Done")
            st.rerun()
    with col2:
        if st.button("Summarize", use_container_width=True):
            with st.spinner("Summarizing..."):
                from extractor import run_extractor
                from summarizer import run_summarizer
                run_extractor()
                run_summarizer()
            st.success("Done")
            st.rerun()

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
        st.markdown(f"**Summary**\n\n{summary.summary_text}")

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

    tab1, tab2, tab3, tab4 = st.tabs([
        "Search", "Browse Recent", "Investment Actions", "Plans"
    ])

    with tab1:
        page_search(plan_id, plan_label)
    with tab2:
        page_browse(plan_id, plan_label)
    with tab3:
        page_investment_actions(plan_id, plan_label)
    with tab4:
        page_plans()


if __name__ == "__main__":
    main()
