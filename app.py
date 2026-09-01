"""
Streamlit UI — search and browse pension plan meeting documents and summaries.

Run with: streamlit run app.py
"""

import json
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

import streamlit as st
from dotenv import load_dotenv

import database
import queries
from database import (
    utcnow,
    sort_key as db_sort_key,
    Document,
    Plan,
    Summary,
    aggregate_managers,
    count_search_summaries,
    get_new_meetings,
    get_session,
    get_twin_snapshot,
    init_db,
    search_summaries,
)
from twin_builder import visible_relationships
from video_storage import RECORDINGS_DIR, recording_path

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="PensionGraph",
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

def _apply_column_migrations() -> None:
    """Add columns present on the ORM model but missing from a local SQLite file.

    Vestigial as of the 2026-08-21 Postgres cutover, and deliberately kept for
    local SQLite work only. It existed because Render's Streamlit read a
    persistent disk that the GHA cron's ``git push`` of ``db/pension.db`` never
    updated, so a new column had to be re-applied at startup — there is no disk
    and no committed database now, and Render never reaches the body below.

    The gap it patches is real on both backends: ``init_db()`` creates missing
    tables, never missing columns. On Postgres that is handled by migrating
    deliberately rather than by guessing at startup, which is why this stays
    SQLite-only rather than being generalised.

    Each guarded ALTER is safe to re-run on a database that already has the
    column.
    """
    import sqlite3
    import os
    if not database.engine.dialect.name == "sqlite":
        return
    db_path = os.environ.get("DB_PATH", str(Path(__file__).parent / "db" / "pension.db"))
    if not os.path.exists(db_path):
        return
    conn = sqlite3.connect(db_path)
    try:
        existing = {row[1] for row in conn.execute("PRAGMA table_info(publications)").fetchall()}
        if "subscribers_notified_at" not in existing:
            conn.execute("ALTER TABLE publications ADD COLUMN subscribers_notified_at DATETIME")
            conn.commit()
    finally:
        conn.close()


@st.cache_resource
def _db_session_singleton():
    init_db()
    _apply_column_migrations()
    return get_session()


def get_db_session():
    """The app's shared Session, guaranteed not to be mid-transaction.

    The rollback is defensive: it heals a session left dirty by an earlier
    rerun that raised between its first query and _release_db_session().
    Without it, one traceback poisons the cached session for the life of
    the process.
    """
    session = _db_session_singleton()
    if session.in_transaction():
        try:
            session.rollback()
        except Exception:                      # noqa: BLE001
            # A rollback on a connection the server already terminated
            # raises. Discarding it is the point -- the next query then
            # checks out a fresh, pre-pinged connection from the pool.
            session.close()
    return session


# get_db_session() used to be the @st.cache_resource'd function itself, so
# callers (tests/test_twin_page_data.py) reach for .clear() on it to drop the
# cached Session. Keep that contract pointing at whatever now holds the cache.
get_db_session.clear = _db_session_singleton.clear


def _release_db_session() -> None:
    """End the transaction at the end of a Streamlit rerun.

    A SQLAlchemy Session opens a transaction on its first query and holds
    it -- together with its connection -- until commit or rollback. This
    app caches one Session for the life of the process, so between reruns
    that connection sat *idle inside a transaction*. Neon terminates those
    after five minutes, and the next rerun died on the first query of
    render_sidebar() with IdleInTransactionSessionTimeout.

    pool_pre_ping and pool_recycle=300 were already set on the engine and
    could not help: both act when a connection is checked out of the pool,
    and a Session mid-transaction never returns its connection to the pool
    at all. Rolling back is what hands it back, so those two settings
    finally apply.

    database.py's create_app_engine() comment already said short sessions
    were the requirement rather than a courtesy. pipeline.py was fixed for
    this at the cutover; the Streamlit app was not.
    """
    try:
        session = _db_session_singleton()
    except Exception:                          # noqa: BLE001
        return                                 # never built one; nothing to end
    try:
        if session.in_transaction():
            session.rollback()
    except Exception:                          # noqa: BLE001
        try:
            session.close()
        except Exception:                      # noqa: BLE001
            pass


def load_plans():
    return queries.plans(get_db_session())


def load_recent_summaries(plan_id=None, limit=20):
    return queries.recent_summaries(get_db_session(), plan_id, limit)


def get_stats():
    return queries.corpus_stats(get_db_session())


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
        headers = {"User-Agent": "Mozilla/5.0 (compatible; PensionGraph/1.0)"}
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
    st.sidebar.markdown(
        "<h1 style='font-size:1.75rem;margin:0 0 1rem 0;font-weight:600;'>"
        "<a href='?' target='_self' style='color:inherit;text-decoration:none;'>"
        "🏛️ PensionGraph</a></h1>",
        unsafe_allow_html=True,
    )
    st.sidebar.markdown("---")

    plans = load_plans()
    plan_options = ["All"] + [p.abbreviation or p.name for p in plans]
    plan_map = {"All": None}
    plan_map.update({(p.abbreviation or p.name): p.id for p in plans})

    # Only four tabs consume the returned plan_id — main() passes it to
    # Activity, Search, Investment Actions and Meeting Recordings. The other
    # five ignore it, so without this caption the control looks broken on
    # more than half the app: you pick a plan, nothing changes, and there is
    # no way to tell whether the filter failed or the tab has no data.
    selected_label = st.sidebar.selectbox("Filter by Plan", plan_options)
    selected_plan_id = plan_map.get(selected_label)
    st.sidebar.caption(
        "Filters Activity, Search, Investment Actions and Meeting Recordings.")

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


def page_activity(plan_id, plan_label):
    """Recency-driven activity feed with three view modes.

    Replaces the previous three browse-mode tabs (Summary / Updates / Browse
    Recent) — same underlying data, different groupings, behind a single tab
    with a view-mode segmented control. The plan filter and the look-back
    control are scoped per view.
    """
    st.title("Recent Activity")
    col_view, col_sort = st.columns([2, 1])
    with col_view:
        view = st.radio(
            "View",
            options=["By plan", "By meeting", "By document"],
            horizontal=True,
            key="activity_view",
            label_visibility="collapsed",
        )
    with col_sort:
        sort = st.radio(
            "Sort",
            options=["Most recent", "Alphabetical"],
            horizontal=True,
            key="activity_sort",
            label_visibility="collapsed",
        )
    if view == "By plan":
        _render_activity_by_plan(plan_id, plan_label, sort)
    elif view == "By meeting":
        _render_activity_by_meeting(plan_id, plan_label, sort)
    else:
        _render_activity_by_document(plan_id, plan_label, sort)


def _render_activity_by_document(plan_id, plan_label, sort: str = "Most recent"):
    """Recency-driven document feed — last N docs across the corpus.

    Sort: 'Most recent' (default, by meeting_date desc), or 'Alphabetical'
    which re-orders the recency-limited result set by plan abbreviation.
    """
    limit = st.slider("Show last N documents", 5, 100, 20, key="activity_doc_limit")
    results = load_recent_summaries(plan_id=plan_id if plan_label != "All" else None,
                                    limit=limit)
    if sort == "Alphabetical":
        results = sorted(
            results,
            key=lambda ds: ((ds[0].plan.abbreviation or ds[0].plan_id or "").lower(),
                            db_sort_key(ds[0].meeting_date)),
        )

    if not results:
        st.warning("No summarized documents yet. Run the pipeline to fetch and process documents.")
        st.code("python pipeline.py calpers  # fetch CalPERS documents")
        return

    st.caption(f"Showing {len(results)} most recent documents"
               + (f" for {plan_label}" if plan_label != "All" else " across all plans"))

    for doc, summary in results:
        render_summary_card(doc, summary)


@st.cache_data(ttl=300, show_spinner=False)
def _plans_index_rows() -> list[dict]:
    """Plans-tab index rows. Cached so Streamlit reruns don't refetch."""
    return queries.plans_index_rows(get_db_session())

def page_plans():
    """Digital-twin index — one row per tracked plan, linking to its twin page."""
    import pandas as pd

    st.title("Tracked Plans")
    rows = _plans_index_rows()

    if not rows:
        st.warning("No plans loaded yet. Run the pipeline to initialize.")
        return

    df = pd.DataFrame(rows)
    st.caption(
        f"**{len(rows)} plan(s)** tracked. Click a row's link to open its "
        "digital-twin detail page."
    )
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Twin": st.column_config.LinkColumn(
                "Twin",
                display_text="open twin →",
                help="Open the digital-twin detail page for this plan.",
            ),
        },
    )


def _truncate_words(text: str, max_words: int) -> tuple[str, bool]:
    """Return (truncated_text, was_truncated)."""
    words = text.split()
    if len(words) <= max_words:
        return text, False
    return " ".join(words[:max_words]) + "…", True


def _render_activity_by_plan(plan_id, plan_label, sort: str = "Most recent"):
    """Plan-grouped headline view — one snapshot per plan, expand for detail."""
    st.caption("One snapshot per plan — up to 100 words. Expand a plan for the full detail.")

    days = st.slider("Look back (days)", 1, 90, 14, key="activity_by_plan_days")
    session = get_db_session()
    _sort_alpha = sort == "Alphabetical"
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

    if _sort_alpha:
        sort_key = lambda kv: (
            (kv[1][0]["plan"].abbreviation or kv[1][0]["plan"].name or kv[0]).lower()
            if kv[1][0]["plan"] else kv[0]
        )
        sort_reverse = False
    else:
        sort_key = lambda kv: db_sort_key(kv[1][0]["meeting_date"])
        sort_reverse = True
    for pid, plan_meetings in sorted(by_plan.items(),
                                     key=sort_key,
                                     reverse=sort_reverse):
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


def _render_activity_by_meeting(plan_id, plan_label, sort: str = "Most recent"):
    """Per-meeting card view — full agenda summary + materials per meeting."""
    st.caption("New meetings detected since last pipeline run, with agenda summaries and links to materials.")

    days = st.slider("Look back (days)", 1, 90, 14, key="activity_by_meeting_days")
    session = get_db_session()
    meetings = get_new_meetings(session, days=days)

    if plan_id:
        meetings = [m for m in meetings if m["plan"] and m["plan"].id == plan_id]

    if not meetings:
        st.info(f"No new meetings found in the last {days} days. Run the pipeline to fetch updates.")
        return

    st.caption(f"**{len(meetings)} new meeting(s)** in the last {days} days"
               + (f" for {plan_label}" if plan_label != "All" else ""))

    if sort == "Alphabetical":
        meetings = sorted(
            meetings,
            key=lambda m: (
                ((m["plan"].abbreviation or m["plan"].name) if m["plan"] else "").lower(),
                -(m["meeting_date"].toordinal() if m["meeting_date"] else 0),
            ),
        )

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
    """Find the note for the Year-to-date tab.

    Shows the most recently *generated* year-scale synthesis — quarterly
    reviews and the annual year-in-review both qualify (both are written
    by ``insights.publish.publish``). Falls back to the legacy rolling
    YTD file if neither has been published yet.
    """
    candidates = []
    for path in (
        list(NOTES_DIR.glob("quarterly_cio_insights_*.md"))
        + list(NOTES_DIR.glob("annual_cio_insights_*.md"))
    ):
        content = path.read_text(encoding="utf-8")
        gen_match = re.search(r"\*Generated:\s*(.+?)\*", content)
        generated_date = gen_match.group(1).strip() if gen_match else "Unknown"
        try:
            sort_key = datetime.strptime(generated_date, "%B %d, %Y")
        except ValueError:
            sort_key = datetime.min
        first_line = content.split("\n")[0] if content else ""
        title = (first_line[2:].strip() if first_line.startswith("# ")
                 else path.stem)
        candidates.append((sort_key, path.name, path, title, generated_date))

    if candidates:
        # Newest Generated date first; filename as deterministic tie-break.
        candidates.sort(key=lambda c: (c[0], c[1]), reverse=True)
        _, _, path, title, generated_date = candidates[0]
        return (path, title, generated_date)

    path = NOTES_DIR / "2026_cio_insights.md"
    if not path.exists():
        return None
    content = path.read_text(encoding="utf-8")
    gen_match = re.search(r"\*Generated:\s*(.+?)\*", content)
    generated_date = gen_match.group(1).strip() if gen_match else "Unknown"
    return (path, "Insights: 2026 Institutional Trends", generated_date)


def _find_latest_insights_recent() -> tuple[Path, str, str] | None:
    """Find the latest Monthly Insights note for the Insights tab.

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
            title = f"Monthly Insights: {month}"
        else:
            title = "Monthly Insights"
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
    return (path, f"Insights: Past {days} Days", generated_date)



from insights.render import markdown_to_pdf_bytes as _markdown_to_pdf_bytes


_INLINE_CITE_RE = re.compile(
    # Match a parenthesised inline citation like:
    #   ([source](?doc=42))
    #   ([source 1](?doc=42), [source 2](?doc=58))
    # The "source" link-text prefix is the discriminator that distinguishes
    # these from the section-level *Sources:* footer links, which use
    # "[Plan — DocType — Date](?doc=N)" and should keep their full text.
    # An optional leading space is consumed so the superscript glues to the
    # preceding word, academic-paper style.
    r"\s?\(\s*"
    r"\[source[^\]]*\]\(\?doc=\d+\)"
    r"(?:\s*,\s*\[source[^\]]*\]\(\?doc=\d+\))*"
    r"\s*\)",
    flags=re.IGNORECASE,
)


def _shrink_inline_cites(text: str) -> str:
    """Compress noisy ``([source](?doc=N))`` citations into superscript ``[N]``
    links. The full bibliography is preserved by the section-level
    ``*Sources:*`` footer the composer also emits."""
    def repl(match: re.Match) -> str:
        ids = re.findall(r"\?doc=(\d+)", match.group(0))
        return "".join(
            f'<sup style="font-size:0.72em;line-height:0;margin:0 1px;">'
            f'<a href="?doc={i}" '
            f'style="color:#4A90D9;text-decoration:none;">[{i}]</a>'
            f'</sup>'
            for i in ids
        )
    return _INLINE_CITE_RE.sub(repl, text)


def _notes_md_to_html(content: str) -> str:
    """Convert notes markdown to HTML with inline styles, bypassing Streamlit's renderer."""
    def inline(text: str) -> str:
        # Inline citations first — must run before the generic link regex
        # so the "[source](?doc=N)" pattern doesn't get matched as a normal link.
        text = _shrink_inline_cites(text)
        # Links: [text](url) → <a>
        text = re.sub(
            r'\[([^\]]+)\]\(([^)]+)\)',
            r'<a href="\2" style="color:#4A90D9;text-decoration:underline;">\1</a>',
            text,
        )
        text = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', text)
        text = re.sub(r'\*(.+?)\*', r'<em>\1</em>', text)
        return text

    def split_row(s: str) -> list[str]:
        # Markdown tables wrap rows in optional leading/trailing pipes.
        inner = s.strip().strip("|")
        return [c.strip() for c in inner.split("|")]

    def is_table_separator(s: str) -> bool:
        # `| --- | :---: | ---: |` — at least one column of dashes per cell.
        if "|" not in s:
            return False
        return all(re.fullmatch(r":?-{3,}:?", c) for c in split_row(s) if c)

    lines = content.splitlines()
    parts: list[str] = []
    para: list[str] = []
    i = 0

    def flush():
        if para:
            parts.append(
                f'<p style="margin:0 0 14px;line-height:1.65;">{inline(" ".join(para))}</p>'
            )
            para.clear()

    while i < len(lines):
        line = lines[i]
        s = line.strip()

        # Markdown table: header row, separator row, then data rows.
        if (
            "|" in s
            and i + 1 < len(lines)
            and is_table_separator(lines[i + 1])
        ):
            flush()
            headers = split_row(s)
            i += 2  # past header + separator
            rows: list[list[str]] = []
            while i < len(lines) and "|" in lines[i] and lines[i].strip():
                rows.append(split_row(lines[i]))
                i += 1
            thead = "".join(
                f'<th style="text-align:left;padding:6px 10px;border-bottom:2px solid #888;'
                f'font-weight:600;">{inline(h)}</th>'
                for h in headers
            )
            tbody_rows = []
            for row in rows:
                cells = "".join(
                    f'<td style="padding:6px 10px;border-bottom:1px solid #444;'
                    f'vertical-align:top;">{inline(c)}</td>'
                    for c in row
                )
                tbody_rows.append(f"<tr>{cells}</tr>")
            parts.append(
                '<div style="overflow-x:auto;margin:0 0 14px;">'
                '<table style="border-collapse:collapse;width:100%;font-size:0.95em;'
                'line-height:1.45;">'
                f'<thead><tr>{thead}</tr></thead>'
                f'<tbody>{"".join(tbody_rows)}</tbody>'
                '</table></div>'
            )
            continue

        if s.startswith("## "):
            flush()
            parts.append(
                f'<h2 style="margin:28px 0 8px;font-size:1.25em;font-weight:600;">'
                f'{inline(s[3:])}</h2>'
            )
        elif s.startswith("### "):
            flush()
            parts.append(
                f'<h3 style="margin:20px 0 6px;font-size:1.1em;font-weight:600;">'
                f'{inline(s[4:])}</h3>'
            )
        elif s == "---":
            flush()
            parts.append('<hr style="margin:16px 0;border:none;border-top:1px solid #555;">')
        elif s.startswith("# "):
            pass  # skip H1 — shown via st.title
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
        i += 1

    flush()
    return "\n".join(parts)


def _render_ai_disclaimer():
    """Standard disclaimer shown above any AI-composed insight surface."""
    st.warning(
        "AI-generated summary — figures and attributions may be wrong; verify against the linked source documents."
    )


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

    _render_ai_disclaimer()
    st.divider()
    html = _notes_md_to_html(content)
    st.markdown(
        f'<div style="font-family:-apple-system,BlinkMacSystemFont,\'Segoe UI\','
        f'Arial,sans-serif;font-size:15px;color:inherit;">{html}</div>',
        unsafe_allow_html=True,
    )


def page_insights():
    # Weekly composition was retired on 2026-08-16; the tab stays because the
    # back catalogue of weekly briefings is still worth reading. It shows only
    # existing notes/ files and never gains new ones.
    (tab_week, tab_insights_monthly, tab_insights_year) = st.tabs([
        "Weekly Insights (archive)",
        "Monthly Insights",
        "Year to date Insights",
    ])

    with tab_insights_monthly:
        st.title("Monthly Insights")
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
        st.title("Year to date Insights")
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
        st.title("Weekly Insights")
        # Read the database, not notes/. Weekly composition sets
        # archive=False, so the last 7day_highlights_*.md file was written on
        # 2026-05-24 while the briefings themselves kept accumulating as
        # Publication rows. Globbing the old filename left this tab three
        # months stale with no error to show for it.
        weeklies = queries.weekly_briefings(get_db_session())
        if not weeklies:
            st.info("No weekly briefings published yet.")
        else:
            def _label(pub):
                return "Week of %s – %s" % (
                    pub.period_start.strftime("%b %d"),
                    pub.period_end.strftime("%b %d, %Y"))

            if len(weeklies) == 1:
                chosen = weeklies[0]
            else:
                idx = st.selectbox(
                    "Select week", range(len(weeklies)),
                    format_func=lambda i: _label(weeklies[i]),
                )
                chosen = weeklies[idx]

            published = chosen.published_at or chosen.composed_at
            st.caption("Generated: %s" % (
                published.strftime("%B %d, %Y") if published else "unknown"))
            st.warning(
                "AI-generated summary — figures and attributions may be wrong; "
                "verify against the linked source documents.")
            st.markdown(chosen.draft_markdown or "_No content._")


_DEPLOYMENT_ACTIONS = {"hire", "fire", "commitment"}
_POLICY_ACTIONS = {"rebalance", "allocation_change", "policy_change", "other"}


def _coerce_amount(val) -> float | None:
    """Tolerantly parse amount_millions which may be int / float / str / None."""
    if val in (None, "", "None"):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _excerpt(text: str, n: int) -> str:
    """Truncate ``text`` to ``n`` characters with an ellipsis when cut."""
    if not text:
        return ""
    text = text.replace("\n", " ").strip()
    return text if len(text) <= n else text[:n].rstrip() + "…"


def _fmt_amount(v) -> str:
    """Format a $M number with comma thousands; blank if missing."""
    a = _coerce_amount(v)
    return f"{a:,.1f}" if a is not None else ""


def page_investment_actions(plan_id, plan_label):
    st.title("Investment Actions")
    st.caption(
        "Manager hires/fires, allocation changes, and new commitments "
        "extracted from board packs. The 'Capital deployment' tab shows "
        "money-flow actions (hire/fire/commitment); 'Policy & rebalancing' "
        "shows allocation shifts, policy changes, and other governance items."
    )

    days_back = st.slider(
        "Look-back window (days)", min_value=30, max_value=365, value=90, step=30,
        key="invest_actions_days",
        help="Filters by document meeting date.",
    )
    cutoff = utcnow().date() - timedelta(days=days_back)

    results = queries.investment_action_docs(get_db_session(), plan_id, cutoff)

    if not results:
        st.info(f"No investment actions in the last {days_back} days.")
        return

    deployment_rows: list[dict] = []
    policy_rows: list[dict] = []
    for doc, summary in results:
        actions = parse_json_field(summary.investment_actions) or []
        for a in actions:
            row = {
                "Plan": doc.plan_id.upper(),
                "Date": doc.meeting_date.strftime("%Y-%m-%d") if doc.meeting_date else "",
                "Action": a.get("action", "") or "",
                "Manager": a.get("manager", "") or "",
                "Asset Class": a.get("asset_class", "") or "",
                "Amount ($M)": _coerce_amount(a.get("amount_millions")),
                "Description": a.get("description", "") or "",
                "doc_id": doc.id,
            }
            action = row["Action"]
            if action in _DEPLOYMENT_ACTIONS:
                deployment_rows.append(row)
            else:
                # Unknown / blank actions land in policy as a catch-all.
                policy_rows.append(row)

    tab_deploy, tab_policy = st.tabs([
        f"Capital deployment ({len(deployment_rows)})",
        f"Policy & rebalancing ({len(policy_rows)})",
    ])
    with tab_deploy:
        _render_deployment_actions(deployment_rows, days_back)
    with tab_policy:
        _render_policy_actions(policy_rows, days_back)


def _render_deployment_actions(rows: list[dict], days_back: int) -> None:
    """Hires, fires, commitments — money-flow view with amount filter."""
    import pandas as pd

    if not rows:
        st.info(f"No capital deployment actions in the last {days_back} days.")
        return

    col1, col2 = st.columns([1, 2])
    with col1:
        min_amount = st.slider(
            "Minimum amount ($M)", 0, 250, 10, step=5,
            key="invest_actions_min_amount",
            help=(
                "Drop small or unspecified amounts. Set to 0 to include "
                "rows with no amount captured."
            ),
        )
    with col2:
        action_types = sorted({r["Action"] for r in rows if r["Action"]})
        selected_actions = st.multiselect(
            "Action type",
            action_types,
            default=action_types,
            key="invest_actions_deploy_types",
        )

    if min_amount > 0:
        filtered = [
            r for r in rows
            if r["Action"] in selected_actions
            and r["Amount ($M)"] is not None
            and r["Amount ($M)"] >= min_amount
        ]
    else:
        filtered = [r for r in rows if r["Action"] in selected_actions]

    total_amount = sum(
        r["Amount ($M)"] for r in filtered if r["Amount ($M)"] is not None
    )
    plan_count = len({r["Plan"] for r in filtered})
    st.markdown(
        f"**{len(filtered)} actions** · "
        f"**${total_amount:,.0f}M total** · "
        f"across **{plan_count} plans** · "
        f"last **{days_back} days**"
    )

    if not filtered:
        st.info("No actions match the current filters.")
        return

    df = pd.DataFrame(filtered)
    df["Excerpt"] = df["Description"].map(lambda t: _excerpt(t, 100))
    df["Amount ($M)"] = df["Amount ($M)"].map(_fmt_amount)

    display = df[["Plan", "Date", "Action", "Manager", "Asset Class",
                  "Amount ($M)", "Excerpt"]]
    st.dataframe(display, use_container_width=True, hide_index=True)
    csv_df = df[["Plan", "Date", "Action", "Manager", "Asset Class",
                 "Amount ($M)", "Description", "doc_id"]]
    st.download_button(
        "Download CSV (full descriptions)",
        csv_df.to_csv(index=False),
        "capital_deployment.csv",
        "text/csv",
    )


def _render_policy_actions(rows: list[dict], days_back: int) -> None:
    """Rebalances, allocation changes, governance — descriptive view.

    'other' is hidden by default because most rows in that bucket are
    low-signal governance items (waivers, committee notes, etc.)."""
    import pandas as pd

    if not rows:
        st.info(f"No policy or rebalancing actions in the last {days_back} days.")
        return

    action_types = sorted({r["Action"] for r in rows if r["Action"]})
    default_selected = [t for t in action_types if t != "other"]

    selected = st.multiselect(
        "Action type",
        action_types,
        default=default_selected,
        key="invest_actions_policy_types",
        help="'other' is hidden by default — usually low-signal governance items.",
    )

    filtered = [r for r in rows if r["Action"] in selected]

    plan_count = len({r["Plan"] for r in filtered})
    st.markdown(
        f"**{len(filtered)} actions** · "
        f"across **{plan_count} plans** · "
        f"last **{days_back} days**"
    )

    if not filtered:
        st.info("No actions match the current filters.")
        return

    df = pd.DataFrame(filtered)
    df["Excerpt"] = df["Description"].map(lambda t: _excerpt(t, 140))

    display = df[["Plan", "Date", "Action", "Asset Class", "Excerpt"]]
    st.dataframe(display, use_container_width=True, hide_index=True)
    csv_df = df[["Plan", "Date", "Action", "Manager", "Asset Class",
                 "Description", "doc_id"]]
    st.download_button(
        "Download CSV (full descriptions)",
        csv_df.to_csv(index=False),
        "policy_actions.csv",
        "text/csv",
    )


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

            docs = queries.documents_by_ids(get_db_session(), r["doc_ids"])
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
        doc, plan, summary = queries.document_with_context(session, doc_id)
        if not doc:
            st.error(f"Document #{doc_id} not found.")
            return

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
        runs = queries.recent_fetch_runs(session, RECENT_RUNS_LIMIT)
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
                rows = queries.documents_for_run(session, doc_ids)
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
        ext_rows = queries.failed_extraction_rows(session)
        skip_rows = queries.skipped_document_rows(session)

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
        for plan_id, plan_name, _doc_id, filename, ext_reason in ext_rows:
            by_plan[plan_id]["name"] = plan_name
            by_plan[plan_id]["extraction"].append(
                f"{filename} *({ext_reason})*" if ext_reason else filename)
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

    st.caption(
        "Latest CAFR/ACFR fiscal year held per plan. 'FY2024+' is the "
        "current freshness target — anything older is a backfill gap. "
        "Plans with no CAFR at all need URL hygiene in known_plans.json."
    )

    session = get_session()
    try:
        summary = queries.cafr_coverage_summary(session)
        latest_by_plan = summary["latest_by_plan"]
        plans = summary["plans"]
        total_plans = len(plans)
        by_fy_rows = summary["by_fiscal_year"]
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
        recent_run_ats = queries.recent_cafr_refresh_runs(
            session, CAFR_REFRESH_LIMIT)
        if not recent_run_ats:
            st.info(
                "No CAFR refresh runs recorded yet. The next monthly cron "
                "(GHA + local Task Scheduler) will populate this tab."
            )
            return

        # Pull every row for those run_ats in one query
        rows = queries.cafr_refresh_rows(session, recent_run_ats)
        plans = queries.plan_labels(session)
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
    """Per-plan coverage table for the Admin page, as a DataFrame."""
    import pandas as pd
    return pd.DataFrame(queries.plan_coverage_rows(get_db_session()))


@st.cache_data(ttl=300)
def _cafr_coverage_df():
    """Per-plan CAFR + extraction coverage, as a DataFrame."""
    import pandas as pd
    return pd.DataFrame(queries.cafr_coverage_rows(get_db_session()))

@st.cache_data(ttl=300)
def _cafr_fiscal_year_counts() -> list[dict]:
    return queries.cafr_fiscal_year_counts(get_db_session())


@st.cache_data(ttl=300)
def _cafr_plan_detail_data(plan_id: str) -> dict:
    """Latest CAFR extract for a plan, with allocations and performance."""
    return queries.cafr_plan_detail(get_db_session(), plan_id)

def _twin_page_data(plan_id: str) -> dict:
    """Load the latest twin snapshot for the twin detail page."""
    session = get_db_session()
    snap = get_twin_snapshot(session, plan_id)
    if snap is None:
        return {}
    return {
        "twin": json.loads(snap.facets),
        "built_at": snap.built_at,
        "changed_facets": json.loads(snap.changed_facets or "[]"),
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


def page_plan_twin(plan_id: str) -> None:
    """Digital-twin detail page. Reached via ``?plan=<plan_id>``."""
    import pandas as pd

    data = _twin_page_data(plan_id)
    if st.button("← Back to all plans", key="twin_back"):
        st.query_params.clear()
        st.rerun()
    if not data:
        st.warning(f"No twin snapshot for '{plan_id}' yet. Run: python twin_builder.py {plan_id}")
        return
    twin = data["twin"]
    f = twin["facets"]
    ident = f["identity"]
    st.title(f"{ident['name']} — Digital Twin")

    aum = ident.get("aum_billions", {}).get("v")
    caption_parts = [p for p in (
        ident.get("state"),
        f"AUM ${aum:,.1f}B" if aum else None,
    ) if p]
    if caption_parts:
        st.caption(" · ".join(caption_parts))
    st.caption(
        f"Snapshot built {data['built_at']:%Y-%m-%d} · schema {twin['schema_version']} — "
        "every fact shows its as-of date; CAFR-derived facts can be a year old."
    )

    comp = twin.get("completeness", {})
    fresh = twin.get("freshness", {})
    cols = st.columns(len(comp) or 1)
    for col, (facet, score) in zip(cols, sorted(comp.items())):
        col.metric(facet.replace("_", " "), f"{score:.0%}",
                   help=f"as of {fresh.get(facet) or 'n/a'}")

    with st.expander("Policy", expanded=True):
        pol = f["policy"].get("investment_policy_text")
        if pol and pol.get("v"):
            st.caption(f"as of {pol['as_of']} · [source](?doc={pol['src']['doc_id']})")
            st.markdown(_safe_md(pol["v"]))
        else:
            st.info("No CAFR policy text captured yet.")

        ips = f["policy"].get("ips")
        if ips:
            st.markdown("#### IPS (structured)")
            # NB: ips["src"]["doc_id"] is an ips_documents.id, not a documents.id —
            # never render it as a ?doc= deep link (see task-7 brief warning).
            caption_bits = [
                f"effective {ips['effective_date']}" if ips.get("effective_date") else None,
                f"as of {ips['as_of']}" if ips.get("as_of") else None,
            ]
            caption = " · ".join(p for p in caption_bits if p)
            if caption:
                st.caption(caption)
            if ips.get("target_return_pct") is not None:
                st.metric("Target return", f"{ips['target_return_pct']:.1f}%")
            rebal = ips.get("rebalancing_policy") or {}
            freq = rebal.get("frequency") if isinstance(rebal, dict) else None
            if freq:
                st.markdown(f"**Rebalancing frequency:** {_safe_md(str(freq))}")
            elif rebal:
                st.write("**Rebalancing policy:**", rebal)
            permitted = ips.get("permitted") or []
            prohibited = ips.get("prohibited") or []
            if permitted:
                st.markdown("**Permitted:** " + _safe_md(", ".join(str(p) for p in permitted)))
            if prohibited:
                st.markdown("**Prohibited:** " + _safe_md(", ".join(str(p) for p in prohibited)))

    with st.expander("Allocation vs targets", expanded=True):
        rows = f["allocation"]["rows"]
        if rows:
            st.caption(f"FY{f['allocation']['fiscal_year']} · as of {f['allocation']['as_of']}")
            df_alloc = pd.DataFrame([
                {"Asset class (raw)": r.get("asset_class_raw"),
                 "Asset class (canonical)": r.get("asset_class_canonical"),
                 "Target %": r.get("target_pct"),
                 "Actual %": r.get("actual_pct"),
                 "Range low %": r.get("range_low"),
                 "Range high %": r.get("range_high"),
                 "Drift %": r.get("drift_pct"),
                 "Outside range": r.get("outside_range")}
                for r in rows
            ])
            st.dataframe(df_alloc, use_container_width=True, hide_index=True)
        else:
            st.info("No structured allocation captured yet.")

        ips_targets = f["allocation"].get("ips_targets")
        if ips_targets and ips_targets.get("rows"):
            st.markdown("#### IPS policy targets")
            # NB: same doc-id-namespace caveat as above — as-of caption only, no source link.
            st.caption(f"as of {ips_targets.get('as_of')}")
            df_ips_targets = pd.DataFrame([
                {"Asset class (raw)": r.get("asset_class_raw"),
                 "Asset class (canonical)": r.get("asset_class_canonical"),
                 "Target %": r.get("target_pct"),
                 "Range low %": r.get("range_low"),
                 "Range high %": r.get("range_high")}
                for r in ips_targets["rows"]
            ])
            st.dataframe(df_ips_targets, use_container_width=True, hide_index=True)

    with st.expander("Performance", expanded=False):
        rows = f["performance"]["rows"]
        if rows:
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        else:
            st.info("No structured performance captured yet.")

    with st.expander("Manager roster (observed activity)", expanded=False):
        entries = f["manager_roster"]["entries"]
        if entries:
            has_roster_detail = any(
                e.get("role") is not None
                or e.get("asset_class_canonical") is not None
                or e.get("confidence") is not None
                for e in entries
            )
            roster_rows = []
            for e in entries:
                row = {"Manager": e["name_canonical"], "Status": e["status"],
                       "Mentions": e["mention_count"], "First seen": e["first_seen"],
                       "Last seen": e["last_seen"]}
                if has_roster_detail:
                    row["Role"] = e.get("role") or ""
                    row["Asset class (canonical)"] = e.get("asset_class_canonical") or ""
                    row["Confidence"] = e.get("confidence")
                roster_rows.append(row)
            st.dataframe(pd.DataFrame(roster_rows), use_container_width=True, hide_index=True)
        else:
            st.info("No manager activity observed.")

    with st.expander("Activity timeline", expanded=False):
        for item in f["activity_timeline"]["items"][:30]:
            label = item.get("action") or "decision"
            desc = _safe_md(item.get("description") or "")
            line = f"**{item.get('date') or '—'}** · {label} — {desc}"
            if item.get("doc_id"):
                line += f" ([doc](?doc={item['doc_id']}))"
            st.markdown(line)

    # The "RFP / search state" expander was removed on 2026-08-19: rfp_records
    # is frozen, so every record shown was stale with no way to tell. The rows
    # remain in the snapshot and the table.

    with st.expander("Governance & relationships", expanded=False):
        rels = visible_relationships(f["governance_people"]["relationships"])
        if rels:
            st.dataframe(pd.DataFrame(rels), use_container_width=True, hide_index=True)
        else:
            st.info("No current relationships derived yet.")

    with st.expander("Funding & actuarial", expanded=True):
        fund = f["funding_actuarial"]
        if fund.get("status") == "captured":
            metrics = fund.get("metrics", {})
            fy = fund.get("fiscal_year")
            caption_bits = [
                f"FY{fy}" if fy else None,
                f"as of {fund['as_of']}" if fund.get("as_of") else None,
            ]
            caption = " · ".join(p for p in caption_bits if p)
            src = fund.get("src") or {}
            doc_id = src.get("doc_id")
            if doc_id:
                caption = (caption + " · " if caption else "") + f"[source](?doc={doc_id})"
            if caption:
                st.caption(caption)

            def _fmt_pct(v):
                return f"{v:.1f}%" if v is not None else "—"

            def _fmt_m(v):
                return f"${v:,.1f}M" if v is not None else "—"

            def _fmt_int(v):
                return f"{v:,.0f}" if v is not None else "—"

            row1 = st.columns(3)
            row1[0].metric("Funded ratio (actuarial)", _fmt_pct(metrics.get("funded_ratio_pct")))
            row1[1].metric("Funded ratio (market)", _fmt_pct(metrics.get("market_funded_ratio_pct")))
            row1[2].metric("Discount rate", _fmt_pct(metrics.get("discount_rate_pct")))

            row2 = st.columns(3)
            row2[0].metric("Unfunded AAL", _fmt_m(metrics.get("unfunded_aal_millions")))
            row2[1].metric("Net pension liability", _fmt_m(metrics.get("net_pension_liability_millions")))
            amort = metrics.get("amortization_years")
            row2[2].metric("Amortization period", f"{amort:.0f} yrs" if amort is not None else "—")

            row3 = st.columns(3)
            row3[0].metric("Employer contribution rate", _fmt_pct(metrics.get("employer_contribution_rate_pct")))
            row3[1].metric("Employee contribution rate", _fmt_pct(metrics.get("employee_contribution_rate_pct")))
            row3[2].metric("ADC % contributed", _fmt_pct(metrics.get("adc_pct_contributed")))

            row4 = st.columns(3)
            row4[0].metric("Active members", _fmt_int(metrics.get("members_active")))
            row4[1].metric("Retired members", _fmt_int(metrics.get("members_retired")))
            row4[2].metric("Actuary", metrics.get("actuary_firm") or "—")
        else:
            st.info("Funding & actuarial data: not captured yet (twin v1).")


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
    {
        # Plans label this allocation many different ways: hedge funds,
        # absolute return, risk mitigation / risk mitigating strategies,
        # diversifying strategies, diversifiers, crisis risk offset, tail
        # risk. Exclusions strip false positives: "Real Assets and
        # Inflation Hedges" matches %hedge% but is real-assets; "Diversified
        # Multi-Sector Fixed Income" matches %diversif% but is fixed income.
        "tab_name": "Hedge Funds",
        "match_patterns": (
            "%hedge%",
            "%absolute return%",
            "%risk mitig%",
            "%diversif%",
            "%crisis risk offset%",
            "%tail risk%",
        ),
        "exclude_patterns": ("%inflation%", "%fixed income%"),
        "exact_label": "hedge funds",
    },
)


@st.cache_data(ttl=300)
def _allocation_fy_range() -> tuple[int, int] | None:
    """Min/max fiscal_year across CAFR extracts. Returns ``None`` if empty."""
    return queries.cafr_extract_fy_range(get_db_session())

@st.cache_data(ttl=300)
def _allocation_df(match_patterns: tuple, exclude_patterns: tuple,
                   exact_label: str, min_fy: int | None = None):
    """Plans with both target and actual weights for a given asset class.

    The SQL lives in ``queries.allocation_rows``. What stays here is the
    presentation-side reduction: when a plan has several matching rows, prefer
    the one whose asset_class equals ``exact_label``, else keep the first.
    """
    import pandas as pd

    rows = queries.allocation_rows(get_db_session(), match_patterns,
                                   exclude_patterns, min_fy)
    df = pd.DataFrame(rows, columns=list(queries.ALLOCATION_COLUMNS))
    if df.empty:
        return df

    df["label"] = df["abbreviation"].fillna("").where(
        df["abbreviation"].astype(bool), df["plan_id"]
    )
    df["_priority"] = df["asset_class"].str.lower().eq(exact_label).astype(int)
    return (
        df.sort_values(["plan_id", "_priority"], ascending=[True, False])
          .drop_duplicates(subset=["plan_id"], keep="first")
          .drop(columns="_priority")
          .reset_index(drop=True)
    )

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

    fy_range = _allocation_fy_range()
    min_fy: int | None = None
    if fy_range:
        lo, hi = fy_range
        if hi > lo:
            min_fy = st.slider(
                "Show CAFRs with fiscal year ≥",
                min_value=lo,
                max_value=hi,
                value=lo,
                step=1,
                key="asset_alloc_min_fy",
                help=(
                    "Drop plans whose latest available CAFR fiscal year is "
                    "older than this. Useful for excluding stale data when "
                    "comparing against current policy targets."
                ),
            )

    sub_tabs = st.tabs([v["tab_name"] for v in ASSET_ALLOCATION_VIEWS])
    for tab, view in zip(sub_tabs, ASSET_ALLOCATION_VIEWS):
        with tab:
            df = _allocation_df(
                view["match_patterns"],
                view["exclude_patterns"],
                view["exact_label"],
                min_fy=min_fy,
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
    aggregator = int((df["Status"] == "Aggregator (skipped)").sum())
    missing = int((df["Status"] == "Missing CAFR").sum())

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Plans tracked", total)
    c2.metric("Extracted", extracted)
    c3.metric("Pending extract", pending)
    c4.metric("Aggregator", aggregator,
              help="System-of-systems CAFRs that don't map to a single plan; "
                   "skipped by design.")
    c5.metric("Missing CAFR", missing)

    # Reporting-year coverage. The headline metrics above say how many plans
    # have a CAFR; this says how *current* those CAFRs are, which is the
    # question that actually matters when reading any figure derived from
    # them. A corpus where most plans' newest CAFR is FY2023 is a different
    # dataset from one where most are FY2025, and the counts above cannot
    # distinguish the two.
    st.subheader("Coverage by reporting year")
    fy_rows = _cafr_fiscal_year_counts()
    if not fy_rows:
        st.caption("No fiscal-year-tagged CAFRs yet.")
    else:
        import pandas as pd
        fy_df = pd.DataFrame(fy_rows)
        st.dataframe(
            fy_df, width="stretch", hide_index=True,
            column_config={
                "Fiscal year": st.column_config.NumberColumn(format="%d"),
                "Change (30d)": st.column_config.NumberColumn(
                    "Change (30d)", format="%+d",
                    help="Movement in the last 30 days, derived from "
                         "downloaded_at. A plan whose FY2024 CAFR was "
                         "superseded by FY2025 shows -1 against 2024 and "
                         "+1 against 2025 — it moved bucket, nothing was lost."),
            },
        )

    status_filter = st.multiselect(
        "Filter by status",
        ["Extracted", "Pending extract", "Aggregator (skipped)", "Missing CAFR"],
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


def page_meeting_recordings(plan_id, plan_label):
    """Smart catalogue of plan video sources and meeting recordings.

    Three views in one tab:
      - Directory:    per-plan summary of where recordings are published
                      (YouTube channel, Granicus archive, etc.).
      - Recordings:   individual videos discovered on plans' meetings pages,
                      with download status and the local D:\\ path the file
                      will live at when downloaded.
      - Coverage:     plans with no known video source — the gap list.
    """
    st.title("Meeting Recordings")
    st.caption(
        f"Local recordings root: `{RECORDINGS_DIR}`. "
        "Files live on the user's D: drive — paths are stored in the DB so "
        "the catalogue can find them again. Source data populated by "
        "`discover_video_sources.py`."
    )

    session = get_db_session()

    sources = queries.video_sources(session, plan_id)
    recordings = queries.meeting_recordings(session, plan_id)
    all_plans = queries.plans_by_id(session)

    # ---- summary metrics
    total_plans = len(all_plans) if not plan_id else 1
    plans_with_source = len({s.plan_id for s in sources if s.status == "active"})
    downloaded = sum(1 for r in recordings if r.download_status == "done")
    pending = sum(1 for r in recordings
                  if r.download_status in ("pending", "downloading"))

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Plans tracked", total_plans)
    c2.metric("Plans with active source", plans_with_source)
    c3.metric("Recordings catalogued", len(recordings))
    c4.metric("Downloaded locally", downloaded,
              delta=f"{pending} pending" if pending else None)

    tab_dir, tab_rec, tab_gap = st.tabs(
        ["Directory", "Recordings", "Coverage gaps"]
    )

    # -----------------------------------------------------------------
    # Directory: where each plan publishes meeting video
    # -----------------------------------------------------------------
    with tab_dir:
        show_inactive = st.checkbox(
            "Show inactive sources (e.g. social-media footer links auto-deactivated)",
            value=False, key="mr_show_inactive",
        )
        active_sources = [s for s in sources if show_inactive or s.status == "active"]
        if not active_sources:
            st.info("No video sources catalogued yet for this filter. "
                    "Run `python discover_video_sources.py --site-crawl` to populate.")
        else:
            from collections import defaultdict
            by_plan: dict[str, list] = defaultdict(list)
            for s in active_sources:
                by_plan[s.plan_id].append(s)

            for pid in sorted(by_plan):
                plan = all_plans.get(pid)
                if not plan:
                    continue
                platforms = sorted({s.platform for s in by_plan[pid]})
                live_flags = {s.live_streamed for s in by_plan[pid] if s.live_streamed}
                live_badge = "📡 live-streamed" if live_flags else ""
                header = (f"**{plan.abbreviation or plan.name}** ({plan.state}) — "
                          f"{', '.join(platforms)}  {live_badge}")
                with st.expander(header, expanded=False):
                    for s in by_plan[pid]:
                        status_tag = (f":green[active]" if s.status == "active"
                                      else f":gray[{s.status}]")
                        live_tag = (":blue[live ✓]" if s.live_streamed
                                    else (":gray[live ?]"))
                        rec_tag = (f":violet[{s.recording_policy}]"
                                   if s.recording_policy else "")
                        st.markdown(
                            f"- **{s.platform}** {status_tag} {live_tag} {rec_tag}<br>"
                            f"&nbsp;&nbsp;[{s.source_url}]({s.source_url})  "
                            f"<small>discovery={s.discovery_method}"
                            + (f" · channel_id=`{s.channel_id}`" if s.channel_id else "")
                            + "</small>",
                            unsafe_allow_html=True,
                        )
                        if s.notes:
                            st.caption(s.notes)

    # -----------------------------------------------------------------
    # Recordings: individual videos with download status + local path
    # -----------------------------------------------------------------
    with tab_rec:
        if not recordings:
            st.info("No recordings catalogued yet. The discovery script "
                    "captures watch URLs found on plan meetings pages.")
        else:
            status_filter = st.selectbox(
                "Download status",
                ["all", "pending", "done", "failed", "skipped"],
                key="mr_status_filter",
            )
            filtered = ([r for r in recordings if r.download_status == status_filter]
                        if status_filter != "all" else recordings)

            rows = []
            for r in filtered[:500]:  # cap render cost; filter further if needed
                plan = all_plans.get(r.plan_id)
                expected_path = recording_path(
                    r.plan_id, r.video_id,
                    meeting_date=r.meeting_date_inferred,
                    published_at=r.published_at,
                )
                local = r.local_path or str(expected_path)
                exists_on_disk = (r.local_path is not None
                                  and Path(r.local_path).exists()) \
                                 if r.local_path else False
                rows.append({
                    "plan": plan.abbreviation if plan else r.plan_id,
                    "platform": r.platform,
                    "title": (r.title or "")[:80],
                    "meeting_date": (r.meeting_date_inferred.date()
                                     if r.meeting_date_inferred else None),
                    "published": (r.published_at.date()
                                  if r.published_at else None),
                    "status": r.download_status,
                    "on_disk": "✓" if exists_on_disk else "",
                    "video_url": r.video_url,
                    "local_path": local,
                    "size_mb": (round(r.file_size_bytes / 1_048_576, 1)
                                if r.file_size_bytes else None),
                })

            if not rows:
                st.info(f"No recordings with status='{status_filter}'.")
            else:
                import pandas as pd
                df = pd.DataFrame(rows)
                st.caption(f"Showing {len(rows)} of {len(filtered)} recording(s)"
                           + (" (capped at 500)" if len(filtered) > 500 else ""))
                st.dataframe(
                    df,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "video_url": st.column_config.LinkColumn("video_url"),
                        "local_path": st.column_config.TextColumn(
                            "local_path",
                            help="Where the file will live on D:\\ once downloaded "
                                 "(or where it lives now if on_disk='✓').",
                        ),
                    },
                )

            # Download control: independent of the status filter above (a
            # downloaded file is always download_status='done', regardless
            # of what the dropdown is currently showing). Admin-gated -- the
            # catalogue metadata isn't sensitive, but pulling a video file
            # off this machine's D: drive is only for James, e.g. when
            # browsing the app from another device on the LAN.
            if _admin_unlocked():
                downloadable = [
                    r for r in recordings
                    if r.download_status == "done" and r.local_path
                    and Path(r.local_path).exists()
                ]
                if downloadable:
                    st.divider()
                    st.subheader("Download a recording")

                    def _rec_label(r):
                        plan = all_plans.get(r.plan_id)
                        plan_tag = plan.abbreviation if plan else r.plan_id
                        when = r.meeting_date_inferred or r.published_at
                        when_str = when.date().isoformat() if when else "undated"
                        title = (r.title or r.video_id)[:60]
                        return f"{plan_tag} — {when_str} — {title}"

                    downloadable.sort(key=_rec_label)
                    chosen = st.selectbox(
                        "Recording", downloadable, format_func=_rec_label,
                        key="mr_download_select",
                    )
                    file_path = Path(chosen.local_path)
                    size_mb = round(file_path.stat().st_size / 1_048_576, 1)
                    st.download_button(
                        f"Download ({size_mb} MB)",
                        data=open(file_path, "rb"),
                        file_name=file_path.name,
                        mime="video/mp4",
                        key="mr_download_button",
                    )

    # -----------------------------------------------------------------
    # Coverage: plans we still have no source for
    # -----------------------------------------------------------------
    with tab_gap:
        plans_with_active = {s.plan_id for s in sources if s.status == "active"}
        gap_plans = [p for pid, p in all_plans.items()
                     if pid not in plans_with_active]
        if plan_id and plan_id in plans_with_active:
            gap_plans = []
        st.markdown(
            f"**{len(gap_plans)} plan(s)** have no active video source row. "
            "These need either a deeper crawl of their meetings sub-pages "
            "or a manual entry."
        )
        if gap_plans:
            import pandas as pd
            gap_df = pd.DataFrame([{
                "plan_id": p.id,
                "name": p.name,
                "state": p.state,
                "aum_b": p.aum_billions,
                "materials_url": p.materials_url,
            } for p in sorted(gap_plans, key=lambda x: -(x.aum_billions or 0))])
            st.dataframe(
                gap_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "materials_url": st.column_config.LinkColumn("materials_url"),
                },
            )


def _admin_unlocked() -> bool:
    """True if Admin/Drafts tabs should be visible.

    Fail-open when ``ADMIN_PASSWORD`` is unset (local dev convenience).
    Otherwise gated on ``st.session_state['_admin_unlocked']`` — set
    by ``_render_admin_login_sidebar`` after a correct password entry.
    """
    if not os.environ.get("ADMIN_PASSWORD", ""):
        return True
    return bool(st.session_state.get("_admin_unlocked", False))


def _render_admin_login_sidebar() -> None:
    """Sidebar password form. No-op when already unlocked or no password set."""
    expected = os.environ.get("ADMIN_PASSWORD", "")
    if not expected:
        return
    if st.session_state.get("_admin_unlocked", False):
        with st.sidebar:
            if st.button("Admin: lock", use_container_width=True):
                st.session_state["_admin_unlocked"] = False
                st.rerun()
        return
    with st.sidebar:
        with st.expander("Admin login", expanded=False):
            with st.form("admin_login", clear_on_submit=True):
                pw = st.text_input(
                    "Password", type="password",
                    label_visibility="collapsed",
                    placeholder="password",
                )
                if st.form_submit_button("Unlock", use_container_width=True):
                    if pw == expected:
                        st.session_state["_admin_unlocked"] = True
                        st.rerun()
                    else:
                        st.error("Incorrect password.")


def page_admin():
    """Admin views: pipeline / data-quality diagnostics for the site owner."""
    st.title("Admin")
    (tab_runs, tab_coverage, tab_backlog, tab_failed,
     tab_cafr, tab_cafr_refreshes, tab_subscribers, tab_spend) = st.tabs(
        ["Recent Runs", "Plan Coverage", "Pipeline Backlog",
         "Failed Docs", "CAFR Coverage", "CAFR Refreshes",
         "Subscribers", "Spend"]
    )

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

    with tab_subscribers:
        _render_admin_subscribers()

    with tab_spend:
        _render_admin_spend()


def _render_admin_spend() -> None:
    """What Claude actually costs, by job and by model.

    Exists because message.usage was discarded on every call, so the design
    spec's cost section was reasoning from assumption — and two of its four
    proposed controls turned out to be already done or a no-op. The table
    below is the correction: it says where the money goes rather than where it
    was expected to go.

    Empty until a real (non-mock) run has happened; nothing backfills, because
    the token counts were never recorded and cannot be reconstructed.
    """
    session = get_db_session()
    days = st.selectbox("Window", [7, 30, 90], index=1,
                        format_func=lambda d: f"last {d} days",
                        key="spend_window")

    total = queries.api_spend_total(session, days)
    st.metric(f"Claude API spend, last {days} days", f"${float(total):,.2f}")

    by_op = queries.api_spend_by_operation(session, days)
    if not by_op:
        st.info(
            "No usage recorded yet. Rows appear after the first live run — "
            "mock runs deliberately record nothing."
        )
        return

    st.subheader("By job")
    st.dataframe(
        [{"job": op,
          "calls": calls,
          "input tokens": int(tin or 0),
          "output tokens": int(tout or 0),
          "cost": f"${float(cost or 0):,.4f}"}
         for op, calls, tin, tout, cost in by_op],
        use_container_width=True, hide_index=True)

    st.subheader("By model")
    st.dataframe(
        [{"model": model, "calls": calls, "cost": f"${float(cost or 0):,.4f}"}
         for model, calls, cost in queries.api_spend_by_model(session, days)],
        use_container_width=True, hide_index=True)

    st.caption(
        "Ordered by cost, so the first row is the answer to what to optimise. "
        "Prices are USD per million tokens as published on 2026-08-24; "
        "cache reads and writes are priced separately from base input."
    )


def _render_admin_subscribers() -> None:
    """Moderation table for the public subscriber list.

    Shows every row regardless of status; lets the admin disable / enable /
    delete individual rows. Sign-ups are auto-confirmed via the email link,
    so this view is for after-the-fact moderation rather than gatekeeping.
    """
    from insights import subscribers as _subs

    st.caption(
        "Public mailing-list subscribers. Sign-up auto-confirms via the "
        "double-opt-in email; use the actions here to suppress an address "
        "without losing its history."
    )

    rows = _subs.list_all_subscribers()
    if not rows:
        st.info("No subscribers yet.")
        return

    # Headline metrics
    confirmed = sum(1 for r in rows if r.status == "confirmed")
    pending = sum(1 for r in rows if r.status == "pending")
    disabled = sum(1 for r in rows if r.status == "disabled")
    unsubscribed = sum(1 for r in rows if r.status == "unsubscribed")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Confirmed", confirmed)
    c2.metric("Pending", pending)
    c3.metric("Disabled", disabled)
    c4.metric("Unsubscribed", unsubscribed)

    import pandas as pd
    df = pd.DataFrame([
        {
            "id": r.id,
            "email": r.email,
            "status": r.status,
            "weekly": r.weekly,
            "monthly": r.monthly,
            "quarterly": r.quarterly,
            "signed up": r.created_at.strftime("%Y-%m-%d") if r.created_at else "",
            "confirmed": r.confirmed_at.strftime("%Y-%m-%d") if r.confirmed_at else "",
            "last sent": r.last_email_sent_at.strftime("%Y-%m-%d %H:%M")
                if r.last_email_sent_at else "",
        }
        for r in rows
    ])
    st.dataframe(df, use_container_width=True, hide_index=True)

    st.markdown("#### Per-subscriber actions")
    options = {f"#{r.id} — {r.email} ({r.status})": r for r in rows}
    label = st.selectbox(
        "Subscriber", list(options.keys()), key="admin_sub_pick"
    )
    target = options[label]
    a1, a2, a3 = st.columns(3)
    if a1.button("Disable", key=f"sub_disable_{target.id}",
                 disabled=target.status == "disabled"):
        _subs.set_status(target.id, "disabled")
        st.success(f"Disabled {target.email}.")
        st.rerun()
    if a2.button("Enable (confirmed)", key=f"sub_enable_{target.id}",
                 disabled=target.status == "confirmed"):
        _subs.set_status(target.id, "confirmed")
        st.success(f"Re-enabled {target.email}.")
        st.rerun()
    if a3.button("Delete", key=f"sub_delete_{target.id}", type="secondary"):
        _subs.delete_subscriber(target.id)
        st.success(f"Deleted {target.email}.")
        st.rerun()


# ---------------------------------------------------------------------------
# Subscriber sign-up tab and magic-link landing pages
# ---------------------------------------------------------------------------

def page_subscribe() -> None:
    """Public sign-up form for the digest mailing list."""
    from insights import subscribers as _subs

    st.title("Subscribe to PensionGraph")
    st.caption(
        "Get the weekly, monthly, or quarterly briefing as soon as it's "
        "published. Double opt-in: we'll email you a confirmation link to "
        "verify the address."
    )

    with st.form("subscribe_form"):
        email = st.text_input("Email address", placeholder="you@example.com")
        st.write("Send me the:")
        c1, c2, c3 = st.columns(3)
        weekly = c1.checkbox("Weekly briefing", value=True)
        monthly = c2.checkbox("Monthly briefing", value=True)
        quarterly = c3.checkbox("Quarterly briefing", value=True)
        submitted = st.form_submit_button("Subscribe", use_container_width=True)

    if not submitted:
        return

    email_clean = (email or "").strip()
    if not email_clean or "@" not in email_clean:
        st.error("Please enter a valid email address.")
        return
    if not (weekly or monthly or quarterly):
        st.error("Pick at least one cadence.")
        return
    if _subs.recent_signup_count(email_clean) >= _subs.RECENT_SIGNUP_LIMIT:
        st.warning(
            "We've already sent a confirmation link to that address recently. "
            "Check your inbox (and spam folder) — if you didn't get it, try "
            "again in an hour."
        )
        return

    def _send_confirmation(sub, raw_token):
        email_obj = _subs.render_confirmation_email(sub, raw_token)
        _subs.send_email(email_obj, to=sub.email)

    try:
        _subs.create_pending_subscriber(
            email_clean,
            weekly=weekly, monthly=monthly, quarterly=quarterly,
            send_callback=_send_confirmation,
        )
    except _subs.SubscriberError as exc:
        st.error(str(exc))
        return
    except Exception as exc:
        st.error(f"Sign-up failed: {exc}")
        return

    st.success(
        "Check your inbox. We sent a confirmation link to "
        f"**{email_clean}** — click it to start receiving briefings."
    )


def page_subscriber_confirm(raw_token: str) -> None:
    """Handle ?confirm=<token>: flip subscriber to ``confirmed``, send welcome."""
    from insights import subscribers as _subs

    try:
        sub = _subs.consume_confirm_token(raw_token)
    except _subs.SubscriberError as exc:
        st.title("Confirmation link error")
        st.error(str(exc))
        st.caption(
            "Your link may have expired or already been used. Sign up "
            "again from the Subscribe tab to get a fresh confirmation email."
        )
        if st.button("Back to dashboard"):
            st.query_params.clear()
            st.rerun()
        return

    # Welcome email — best effort, don't block the confirmation page.
    try:
        welcome = _subs.render_welcome_email(sub)
        _subs.send_email(welcome, to=sub.email)
    except Exception as exc:
        import logging as _logging
        _logging.getLogger(__name__).warning(
            "Welcome email failed for %s: %s", sub.email, exc
        )

    st.title("You're subscribed")
    cadences = [c for c in _subs.CADENCES if getattr(sub, c)]
    label = ", ".join(cadences) if cadences else "no cadence"
    st.success(
        f"**{sub.email}** is now confirmed for the **{label}** briefing(s)."
    )
    st.caption("Every email will include an unsubscribe link in the footer.")
    if st.button("Back to dashboard"):
        st.query_params.clear()
        st.rerun()


def page_subscriber_unsubscribe(raw_token: str) -> None:
    """Handle ?unsub=<token>: flip subscriber to ``unsubscribed``."""
    from insights import subscribers as _subs

    try:
        sub = _subs.consume_unsubscribe_token(raw_token)
    except _subs.SubscriberError as exc:
        st.title("Unsubscribe link error")
        st.error(str(exc))
        if st.button("Back to dashboard"):
            st.query_params.clear()
            st.rerun()
        return

    st.title("You're unsubscribed")
    st.info(f"**{sub.email}** will no longer receive briefings.")
    st.caption(
        "Changed your mind? Sign up again from the Subscribe tab — "
        "we'll send a fresh confirmation link."
    )
    if st.button("Back to dashboard"):
        st.query_params.clear()
        st.rerun()


def page_subscriber_preferences(raw_token: str) -> None:
    """Handle ?prefs=<token>: edit cadence checkboxes for an existing subscriber."""
    from insights import subscribers as _subs

    try:
        sub = _subs.consume_preferences_token(raw_token)
    except _subs.SubscriberError as exc:
        st.title("Preferences link error")
        st.error(str(exc))
        if st.button("Back to dashboard"):
            st.query_params.clear()
            st.rerun()
        return

    st.title("Update your subscription")
    st.caption(f"Editing preferences for **{sub.email}**.")

    with st.form("prefs_form"):
        weekly = st.checkbox("Weekly briefing", value=bool(sub.weekly))
        monthly = st.checkbox("Monthly briefing", value=bool(sub.monthly))
        quarterly = st.checkbox("Quarterly briefing", value=bool(sub.quarterly))
        submitted = st.form_submit_button("Save", use_container_width=True)

    if submitted:
        updated = _subs.set_preferences(
            sub.id, weekly=weekly, monthly=monthly, quarterly=quarterly,
        )
        if updated.status == "unsubscribed":
            st.info("All cadences cleared — you've been unsubscribed.")
        else:
            picks = [c for c in _subs.CADENCES if getattr(updated, c)]
            st.success(f"Saved. You'll receive: {', '.join(picks)}.")
        if st.button("Back to dashboard"):
            st.query_params.clear()
            st.rerun()


def page_drafts():
    """List publications awaiting founder approval."""
    st.title("Drafts awaiting approval")
    st.caption(
        "Insights publications generated by the scheduler that haven't "
        "yet been approved or rejected. The approval link in the email is "
        "the canonical way to act on these — this view is for visibility."
    )

    rows = queries.drafts_awaiting_approval(get_db_session())

    if not rows:
        st.info("No drafts awaiting approval right now.")
        return

    for pub in rows:
        composed = pub.composed_at.strftime("%Y-%m-%d %H:%M") if pub.composed_at else "—"
        expires = pub.expires_at.strftime("%Y-%m-%d %H:%M") if pub.expires_at else "—"
        period = f"{pub.period_start.isoformat()} – {pub.period_end.isoformat()}"
        with st.expander(
            f"**{pub.cadence.title()}** — {period} "
            f"(composed {composed}) · id `{pub.id}`",
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


def page_archive():
    """List approved/published Insights publications with PDF downloads."""
    st.title("Insights Archive")
    st.caption(
        "Every Insights publication that has cleared the approval flow. "
        "The 'Insights' tab serves the live versions; this view is the "
        "audit trail."
    )

    rows = queries.publications_by_status(
        get_db_session(), ("approved", "published"))
    if not rows:
        st.info("No approved publications yet.")
        return

    for pub in rows:
        period = f"{pub.period_start.isoformat()} – {pub.period_end.isoformat()}"
        when = pub.published_at or pub.approved_at
        when_str = when.strftime("%Y-%m-%d %H:%M") if when else "—"
        with st.expander(
            f"**{pub.cadence.title()}** — {period} "
            f"({pub.status}, {when_str}) · id `{pub.id}`",
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


@st.cache_data(ttl=300)
def _load_asset_class_mappings() -> dict:
    """data/asset_class_mappings.json, or {} if absent.

    Read directly rather than through twin_builder.load_asset_class_mappings
    so the app does not import the twin builder just for a JSON file.
    Missing file is not an error: the performance table simply shows fewer
    asset-class columns filled in.
    """
    path = Path(__file__).resolve().parent / "data" / "asset_class_mappings.json"
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


@st.cache_data(ttl=300)
def _performance_rows() -> list[dict]:
    return queries.performance_report_rows(
        get_db_session(), _load_asset_class_mappings())


@st.cache_data(ttl=900)
def _collated_performance_rows() -> list[dict]:
    """Reads the derived table, so this is a few hundred small rows.

    Longer TTL than the CAFR view because the source is a nightly build, not
    live extraction — re-reading it every five minutes would buy nothing and
    cost egress, which is the mistake that took the site down in August.
    """
    return queries.collated_performance_rows(get_db_session())


def _percent_display(df, numeric_cols):
    """A display copy with returns as "12.6%" strings and blanks for missing.

    Returns a *copy*: callers keep the numeric frame for the CSV download, so
    a spreadsheet still gets numbers.

    Pre-formatted strings rather than `NumberColumn(format="%.1f%%")`,
    because that renders a missing value as the literal word "None" and
    these tables are mostly empty by nature — a plan reports four asset
    classes, not thirteen. A Styler with `na_rep=""` looks like the fix and
    is not: `st.dataframe` honours a Styler's number format but still writes
    "None" for the gaps, and a blanket `.format(na_rep="")` also turns the
    integer "2023" into "2023.000000". Both were tried against the real
    tables before this.

    The cost is that these columns sort as text. Worth it: a column of
    "None" is unreadable, and the numeric frame is one click away in the CSV.
    """
    import pandas as pd          # imported per-function, as elsewhere here

    out = df.copy()
    numeric = [c for c in numeric_cols if c in out.columns]
    for c in numeric:
        out[c] = out[c].map(lambda v: "" if pd.isna(v) else f"{v:.1f}%")
    # Text columns get the same treatment. A handful of rows carry no period
    # label or no date, and Streamlit writes those as the word "None" too --
    # which reads as a bug rather than as "we don't know". Only object
    # columns: a real datetime column keeps its type for DatetimeColumn.
    for c in out.columns:
        if c not in numeric and out[c].dtype == object:
            out[c] = out[c].map(lambda v: "" if v is None or pd.isna(v) else v)
    return out


def _render_collated_performance():
    """Latest asset-class return per plan, from CAFRs and board documents.

    Sits above the CAFR-only table because it is both broader (126 plans
    against 84) and more current (2026 board figures against FY2024-25
    CAFRs). The CAFR table stays because it is internally consistent -- every
    number in one of its rows comes from the same document and the same
    fiscal year -- which this one deliberately trades away for recency.
    """
    import pandas as pd

    rows = _collated_performance_rows()
    if not rows:
        st.info(
            "No collated performance data yet. Build it with "
            "`python -m scripts.build_performance_view`."
        )
        return

    st.subheader("Latest return by asset class")
    st.caption(
        "Every figure in a row comes from **one document** — the plan's most "
        "recent source carrying an asset-class breakdown — so a row can be "
        "read across. Period and Frequency describe the whole row: a 2.1% "
        "quarter and a 2.1% year are not the same result, and the number "
        "alone cannot tell you which it is. Sourced from CAFRs and from "
        "returns the summariser extracts out of board documents; class names "
        "are normalised, so 'US Equities', 'Domestic Equity' and 'S&P 500' "
        "land in one column."
    )

    df = pd.DataFrame(rows)
    class_cols = [label for _, label in queries.COLLATED_CLASSES
                  if label in df.columns]
    ordered = ["Plan", "Period", "Frequency"] + class_cols + ["As of", "Source"]
    df = df[[c for c in ordered if c in df.columns]]

    freqs = sorted(df["Frequency"].dropna().unique()) if "Frequency" in df else []
    chosen = st.multiselect(
        "Frequency", freqs, default=[f for f in freqs if f == "Annual"] or freqs,
        help="Annual figures are comparable across plans. Quarterly and "
             "part-year ones are not, and are kept for plans that publish "
             "nothing else.")
    if chosen:
        df = df[df["Frequency"].isin(chosen)]

    c1, c2, c3 = st.columns(3)
    c1.metric("Plans shown", len(df))
    non_total = [c for c in class_cols if c != "Total plan"]
    c2.metric("With asset-class detail",
              int(df[non_total].notna().any(axis=1).sum()) if non_total else 0)
    c3.metric("Newest source",
              df["As of"].dropna().max() if "As of" in df and df["As of"].notna().any()
              else "—")

    for c in class_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    st.dataframe(
        _percent_display(df, class_cols),
        width="stretch", hide_index=True,
        column_config={
            # Period labels run long and verbatim — "FY2025 (year ending June
            # 30, 2025)" — and truncating them defeats the point of showing
            # the period at all.
            "Period": st.column_config.TextColumn("Period", width="medium"),
            "Frequency": st.column_config.TextColumn("Frequency", width="small"),
            "Source": st.column_config.LinkColumn(
                "Source", display_text="open", width="small",
                help="The document these figures were read from"),
        })
    st.download_button(
        "Download CSV", df.to_csv(index=False).encode("utf-8"),
        "asset_class_performance.csv", "text/csv",
        key="collated_perf_csv")
    st.divider()


@st.cache_data(ttl=900)
def _asset_class_horizon_rows(asset_class: str) -> list[dict]:
    """Reads the derived ``plan_asset_class_horizon`` table.

    Same TTL and same reasoning as ``_collated_performance_rows`` above: the
    source is a nightly build (``scripts/build_performance_view.py``), so a
    short TTL buys nothing and costs egress.
    """
    return queries.asset_class_horizon_rows(get_db_session(), asset_class)


def _render_asset_class_horizons():
    """One asset class, every plan, across time horizons.

    The mirror of ``_render_collated_performance`` above: that one fixes the
    plan and shows every asset class; this fixes the asset class and shows
    every plan. Sits below it because "what does this plan hold" is the
    more common question and keeps the top slot.
    """
    import pandas as pd

    st.subheader("One asset class, every plan")

    keys = [k for k, _ in queries.COLLATED_CLASSES]
    labels = [label for _, label in queries.COLLATED_CLASSES]
    default_index = keys.index("real_estate") if "real_estate" in keys else 0
    chosen_label = st.selectbox(
        "Asset class", labels, index=default_index,
        key="asset_class_horizon_picker")
    asset_class = dict(zip(labels, keys))[chosen_label]

    rows = _asset_class_horizon_rows(asset_class)
    if not rows:
        st.info(f"No performance data yet for {chosen_label}.")
        return

    st.caption(
        "A row can mix documents across its columns -- a plan's 1-year "
        "figure might come from a 2026 board pack while its 10-year comes "
        "from an FY2023 CAFR. That's a deliberate choice (coverage over "
        "single-source purity): **As of** is the newest of the cells "
        "actually used, and **Sources** counts the distinct documents the "
        "row draws on."
    )

    df = pd.DataFrame(rows)
    horizon_cols = [label for _, label in queries.ASSET_CLASS_HORIZONS
                    if label in df.columns]
    ordered = ["Plan"] + horizon_cols + ["As of", "Sources", "Source"]
    df = df[[c for c in ordered if c in df.columns]]

    for c in horizon_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    c1, c2 = st.columns(2)
    c1.metric("Plans shown", len(df))
    three_year = dict(queries.ASSET_CLASS_HORIZONS).get("3y")
    c2.metric("With a 3-year figure",
              int(df[three_year].notna().sum())
              if three_year and three_year in df else 0)

    st.dataframe(
        _percent_display(df, horizon_cols),
        width="stretch", hide_index=True,
        column_config={
            "Sources": st.column_config.NumberColumn("Sources", width="small"),
            "Source": st.column_config.LinkColumn(
                "Source", display_text="open", width="small",
                help="The newest document behind this row"),
        })
    st.download_button(
        "Download CSV", df.to_csv(index=False).encode("utf-8"),
        f"{asset_class}_horizon_performance.csv", "text/csv",
        key="asset_class_horizon_csv")
    st.divider()


def page_performance():
    """Headline returns by asset class, one row per plan.

    Deliberately a table over a data structure rather than prose: the rows
    come back as dicts so charts, per-class rankings and time series can be
    built on the same query without re-deriving anything.
    """
    import pandas as pd

    st.title("Performance Reports")

    _render_collated_performance()
    _render_asset_class_horizons()

    rows = _performance_rows()
    if not rows:
        st.info("No extracted performance data yet.")
        return

    # Say what the numbers are. The source is the CAFR, so these are
    # fiscal-year returns, not the latest quarter. A true quarterly source
    # exists for one plan (see the section below) -- extract_performance_
    # reports.py's docstring explains why the other 48-document plans don't
    # qualify. Labelling this precisely matters more than the column being
    # short: a reader comparing plans needs to know that "2025" and "2023"
    # rows are different fiscal years, not stale data.
    st.caption(
        "Fiscal-year returns from each plan's most recent CAFR — not "
        "calendar quarters. The Period column shows whether a plan reported "
        "a fiscal-year (`fy`) or trailing-twelve-month (`1y`) figure. "
        "Fiscal years differ between plans, so rows are not directly "
        "comparable without checking the Fiscal year column."
    )

    df = pd.DataFrame(rows)
    display_cols = (["Plan", "Fiscal year", "Period"]
                    + [label for _, label in queries.PERFORMANCE_CLASSES]
                    + ["Source", "Source date"])
    df = df[[c for c in display_cols if c in df.columns]]

    c1, c2, c3 = st.columns(3)
    c1.metric("Plans with performance data", len(df))
    have_pe = int(df["Private equity"].notna().sum()) if "Private equity" in df else 0
    c2.metric("With private equity", have_pe)
    latest_fy = df["Fiscal year"].dropna()
    c3.metric("Most recent fiscal year",
              int(latest_fy.max()) if not latest_fy.empty else "—")

    st.dataframe(
        # Same treatment as the collated table above: percentages to one
        # decimal, blanks rather than "None". Two performance tables on one
        # page formatting their numbers differently is its own small lie
        # about whether they mean the same thing.
        _percent_display(df, [label for _, label in queries.PERFORMANCE_CLASSES]),
        width="stretch",
        hide_index=True,
        column_config={
            "Source": st.column_config.LinkColumn("Source", display_text="CAFR"),
            "Source date": st.column_config.DatetimeColumn(
                "Source date", format="YYYY-MM-DD"),
        },
    )

    st.download_button(
        "Download CSV",
        df.to_csv(index=False).encode("utf-8"),
        file_name="pensiongraph_performance.csv",
        mime="text/csv",
    )

    quarterly_rows = queries.quarterly_performance_rows(get_db_session())
    if quarterly_rows:
        st.divider()
        st.subheader("Quarterly performance (where available)")
        st.caption(
            "True periodic total-fund returns, sourced from monthly "
            "performance-review reports rather than the annual CAFR. "
            "Currently only New York City's comptroller-published reports "
            "qualify — one row per constituent system, since the reports "
            "cover NYCERS/TRS/POLICE/FIRE/BERS separately rather than one "
            "combined fund. '3 months' is the closest figure to a calendar "
            "quarter these reports state."
        )
        qdf = pd.DataFrame(quarterly_rows)
        qdf = qdf[[c for c in
                   ["Plan", "Fund", "As of", "1 month", "3 months", "FYTD",
                    "Source", "Source date"]
                   if c in qdf.columns]]
        st.dataframe(
            qdf,
            width="stretch",
            hide_index=True,
            column_config={
                "Source": st.column_config.LinkColumn("Source", display_text="Report"),
                "Source date": st.column_config.DatetimeColumn(
                    "Source date", format="YYYY-MM-DD"),
            },
        )


def main():
    plan_id, plan_label = render_sidebar()

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

    plan_param = st.query_params.get("plan")
    if plan_param:
        page_plan_twin(plan_param)
        return

    confirm_param = st.query_params.get("confirm")
    if confirm_param:
        page_subscriber_confirm(confirm_param)
        return

    unsub_param = st.query_params.get("unsub")
    if unsub_param:
        page_subscriber_unsubscribe(unsub_param)
        return

    prefs_param = st.query_params.get("prefs")
    if prefs_param:
        page_subscriber_preferences(prefs_param)
        return

    # Sidebar admin login affordance — no-op when ADMIN_PASSWORD is unset
    # (local dev) or when already unlocked (shows a "lock" button instead).
    _render_admin_login_sidebar()

    # Tabs are built from a (label, render) list so the gated tabs can be
    # appended only when unlocked. When locked, Archive, Drafts and Admin
    # disappear entirely from the tab strip — visitors don't see them at all.
    tab_specs: list[tuple[str, callable]] = [
        ("Insights",            lambda: page_insights()),
        ("Activity",            lambda: page_activity(plan_id, plan_label)),
        ("Search",              lambda: page_search(plan_id, plan_label)),
        ("Investment Actions",  lambda: page_investment_actions(plan_id, plan_label)),
        ("Managers",            lambda: page_managers()),
        ("CAFR",                lambda: page_cafr()),
        ("Asset Allocation",    lambda: page_asset_allocation()),
        ("Performance",         lambda: page_performance()),
        ("Meeting Recordings",  lambda: page_meeting_recordings(plan_id, plan_label)),
        ("Plans",               lambda: page_plans()),
        ("Subscribe",           lambda: page_subscribe()),
    ]
    if _admin_unlocked():
        tab_specs.append(("Archive", lambda: page_archive()))
        tab_specs.append(("Drafts",  lambda: page_drafts()))
        tab_specs.append(("Admin",   lambda: page_admin()))

    tabs = st.tabs([label for label, _ in tab_specs])
    for tab, (_label, render) in zip(tabs, tab_specs):
        with tab:
            render()


if __name__ == "__main__":
    # Streamlit re-executes this module top-to-bottom on every interaction,
    # so the finally runs once per rerun -- which is exactly the boundary
    # the shared Session's transaction must not outlive.
    try:
        main()
    finally:
        _release_db_session()
