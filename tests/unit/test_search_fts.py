"""FTS5-backed summary search: ranking, sync triggers, and ILIKE fallback."""

from __future__ import annotations

from datetime import datetime

import database
from database import (
    Document, Plan, Summary,
    _build_fts_match,
    count_search_summaries,
    search_summaries,
    tokenize_search_query,
)


def _seed(session, *, plan_id="calpers", doc_id, summary_text,
          key_topics="[]", investment_actions="[]", decisions="[]",
          meeting_date=None):
    if session.get(Plan, plan_id) is None:
        session.add(Plan(id=plan_id, name=plan_id.upper(),
                         abbreviation=plan_id.upper(), state="CA",
                         aum_billions=100.0))
        session.flush()
    doc = Document(
        id=doc_id,
        plan_id=plan_id,
        url=f"https://example.com/{plan_id}-{doc_id}.pdf",
        filename=f"{plan_id}-{doc_id}.pdf",
        doc_type="board_pack",
        local_path=f"/tmp/{plan_id}-{doc_id}.pdf",
        extraction_status="done",
        meeting_date=meeting_date or datetime(2025, 1, 1),
        page_count=1,
    )
    session.add(doc)
    session.flush()
    summary = Summary(
        document_id=doc.id,
        summary_text=summary_text,
        key_topics=key_topics,
        investment_actions=investment_actions,
        decisions=decisions,
        generated_at=datetime.utcnow(),
    )
    session.add(summary)
    session.commit()
    return doc, summary


def test_init_db_creates_fts_virtual_table(session):
    row = session.execute(database.text(
        "SELECT name FROM sqlite_master "
        "WHERE type='table' AND name='summaries_fts'"
    )).fetchone()
    assert row is not None, "init_db should create the summaries_fts virtual table"


def test_insert_trigger_populates_fts(session):
    _seed(session, doc_id=1, summary_text="Discussion of private equity allocation")
    rows = session.execute(database.text(
        "SELECT rowid FROM summaries_fts WHERE summaries_fts MATCH :m"
    ), {"m": '"private"'}).fetchall()
    assert len(rows) == 1


def test_search_returns_results_for_keyword(session):
    _seed(session, doc_id=1,
          summary_text="Approved allocation increase to BlackRock infrastructure fund.")
    _seed(session, doc_id=2,
          summary_text="Reviewed quarterly performance with no manager changes.")
    results = search_summaries(session, "BlackRock")
    assert len(results) == 1
    assert results[0][0].id == 1


def test_search_ranks_more_relevant_first(session):
    _seed(session, doc_id=1,
          summary_text="Brief mention of infrastructure once.")
    _seed(session, doc_id=2,
          summary_text=("Infrastructure infrastructure infrastructure — board "
                        "approved a new infrastructure mandate."))
    results = search_summaries(session, "infrastructure")
    assert [doc.id for doc, _, _ in results] == [2, 1], "bm25 should rank doc 2 first"


def test_search_and_joins_multiple_terms(session):
    _seed(session, doc_id=1, summary_text="Private equity mandate awarded.")
    _seed(session, doc_id=2, summary_text="Private credit allocation increased.")
    results = search_summaries(session, "private equity")
    assert [doc.id for doc, _, _ in results] == [1]


def test_search_searches_key_topics_and_investment_actions(session):
    _seed(session, doc_id=1,
          summary_text="Quarterly review.",
          key_topics='["climate-related divestment", "asset allocation"]')
    _seed(session, doc_id=2,
          summary_text="Portfolio rebalancing.",
          investment_actions='[{"manager": "Vanguard", "action": "hire"}]')
    results = search_summaries(session, "Vanguard")
    assert [doc.id for doc, _, _ in results] == [2]
    results = search_summaries(session, "divestment")
    assert [doc.id for doc, _, _ in results] == [1]


def test_search_handles_special_characters_safely(session):
    _seed(session, doc_id=1, summary_text="Discussion of M&A activity.")
    # None of these should raise; sanitisation strips FTS5 operator chars.
    for q in ['(parens)', 'star*', 'colon:value', '"quoted"', '^caret', 'a-b']:
        assert isinstance(search_summaries(session, q), list)


def test_search_filters_by_plan(session):
    _seed(session, plan_id="calpers", doc_id=1,
          summary_text="Infrastructure allocation review.")
    _seed(session, plan_id="calstrs", doc_id=2,
          summary_text="Infrastructure allocation review.")
    cal = search_summaries(session, "infrastructure", plan_id="calpers")
    assert [doc.plan_id for doc, _, _ in cal] == ["calpers"]
    strs = search_summaries(session, "infrastructure", plan_id="calstrs")
    assert [doc.plan_id for doc, _, _ in strs] == ["calstrs"]


def test_count_matches_search_results(session):
    for i in range(1, 6):
        _seed(session, doc_id=i, summary_text=f"Document {i} discusses infrastructure.")
    total = count_search_summaries(session, "infrastructure")
    assert total == 5
    assert len(search_summaries(session, "infrastructure", limit=100)) == 5


def test_update_trigger_keeps_fts_in_sync(session):
    _, summary = _seed(session, doc_id=1, summary_text="Original about housing.")
    assert search_summaries(session, "housing")
    summary.summary_text = "Rewritten to discuss healthcare."
    session.commit()
    assert search_summaries(session, "healthcare")
    assert not search_summaries(session, "housing")


def test_delete_trigger_removes_from_fts(session):
    _, summary = _seed(session, doc_id=1, summary_text="To be deleted: airports.")
    assert search_summaries(session, "airports")
    session.delete(summary)
    session.commit()
    assert not search_summaries(session, "airports")


def test_empty_query_returns_no_results(session):
    _seed(session, doc_id=1, summary_text="Anything.")
    assert search_summaries(session, "") == []
    assert search_summaries(session, "   ") == []
    assert count_search_summaries(session, "") == 0


def test_falls_back_to_ilike_when_fts_missing(session):
    _seed(session, doc_id=1, summary_text="Fallback substring test.")
    session.execute(database.text("DROP TRIGGER IF EXISTS summaries_ai"))
    session.execute(database.text("DROP TRIGGER IF EXISTS summaries_au"))
    session.execute(database.text("DROP TRIGGER IF EXISTS summaries_ad"))
    session.execute(database.text("DROP TABLE IF EXISTS summaries_fts"))
    session.commit()
    # FTS path raises in execute() — search_summaries falls back to ILIKE.
    results = search_summaries(session, "substring")
    assert [doc.id for doc, _, _ in results] == [1]
    assert count_search_summaries(session, "substring") == 1


def test_search_returns_snippet_with_mark_tags(session):
    _seed(session, doc_id=1,
          summary_text=("The board approved a sizeable allocation to "
                        "infrastructure during the quarterly review session."))
    results = search_summaries(session, "infrastructure")
    assert len(results) == 1
    _, _, snippet = results[0]
    assert "<mark>infrastructure</mark>" in snippet


def test_search_snippet_marks_each_query_token(session):
    _seed(session, doc_id=1,
          summary_text=("Private equity mandate awarded to mid-market manager "
                        "after a competitive RFP process."))
    results = search_summaries(session, "private equity")
    assert len(results) == 1
    _, _, snippet = results[0]
    assert "<mark>Private</mark>" in snippet or "<mark>private</mark>" in snippet
    assert "<mark>equity</mark>" in snippet


def test_ilike_fallback_returns_empty_snippet(session):
    _seed(session, doc_id=1, summary_text="Fallback substring test.")
    session.execute(database.text("DROP TRIGGER IF EXISTS summaries_ai"))
    session.execute(database.text("DROP TRIGGER IF EXISTS summaries_au"))
    session.execute(database.text("DROP TRIGGER IF EXISTS summaries_ad"))
    session.execute(database.text("DROP TABLE IF EXISTS summaries_fts"))
    session.commit()
    results = search_summaries(session, "substring")
    assert len(results) == 1
    assert results[0][2] == ""


def test_tokenize_search_query_unit():
    assert tokenize_search_query("") == []
    assert tokenize_search_query("   ") == []
    assert tokenize_search_query("BlackRock") == ["BlackRock"]
    assert tokenize_search_query("private equity") == ["private", "equity"]
    assert tokenize_search_query('(foo bar)') == ["foo", "bar"]
    assert tokenize_search_query('foo*') == ["foo"]


def test_build_fts_match_unit():
    assert _build_fts_match("") is None
    assert _build_fts_match("   ") is None
    assert _build_fts_match("BlackRock") == '"BlackRock"'
    assert _build_fts_match("private equity") == '"private" "equity"'
    # Operator characters get stripped, not escaped, so the result still parses.
    assert _build_fts_match('foo*') == '"foo"'
    assert _build_fts_match('(foo bar)') == '"foo" "bar"'
