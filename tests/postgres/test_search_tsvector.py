"""Ranked search on Postgres, replacing SQLite FTS5.

Both engines must work simultaneously: step 4 dual-runs staging on Postgres
beside production on SQLite.
"""
from __future__ import annotations

from sqlalchemy.orm import Session

import database


def _seed(pg_engine):
    with Session(pg_engine) as s:
        s.add(database.Plan(id="calpers", name="CalPERS", state="CA"))
        for i, text in enumerate([
            "the board approved a private equity manager search",
            "routine minutes with no investment content",
            "private equity pacing plan and manager search update",
        ], start=1):
            doc = database.Document(
                id=i, plan_id="calpers", url=f"https://x/{i}.pdf",
                filename=f"{i}.pdf", doc_type="minutes",
                extraction_status="done", downloaded_at=database.utcnow())
            s.add(doc)
            s.flush()
            s.add(database.Summary(document_id=doc.id, summary_text=text))
        s.commit()


def test_search_finds_matching_summaries(pg_engine):
    _seed(pg_engine)
    with Session(pg_engine) as s:
        rows = database.search_summaries(s, "manager search")
    texts = [summary.summary_text for _doc, summary in rows]
    assert len(texts) == 2, texts
    assert all("manager search" in t for t in texts)


def test_search_excludes_non_matching(pg_engine):
    _seed(pg_engine)
    with Session(pg_engine) as s:
        rows = database.search_summaries(s, "private equity")
    assert all("routine minutes" not in su.summary_text for _d, su in rows)


def test_search_handles_a_phrase_and_an_exclusion(pg_engine):
    """websearch_to_tsquery gives quoted phrases and -exclusions for free."""
    _seed(pg_engine)
    with Session(pg_engine) as s:
        rows = database.search_summaries(s, '"manager search" -pacing')
    assert len(rows) == 1, [su.summary_text for _d, su in rows]


def test_count_matches_the_result_set(pg_engine):
    _seed(pg_engine)
    with Session(pg_engine) as s:
        rows = database.search_summaries(s, "private equity")
        total = database.count_search_summaries(s, "private equity")
    assert total == len(rows)


def test_empty_query_returns_nothing(pg_engine):
    _seed(pg_engine)
    with Session(pg_engine) as s:
        assert database.search_summaries(s, "   ") == []


def test_the_gin_index_actually_exists(pg_engine):
    """Without this, an inert index is invisible.

    Postgres answers a tsvector query correctly via sequential scan, so every
    other test here passes whether or not the index was created. At 4,200
    summaries that is a silent performance cliff rather than a failure.
    """
    import sqlalchemy as sa
    with pg_engine.connect() as conn:
        rows = conn.execute(sa.text(
            "SELECT indexdef FROM pg_indexes "
            "WHERE tablename = 'summaries' AND indexname = :n"),
            {"n": "ix_summaries_search_vector"}).fetchall()
    assert rows, "ix_summaries_search_vector was not created"
    assert "gin" in rows[0][0].lower(), rows[0][0]
