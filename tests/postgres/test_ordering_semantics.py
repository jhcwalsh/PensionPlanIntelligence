"""NULL ordering, verified where the two backends actually disagree.

SQLite sorts NULLs last in a DESC ordering; Postgres treats NULL as larger
than any value and sorts them first. So a test of this behaviour on SQLite
passes whether or not the fix is present -- which is exactly how the defect
survived until the step-4 dual run compared the read layer against Neon.

The consequence in production data: recent_summaries deliberately keeps
documents with no meeting_date, and on Postgres those sorted to the top. The
Activity page led with 2006 board agendas while the current week's meetings
fell off the end of the limit.

See docs/superpowers/plans/2026-08-21-postgres-dual-run.md, Task 3.
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy.orm import sessionmaker

import queries
from database import Document, Plan, Summary


def _seed(session):
    session.add(Plan(id="p1", name="Plan One"))
    session.flush()
    # Deliberately inserted undated-first, so an id-ordered fallback would
    # also put the NULLs at the top and the test could not pass by accident.
    rows = [None,
            datetime(2026, 8, 20, tzinfo=timezone.utc),
            None,
            datetime(2026, 8, 19, tzinfo=timezone.utc)]
    for i, meeting in enumerate(rows):
        doc = Document(plan_id="p1", url="https://x/%d.pdf" % i,
                       filename="%d.pdf" % i, doc_type="minutes",
                       extraction_status="done", meeting_date=meeting)
        session.add(doc)
        session.flush()
        session.add(Summary(document_id=doc.id, summary_text="summary %d" % i))
    session.commit()


def test_recent_summaries_puts_undated_documents_last(pg_engine):
    with sessionmaker(bind=pg_engine)() as session:
        _seed(session)
        dates = [d.meeting_date for d, _ in queries.recent_summaries(session)]

    assert dates[0] is not None, (
        "an undated document sorted first — meeting_date.desc() needs "
        "nullslast() or Postgres buries every dated row")
    assert [d.day for d in dates[:2]] == [20, 19], dates
    assert all(x is None for x in dates[2:]), dates


def test_dated_documents_survive_the_limit(pg_engine):
    """The user-visible harm, stated directly.

    With NULLs first and a limit, dated documents do not merely appear lower
    down — they are not returned at all.
    """
    with sessionmaker(bind=pg_engine)() as session:
        _seed(session)
        rows = queries.recent_summaries(session, None, 2)

    assert [d.meeting_date.day for d, _ in rows] == [20, 19], \
        [d.meeting_date for d, _ in rows]


def test_documents_by_ids_puts_undated_documents_last(pg_engine):
    """The same ordering, the same nullable column, the same fix."""
    with sessionmaker(bind=pg_engine)() as session:
        _seed(session)
        ids = [d.id for d in session.query(Document).all()]
        dates = [d.meeting_date for d in queries.documents_by_ids(session, ids)]

    assert dates[0] is not None, dates
    assert all(x is None for x in dates[2:]), dates
