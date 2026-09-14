"""Breadth in the weekly briefing's input (built 2026-09-14).

Six months of briefings were dominated by a handful of small plans. Three
mechanisms, each pinned here:

1. Item-level PDFs. El Paso and Metro Nashville publish every agenda item
   as its own file, so one meeting arrived as twenty summaries next to
   CalPERS's one. Summaries per meeting are now capped, minutes and packs
   kept ahead of agenda items.
2. Undated documents. 467 of 2,358 documents in the window had no meeting
   date and were placed by download date; NM PERA's 2018 minutes reached
   an April briefing that way. Undated meetings no longer enter the
   composer's input (the Activity tab still shows them).
3. No cap per plan and "meetings with investment actions first" ordering,
   with truncation at MAX_PROMPT_CHARS dropping whatever came last. Now at
   most three meetings per plan, ordered by AUM so truncation drops the
   smallest funds, and the prompt asks for breadth.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

import generate_notes
from database import Document, Plan, Summary, get_session


def _now():
    return datetime.now(timezone.utc)


@pytest.fixture()
def seeded(tmp_db):
    s = get_session()
    s.add(Plan(id="big", name="Big Fund", abbreviation="BIG", aum_billions=500.0))
    s.add(Plan(id="small", name="Small Fund", abbreviation="SML", aum_billions=2.0))
    s.add(Plan(id="mid", name="Mid Fund", abbreviation="MID", aum_billions=50.0))
    n = 0

    def doc(pid, meeting, doc_type, actions=0, decisions=0):
        nonlocal n
        n += 1
        d = Document(plan_id=pid, url=f"https://x/{n}.pdf", filename=f"{n}.pdf",
                     doc_type=doc_type, extraction_status="done",
                     meeting_date=meeting, downloaded_at=_now() - timedelta(days=1))
        s.add(d); s.flush()
        s.add(Summary(document_id=d.id, summary_text=f"summary {n}",
                      investment_actions=json.dumps([{"action": "hire", "description": f"a{n}"}] * actions),
                      decisions=json.dumps([{"description": f"d{n}"}] * decisions)))
        return d

    m1 = _now() - timedelta(days=3)
    # Small fund, one meeting published as twelve item-level agendas plus
    # one minutes file: the minutes must survive the per-meeting cap.
    for _ in range(12):
        doc("small", m1, "agenda", actions=1)
    doc("small", m1, "minutes", decisions=3)
    # Small fund also met on five other days this week.
    for k in range(1, 6):
        doc("small", m1 - timedelta(days=k, hours=1), "agenda")
    # Big fund: one meeting, one board pack, no investment actions.
    doc("big", m1 - timedelta(days=1), "board_pack")
    # Mid fund: one dated meeting with actions, one undated document.
    doc("mid", m1 - timedelta(days=2), "minutes", actions=2)
    doc("mid", None, "minutes", actions=5)
    s.commit()
    yield s
    s.close()


def _by_plan(data):
    out = {}
    for m in data["meetings"]:
        out.setdefault(m["plan"].id, []).append(m)
    return out


def test_undated_meetings_do_not_enter_the_composer_input(seeded):
    data = generate_notes.gather_highlights_data(seeded, days=7)
    assert all(m["meeting_date"] is not None for m in data["meetings"])
    assert data["dropped"]["undated_meetings"] == 1


def test_at_most_three_meetings_per_plan_the_most_recent(seeded):
    data = generate_notes.gather_highlights_data(seeded, days=7)
    small = _by_plan(data)["small"]
    assert len(small) == generate_notes.MAX_MEETINGS_PER_PLAN == 3
    dates = [m["meeting_date"] for m in small]
    assert dates == sorted(dates, reverse=True)
    assert data["dropped"]["meetings_over_cap"] == 3


def test_summaries_per_meeting_are_capped_with_minutes_kept(seeded):
    data = generate_notes.gather_highlights_data(seeded, days=7)
    small = _by_plan(data)["small"]
    packed = max(small, key=lambda m: len(m["all_docs"]))
    kept = packed["all_summaries"]
    assert len(kept) == generate_notes.MAX_SUMMARIES_PER_MEETING == 6
    assert kept[0]["doc_type"] == "minutes", "minutes rank ahead of agenda items"
    assert data["dropped"]["summaries_over_cap"] == 7


def test_prompt_orders_plans_by_aum_not_by_who_has_actions(seeded):
    data = generate_notes.gather_highlights_data(seeded, days=7)
    text = generate_notes.format_meetings_for_prompt(data["meetings"])
    first = text.index("=== BIG ")
    mid = text.index("=== MID ")
    small = text.index("=== SML ")
    assert first < mid < small


def test_prompt_asks_for_breadth(seeded):
    data = generate_notes.gather_highlights_data(seeded, days=7)
    prompt = generate_notes.build_highlights_prompt(data, days=7)
    assert "BREADTH" in prompt
    assert "larger fund" in prompt


def test_plans_with_activity_counts_only_what_the_model_sees(seeded):
    data = generate_notes.gather_highlights_data(seeded, days=7)
    assert data["plans_with_activity"] == 3
    assert {p.id for p in data["plans"]} == {"big", "small", "mid"}
