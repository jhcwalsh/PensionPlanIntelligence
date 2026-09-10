"""The weekly watch: Albourne, consultant RFPs, senior hires and departures.

Emailed to the approval recipient only. Never archived, never listed on
the public Weekly tab, never offered to subscribers.
"""
from __future__ import annotations

import types
from datetime import date, datetime, timedelta, timezone

import pytest

import summarizer
from database import Document, Plan, Publication, get_session
from insights import config, watch


NOW = datetime(2026, 9, 13, 13, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Matchers
# ---------------------------------------------------------------------------

def _categories(text):
    return sorted({h.category for h in watch.find_hits(text)})


def test_albourne_is_matched_by_name():
    assert _categories("The board received Albourne's quarterly hedge fund review.") \
        == ["albourne"]


def test_consultant_near_rfp_is_a_consultant_search():
    text = ("Staff recommends the Board authorize an RFP for general investment "
            "consultant services, the current contract expiring in June.")
    assert "consultant_rfp" in _categories(text)


def test_consultant_far_from_rfp_is_not_a_search():
    text = ("The consultant presented the asset allocation study. " + "x " * 400
            + "Separately, an RFP for printing services was issued.")
    assert "consultant_rfp" not in _categories(text)


def test_search_alone_is_not_a_consultant_search():
    assert _categories("The manager search for small-cap equity continues.") == []


def test_the_phrase_consultant_search_is_matched_without_an_rfp_word():
    assert "consultant_rfp" in _categories(
        "The Board discussed the timeline for the general investment consultant search.")


def test_tax_advisor_boilerplate_is_not_a_consultant_search():
    text = ("Please consult your tax advisor before making investment decisions. "
            "Use the search bar on the home page to find your FC's biography.")
    assert _categories(text) == []


def test_an_attendance_list_is_not_a_senior_move():
    text = ("ATTENDEES: Ms. Laura Gilson, General Counsel; Mr. Carlos Borromeo, Deputy "
            "Director; Commissioner Marshall was appointed Vice-Chair of the Board.")
    assert _categories(text) == []


def test_a_manager_bio_is_not_a_senior_move():
    text = ("John Smith, Managing Director, joined the firm in 2015 and was named "
            "head of the private credit team.")
    assert _categories(text) == []


def test_a_signature_block_naming_the_retirement_system_is_not_a_move():
    text = ("Larry Walther, Committee Chair; Amy Fecher, APERS Executive Director. "
            "Arkansas Public Employees Retirement System Board of Trustees.")
    assert _categories(text) == []


def test_an_announced_retirement_is_a_move():
    assert _categories("The Executive Director announced her retirement effective June 30.")         == ["senior_move"]


def test_a_retired_member_appeal_is_not_a_move():
    text = ("The Executive Director's determination to deny the retired member's "
            "request was upheld.")
    assert _categories(text) == []


def test_a_cio_search_is_a_senior_move():
    assert _categories("The Board authorized the Executive Director to begin a CIO search.")         == ["senior_move"]


def test_senior_title_near_a_move_verb_is_a_senior_move():
    text = "Chief Investment Officer Jane Doe announced she will retire in December."
    assert _categories(text) == ["senior_move"]


def test_senior_title_without_a_move_is_not_reported():
    assert _categories("The Chief Investment Officer presented the performance report.") == []


def test_a_hit_carries_a_snippet_around_the_match():
    text = "a " * 500 + "Albourne recommended the commitment." + " b" * 500
    (hit,) = watch.find_hits(text)
    assert "Albourne recommended" in hit.snippet
    assert len(hit.snippet) <= 2 * watch.SNIPPET_CHARS + 60


def test_hits_per_document_are_capped():
    text = " ".join(["Albourne"] * 50)
    doc = Document(plan_id="p", url="u", filename="f.pdf", extracted_text=text)
    assert len(watch.scan_document(doc, None)) <= watch.MAX_HITS_PER_DOC


# ---------------------------------------------------------------------------
# Window
# ---------------------------------------------------------------------------

@pytest.fixture
def plan():
    s = get_session()
    s.add(Plan(id="calpers", name="CalPERS", abbreviation="CalPERS"))
    s.commit()
    s.close()


def _doc(filename, downloaded_at, text="board met", **kw):
    s = get_session()
    d = Document(plan_id="calpers", url=f"https://x/{filename}", filename=filename,
                 doc_type="agenda", downloaded_at=downloaded_at,
                 extraction_status="done", extracted_text=text, **kw)
    s.add(d)
    s.commit()
    doc_id = d.id
    s.close()
    return doc_id


def test_window_is_the_last_seven_days_of_downloads(plan):
    recent = _doc("recent.pdf", NOW - timedelta(days=3))
    _doc("old.pdf", NOW - timedelta(days=10))
    _doc("undownloaded.pdf", None)

    s = get_session()
    ids = [d.id for d in watch.select_window_docs(s, now_utc=NOW)]
    s.close()
    assert ids == [recent]


# ---------------------------------------------------------------------------
# Compose and cycle
# ---------------------------------------------------------------------------

def test_empty_week_composes_without_the_model(monkeypatch):
    monkeypatch.setenv("INSIGHTS_MODE", "live")
    monkeypatch.setattr(summarizer, "_get_client",
                        lambda: pytest.fail("model called on an empty week"))
    md = watch.compose_watch([], docs_scanned=12, plans_scanned=5,
                             period_start=date(2026, 9, 6), period_end=date(2026, 9, 13))
    assert "Nothing matched" in md
    assert "12 documents" in md


def test_live_compose_sends_grouped_snippets_to_sonnet_thinking_off(monkeypatch):
    monkeypatch.setenv("INSIGHTS_MODE", "live")
    seen = {}

    def create(**kwargs):
        seen.update(kwargs)
        return types.SimpleNamespace(
            content=[types.SimpleNamespace(type="text", text="# Weekly Watch\n\nbody")],
            stop_reason="end_turn")

    monkeypatch.setattr(summarizer, "_get_client", lambda: types.SimpleNamespace(
        messages=types.SimpleNamespace(create=create)))

    doc = Document(plan_id="calpers", url="https://x/a.pdf", filename="a.pdf",
                   meeting_date=datetime(2026, 9, 10))
    hits = [watch.Hit(category="albourne", doc=doc, plan_name="CalPERS",
                      snippet="Albourne reviewed the hedge fund book.")]

    md = watch.compose_watch(hits, docs_scanned=1, plans_scanned=1,
                             period_start=date(2026, 9, 6), period_end=date(2026, 9, 13))

    assert md.startswith("# Weekly Watch")
    assert seen["model"] == summarizer.MODEL_SONNET
    assert seen["thinking"] == {"type": "disabled"}
    assert "temperature" not in seen
    user = seen["messages"][0]["content"]
    assert "CalPERS" in user and "https://x/a.pdf" in user \
        and "Albourne reviewed" in user and "[albourne]" in user


def test_cycle_publishes_a_watch_row_that_is_emailed_but_not_archived(plan, monkeypatch):
    _doc("pack.pdf", NOW - timedelta(days=2),
         text="Albourne Partners presented the private markets pacing plan.")
    written = []
    from insights import publish
    monkeypatch.setattr(publish, "write_note", lambda pub: written.append(pub))
    sent = []
    from insights import approval
    real_send = approval.send_email
    monkeypatch.setattr(approval, "send_email",
                        lambda email, **kw: sent.append(email) or real_send(email, **kw))

    pub = watch.run_watch_cycle(now=NOW)

    assert pub.cadence == "watch"
    assert pub.status == "published"
    assert pub.period_start == date(2026, 9, 6) and pub.period_end == date(2026, 9, 13)
    assert written == []
    assert len(sent) == 1


def test_cycle_is_idempotent_for_the_period(plan):
    first = watch.run_watch_cycle(now=NOW)
    second = watch.run_watch_cycle(now=NOW)
    assert first.id == second.id
    s = get_session()
    assert s.query(Publication).filter_by(cadence="watch").count() == 1
    s.close()


def test_watch_rows_stay_off_the_public_weekly_tab(plan):
    watch.run_watch_cycle(now=NOW)
    import queries
    s = get_session()
    assert queries.weekly_briefings(s) == []
    s.close()


def test_watch_is_not_a_subscriber_cadence():
    from insights import subscribers
    assert "watch" not in subscribers.CADENCES


def test_watch_has_its_own_subject_line():
    prefix, product, slug = config.cadence_display("watch")
    assert prefix == "Weekly Watch"
    assert slug == "weekly_watch"


def test_scheduler_dispatches_the_watch_cycle(monkeypatch):
    calls = []
    monkeypatch.setattr(watch, "run_watch_cycle",
                        lambda **kw: calls.append(kw) or types.SimpleNamespace(id=1, status="published"))
    from insights import scheduler
    assert scheduler.main(["watch", "--force"]) == 0
    assert calls == [{"force": True}]


def test_prompt_confines_moves_to_plan_staff_and_rfps_to_consultants():
    """The first issue drifted: TPG's CFO and a board assistant's farewell
    under senior moves, an outside-counsel RFP under consultant searches."""
    p = watch._SYSTEM_PROMPT
    assert "external investment managers" in p and "trustees" in p
    assert "legal counsel" in p
