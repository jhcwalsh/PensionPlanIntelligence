"""Stage-1 diagnostic verdict logic — exercised through the loader injection seam."""

from __future__ import annotations

from lib.pipeline_diagnostic import (
    NO_TASK_CONTENT,
    STAGE_1_HEALTHY,
    STAGE_1_SUSPECTED,
    TASK_PROFILES,
    diagnose_document,
)


RFP_PROFILE = TASK_PROFILES["rfp"]


def _loader(pages: list[tuple[str, bool]]):
    texts = [p[0] for p in pages]
    has_image = [p[1] for p in pages]
    return lambda: (texts, has_image)


def test_healthy_when_relevant_text_present():
    pages = [
        ("CalPERS Board of Administration meeting agenda for March 15, 2024. "
         "Item 3: Discussion of Q3 returns and asset allocation review by the "
         "Investment Committee. Item 4: Public comment period.", False),
        ("RFP for general investment consultant — finalists named: Wilshire, "
         "Verus, Callan. Response due 2024-05-01. Mandate covers asset "
         "allocation review and manager monitoring.", False),
        ("Meeting adjournment scheduled for 4:00pm. Next regular meeting will "
         "be held on April 19, 2024 at the same location. Public materials "
         "available on the CalPERS website.", False),
    ]
    diag = diagnose_document("ignored", RFP_PROFILE, _loader(pages))
    assert diag.verdict == STAGE_1_HEALTHY
    assert diag.task_relevant_pages == 1
    assert diag.blank_pages == 0
    assert diag.scanned_pages == 0


def test_no_task_content_when_no_keyword_match():
    pages = [
        ("Quarterly returns by asset class were discussed. The Total Fund "
         "returned 8.2% versus benchmark of 7.9%. Fixed income lagged at 1.1%.",
         False),
        ("Public comment received. Meeting adjourned at 3:45pm.", False),
    ]
    diag = diagnose_document("ignored", RFP_PROFILE, _loader(pages))
    assert diag.verdict == NO_TASK_CONTENT
    assert diag.task_relevant_pages == 0


def test_no_pages_returns_no_task_content():
    diag = diagnose_document("ignored", RFP_PROFILE, lambda: ([], []))
    assert diag.verdict == NO_TASK_CONTENT


def test_blank_pages_detected():
    pages = [
        ("", False),
        ("", False),
        ("RFP for consulting services issued 2024-03-15. Response due 2024-04-30.",
         False),
    ]
    diag = diagnose_document("ignored", RFP_PROFILE, _loader(pages))
    assert diag.blank_pages == 2
    # 2/3 bad pages → SUSPECTED
    assert diag.verdict == STAGE_1_SUSPECTED


def test_scanned_pages_detected():
    pages = [
        ("", True),    # scanned: no text, has image
        ("", True),
        ("", True),
        ("RFP for actuarial services. Search committee will review responses.",
         False),
    ]
    diag = diagnose_document("ignored", RFP_PROFILE, _loader(pages))
    assert diag.scanned_pages == 3
    assert diag.verdict == STAGE_1_SUSPECTED
    assert any("scanned" in r.lower() for r in diag.rationale)


def test_garbled_pages_detected():
    # Long page with mostly punctuation/symbols
    garbled_text = ("$%^&*()@#" * 50) + " " + ("!@#$%^&*()" * 50)
    pages = [
        (garbled_text, False),
        (garbled_text, False),
        ("Issued: RFP for global equity manager. Mandate size $500M. "
         "Finalists named: BlackRock, State Street, Vanguard.", False),
    ]
    diag = diagnose_document("ignored", RFP_PROFILE, _loader(pages))
    assert diag.garbled_pages == 2
    assert diag.verdict == STAGE_1_SUSPECTED


def test_keyword_word_boundary_avoids_false_match():
    # 'inissued' should not match 'issued'; 'rfpcorp' should not match 'rfp'
    pages = [("inissued rfpcorp something else entirely", False)]
    diag = diagnose_document("ignored", RFP_PROFILE, _loader(pages))
    assert diag.task_relevant_pages == 0
    assert diag.verdict == NO_TASK_CONTENT


def test_structure_score_drops_with_bad_pages():
    relevant = (
        "Item 5 of the agenda: Issued RFP for general investment consulting "
        "services. Response due date is 2024-06-01. Search committee has "
        "been formed and will review responses."
    )
    pages = [
        ("", False),
        ("", False),
        (relevant, False),
        (relevant, False),
    ]
    diag = diagnose_document("ignored", RFP_PROFILE, _loader(pages))
    # 2 blanks of 4 pages = score 0.5
    assert abs(diag.structure_score - 0.5) < 1e-6


def test_min_relevant_pages_required():
    # Single relevant page is enough for default rfp profile (min=1)
    pages = [(
        "Agenda Item 7 — Legal Services. The Board issued an RFP for "
        "outside legal counsel covering investment-related matters. "
        "Responses due 2024-07-01. Selection committee comprises three "
        "trustees and two staff members. Estimated mandate value $2M annually.",
        False,
    )]
    diag = diagnose_document("ignored", RFP_PROFILE, _loader(pages))
    assert diag.verdict == STAGE_1_HEALTHY
