"""Discovery fixes for the eight largest plans that found zero links.

Found on 2026-09-10: 31 plans, including the largest funds tracked, had
had no download since the April backfill. Their listing pages rendered;
the fetcher discarded what it saw. Three causes, each pinned here.
"""
import json
import pathlib
from datetime import datetime

from bs4 import BeautifulSoup

import fetcher
from scripts import waf_blocked_ids

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _plans():
    return {p["id"]: p for p in json.loads(
        (ROOT / "data" / "known_plans.json").read_text(encoding="utf-8"))}


# ---------------------------------------------------------------------------
# Dates. The YYYY-MM-DD pattern had three capture groups joined with spaces
# and a hyphenated strptime format, so it never matched anything: every
# "board-meeting-minutes-2026-07-29.pdf" in the corpus was undated unless its
# link text carried a spelled-out date.
# ---------------------------------------------------------------------------

def test_iso_date_in_a_filename_parses():
    assert fetcher.parse_date_from_text("board-meeting-minutes-2026-07-29.pdf") \
        == datetime(2026, 7, 29)
    assert fetcher.parse_date_from_text("2026_07_29_agenda.pdf") == datetime(2026, 7, 29)


def test_year_month_filename_parses_to_the_first_of_the_month():
    """OPERS: agenda-2026-01.pdf; IMRF names by year and committee."""
    assert fetcher.parse_date_from_text("agenda-2026-01.pdf") == datetime(2026, 1, 1)


def test_month_year_filename_parses():
    """NYSCRF: monthly reports named june-2026.pdf."""
    assert fetcher.parse_date_from_text("june-2026.pdf") == datetime(2026, 6, 1)
    assert fetcher.parse_date_from_text("Sep 2026 Investment Report") == datetime(2026, 9, 1)


def test_an_act_number_is_not_a_date():
    assert fetcher.parse_date_from_text("Act 2026-286 Return-to-Work") is None


def test_spelled_out_dates_still_parse():
    assert fetcher.parse_date_from_text("July 29, 2026 Board Meeting Minutes") \
        == datetime(2026, 7, 29)
    assert fetcher.parse_date_from_text("06/11/2026") == datetime(2026, 6, 11)


# ---------------------------------------------------------------------------
# Keyword filter opt-out for pages that are already a document list.
# ---------------------------------------------------------------------------

_MONTHLY = BeautifulSoup(
    '<a href="/files/common-retirement-fund/pdf/june-2026.pdf">June</a>'
    '<a href="/files/common-retirement-fund/pdf/may-2026.pdf">May</a>',
    "html.parser")


def test_links_without_a_relevant_keyword_are_dropped_by_default():
    assert fetcher.extract_doc_links(_MONTHLY, "https://www.osc.ny.gov/x",
                                     investment_only=False) == []


def test_keep_all_documents_keeps_them_and_dates_them():
    found = fetcher.extract_doc_links(_MONTHLY, "https://www.osc.ny.gov/x",
                                      investment_only=False, keep_all_documents=True)
    assert [d["meeting_date"] for d in found] == [datetime(2026, 6, 1), datetime(2026, 5, 1)]


# ---------------------------------------------------------------------------
# Registry: what each of the eight needed.
# ---------------------------------------------------------------------------

def test_nystrs_opers_imrf_opt_out_of_the_investment_filter():
    """Their pages carry board agendas and minutes; the investment-only
    filter (default on, off for 138 other plans) dropped every one. IMRF's
    investment-committee agendas were dropped too: the path contains
    board-of-trustees, which the exclusion list reads as a non-investment
    committee."""
    plans = _plans()
    for pid in ("nystrs", "opers", "imrf"):
        assert plans[pid].get("investment_only") is False, pid


def test_trs_il_and_trs_nyc_follow_their_year_sub_pages():
    plans = _plans()
    assert plans["il_trs"]["sub_page_pattern"] == r"trsil\.org/trustees/minutes/\d{4}"
    assert plans["trsnyc"]["materials_url"].endswith("/ourretirementboard/meetingsum")
    assert plans["trsnyc"]["sub_page_pattern"] == r"ourretirementboard/meetingsum/\d{4}$"
    for pid in ("il_trs", "trsnyc"):
        assert plans[pid]["max_sub_pages"] == 2, "only the current and prior year"


def test_nyscrf_reads_the_monthly_report_page():
    """A sole-trustee fund: no board packs exist. Its monthly performance and
    asset-allocation reports are the materials."""
    p = _plans()["nyscrf"]
    assert p["materials_url"].endswith("/financial-reporting-and-asset-allocation")
    assert p["keep_all_documents"] is True


def test_urs_is_residential_only_and_rsa_has_no_materials():
    """URS: the runner's browser gets an empty page where a residential one
    gets 136 links and the PDFs download, so the Mac mini's list. RSA: its
    board pages carry Vimeo recordings and nothing else; transcripts
    stopped in 2021."""
    assert "urs_ut" in waf_blocked_ids.materials_ids()
    assert "rsa_al" in waf_blocked_ids.all_blocked_materials_ids()
    assert "rsa_al" not in waf_blocked_ids.materials_ids()


# ---------------------------------------------------------------------------
# URS. The Mac mini's residential IP renders newsroom.urs.org/board-meetings
# fine, but the 155 current board documents on it are router links with no
# extension: /documents/byfilename/@Public Web Documents@...@09-09-2026Agenda
# @@application@pdf/. The fetcher rejected every one as not-a-document and
# kept only the 2013-2019 PEHP committee PDFs further down, all fetched in
# April, so URS looked current and was not.
# ---------------------------------------------------------------------------

_URS = ("https://www.urs.org/documents/byfilename/@Public%20Web%20Documents@URS"
        "@External@BoardMeetings@2026%20Board%20Meeting%20Documents"
        "@09-09-2026Agenda@@application@pdf/")


def test_urs_byfilename_router_links_are_documents():
    assert fetcher.is_doc_url(_URS, "Agenda")


def test_urs_byfilename_links_are_named_and_dated_by_their_own_segment():
    assert fetcher.make_filename(_URS, "Agenda") == "09-09-2026Agenda.pdf"
    assert fetcher.parse_date_from_text(_URS) == datetime(2026, 9, 9)


def test_hyphenated_us_dates_parse():
    """m-d-Y with hyphens, the other half of the slash pattern. NYSTRS names
    audit minutes this way; OPERS uses two-digit years."""
    assert fetcher.parse_date_from_text("audit-committee-meeting-minutes-12-10-2025.pdf") \
        == datetime(2025, 12, 10)
    assert fetcher.parse_date_from_text("BoardMeeting_7-31-24.pdf") == datetime(2024, 7, 31)
