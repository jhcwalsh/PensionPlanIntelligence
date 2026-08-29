"""Backfill: local files first, then re-fetch, resumable throughout.

Ordering is not cosmetic. Local-file uploads are free and risk-free; the
re-fetch path costs bandwidth and races link rot (a 20-URL sample on
2026-08-29 found 19 still live, so ~5% is already unrecoverable and rising).
Doing the free half first means an interrupted run has still made progress.
"""
from __future__ import annotations

import pytest
import requests

import pdf_store
from database import Document, Plan, get_session
from scripts import backfill_pdf_store


@pytest.fixture(autouse=True)
def _no_rate_limit_sleep(monkeypatch):
    """The 0.5s inter-request delay is right in production and pure waste in
    a test -- three re-fetch tests spent ~1.5s doing nothing."""
    monkeypatch.setattr(backfill_pdf_store, "REQUEST_DELAY_SECONDS", 0)


def _seed(session, url, local_path=None, sha=None, plan_id="p1",
          retention_status=None):
    if session.get(Plan, plan_id) is None:
        session.add(Plan(id=plan_id, name=plan_id, abbreviation=plan_id,
                         state="CA"))
        session.flush()
    doc = Document(plan_id=plan_id, url=url, filename="d.pdf",
                   doc_type="board_pack", local_path=local_path,
                   content_sha256=sha, retention_status=retention_status)
    session.add(doc)
    session.commit()
    return doc


def _ok(data=b"%PDF-1.4 refetched"):
    """A _fetch_bytes stand-in that succeeds."""
    return lambda url: (data, None)


def _dead(reason="unrecoverable"):
    """A _fetch_bytes stand-in that fails with a given classification."""
    return lambda url: (None, reason)


def test_uploads_local_file(r2, tmp_db, tmp_path, monkeypatch):
    pdf = tmp_path / "have.pdf"
    pdf.write_bytes(b"%PDF-1.4 have")
    session = get_session()
    _seed(session, "https://x/have.pdf", local_path=str(pdf))
    session.close()

    counts = backfill_pdf_store.run(cfg=r2, refetch=False)
    assert counts["stored_local"] == 1

    session = get_session()
    try:
        doc = session.query(Document).one()
        assert doc.content_sha256 == pdf_store.sha256_bytes(b"%PDF-1.4 have")
    finally:
        session.close()


def test_skips_already_stored(r2, tmp_db, tmp_path):
    """Resume must cost nothing: a document already carrying a sha is not
    re-read, re-hashed, or re-uploaded."""
    session = get_session()
    _seed(session, "https://x/done.pdf", sha="a" * 64)
    session.close()

    counts = backfill_pdf_store.run(cfg=r2, refetch=False)
    assert counts["already"] == 1
    assert counts["stored_local"] == 0


def test_refetches_when_local_missing(r2, tmp_db, tmp_path, monkeypatch):
    monkeypatch.setattr(backfill_pdf_store, "_fetch_bytes", _ok())
    session = get_session()
    _seed(session, "https://x/gone.pdf", local_path=str(tmp_path / "nope.pdf"))
    session.close()

    counts = backfill_pdf_store.run(cfg=r2, refetch=True)
    assert counts["stored_refetch"] == 1


def test_dead_url_recorded_as_unrecoverable_and_continues(r2, tmp_db, tmp_path,
                                                          monkeypatch):
    """A 404 is a permanent fact about the corpus, not a crash.

    The run must continue, the count must be visible, and -- the part a
    counter alone cannot do -- the fact must land on the row. A null
    content_sha256 by itself cannot distinguish "not yet stored" from "gone
    forever", and that floor is what the corpus needs to be honest about.
    """
    monkeypatch.setattr(backfill_pdf_store, "_fetch_bytes", _dead())
    session = get_session()
    _seed(session, "https://x/dead.pdf", local_path=None)
    _seed(session, "https://x/dead2.pdf", local_path=None)
    session.close()

    counts = backfill_pdf_store.run(cfg=r2, refetch=True)
    assert counts["unrecoverable"] == 2

    session = get_session()
    try:
        docs = session.query(Document).all()
        assert [d.retention_status for d in docs] == \
            ["unrecoverable", "unrecoverable"]
        assert all(d.content_sha256 is None for d in docs)
    finally:
        session.close()


def test_transient_failure_is_not_recorded_as_unrecoverable(r2, tmp_db,
                                                            monkeypatch):
    """A network drop must not be written down as permanent loss.

    This is the expensive direction of the error: a 90-second outage
    mid-run would otherwise mark every document it touched as gone forever,
    and resume would then skip them without ever asking again.
    """
    monkeypatch.setattr(backfill_pdf_store, "_fetch_bytes", _dead("transient"))
    session = get_session()
    _seed(session, "https://x/blip.pdf", local_path=None)
    session.close()

    counts = backfill_pdf_store.run(cfg=r2, refetch=True)
    assert counts["transient"] == 1
    assert counts["unrecoverable"] == 0

    session = get_session()
    try:
        assert session.query(Document).one().retention_status == "transient"
    finally:
        session.close()


def test_resume_skips_unrecoverable_without_a_network_call(r2, tmp_db,
                                                           monkeypatch):
    """The point of persisting the marker: a second run costs nothing for
    documents already established as dead."""
    called = []
    monkeypatch.setattr(backfill_pdf_store, "_fetch_bytes",
                        lambda url: called.append(url) or (b"%PDF x", None))
    session = get_session()
    _seed(session, "https://x/dead.pdf", retention_status="unrecoverable")
    session.close()

    counts = backfill_pdf_store.run(cfg=r2, refetch=True)
    assert counts["skipped_unrecoverable"] == 1
    assert called == []


def test_transient_documents_are_re_attempted(r2, tmp_db, monkeypatch):
    """The other half of the same decision: "may resolve later" means the
    next run actually asks again."""
    monkeypatch.setattr(backfill_pdf_store, "_fetch_bytes", _ok())
    session = get_session()
    _seed(session, "https://x/blip.pdf", retention_status="transient")
    session.close()

    counts = backfill_pdf_store.run(cfg=r2, refetch=True)
    assert counts["stored_refetch"] == 1
    assert counts["skipped_unrecoverable"] == 0


def test_retry_unrecoverable_forces_another_attempt(r2, tmp_db, monkeypatch):
    """An escape hatch for the case the marker cannot know about: a plan
    restoring an archive that 404'd when the backfill first ran."""
    monkeypatch.setattr(backfill_pdf_store, "_fetch_bytes", _ok())
    session = get_session()
    _seed(session, "https://x/back.pdf", retention_status="unrecoverable")
    session.close()

    counts = backfill_pdf_store.run(cfg=r2, refetch=True,
                                    retry_unrecoverable=True)
    assert counts["stored_refetch"] == 1
    assert counts["skipped_unrecoverable"] == 0


def test_waf_blocked_plans_are_not_refetched(r2, tmp_db, monkeypatch):
    """No runner can reach these; attempting is guaranteed waste."""
    monkeypatch.setattr(backfill_pdf_store, "_waf_blocked_plan_ids",
                        lambda: {"blocked"})
    called = []
    monkeypatch.setattr(backfill_pdf_store, "_fetch_bytes",
                        lambda url: called.append(url) or (b"x", None))
    session = get_session()
    _seed(session, "https://x/w.pdf", plan_id="blocked")
    session.close()

    counts = backfill_pdf_store.run(cfg=r2, refetch=True)
    assert counts["skipped_waf"] == 1
    assert called == []


def test_no_refetch_reports_deferred_count(r2, tmp_db, tmp_path):
    """A --no-refetch pass must say how many it left waiting, so the
    operator's follow-up question ("how many are still pending?") is
    answered by the summary rather than requiring a second query."""
    session = get_session()
    _seed(session, "https://x/pending.pdf", local_path=None)
    session.close()

    counts = backfill_pdf_store.run(cfg=r2, refetch=False)
    assert counts["deferred_refetch"] == 1
    assert counts["stored_refetch"] == 0


def test_limit_applies_to_each_phase_independently(r2, tmp_db, tmp_path,
                                                   monkeypatch):
    """--limit is a taste of the *whole* job.

    Applied to the merged list, --limit 50 would be consumed entirely by the
    local-file phase (1,909 documents sort ahead of every re-fetch) and the
    operator would sample only the free, safe half -- learning nothing about
    the half that actually races link rot.
    """
    monkeypatch.setattr(backfill_pdf_store, "_fetch_bytes", _ok())
    session = get_session()
    for i in range(2):
        pdf = tmp_path / f"have{i}.pdf"
        pdf.write_bytes(f"%PDF-1.4 have {i}".encode())
        _seed(session, f"https://x/have{i}.pdf", local_path=str(pdf))
    for i in range(2):
        _seed(session, f"https://x/gone{i}.pdf")
    session.close()

    counts = backfill_pdf_store.run(cfg=r2, refetch=True, limit=1)
    assert counts["stored_local"] == 1
    assert counts["stored_refetch"] == 1


# ---------------------------------------------------------------------------
# Guard rails: don't let a misconfigured run download gigabytes and store none
# ---------------------------------------------------------------------------

def test_preflight_failure_stops_before_any_document(r2, tmp_db, tmp_path,
                                                     monkeypatch):
    """exists() re-raises a 403 by design, so a wrong secret key turns every
    upload into a logged failure while the loop keeps going -- ~7 GB
    downloaded, nothing stored, discovered hours later. One round-trip up
    front turns that into an immediate, non-zero exit."""
    def boom(cfg):
        raise RuntimeError("Access Denied")

    monkeypatch.setattr(pdf_store, "preflight", boom)
    called = []
    monkeypatch.setattr(backfill_pdf_store, "_fetch_bytes",
                        lambda url: called.append(url) or (b"x", None))
    session = get_session()
    _seed(session, "https://x/untouched.pdf")
    session.close()

    counts = backfill_pdf_store.run(cfg=r2, refetch=True)
    assert counts == {"preflight_failed": 1}
    assert called == []

    session = get_session()
    try:
        doc = session.query(Document).one()
        assert doc.content_sha256 is None
        assert doc.retention_status is None      # not even classified
    finally:
        session.close()


def test_preflight_failure_exits_non_zero(tmp_db, monkeypatch):
    monkeypatch.setattr(backfill_pdf_store, "run",
                        lambda **kw: {"preflight_failed": 1})
    monkeypatch.setattr("sys.argv", ["backfill_pdf_store"])
    with pytest.raises(SystemExit) as exc:
        backfill_pdf_store.main()
    assert exc.value.code == 1


def test_unconfigured_run_exits_non_zero(tmp_db, monkeypatch):
    """A run that stored nothing because R2 was unconfigured must not look
    successful to whatever wrapper invoked it."""
    monkeypatch.setattr(pdf_store, "config_from_env", lambda: None)
    monkeypatch.setattr("sys.argv", ["backfill_pdf_store"])
    with pytest.raises(SystemExit) as exc:
        backfill_pdf_store.main()
    assert exc.value.code == 1


def test_run_of_failures_stops_the_loop_early(r2, tmp_db, tmp_path,
                                              monkeypatch):
    """One failure is a bad object; twenty-five in a row is R2 being down,
    and continuing just burns bandwidth on documents nothing will store."""
    def explode(*a, **k):
        raise RuntimeError("R2 is down")

    monkeypatch.setattr(pdf_store, "put", explode)
    session = get_session()
    for i in range(30):
        pdf = tmp_path / f"f{i}.pdf"
        pdf.write_bytes(f"%PDF-1.4 {i}".encode())
        _seed(session, f"https://x/f{i}.pdf", local_path=str(pdf))
    session.close()

    counts = backfill_pdf_store.run(cfg=r2, refetch=False)
    assert counts["failed"] == backfill_pdf_store.CONSECUTIVE_FAILURE_LIMIT
    assert counts["stored_local"] == 0


# ---------------------------------------------------------------------------
# _fetch_bytes classification
#
# The highest-risk function in the script: its answer is written to the
# database, and after C1 a wrong "unrecoverable" is a permanent record that
# resume then honours by never asking again. Every other re-fetch test
# monkeypatches it out, so these are the only tests that see the real thing.
# ---------------------------------------------------------------------------

class _Resp:
    def __init__(self, status_code, content):
        self.status_code = status_code
        self.content = content


def _patch_get(monkeypatch, result):
    def fake_get(url, **kwargs):
        if isinstance(result, Exception):
            raise result
        return result
    monkeypatch.setattr(requests, "get", fake_get)


def test_fetch_bytes_returns_pdf_bytes_on_200(monkeypatch):
    _patch_get(monkeypatch, _Resp(200, b"%PDF-1.4 real"))
    assert backfill_pdf_store._fetch_bytes("https://x/a.pdf") == \
        (b"%PDF-1.4 real", None)


def test_fetch_bytes_treats_a_200_html_body_as_unrecoverable(monkeypatch):
    """A login wall or "this page has moved" stub. The bytes are not at this
    URL and a retry gets the identical page."""
    _patch_get(monkeypatch, _Resp(200, b"<html>Sign in</html>"))
    assert backfill_pdf_store._fetch_bytes("https://x/a.pdf") == \
        (None, "unrecoverable")


def test_fetch_bytes_treats_404_as_unrecoverable(monkeypatch):
    _patch_get(monkeypatch, _Resp(404, b"Not Found"))
    assert backfill_pdf_store._fetch_bytes("https://x/a.pdf") == \
        (None, "unrecoverable")


def test_fetch_bytes_treats_503_as_transient(monkeypatch):
    """A server error says nothing about whether the document exists."""
    _patch_get(monkeypatch, _Resp(503, b"Service Unavailable"))
    assert backfill_pdf_store._fetch_bytes("https://x/a.pdf") == \
        (None, "transient")


def test_fetch_bytes_treats_429_as_transient(monkeypatch):
    """429 and 408 are 4xx that literally mean "come back later" -- the one
    place the 4xx-is-permanent rule must not apply."""
    _patch_get(monkeypatch, _Resp(429, b"Too Many Requests"))
    assert backfill_pdf_store._fetch_bytes("https://x/a.pdf") == \
        (None, "transient")


def test_fetch_bytes_treats_408_as_transient(monkeypatch):
    _patch_get(monkeypatch, _Resp(408, b"Request Timeout"))
    assert backfill_pdf_store._fetch_bytes("https://x/a.pdf") == \
        (None, "transient")


def test_fetch_bytes_treats_a_raised_timeout_as_transient(monkeypatch):
    """The case that motivated the split: a network drop mid-run must not
    write "gone forever" across every document it touches."""
    _patch_get(monkeypatch, requests.Timeout("timed out"))
    assert backfill_pdf_store._fetch_bytes("https://x/a.pdf") == \
        (None, "transient")


def test_fetch_bytes_defaults_unclassified_responses_to_transient(monkeypatch):
    """Mislabelling a live document as dead costs the document; mislabelling
    a dead one as retryable costs one wasted request."""
    _patch_get(monkeypatch, _Resp(302, b""))
    assert backfill_pdf_store._fetch_bytes("https://x/a.pdf") == \
        (None, "transient")
