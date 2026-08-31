"""Clearing the OCR backlog, and the guarantee it cannot spend by accident.

Mirrors tests/test_catalogue.py: the no-spend test asserts that OCR was never
enabled, not that the cost came out zero.
"""
import pytest

import database
from database import Document, ExtractionDetail, Plan
from scripts import clear_ocr_backlog as cob


@pytest.fixture
def session(tmp_db):
    s = database.get_session()
    s.add(Plan(id="mcera", name="MCERA", state="CA"))
    s.commit()
    yield s
    s.close()


def _owed(session, tmp_path, reason="ocr_deferred", pages=3, with_file=True):
    path = tmp_path / f"{reason}-{session.query(Document).count()}.pdf"
    if with_file:
        path.write_bytes(b"%PDF-1.4\n")
    d = Document(plan_id="mcera", url=f"https://x/{path.name}",
                 filename=path.name, extraction_status="failed",
                 local_path=str(path) if with_file else None)
    session.add(d)
    session.commit()
    session.add(ExtractionDetail(document_id=d.id, reason=reason,
                                 pages_total=pages))
    session.commit()
    return d


def test_both_owed_reasons_are_picked_up(session, tmp_path):
    """ocr_deferred is a funding decision; ocr_unavailable is an API outage.
    Neither is a fact about the document, so both are still owed."""
    a = _owed(session, tmp_path, "ocr_deferred")
    b = _owed(session, tmp_path, "ocr_unavailable")
    got, _missing = cob.backlog(session)
    assert {d.id for d, _ in got} == {a.id, b.id}


def test_ocr_empty_is_not_retried(session, tmp_path):
    """That one *is* a claim about the document: OCR ran and found nothing.
    Retrying it pays to learn the same thing again."""
    _owed(session, tmp_path, "ocr_empty")
    got, _missing = cob.backlog(session)
    assert got == []


def test_documents_without_a_file_are_skipped_not_attempted(session, tmp_path):
    """OCR needs the bytes. 105 documents have a row whose PDF the plan
    deleted; attempting them burns a call to discover that."""
    _owed(session, tmp_path, "ocr_deferred", with_file=False)
    got, missing = cob.backlog(session)
    assert got == [] and missing == 1


def test_without_approve_ocr_is_never_enabled(session, tmp_path, monkeypatch,
                                              capsys):
    """Not 'spend was zero' — 'spending was unreachable'."""
    import extractor
    _owed(session, tmp_path, "ocr_deferred")
    monkeypatch.setattr(extractor, "OCR_ENABLED", False)

    def boom(*a, **kw):
        raise AssertionError("run_extractor reached on a no-spend path")

    monkeypatch.setattr(extractor, "run_extractor", boom)
    monkeypatch.setattr("sys.argv", ["clear_ocr_backlog"])

    assert cob.main() == 0
    assert extractor.OCR_ENABLED is False
    assert "Nothing spent" in capsys.readouterr().out


def test_an_estimate_over_the_ceiling_stops_before_spending(session, tmp_path,
                                                            monkeypatch, capsys):
    """The ceiling is checked against the whole estimate up front, because OCR
    bills per page and a 2,596-page packet is one document."""
    import extractor
    _owed(session, tmp_path, "ocr_deferred", pages=10_000)

    def boom(*a, **kw):
        raise AssertionError("spent despite exceeding the ceiling")

    monkeypatch.setattr(extractor, "run_extractor", boom)
    monkeypatch.setattr("sys.argv",
                        ["clear_ocr_backlog", "--approve", "--budget", "1.00"])

    assert cob.main() == 1
    assert "exceeds the ceiling" in capsys.readouterr().out


def test_approve_passes_exactly_the_priced_documents(session, tmp_path,
                                                     monkeypatch):
    """pipeline.py --retry-failed would sweep every failed document, ignoring
    plan ids. This must hand run_extractor the set it just priced."""
    import extractor
    a = _owed(session, tmp_path, "ocr_deferred")
    b = _owed(session, tmp_path, "ocr_unavailable")
    _owed(session, tmp_path, "ocr_empty")            # not owed
    _owed(session, tmp_path, "ocr_deferred", with_file=False)  # no bytes

    seen = {}
    monkeypatch.setattr(extractor, "run_extractor",
                        lambda doc_ids=None, **kw: seen.update(ids=doc_ids))
    monkeypatch.setattr("sys.argv", ["clear_ocr_backlog", "--approve"])
    cob.main()

    assert set(seen["ids"]) == {a.id, b.id}
