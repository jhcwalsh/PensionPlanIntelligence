"""Which documents need their bytes fetched again.

Two selection bugs lived here, in opposite directions, and both came from the
same assumption: that "the file is absent" is the same question as "this
document needs re-downloading".
"""
import pytest

import database
from database import Document, Plan
from scripts import refetch_missing_files as rmf


@pytest.fixture
def session(tmp_db):
    s = database.get_session()
    s.add(Plan(id="mcera", name="MCERA", state="CA"))
    s.commit()
    yield s
    s.close()


def _doc(session, tmp_path, name, body=None, text=None):
    path = tmp_path / name
    if body is not None:
        path.write_bytes(body)
    d = Document(plan_id="mcera", url=f"https://x/{name}", filename=name,
                 local_path=str(path), extracted_text=text)
    session.add(d)
    session.commit()
    return d


def test_a_missing_file_is_selected(session, tmp_path):
    d = _doc(session, tmp_path, "gone.pdf", body=None)
    assert [x.id for x in rmf.missing_file_documents(session, None)] == [d.id]


def test_html_saved_as_pdf_is_selected(session, tmp_path):
    """The file is present, so absence never caught it; the extractor could
    not read it and the refetcher could not see it. Permanently stuck."""
    d = _doc(session, tmp_path, "block.pdf",
             body=b"<!DOCTYPE html><html>Access denied</html>")
    assert [x.id for x in rmf.missing_file_documents(session, None)] == [d.id]


def test_a_real_pdf_is_left_alone(session, tmp_path):
    _doc(session, tmp_path, "fine.pdf", body=b"%PDF-1.4\n")
    assert rmf.missing_file_documents(session, None) == []


def test_documents_that_already_hold_text_are_not_proposed(session, tmp_path):
    """The pipeline is cloud-only: runners fetch, extract and discard, so
    2,557 of 5,084 documents have no local file and nearly all are fine.
    Without this filter the script proposes re-downloading all of them."""
    _doc(session, tmp_path, "extracted.pdf", body=None,
         text="plenty of text already")
    assert rmf.missing_file_documents(session, None) == []
    assert len(rmf.missing_file_documents(session, None,
                                          only_unextracted=False)) == 1
