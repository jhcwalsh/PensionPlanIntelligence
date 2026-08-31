"""Extraction reads through the store, so it stops caring where the PDF is.

Before this, extraction only ever worked on the machine that did the
fetching. CLAUDE.md records that coupling as the cause of two separate
defects, and 128 documents sit at `file_missing` with nothing to re-extract
from. The retention design says extractors call `open_local_or_remote` so
they "stop caring where the PDF lives" — it was written, tested, and wired to
nothing.
"""
import pathlib

import pytest

import extractor
import pdf_store
from database import Document


class _Doc:
    """Minimal stand-in: these paths never touch a session."""

    def __init__(self, local_path=None, sha=None, filename="pack.pdf"):
        self.id = 1
        self.local_path = str(local_path) if local_path else None
        self.content_sha256 = sha
        self.filename = filename
        self.doc_type = "board_pack"


def test_a_present_local_file_is_used_and_never_deleted(tmp_path):
    """The local file is the corpus's only copy on this machine. Cleaning it
    up because it came back from the same function as a temp file would be
    the most expensive bug in this module."""
    p = tmp_path / "real.pdf"
    p.write_bytes(b"%PDF-1.4\n")
    doc = _Doc(local_path=p)

    with pdf_store.document_pdf(doc) as got:
        assert got == p
    assert p.exists(), "deleted the local file"


def test_a_retained_copy_is_pulled_and_then_cleaned_up(r2, tmp_path):
    sha = pdf_store.put(r2, b"%PDF-1.4\nretained\n")
    doc = _Doc(local_path=tmp_path / "gone.pdf", sha=sha)

    with pdf_store.document_pdf(doc, cfg=r2) as got:
        assert got.read_bytes() == b"%PDF-1.4\nretained\n"
        pulled = got
    assert not pulled.exists(), "leaked a temp file; 90 MB each over a corpus"


def test_the_temp_file_goes_even_when_the_body_raises(r2, tmp_path):
    sha = pdf_store.put(r2, b"%PDF-1.4\n")
    doc = _Doc(local_path=None, sha=sha)

    with pytest.raises(ValueError):
        with pdf_store.document_pdf(doc, cfg=r2) as got:
            pulled = got
            raise ValueError("extraction blew up")
    assert not pulled.exists()


def test_nowhere_at_all_raises_file_not_found(r2, tmp_path):
    doc = _Doc(local_path=tmp_path / "gone.pdf", sha=None)
    with pytest.raises(FileNotFoundError):
        with pdf_store.document_pdf(doc, cfg=r2):
            pass


def test_extract_document_reports_file_missing_only_when_truly_nowhere(
        tmp_path, monkeypatch):
    """`file_missing` now means not on disk *and* not retained."""
    monkeypatch.setattr(pdf_store, "config_from_env", lambda: None)
    doc = _Doc(local_path=tmp_path / "gone.pdf", sha=None)
    assert extractor.extract_document(doc).reason == "file_missing"


def test_extraction_runs_against_a_retained_copy(r2, tmp_path, monkeypatch):
    """The whole point: a document whose local file is gone still extracts."""
    sha = pdf_store.put(r2, b"%PDF-1.4\n")
    doc = _Doc(local_path=tmp_path / "gone.pdf", sha=sha)

    monkeypatch.setattr(pdf_store, "config_from_env", lambda: r2)
    monkeypatch.setattr(extractor, "extract_pdf_pdfplumber",
                        lambda p: ("recovered from the store " * 20, 4))

    out = extractor.extract_document(doc)
    assert out.status == "done"
    assert "recovered from the store" in out.text
    assert out.pages == 4


def test_the_file_type_comes_from_the_document_not_the_temp_file(
        r2, tmp_path, monkeypatch):
    """A copy pulled from the store is always written to a `.pdf` temp file,
    whatever it holds. Reading the suffix off that path hands every retained
    .docx to the PDF extractor."""
    sha = pdf_store.put(r2, b"PK\x03\x04 not really a pdf")
    doc = _Doc(local_path=None, sha=sha, filename="minutes.docx")

    monkeypatch.setattr(pdf_store, "config_from_env", lambda: r2)
    seen = {}

    def fake_docx(path):
        seen["called"] = True
        return "docx text " * 20, 1

    monkeypatch.setattr(extractor, "extract_docx", fake_docx)
    monkeypatch.setattr(extractor, "extract_pdf_pdfplumber",
                        lambda p: (_ for _ in ()).throw(
                            AssertionError("sent a .docx to the PDF path")))

    out = extractor.extract_document(doc)
    assert seen.get("called"), "did not route by the document's own extension"
    assert out.status == "done"
