"""Following a landing page to the document it points at.

Rhode Island publishes board packs through a CKAN data portal, and the
scraper stores the dataset page rather than the file. The download then
returns HTML, gets rejected as an error page, and nine documents sit
permanently unfetchable while the PDF link is right there in the markup.

Strictness is the point: a page offering several PDFs is a listing, and
attaching the wrong file to a document row is worse than leaving it empty,
because an empty row is visible and a wrong one is not.
"""
import fetcher

BASE = "https://data.treasury.ri.gov/en/dataset/2026-6-10-ersri/resource/"
PDF = ("https://data.treasury.ri.gov/dataset/263661de/resource/d03aea9d/"
       "download/retirement-board-meeting-public-book-6-10-20.pdf")


def _page(*hrefs):
    body = "".join(f'<a href="{h}">link</a>' for h in hrefs)
    return f"<!DOCTYPE html><html><body>{body}</body></html>".encode()


def test_one_pdf_link_is_followed():
    assert fetcher._sole_pdf_link(_page(PDF), BASE) == PDF


def test_the_same_link_twice_is_still_one_link():
    """CKAN renders the download link in both the header and the table."""
    assert fetcher._sole_pdf_link(_page(PDF, PDF), BASE) == PDF


def test_a_relative_link_resolves_against_the_page():
    got = fetcher._sole_pdf_link(_page("../download/book.pdf"), BASE)
    assert got == ("https://data.treasury.ri.gov/en/dataset/2026-6-10-ersri/"
                   "download/book.pdf")


def test_two_different_pdfs_are_not_guessed_between():
    assert fetcher._sole_pdf_link(_page(PDF, PDF + "?x=2"), BASE) is None
    assert fetcher._sole_pdf_link(
        _page("https://x/a.pdf", "https://x/b.pdf"), BASE) is None


def test_a_page_with_no_pdf_link_is_left_alone():
    """The sdcers case: a soft-404 serving the site homepage. There is
    nothing to follow, and it must stay a rejection."""
    assert fetcher._sole_pdf_link(_page("/about", "/contact"), BASE) is None
    assert fetcher._sole_pdf_link(b"<html>Access denied</html>", BASE) is None


def test_undecodable_bytes_do_not_raise():
    assert fetcher._sole_pdf_link(b"\xff\xfe\x00\x01", BASE) is None
