"""Documents that a page never renders as anchors.

NM PERA's meetings page hands the browser a JSON blob and builds its links
client-side, so `<a href>` scraping finds the nav and nothing else while the
page carries 700+ PDF URLs. `extract_embedded_doc_urls` is the opt-in escape
hatch; these tests pin the two things that make it safe -- it finds the URLs,
and it stays switched off unless a plan asks for it.
"""
from bs4 import BeautifulSoup

import fetcher

# Shaped like the real page: PDF URLs live inside a JSON payload, and the only
# anchors are navigation.
NMPERA_LIKE = """
<html><body>
  <nav><a href="/pera-board/overview">Overview</a>
       <a href="/members/life-events">Life Events</a></nav>
  <script>
    window.__DATA__ = {"rows":[
      {"npbmmaf_file3_title":"Minutes",
       "npbmmaf_file3_name":"7.30.2026_Board_signed_minutes.pdf",
       "npbmmaf_file3_link":"https://files.nmpera.org/7.30.2026_Board_signed_minutes.pdf"},
      {"npbmmaf_file2_link":"https://media.nmpera.org/July_2026_Board_Meeting_Audio.transcript.vtt"},
      {"npbmmaf_file1_link":"https://files.nmpera.org/6.9.2026_Investment_Committee_agenda.pdf"}
    ]};
  </script>
</body></html>
"""


def _soup(html):
    return BeautifulSoup(html, "lxml")


def test_finds_pdf_urls_that_are_not_anchors():
    soup = _soup(NMPERA_LIKE)
    assert fetcher.extract_doc_links(soup, "https://www.nmpera.org/x",
                                     investment_only=False) == []

    found = fetcher.extract_embedded_doc_urls(soup, investment_only=False)
    urls = {d["url"] for d in found}
    assert "https://files.nmpera.org/7.30.2026_Board_signed_minutes.pdf" in urls
    assert "https://files.nmpera.org/6.9.2026_Investment_Committee_agenda.pdf" in urls


def test_ignores_non_document_media():
    found = fetcher.extract_embedded_doc_urls(_soup(NMPERA_LIKE),
                                              investment_only=False)
    assert not any(d["url"].endswith(".vtt") for d in found)


def test_deduplicates_repeated_urls():
    html = NMPERA_LIKE.replace(
        '"rows":[',
        '"rows":[{"a":"https://files.nmpera.org/7.30.2026_Board_signed_minutes.pdf"},')
    found = fetcher.extract_embedded_doc_urls(_soup(html), investment_only=False)
    urls = [d["url"] for d in found]
    assert len(urls) == len(set(urls))


def test_investment_filter_still_applies():
    """A plan with investment_only=True must not get everything on the page."""
    found = fetcher.extract_embedded_doc_urls(_soup(NMPERA_LIKE),
                                              investment_only=True)
    assert all("investment" in d["url"].lower() for d in found)


def test_off_unless_the_plan_opts_in(monkeypatch):
    """The guard that keeps this away from the ~140 plans that scrape fine."""
    calls = []
    monkeypatch.setattr(fetcher, "fetch_page", lambda plan, url=None: _soup(NMPERA_LIKE))
    monkeypatch.setattr(fetcher, "extract_embedded_doc_urls",
                        lambda *a, **kw: calls.append(1) or [])

    base = {"id": "x", "abbreviation": "X", "investment_only": False,
            "materials_url": "https://www.nmpera.org/x"}

    fetcher.discover_document_links(dict(base))
    assert calls == [], "ran without scan_embedded_urls set"

    fetcher.discover_document_links(dict(base, scan_embedded_urls=True))
    assert calls == [1]
