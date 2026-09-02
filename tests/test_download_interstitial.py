"""The "Downloading, please wait..." page, and the TLS exception it needs.

SDCERS looked blocked for weeks. It was not: every DownloadFile link returned
1,435 bytes that nobody decoded, holding a spinner and one line of jQuery that
rewrites the address to DownloadFileBytes. These tests pin the two halves of
the fix -- read the page's own instruction, and do not widen TLS tolerance
beyond the host that needs it.
"""
import fetcher

SPINNER = b"""\r\n\r\n<!DOCTYPE html>
<html><head><title>Downloading, Please wait...</title></head>
<body>
  <div id="divDownloadIndicator"><span>Downloading, Please wait...</span></div>
  <script type="text/javascript">
    $(document).ready(function (e) {
        if (window.location.toString().indexOf("DownloadFileBytes") < 0) {
            window.location = window.location.toString().replace("DownloadFile", "DownloadFileBytes");
        }
    });
  </script>
</body></html>
"""

URL = ("https://board.sdcers.gov/OnBaseAgendaOnline/Documents/DownloadFile/"
       "AUDIT_COMMITTEE_MEETING_2304_Agenda.pdf?documentType=1&meetingId=2304")


def test_the_interstitial_names_its_own_target():
    assert fetcher._js_redirect_target(SPINNER, URL) == URL.replace(
        "DownloadFile", "DownloadFileBytes")


def test_the_rewrite_is_read_from_the_page_not_hardcoded():
    """A different vendor's interstitial must work without a code change."""
    html = SPINNER.replace(b"DownloadFile", b"GetDoc").replace(
        b"GetDocBytes", b"GetDocRaw")
    url = "https://example.gov/portal/GetDoc/agenda.pdf?id=7"
    assert fetcher._js_redirect_target(html, url) == (
        "https://example.gov/portal/GetDocRaw/agenda.pdf?id=7")


def test_a_page_without_the_instruction_is_refused():
    """No guessing. A landing page is _sole_pdf_link's problem, not this one."""
    assert fetcher._js_redirect_target(b"<html><body>nope</body></html>",
                                       URL) is None


def test_a_rewrite_that_does_not_apply_to_this_url_is_refused():
    """The page's replace() target must actually appear in the address."""
    assert fetcher._js_redirect_target(
        SPINNER, "https://board.sdcers.gov/somewhere/else.pdf") is None


def test_a_rewrite_that_changes_nothing_is_refused():
    """Guards the recursion: returning the same URL would loop."""
    already = URL.replace("DownloadFile", "DownloadFileBytes")
    assert fetcher._js_redirect_target(SPINNER, already) is None


def test_tls_tolerance_is_scoped_to_the_one_broken_host():
    assert fetcher._verify_tls(URL) is False
    assert fetcher._verify_tls("https://www.calpers.ca.gov/x.pdf") is True
    assert fetcher._verify_tls("https://sdcers.gov/other.pdf") is True


def test_tls_tolerance_does_not_leak_to_lookalike_hosts():
    """A suffix match would hand every attacker-controlled subdomain a pass."""
    for host in ("board.sdcers.gov.evil.test", "notboard.sdcers.gov",
                 "board.sdcers.government"):
        assert fetcher._verify_tls(f"https://{host}/x.pdf") is True, host
