"""Rate-limited yt-dlp errors must never classify a recording as 'gone'.

YouTube's rate-limit message contains the words "Video unavailable", which
is also how genuinely deleted videos read — the 2026-07-07 hydration run
mis-marked 1,775 live recordings as gone because of it. Classification
must treat rate-limit / try-again-later as transient, and a row that has
a downloaded local file must never be downgraded.
"""

from scripts.hydrate_recording_metadata import _classify_error


def test_rate_limited_unavailable_is_transient():
    msg = ("ERROR: [youtube] XNZrQeo4RDI: Video unavailable. This content "
           "isn't available, try again later. The current session has been "
           "rate-limited by YouTube for up to an hour.")
    assert _classify_error(msg) == "transient"


def test_plain_unavailable_is_gone():
    assert _classify_error("ERROR: [youtube] 4ewaCzzm3r0: Video unavailable") == "gone"


def test_private_and_removed_are_gone():
    assert _classify_error("Private video. Sign in if you've been granted access") == "gone"
    assert _classify_error("This video has been removed by the uploader") == "gone"


def test_network_blip_is_transient():
    assert _classify_error("HTTP Error 500: Internal Server Error") == "transient"
