"""Tests for insights.notice — publication-notice email rendered after a
weekly briefing transitions to 'published'."""
from __future__ import annotations

from datetime import date, datetime

import pytest

from database import Publication, get_session
from insights.notice import (
    _extract_tldr,
    _make_preview,
    _strip_markdown,
    _trim_leading_metadata,
    render_publication_notice,
    send_publication_notice,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SAMPLE_MD = """\
# 7-Day Highlights — April 26 to May 2, 2026

## TL;DR

CalPERS' Investment Committee approved a **$2 billion private credit
allocation** to focus on direct lending. CalSTRS reaffirmed its long-term
asset allocation but trimmed REIT exposure by 50 bps. NYSCRF disclosed
underweight positioning in international developed equities.

## Highlights

- **CalPERS** approved private credit mandate at the April 28 board meeting.
- **CalSTRS** released the [FY24 ACFR](https://example.com) showing 8.2%
  one-year return.
- **NYSCRF** noted continued caution on duration in fixed income.

The week's other notable items included PSERS publishing its annual
investment plan and ORSC releasing FY25 budget documents.
"""


def _seed_published_pub(
    session,
    cadence="weekly",
    period_start=date(2026, 4, 26),
    period_end=date(2026, 5, 2),
    markdown=SAMPLE_MD,
    pub_id=None,
) -> Publication:
    pub = Publication(
        cadence=cadence,
        period_start=period_start,
        period_end=period_end,
        status="published",
        draft_markdown=markdown,
        composed_at=datetime(2026, 5, 5, 12, 0, 0),
        approved_at=datetime(2026, 5, 5, 16, 12, 0),
        published_at=datetime(2026, 5, 5, 16, 12, 5),
    )
    if pub_id is not None:
        pub.id = pub_id
    session.add(pub)
    session.commit()
    return pub


# ---------------------------------------------------------------------------
# _strip_markdown
# ---------------------------------------------------------------------------

def test_strip_markdown_removes_headings():
    out = _strip_markdown("# Heading\n\n## Sub\n\nbody.")
    assert "Heading" in out
    assert "#" not in out


def test_strip_markdown_removes_bullets_and_links():
    md = "- **CalPERS** approved [the mandate](https://example.com) today."
    out = _strip_markdown(md)
    assert "CalPERS approved the mandate today." == out
    assert "[" not in out
    assert "**" not in out


# ---------------------------------------------------------------------------
# _make_preview
# ---------------------------------------------------------------------------

def test_preview_handles_empty_markdown():
    assert _make_preview("") == ""


def test_preview_truncates_with_ellipsis_at_sentence_boundary():
    md = " ".join("Sentence number {}.".format(i) for i in range(50))
    out = _make_preview(md)
    assert out.endswith("…")
    assert len(out) <= 750  # below the 700-char limit + "…"


def test_preview_keeps_short_markdown_intact():
    md = "A short briefing of less than 700 chars."
    assert _make_preview(md) == md


def test_preview_prefers_tldr_block_when_present():
    out = _make_preview(SAMPLE_MD)
    # SAMPLE_MD's TL;DR opens with "CalPERS' Investment Committee approved..."
    assert out.startswith("CalPERS")
    # Items below TL;DR (e.g. the FY24 ACFR bullet) should NOT appear
    assert "FY24 ACFR" not in out
    assert "PSERS publishing" not in out


def test_preview_skips_title_and_generated_line_when_no_tldr():
    md = (
        "# 7-Day Highlights: April 24 – May 1, 2026\n"
        "*Generated: May 01, 2026*\n\n"
        "---\n\n"
        "## Private Equity & Alternatives Commitments\n\n"
        "The week's most active deal flow came from KPPA. Lots happened.\n"
    )
    out = _make_preview(md)
    assert out.startswith("Private Equity")
    assert "Generated:" not in out
    assert "7-Day Highlights" not in out


def test_extract_tldr_is_case_insensitive_and_supports_synonyms():
    md_summary = "## Summary\n\nThe key point is X.\n\n## Other\n\nignored."
    assert _extract_tldr(md_summary) == "The key point is X."
    md_overview = "## overview\n\nQuick take.\n\n## Detail\n\nignored."
    assert _extract_tldr(md_overview) == "Quick take."
    md_none = "## Highlights\n\nNo TL;DR header here."
    assert _extract_tldr(md_none) is None


def test_trim_leading_metadata_drops_title_generated_and_rule():
    md = (
        "\n# Title line\n*Generated: 2026-05-05*\n---\n\n"
        "Real content starts here."
    )
    assert _trim_leading_metadata(md).strip() == "Real content starts here."


# ---------------------------------------------------------------------------
# render_publication_notice
# ---------------------------------------------------------------------------

def test_render_notice_includes_cadence_period_and_link(session, monkeypatch):
    monkeypatch.setenv("APPROVAL_BASE_URL", "https://app.test.local")
    # Re-import config so the env var change takes effect for the new send.
    import importlib
    from insights import config as _config
    importlib.reload(_config)
    from insights import notice as _notice
    importlib.reload(_notice)

    pub = _seed_published_pub(session)
    email = _notice.render_publication_notice(pub)

    assert email.subject.startswith("[PensionGraph]")
    assert "Weekly CIO Insights" in email.subject
    assert "Apr 26" in email.subject and "May 2" in email.subject
    # HTML body
    assert "https://app.test.local" in email.html
    assert "Read the full briefing" in email.html
    assert "CalPERS" in email.html
    # Text body
    assert "https://app.test.local" in email.text
    assert email.pdf_attachment is None


def test_render_notice_supports_monthly_cadence(session):
    pub = _seed_published_pub(
        session, cadence="monthly",
        period_start=date(2026, 4, 1), period_end=date(2026, 4, 30),
    )
    email = render_publication_notice(pub)
    assert "Monthly CIO Insights" in email.subject


# ---------------------------------------------------------------------------
# send_publication_notice
# ---------------------------------------------------------------------------

def test_send_publication_notice_writes_mock_email(session, tmp_path, monkeypatch):
    # Redirect the mock-output dir to tmp_path so we can assert on it
    from insights import config as _config
    monkeypatch.setattr(_config, "SENT_EMAILS_DIR", tmp_path / "sent")

    pub = _seed_published_pub(session)
    delivery_id = send_publication_notice(pub, to="explicit@test.local")

    assert delivery_id  # non-empty (path string in mock mode)
    eml_files = list((tmp_path / "sent").glob("*.eml"))
    assert len(eml_files) == 1
    body = eml_files[0].read_text()
    assert "explicit@test.local" in body
    assert "PensionGraph" in body
    assert "Weekly CIO Insights" in body


def test_send_publication_notice_rejects_non_published_status(session):
    pub = _seed_published_pub(session)
    pub.status = "approved"   # not yet published
    session.commit()
    with pytest.raises(ValueError, match="status='published'"):
        send_publication_notice(pub)
