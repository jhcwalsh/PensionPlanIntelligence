"""Publication notice email — sent AFTER a publication transitions to
'published' (i.e. after the user clicks Approve and publish() succeeds).

Distinct from the approval email (which goes out at compose time and asks
for sign-off). The notice is a confirmation: "the weekly briefing you
just approved is now live, here's a preview and a link to read it."

For now this is single-recipient (APPROVAL_EMAIL_RECIPIENT) — same model
as the daily digest. When a real subscriber list is added later, the
recipient list goes here without changing call sites; render_*
already returns subject/html/text agnostic of who it goes to.
"""
from __future__ import annotations

import html as _html
import logging
import re
from datetime import date

from database import Publication
from insights import config
from insights.approval import ApprovalEmail, send_email

logger = logging.getLogger(__name__)


PREVIEW_CHAR_LIMIT = 700
CADENCE_LABELS = {
    "weekly": "Weekly CIO Insights",
    "monthly": "Monthly CIO Insights",
    "annual": "Annual CIO Insights",
}


def _strip_markdown(md: str) -> str:
    """Best-effort plaintext extraction from Markdown for the preview block.

    Drops headings/bullets/links/bold but keeps the prose readable.
    """
    text = md
    # Strip ATX headings (#, ##, etc.)
    text = re.sub(r"^#{1,6}\s+", "", text, flags=re.MULTILINE)
    # Strip bullet markers
    text = re.sub(r"^\s*[-*+]\s+", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s*\d+\.\s+", "", text, flags=re.MULTILINE)
    # Convert [text](url) to text
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
    # Strip emphasis markers
    text = re.sub(r"\*\*([^*]+)\*\*", r"\1", text)
    text = re.sub(r"\*([^*]+)\*", r"\1", text)
    text = re.sub(r"`([^`]+)`", r"\1", text)
    # Collapse blank-line runs
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _make_preview(draft_markdown: str) -> str:
    """First ~700 chars of stripped Markdown, ending on a sentence boundary."""
    if not draft_markdown:
        return ""
    text = _strip_markdown(draft_markdown)
    if len(text) <= PREVIEW_CHAR_LIMIT:
        return text
    truncated = text[:PREVIEW_CHAR_LIMIT]
    # Try to end at the last full sentence within the window
    last_break = max(
        truncated.rfind(". "),
        truncated.rfind(".\n"),
        truncated.rfind("? "),
        truncated.rfind("! "),
    )
    if last_break > PREVIEW_CHAR_LIMIT * 0.6:
        truncated = truncated[: last_break + 1]
    return truncated.rstrip() + " …"


def _format_period(start: date, end: date) -> str:
    """E.g. 'Apr 26 – May 2, 2026' for a weekly span; '2026-04' for monthly."""
    if start.year == end.year and start.month == end.month:
        return f"{start.strftime('%b %-d')}–{end.strftime('%-d, %Y')}"
    if start.year == end.year:
        return f"{start.strftime('%b %-d')} – {end.strftime('%b %-d, %Y')}"
    return f"{start.strftime('%b %-d, %Y')} – {end.strftime('%b %-d, %Y')}"


def render_publication_notice(publication: Publication) -> ApprovalEmail:
    """Build the notice email for a published Publication.

    Returns an ApprovalEmail dataclass so it can be sent through the
    existing send_email() infrastructure.
    """
    cadence_label = CADENCE_LABELS.get(publication.cadence, publication.cadence)
    period_label = _format_period(publication.period_start, publication.period_end)
    preview = _make_preview(publication.draft_markdown or "")
    app_url = config.APPROVAL_BASE_URL.rstrip("/")

    subject = f"[PensionGraph] {cadence_label} — {period_label}"

    # ---- HTML body
    preview_html = _html.escape(preview).replace("\n\n", "</p><p>").replace("\n", "<br>")
    html = (
        '<html><body style="font-family:-apple-system,sans-serif;'
        'max-width:680px;margin:1.5em auto;line-height:1.55;color:#222;">'
        f'<h2 style="margin-bottom:0.1em;color:#003366;">{cadence_label}</h2>'
        f'<p style="color:#555;margin-top:0;">{period_label}</p>'
        '<hr style="border:0;border-top:1px solid #e0e0e0;margin:1em 0;">'
        f'<div style="font-size:0.95em;"><p>{preview_html}</p></div>'
        '<p style="margin-top:1.5em;">'
        f'<a href="{app_url}" '
        'style="display:inline-block;padding:10px 18px;background:#003366;'
        'color:#fff;text-decoration:none;border-radius:4px;'
        'font-weight:500;">Read the full briefing →</a></p>'
        '<hr style="border:0;border-top:1px solid #eee;margin:2em 0 1em 0;">'
        '<p style="color:#888;font-size:0.85em;">'
        f'Briefing #{publication.id} · approved '
        f'{publication.approved_at.strftime("%Y-%m-%d %H:%M UTC") if publication.approved_at else ""}'
        '</p>'
        '</body></html>'
    )

    # ---- Plain-text body
    text = (
        f"{cadence_label} — {period_label}\n"
        + "=" * len(f"{cadence_label} — {period_label}") + "\n\n"
        + preview + "\n\n"
        + f"Read the full briefing: {app_url}\n\n"
        + "—\n"
        + f"Briefing #{publication.id}\n"
    )

    return ApprovalEmail(
        subject=subject, html=html, text=text,
        pdf_attachment=None, pdf_filename=None,
    )


def send_publication_notice(publication: Publication, to: str | None = None) -> str:
    """Send the post-approval notice. Returns the Resend delivery id (or
    mock-mode file path).

    Recipient defaults to ``APPROVAL_EMAIL_RECIPIENT``. Pass ``to`` to
    override (used by tests). Honors INSIGHTS_MODE=mock through the
    underlying send_email() helper.
    """
    if publication.status != "published":
        raise ValueError(
            f"send_publication_notice() requires status='published'; "
            f"got '{publication.status}' on publication {publication.id}"
        )
    email = render_publication_notice(publication)
    delivery_id = send_email(email, to=to)
    logger.info(
        "Publication notice sent for publication %s (delivery_id=%s)",
        publication.id, delivery_id,
    )
    return delivery_id
