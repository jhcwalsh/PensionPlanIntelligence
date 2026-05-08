"""LinkedIn auto-post via Zapier/Make catch-hook.

Posting to LinkedIn requires Community-Management API approval, which is
gated. To skip that, we POST a JSON payload to a Zapier "Catch Hook" URL
configured in ``LINKEDIN_POST_WEBHOOK_URL``; the Zap fans the body out to
LinkedIn (Company Page update).

The button is wired through the same magic-link approval system as
Approve / Reject — ``ApprovalToken.action == "post_linkedin"``. Clicking
the link calls :func:`post_to_linkedin`, which builds the post body from
``publication.draft_markdown`` and POSTs it to the webhook.

In ``INSIGHTS_MODE=mock`` the webhook call is replaced by a JSON file in
``tmp/sent_social/`` so tests can assert what would have been sent.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from pathlib import Path

import requests

from database import Publication
from insights import config

logger = logging.getLogger(__name__)


# LinkedIn caps a single share at 3000 characters. Leave room for the
# trailing read-more link plus a blank line.
LINKEDIN_MAX_CHARS = 3000
_READ_MORE_RESERVED = 120


def build_linkedin_post(publication: Publication) -> str:
    """Compose the LinkedIn post body from a publication's draft markdown.

    Strategy:
      - Take the first non-heading paragraph as the lede.
      - Strip markdown emphasis / link syntax (LinkedIn doesn't render it).
      - Append a "Read the full briefing →" link back to the Streamlit app.
      - Cap at LINKEDIN_MAX_CHARS (LinkedIn's hard limit).
    """
    md = (publication.draft_markdown or "").strip()
    if not md:
        raise ValueError("publication has no draft_markdown to post")

    cadence = publication.cadence.title()
    period = publication.period_start.strftime("%b %-d, %Y")
    headline = f"{cadence} pension briefing — {period}"

    body = _strip_markdown(_first_paragraphs(md, max_paragraphs=3))

    read_more = f"Read the full briefing: {config.APPROVAL_BASE_URL}"

    budget = LINKEDIN_MAX_CHARS - len(headline) - len(read_more) - len("\n\n") * 2
    if len(body) > budget:
        body = body[: budget - 1].rstrip() + "…"

    return f"{headline}\n\n{body}\n\n{read_more}"


def post_to_linkedin(publication: Publication) -> str:
    """POST the LinkedIn post body to the Zapier webhook.

    Returns the webhook response id (live mode) or the mock file path.
    """
    text = build_linkedin_post(publication)
    payload = {
        "text": text,
        "publication_id": publication.id,
        "cadence": publication.cadence,
        "period_start": publication.period_start.isoformat(),
        "period_end": publication.period_end.isoformat(),
    }

    if config.is_mock():
        return _write_mock_post(payload)

    if not config.LINKEDIN_POST_WEBHOOK_URL:
        raise RuntimeError(
            "LINKEDIN_POST_WEBHOOK_URL not set — can't post to LinkedIn in live mode. "
            "Set INSIGHTS_MODE=mock for local dev."
        )

    resp = requests.post(
        config.LINKEDIN_POST_WEBHOOK_URL,
        json=payload,
        timeout=30,
    )
    if resp.status_code >= 400:
        raise RuntimeError(
            f"LinkedIn webhook returned {resp.status_code}: {resp.text[:300]}"
        )
    try:
        return resp.json().get("id", "") or resp.json().get("request_id", "")
    except ValueError:
        return resp.text[:200]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _first_paragraphs(md: str, max_paragraphs: int = 3) -> str:
    """Return the first non-heading paragraphs from a markdown body."""
    paragraphs: list[str] = []
    for chunk in re.split(r"\n\s*\n", md):
        chunk = chunk.strip()
        if not chunk:
            continue
        if chunk.startswith("#"):
            continue
        paragraphs.append(chunk)
        if len(paragraphs) >= max_paragraphs:
            break
    return "\n\n".join(paragraphs)


def _strip_markdown(text: str) -> str:
    """Best-effort markdown → plaintext for LinkedIn."""
    # Inline links: [label](url) → label (url)
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1 (\2)", text)
    # Bold / italic
    text = re.sub(r"\*\*([^*]+)\*\*", r"\1", text)
    text = re.sub(r"\*([^*]+)\*", r"\1", text)
    text = re.sub(r"__([^_]+)__", r"\1", text)
    text = re.sub(r"_([^_]+)_", r"\1", text)
    # Inline code
    text = re.sub(r"`([^`]+)`", r"\1", text)
    # List bullets at start of line
    text = re.sub(r"^[-*+]\s+", "• ", text, flags=re.MULTILINE)
    return text


def _write_mock_post(payload: dict) -> str:
    config.SENT_SOCIAL_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S%f")
    path = config.SENT_SOCIAL_DIR / f"{ts}_linkedin.json"
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return str(path)


def list_mock_posts() -> list[Path]:
    """Test helper: return the sorted list of mock social-post files."""
    if not config.SENT_SOCIAL_DIR.exists():
        return []
    return sorted(config.SENT_SOCIAL_DIR.glob("*.json"))


def clear_mock_posts() -> None:
    """Test helper: reset the mock social outbox."""
    if config.SENT_SOCIAL_DIR.exists():
        for p in config.SENT_SOCIAL_DIR.iterdir():
            p.unlink()
