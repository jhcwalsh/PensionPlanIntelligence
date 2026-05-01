"""One-shot fix: convert the April monthly note's bare ``(?doc=N)``
parentheticals into proper ``([source](?doc=N))`` markdown links and
expand a malformed multi-URL footer link into separate links.

Updates both notes/monthly_cio_insights_2026-04-01.md on disk and
publication 3's draft_markdown column so the Notes tab and Insights
tab stay in sync.

Background: compose_monthly synthesizes from approved weeklies. The
weeklies use ``[source](?doc=N)`` inline citations; the monthly
composer in this run produced bare ``(?doc=N)`` markers in the body
plus one aggregated "Sources referenced" footer with proper links.
The bare markers render as literal text, not anchors, so users see
no clickable links throughout the body.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

from database import Publication, get_session

NOTE_PATH = Path(__file__).parent.parent / "notes" / "monthly_cio_insights_2026-04-01.md"
PUB_ID = 3


def _fix_bare_doc_refs(text: str) -> str:
    """Wrap bare (?doc=N[, ?doc=M, ...]) parentheticals into [source] links.

    Skips groups already inside a markdown link, i.e. preceded by ``]``.
    """
    pattern = re.compile(r"(?<!\])\((\?doc=[\d, ?=doc]*)\)")

    def replace(m: re.Match) -> str:
        inner = m.group(1)
        ids = re.findall(r"\?doc=(\d+)", inner)
        if not ids:
            return m.group(0)
        if len(ids) == 1:
            return f"([source](?doc={ids[0]}))"
        links = ", ".join(f"[source {i + 1}](?doc={d})" for i, d in enumerate(ids))
        return f"({links})"

    return pattern.sub(replace, text)


def _fix_multi_url_links(text: str) -> str:
    """Expand ``[label](?doc=N, ?doc=M, ...)`` into per-doc separate links.

    Each ``?doc=N`` after the first reuses the original label with " (n)".
    """
    pattern = re.compile(r"\[([^\]]+)\]\((\?doc=[\d, ?=doc]*)\)")

    def replace(m: re.Match) -> str:
        label, href = m.group(1), m.group(2)
        ids = re.findall(r"\?doc=(\d+)", href)
        if len(ids) <= 1:
            return m.group(0)
        parts = [f"[{label}](?doc={ids[0]})"]
        for i, d in enumerate(ids[1:], start=2):
            parts.append(f"[{label} ({i})](?doc={d})")
        return ", ".join(parts)

    return pattern.sub(replace, text)


def fix(text: str) -> str:
    return _fix_multi_url_links(_fix_bare_doc_refs(text))


def main() -> int:
    original = NOTE_PATH.read_text(encoding="utf-8")
    fixed = fix(original)
    if fixed == original:
        print(f"{NOTE_PATH.name}: no changes needed")
    else:
        NOTE_PATH.write_text(fixed, encoding="utf-8")
        print(f"{NOTE_PATH.name}: rewritten")

    session = get_session()
    try:
        pub = session.get(Publication, PUB_ID)
        if pub is None:
            print(f"pub {PUB_ID} missing")
            return 1
        new_md = fix(pub.draft_markdown or "")
        if new_md == pub.draft_markdown:
            print(f"pub {PUB_ID}: no changes needed")
        else:
            pub.draft_markdown = new_md
            session.commit()
            print(f"pub {PUB_ID}: draft_markdown updated")
    finally:
        session.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
