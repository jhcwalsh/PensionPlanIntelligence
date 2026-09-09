"""The daily digest paragraph is a faithful rewrite, capped at 600 tokens.

It runs on Haiku: the prompt forbids inference or editorialising and the
input is a handful of already-structured summaries, so the work is
restatement, not judgement.
"""

import types
from datetime import datetime

import summarizer
from database import Document, Summary
from insights import daily


def test_daily_synthesis_uses_haiku(monkeypatch):
    seen = []

    def create(**kwargs):
        seen.append(kwargs["model"])
        return types.SimpleNamespace(content=[types.SimpleNamespace(text="para")])

    monkeypatch.setattr(summarizer, "_get_client",
                        lambda: types.SimpleNamespace(
                            messages=types.SimpleNamespace(create=create)))

    doc = Document(id=1, plan_id="calpers", url="https://x/a.pdf",
                   filename="a.pdf", doc_type="agenda",
                   meeting_date=datetime(2026, 9, 1))
    summary = Summary(document_id=1, summary_text="Board met.")

    out = daily._synthesize_via_anthropic("CalPERS", [doc], {1: summary})

    assert out == "para"
    assert seen == [summarizer.MODEL_HAIKU]
