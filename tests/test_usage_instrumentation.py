"""Every Claude call records usage, because the client records it.

Instrumenting the client rather than the call sites is the design: there are
thirteen `messages.create(` calls across six modules, and the next one added
would not be instrumented if this were done per-call-site.

But there are FOUR client factories, not one. summarizer._get_client() is the
one most modules share; extract_cafr_investments, extract_cafr_actuarial and
extract_ips each build their own with duplicated credential logic. Those three
are the Sonnet calls over 100+ page CAFR PDFs — the most likely answer to where
the money goes — so instrumenting only the summariser would have measured
everything except the thing this exists to find.
"""

from __future__ import annotations

import pathlib
import re
import types

import pytest

import costs
import database
import summarizer

ROOT = pathlib.Path(__file__).resolve().parents[1]


class _FakeMessages:
    def __init__(self):
        self.calls = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return types.SimpleNamespace(
            content=[types.SimpleNamespace(text="{}")],
            stop_reason="end_turn",
            usage=types.SimpleNamespace(
                input_tokens=1234, output_tokens=56,
                cache_creation_input_tokens=0, cache_read_input_tokens=0))


class _FakeClient:
    def __init__(self):
        self.messages = _FakeMessages()
        self.base_url = "https://example.invalid"


@pytest.fixture()
def live_mode(monkeypatch):
    """Clear the mock flags conftest sets for the whole suite.

    costs.mock_mode() suppresses recording, and the autouse fixture in
    conftest.py sets both LLM_MODE and INSIGHTS_MODE to "mock" — so a test that
    expects a row has to opt back into live behaviour explicitly. Forgetting
    this makes a test pass by recording nothing and asserting nothing.
    """
    monkeypatch.delenv("LLM_MODE", raising=False)
    monkeypatch.delenv("INSIGHTS_MODE", raising=False)
    assert not costs.mock_mode(), "still in mock mode — no row would be written"


@pytest.fixture()
def wrapped():
    return costs.instrument(_FakeClient())


def test_a_call_through_the_wrapper_records_usage(tmp_db, wrapped, live_mode):
    wrapped.messages.create(model="claude-haiku-4-5-20251001",
                            max_tokens=10, messages=[])

    session = database.get_session()
    try:
        rows = session.query(database.ApiUsage).all()
        assert len(rows) == 1
        assert rows[0].input_tokens == 1234 and rows[0].output_tokens == 56
        assert rows[0].model == "claude-haiku-4-5-20251001"
    finally:
        session.close()


def test_the_response_is_returned_unchanged(tmp_db, wrapped, live_mode):
    """The wrapper must be transparent — callers read .content[0].text."""
    msg = wrapped.messages.create(model="claude-haiku-4-5-20251001",
                                  max_tokens=10, messages=[])
    assert msg.content[0].text == "{}"
    assert msg.usage.input_tokens == 1234


def test_arguments_pass_through_untouched(tmp_db, wrapped, live_mode):
    wrapped.messages.create(model="claude-sonnet-4-6", max_tokens=99,
                            system="sys", messages=[])
    call = wrapped.messages._inner.calls[0]
    assert call["max_tokens"] == 99 and call["system"] == "sys"


def test_other_client_attributes_still_reach_through(wrapped):
    """Callers touch more than .messages — the proxy must not hide the rest."""
    assert wrapped.base_url == "https://example.invalid"


def test_a_recording_failure_does_not_break_the_call(tmp_db, wrapped,
                                                     live_mode, monkeypatch):
    """The work survives a broken measurement."""
    def boom(*a, **k):
        raise RuntimeError("recorder exploded")

    monkeypatch.setattr(database, "record_api_usage", boom)
    msg = wrapped.messages.create(model="claude-haiku-4-5-20251001",
                                  max_tokens=10, messages=[])
    assert msg.content[0].text == "{}"


def test_mock_mode_records_nothing(tmp_db, wrapped, monkeypatch):
    """A mock run has no spend, so a row would be a lie."""
    monkeypatch.setenv("LLM_MODE", "mock")
    wrapped.messages.create(model="claude-haiku-4-5-20251001",
                            max_tokens=10, messages=[])

    session = database.get_session()
    try:
        assert session.query(database.ApiUsage).count() == 0
    finally:
        session.close()


def test_instrumenting_twice_does_not_double_count(tmp_db, live_mode):
    """_get_client caches, but a caller may wrap defensively."""
    once = costs.instrument(_FakeClient())
    twice = costs.instrument(once)
    assert twice is once

    twice.messages.create(model="claude-haiku-4-5-20251001",
                          max_tokens=10, messages=[])
    session = database.get_session()
    try:
        assert session.query(database.ApiUsage).count() == 1
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Coverage of the factories themselves
# ---------------------------------------------------------------------------

# Every module that builds its own Anthropic client. Each must instrument it,
# or its spend is invisible — and three of these are the expensive ones.
_CLIENT_FACTORIES = (
    "summarizer.py",
    "extract_cafr_investments.py",
    "extract_cafr_actuarial.py",
    "extract_ips.py",
)


@pytest.mark.parametrize("module", _CLIENT_FACTORIES)
def test_every_client_factory_instruments_its_client(module):
    src = (ROOT / module).read_text(encoding="utf-8")
    assert "def _get_client" in src, "%s no longer builds a client" % module
    assert "instrument(" in src, (
        "%s builds an Anthropic client without costs.instrument() — its spend "
        "would not be recorded" % module)


def test_no_new_module_builds_an_uninstrumented_client():
    """Catches the fifth factory before it is written.

    insights/daily.py used to construct Anthropic() directly, so its spend was
    invisible; it now goes through the summariser's factory.
    """
    offenders = []
    for path in ROOT.rglob("*.py"):
        rel = path.relative_to(ROOT).as_posix()
        if any(rel.startswith(p) for p in
               (".venv/", ".claude/", "tests/", "scripts/", "build/")):
            continue
        if rel in _CLIENT_FACTORIES:
            continue
        if re.search(r"\bAnthropic\(",
                     path.read_text(encoding="utf-8", errors="replace")):
            offenders.append(rel)
    assert offenders == [], (
        "these construct an Anthropic client directly, so their spend is "
        "invisible — use an instrumented factory: %s" % offenders)


def test_the_summariser_factory_returns_the_wrapper(monkeypatch):
    """Not just that the word appears in the file."""
    monkeypatch.setattr(summarizer, "_client", None)
    monkeypatch.setattr(summarizer, "_build_client", lambda: _FakeClient())
    assert isinstance(summarizer._get_client(), costs._RecordingClient)


# ---------------------------------------------------------------------------
# Attribution coverage
#
# Unlabelled spend answers "how much" but not "on what", and "on what" is the
# question this whole exercise exists to answer.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("module,label", [
    ("pipeline.py", "summarize"),
    ("extractor.py", "ocr"),
    ("refresh_cafrs.py", "cafr_extract"),
    ("insights/scheduler.py", "insights:"),
])
def test_each_spending_entry_point_labels_its_calls(module, label):
    src = (ROOT / module).read_text(encoding="utf-8")
    assert "import costs" in src, "%s does not import costs" % module
    assert ("costs.track(" in src or "costs.label_process(" in src), \
        "%s labels nothing" % module
    assert label in src, "%s does not use the %r label" % (module, label)


def test_label_process_sets_without_needing_a_reset():
    """Used by CLI entry points, where the process exits at the end of main()."""
    costs.label_process("insights:monthly", run_id="2026-08")
    try:
        assert costs.current_attribution() == ("insights:monthly", "2026-08")
    finally:
        costs.label_process("unattributed", None)


def test_ocr_calls_are_labelled_through_the_real_function(tmp_db, live_mode,
                                                          monkeypatch):
    """Behavioural, not just textual: the label has to reach the recorder.

    extract_pdf_ocr wraps a private worker so the per-page loop needed no
    re-indenting; that indirection is exactly the kind of thing a source-only
    assertion would miss if it were wired up wrong.
    """
    import extractor

    seen = {}

    def fake_worker(path):
        seen["attribution"] = costs.current_attribution()
        return "", 0, extractor.OcrInfo()

    monkeypatch.setattr(extractor, "_extract_pdf_ocr", fake_worker)
    extractor.extract_pdf_ocr("irrelevant.pdf")
    assert seen["attribution"][0] == "ocr"
