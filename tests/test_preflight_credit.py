"""The credit preflight: fail the job loudly when the balance is empty.

Three times in September 2026 the prepaid Anthropic balance ran out and
nothing said so: the pipeline went green producing only dedup rows, the
digest sent "synthesis failed" paragraphs, and days of coverage were lost
before anyone looked. This check runs one minimal Haiku call at the start
of every Claude-calling workflow. An empty balance fails the step, which
turns the job red, and emails the approval recipient. Anything else that
goes wrong with the probe is reported and lets the job continue: a
transient API error is not a reason to skip a day's fetch.
"""
from __future__ import annotations

import pathlib

import httpx
import pytest
import yaml
import anthropic

from scripts import preflight_credit

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _bad_request(message: str) -> anthropic.BadRequestError:
    resp = httpx.Response(400, request=httpx.Request("POST", "https://api.anthropic.com/v1/messages"))
    return anthropic.BadRequestError(message, response=resp,
                                     body={"error": {"type": "invalid_request_error", "message": message}})


class _Client:
    def __init__(self, exc=None):
        self.exc = exc
        self.calls = 0

    class _Messages:
        def __init__(self, outer): self.outer = outer

        def create(self, **kw):
            self.outer.calls += 1
            self.outer.kwargs = kw
            if self.outer.exc:
                raise self.outer.exc
            return object()

    @property
    def messages(self):
        return self._Messages(self)


def _sent_emails(tmp_path_factory=None):
    d = ROOT / "tmp" / "sent_emails"
    return sorted(d.glob("*.eml")) if d.exists() else []


def test_a_funded_key_passes_without_email(monkeypatch):
    sent = []
    monkeypatch.setattr(preflight_credit, "_send", lambda subject, text: sent.append(subject))
    client = _Client()
    assert preflight_credit.check(client) == 0
    assert client.calls == 1
    assert client.kwargs["max_tokens"] == 1, "the probe must cost nothing"
    assert sent == []


def test_an_empty_balance_fails_and_emails(monkeypatch):
    sent = []
    monkeypatch.setattr(preflight_credit, "_send", lambda subject, text: sent.append((subject, text)))
    client = _Client(_bad_request(
        "Your credit balance is too low to access the Anthropic API. "
        "Please go to Plans & Billing to upgrade or purchase credits."))
    assert preflight_credit.check(client, job="daily-pipeline") == 1
    assert len(sent) == 1
    subject, text = sent[0]
    assert "credit" in subject.lower()
    assert "daily-pipeline" in text


def test_an_email_failure_does_not_hide_the_lapse(monkeypatch):
    def boom(subject, text):
        raise RuntimeError("RESEND_API_KEY not set")
    monkeypatch.setattr(preflight_credit, "_send", boom)
    client = _Client(_bad_request("Your credit balance is too low to access the Anthropic API."))
    assert preflight_credit.check(client) == 1


def test_a_bad_key_fails_too(monkeypatch):
    sent = []
    monkeypatch.setattr(preflight_credit, "_send", lambda subject, text: sent.append(subject))
    resp = httpx.Response(401, request=httpx.Request("POST", "https://api.anthropic.com/v1/messages"))
    client = _Client(anthropic.AuthenticationError("invalid x-api-key", response=resp, body=None))
    assert preflight_credit.check(client) == 1
    assert len(sent) == 1


def test_any_other_error_warns_and_lets_the_job_run(monkeypatch, capsys):
    sent = []
    monkeypatch.setattr(preflight_credit, "_send", lambda subject, text: sent.append(subject))
    client = _Client(anthropic.APIConnectionError(request=httpx.Request("POST", "https://api.anthropic.com")))
    assert preflight_credit.check(client) == 0
    assert sent == []
    assert "warning" in capsys.readouterr().out.lower()


def test_a_missing_key_fails(monkeypatch):
    """The workflow's own key check runs first, so this is belt and braces
    for a local run and for a step added later that forgets the env."""
    sent = []
    monkeypatch.setattr(preflight_credit, "_send", lambda subject, text: sent.append(subject))
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setattr("dotenv.load_dotenv", lambda *a, **k: False)
    assert preflight_credit.check(None, job="x") == 1
    assert sent and "key" in sent[0].lower()


CLAUDE_WORKFLOWS = [
    "daily-pipeline.yml", "weekly-insights.yml", "weekly-watch.yml",
    "monthly-cafr-refresh.yml", "monthly-ips.yml", "monthly-insights.yml",
    "quarterly-insights.yml", "annual-insights.yml",
]


@pytest.mark.parametrize("name", CLAUDE_WORKFLOWS)
def test_every_claude_workflow_runs_the_preflight_before_its_work(name):
    wf = yaml.safe_load((ROOT / ".github" / "workflows" / name).read_text(encoding="utf-8"))
    for job_name, job in wf["jobs"].items():
        steps = job.get("steps") or []
        runs = [s.get("run", "") for s in steps]
        idx = [i for i, r in enumerate(runs) if "scripts.preflight_credit" in r]
        assert idx, f"{name}:{job_name} has no preflight step"
        # It must come before the first step that does real work, which is
        # every python invocation that is not the preflight itself.
        later_python = [i for i, r in enumerate(runs) if "python" in r and i not in idx
                        and "pip install" not in r and "playwright install" not in r]
        assert all(i > idx[0] for i in later_python), f"{name}:{job_name} preflight runs late"
        env = job.get("env") or {}
        for var in ("RESEND_API_KEY", "APPROVAL_EMAIL_RECIPIENT", "APPROVAL_EMAIL_FROM"):
            assert var in env, f"{name}:{job_name} cannot email without {var}"
