"""Email defaults name domains we actually control.

``insights/config.py`` resolves the sender and recipient at import, each with
a fallback for when the environment variable is missing. Both fallbacks named
``pensionintel.com`` -- a domain from an abandoned naming round that nobody
here owns.

The two failure modes are not symmetric, which is why they get separate
treatment below:

* A wrong **sender** is self-limiting. Resend refuses to send from an
  unverified domain, so the mail simply does not go out.
* A wrong **recipient** delivers. If that domain ever accepted mail, a
  missing ``APPROVAL_EMAIL_RECIPIENT`` would have quietly posted briefing
  content to a stranger. Low probability, but the kind of thing that is
  embarrassing exactly once.

Production sets both in three places -- ``.env``, the Render service, and the
repo secret -- so these defaults only fire in local dev or a misconfiguration.
That is precisely when you want them pointing somewhere harmless.
"""

from __future__ import annotations

import importlib
import os

import pytest

OWNED_DOMAIN = "pensiongraph.com"
RETIRED_DOMAIN = "pensionintel.com"


@pytest.fixture()
def bare_config(monkeypatch):
    """config.py re-imported with no email env vars set.

    conftest.py sets both as autouse fixtures, so a test that wants to see
    the *defaults* has to clear them and force a re-import -- otherwise it
    asserts on the test values and proves nothing.
    """
    # config.py calls load_dotenv() at import, so clearing os.environ is not
    # enough -- the reload reads .env straight back in. Patch it at the dotenv
    # module so the reload's ``from dotenv import load_dotenv`` picks up the
    # no-op. (Found the hard way: without this the test read the real .env and
    # asserted on james@walsh.nu, proving nothing about the default.)
    import dotenv
    monkeypatch.setattr(dotenv, "load_dotenv", lambda *a, **k: False)
    for var in ("APPROVAL_EMAIL_FROM", "APPROVAL_EMAIL_RECIPIENT"):
        monkeypatch.delenv(var, raising=False)
    assert "APPROVAL_EMAIL_FROM" not in os.environ

    import insights.config as config
    reloaded = importlib.reload(config)
    assert "walsh.nu" not in reloaded.APPROVAL_EMAIL_RECIPIENT, (
        "the fixture is not isolating -- .env leaked back in")
    return reloaded


def test_the_sender_default_is_a_domain_we_own(bare_config):
    assert bare_config.APPROVAL_EMAIL_FROM.endswith("@" + OWNED_DOMAIN) or \
        bare_config.APPROVAL_EMAIL_FROM.endswith("." + OWNED_DOMAIN), \
        "sender default is not at %s: %r" % (
            OWNED_DOMAIN, bare_config.APPROVAL_EMAIL_FROM)


def test_the_recipient_default_is_a_domain_we_own(bare_config):
    """The one that would actually deliver if it were wrong."""
    assert bare_config.APPROVAL_EMAIL_RECIPIENT.endswith("@" + OWNED_DOMAIN) or \
        bare_config.APPROVAL_EMAIL_RECIPIENT.endswith("." + OWNED_DOMAIN), \
        "recipient default is not at %s: %r" % (
            OWNED_DOMAIN, bare_config.APPROVAL_EMAIL_RECIPIENT)


def test_the_retired_domain_is_gone(bare_config):
    for value in (bare_config.APPROVAL_EMAIL_FROM,
                  bare_config.APPROVAL_EMAIL_RECIPIENT):
        assert RETIRED_DOMAIN not in value, (
            "%r still names the retired domain" % value)


def test_the_recipient_list_derives_from_the_default(bare_config):
    """APPROVAL_EMAIL_RECIPIENTS is parsed from the singular at import."""
    assert bare_config.APPROVAL_EMAIL_RECIPIENTS == \
        [bare_config.APPROVAL_EMAIL_RECIPIENT]


def test_the_env_vars_still_win(monkeypatch):
    """The defaults must stay defaults -- production overrides all of them."""
    monkeypatch.setenv("APPROVAL_EMAIL_FROM", "a@example.invalid")
    monkeypatch.setenv("APPROVAL_EMAIL_RECIPIENT", "b@example.invalid")

    import insights.config as config
    reloaded = importlib.reload(config)
    try:
        assert reloaded.APPROVAL_EMAIL_FROM == "a@example.invalid"
        assert reloaded.APPROVAL_EMAIL_RECIPIENT == "b@example.invalid"
    finally:
        importlib.reload(config)


def test_a_comma_separated_recipient_still_fans_out(monkeypatch):
    """The multi-recipient path, which the singular default must not break."""
    monkeypatch.setenv("APPROVAL_EMAIL_RECIPIENT",
                       "one@example.invalid, two@example.invalid")

    import insights.config as config
    reloaded = importlib.reload(config)
    try:
        assert reloaded.APPROVAL_EMAIL_RECIPIENTS == [
            "one@example.invalid", "two@example.invalid"]
    finally:
        importlib.reload(config)


def test_the_subscribe_sender_follows_the_approval_sender(bare_config):
    """One variable drives four flows; a wrong value breaks all of them.

    SUBSCRIBE_FROM_ADDRESS falls back to APPROVAL_EMAIL_FROM, so subscriber
    confirmation, welcome mail and digest fan-out inherit whatever the
    approval sender is.
    """
    assert bare_config.SUBSCRIBE_FROM_ADDRESS == bare_config.APPROVAL_EMAIL_FROM
