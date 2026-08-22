# Auth0 Invite-Only Access Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Put the whole app behind per-person login — Auth0 passwordless email for identity, an `allowed_emails` table for permission — replacing the single shared `ADMIN_PASSWORD`.

**Architecture:** Two layers, deliberately separate. Streamlit's native `st.login()` (OIDC) asks Auth0 *who are you* and returns a verified email. The app then asks its own database *are you allowed*. Auth0 never decides access, so adding a user is one row and never involves logging into Auth0. The decision itself is a pure function taking four booleans, because `st.login()` cannot be driven from a test.

**Tech Stack:** Streamlit 1.58 (`st.login` / `st.user`, OIDC), Auth0 passwordless email, SQLAlchemy 2.x, Neon Postgres, Render.

**Spec:** `docs/superpowers/specs/2026-08-16-low-maintenance-app-design.md` §5 (Access and auth), with `docs/superpowers/specs/2026-08-19-portal-readiness-design.md` §1 for the closed-audience assumption this rests on. Step 6 of the migration sequence in `nextsteps.md`.

## Global Constraints

- **Never reload `database.py` in a test.** `tests/conftest.py` rebinds `database.engine` / `SessionLocal` by `monkeypatch.setattr`; a reload orphans the ORM classes and breaks the mapper registry.
- **Never write SQL `ALTER TABLE` migrations.** Add the model class and call `init_db()`; `create_all` is idempotent and creates missing tables.
- **Do not run `git add .`** — the repo root holds many intentionally untracked scratch files. Stage by name.
- **Never commit a secret.** `client_secret` and `cookie_secret` reach Render as env vars with `sync: false`, and the generated `.streamlit/secrets.toml` must be gitignored.
- The app is invite-only after this. That reopens *together* with the phase-2 static-site port if public access is ever wanted — one decision, not two (portal spec §1).

## Facts established before writing this plan

Verified against the installed Streamlit 1.58 and the current `app.py`:

- **`st.login()` reads `[auth]` from `secrets.toml` only.** Streamlit's secrets manager writes top-level scalars *into* `os.environ`; it never reads `[auth]` *from* the environment. Render supplies env vars and no file, so the file has to be generated at startup. This is the single detail most likely to break the deploy.
- **`st.user.is_logged_in` only exists when an `[auth]` section is present.** With no auth configured the key is absent entirely, so the local-dev path must test for configuration rather than for logged-out-ness.
- **`main()` has six query-param early returns before the tabs** (`doc`, `cafr_plan`, `plan`, `confirm`, `unsub`, `prefs`). Three are email-link flows carrying their own tokens. Gate placement is therefore not "top of `main()`" — see Task 3.
- The current gate is `_admin_unlocked()` (fail-open when `ADMIN_PASSWORD` is unset) plus `_render_admin_login_sidebar()`, and it hides only Archive / Drafts / Admin by appending them to `tab_specs`.
- `database.py` has no `AllowedEmail` model. `init_db()` will create the table on both backends.

## File structure

| File | Responsibility |
|---|---|
| `database.py` (modify) | `AllowedEmail` model; `is_allowed()` / `is_admin()` lookups |
| `tests/test_allowed_emails.py` (create) | The permission layer, against a real session |
| `scripts/render_streamlit_secrets.py` (create) | Write `.streamlit/secrets.toml` from env vars |
| `tests/test_streamlit_secrets.py` (create) | Rendering, escaping, refusal to overwrite |
| `auth.py` (create) | `gate_decision()` — the pure predicate |
| `tests/test_auth_gate.py` (create) | Every branch of the decision |
| `app.py` (modify) | Wire Streamlit to the gate; user management in Admin |
| `render.yaml` (modify) | Auth env vars; `ADMIN_PASSWORD` removed |
| `.gitignore` (modify) | `.streamlit/secrets.toml` |

---

## Task 1: The permission layer

**Files:**
- Modify: `database.py` (new model beside the others; helpers near `get_session`)
- Test: `tests/test_allowed_emails.py`

**Interfaces:**
- Produces: `AllowedEmail` (columns `email`, `added_at`, `added_by`, `revoked_at`, `is_admin`), `is_allowed(session, email) -> bool`, `is_admin(session, email) -> bool`, `grant(session, email, added_by, is_admin=False) -> AllowedEmail`, `revoke(session, email) -> bool`

- [ ] **Step 1: Write the failing tests**

```python
"""Permission is ours; identity is Auth0's.

Auth0 answers "who are you" and returns a verified email. This table answers
"are you allowed", which is why adding a user never involves logging into
Auth0 — and why revocation is immediate rather than waiting on a token expiry.

See docs/superpowers/specs/2026-08-16-low-maintenance-app-design.md §5.
"""

from __future__ import annotations

import pytest

import database
from database import get_session


@pytest.fixture()
def session(tmp_db):
    s = get_session()
    yield s
    s.close()


def test_an_unknown_email_is_not_allowed(session):
    assert database.is_allowed(session, "stranger@example.com") is False


def test_a_granted_email_is_allowed(session):
    database.grant(session, "reader@example.com", added_by="james@walsh.nu")
    assert database.is_allowed(session, "reader@example.com") is True


def test_revocation_is_immediate(session):
    database.grant(session, "gone@example.com", added_by="james@walsh.nu")
    assert database.revoke(session, "gone@example.com") is True
    assert database.is_allowed(session, "gone@example.com") is False


def test_revoking_keeps_the_row_as_an_audit_trail(session):
    """A deleted row loses who was ever granted access and when."""
    database.grant(session, "gone@example.com", added_by="james@walsh.nu")
    database.revoke(session, "gone@example.com")
    row = session.query(database.AllowedEmail).filter_by(
        email="gone@example.com").one()
    assert row.revoked_at is not None
    assert row.added_by == "james@walsh.nu"


def test_a_revoked_user_can_be_granted_again(session):
    """Re-granting must clear revoked_at, not add a second row."""
    database.grant(session, "back@example.com", added_by="james@walsh.nu")
    database.revoke(session, "back@example.com")
    database.grant(session, "back@example.com", added_by="james@walsh.nu")

    rows = session.query(database.AllowedEmail).filter_by(
        email="back@example.com").all()
    assert len(rows) == 1, "re-granting created a duplicate row"
    assert rows[0].revoked_at is None
    assert database.is_allowed(session, "back@example.com") is True


def test_email_matching_is_case_insensitive(session):
    """Auth0 may return a different case than the one typed into Admin.

    Treating Reader@Example.com as a different person from reader@example.com
    locks out a legitimate user in a way that looks like an Auth0 fault.
    """
    database.grant(session, "Reader@Example.com", added_by="james@walsh.nu")
    assert database.is_allowed(session, "reader@example.com") is True
    assert database.is_allowed(session, "READER@EXAMPLE.COM") is True


def test_surrounding_whitespace_is_ignored(session):
    """Emails arrive pasted from a text field."""
    database.grant(session, "  spaced@example.com \n", added_by="a@b.c")
    assert database.is_allowed(session, "spaced@example.com") is True


def test_admin_is_separate_from_allowed(session):
    database.grant(session, "reader@example.com", added_by="a@b.c")
    database.grant(session, "boss@example.com", added_by="a@b.c", is_admin=True)
    assert database.is_admin(session, "reader@example.com") is False
    assert database.is_admin(session, "boss@example.com") is True


def test_a_revoked_admin_is_not_an_admin(session):
    """Revocation has to clear both, or a revoked admin keeps the Admin tab."""
    database.grant(session, "boss@example.com", added_by="a@b.c", is_admin=True)
    database.revoke(session, "boss@example.com")
    assert database.is_admin(session, "boss@example.com") is False


def test_none_and_empty_are_never_allowed(session):
    """st.user.email is absent for a logged-out user; that must not pass."""
    assert database.is_allowed(session, None) is False
    assert database.is_allowed(session, "") is False
    assert database.is_admin(session, None) is False
```

- [ ] **Step 2: Run them and watch them fail**

Run: `LLM_MODE=mock python -m pytest tests/test_allowed_emails.py -q`
Expected: FAIL, `AttributeError: module 'database' has no attribute 'grant'`

- [ ] **Step 3: Implement — add to `database.py`, after the other models**

```python
class AllowedEmail(Base):
    """Who may use the app. Identity comes from Auth0; permission comes here.

    Rows are never deleted: `revoked_at` is stamped instead, so the table stays
    an audit trail of who was granted access, by whom, and when it ended.
    Email is stored lower-cased and stripped — the address Auth0 returns need
    not match the case someone typed into the Admin tab, and treating those as
    two people locks out a legitimate user in a way that looks like an Auth0
    fault.
    """

    __tablename__ = "allowed_emails"

    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String, nullable=False, unique=True)
    added_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    added_by = Column(String)
    revoked_at = Column(DateTime(timezone=True))
    is_admin = Column(Boolean, default=False, nullable=False)


def _canonical_email(email: Optional[str]) -> str:
    return (email or "").strip().lower()


def _active_grant(session: Session, email: Optional[str]):
    canonical = _canonical_email(email)
    if not canonical:
        return None
    return (session.query(AllowedEmail)
            .filter(AllowedEmail.email == canonical,
                    AllowedEmail.revoked_at.is_(None))
            .first())


def is_allowed(session: Session, email: Optional[str]) -> bool:
    return _active_grant(session, email) is not None


def is_admin(session: Session, email: Optional[str]) -> bool:
    row = _active_grant(session, email)
    return bool(row and row.is_admin)


def grant(session: Session, email: str, added_by: str,
          is_admin: bool = False) -> AllowedEmail:
    """Add or reinstate a user. Idempotent on the email."""
    canonical = _canonical_email(email)
    if not canonical:
        raise ValueError("email is required")
    row = (session.query(AllowedEmail)
           .filter(AllowedEmail.email == canonical).first())
    if row is None:
        row = AllowedEmail(email=canonical)
        session.add(row)
    row.revoked_at = None
    row.added_by = added_by
    row.is_admin = is_admin
    row.added_at = utcnow()
    session.commit()
    return row


def revoke(session: Session, email: str) -> bool:
    """Stamp revoked_at. Returns False if there was nothing to revoke."""
    row = _active_grant(session, email)
    if row is None:
        return False
    row.revoked_at = utcnow()
    session.commit()
    return True
```

Note `is_admin` is both a column and a module function. That is deliberate — the column reads naturally on the model and the function on the module — but do not import the function into a scope where the model attribute is also bare.

- [ ] **Step 4: Run the tests**

Run: `LLM_MODE=mock python -m pytest tests/test_allowed_emails.py -q`
Expected: PASS (10 tests)

- [ ] **Step 5: Confirm the table is created on both backends**

```bash
python -c "import database; database.init_db(); print('allowed_emails' in database.Base.metadata.tables)"
TEST_POSTGRES_URL="$(cat .test_pg_url)" LLM_MODE=mock python -m pytest tests/postgres/ -q
```

- [ ] **Step 6: Commit**

```bash
git add database.py tests/test_allowed_emails.py
git commit -m "Add allowed_emails: the permission half of invite-only access"
```

---

## Task 2: Render `secrets.toml` from the environment

**Files:**
- Create: `scripts/render_streamlit_secrets.py`
- Test: `tests/test_streamlit_secrets.py`
- Modify: `.gitignore`

**Interfaces:**
- Produces: `render_secrets(env: Mapping, dest: Path) -> bool` (True when written, False when auth is not configured)

`st.login()` reads `[auth]` from `secrets.toml` and nothing else — Streamlit's secrets manager writes secrets *into* the environment, never the reverse. Render supplies env vars and no file, so the file is generated before Streamlit starts.

- [ ] **Step 1: Write the failing tests**

```python
"""st.login() reads secrets.toml; Render supplies environment variables.

Streamlit's secrets manager writes top-level scalars *into* os.environ and
never reads [auth] out of it, so there is no env-var route to configuring
login. The file has to exist before Streamlit starts.
"""

from __future__ import annotations

import tomllib

import pytest

from scripts.render_streamlit_secrets import render_secrets

FULL = {
    "AUTH_CLIENT_ID": "abc123",
    "AUTH_CLIENT_SECRET": "sh$h-secret\"with'quotes",
    "AUTH_SERVER_METADATA_URL":
        "https://example.auth0.com/.well-known/openid-configuration",
    "AUTH_COOKIE_SECRET": "0123456789abcdef0123456789abcdef",
    "APP_BASE_URL": "https://pensionplanintelligence.onrender.com",
}


def test_writes_a_parseable_auth_section(tmp_path):
    dest = tmp_path / "secrets.toml"
    assert render_secrets(FULL, dest) is True

    parsed = tomllib.loads(dest.read_text(encoding="utf-8"))
    assert parsed["auth"]["client_id"] == "abc123"
    assert parsed["auth"]["server_metadata_url"].endswith("openid-configuration")


def test_quotes_and_backslashes_survive(tmp_path):
    """Auth0 secrets are random and routinely contain both.

    Naive string interpolation produces a file that either fails to parse or,
    worse, parses to a truncated secret and fails authentication with a
    misleading error.
    """
    dest = tmp_path / "secrets.toml"
    render_secrets({**FULL, "AUTH_CLIENT_SECRET": 'a"b\\c'}, dest)
    parsed = tomllib.loads(dest.read_text(encoding="utf-8"))
    assert parsed["auth"]["client_secret"] == 'a"b\\c'


def test_the_redirect_uri_is_derived_from_the_base_url(tmp_path):
    """It must be exactly APP_BASE_URL + /oauth2callback and registered in
    Auth0. A trailing slash on the base URL yields a double slash, which Auth0
    treats as a different URI and rejects."""
    dest = tmp_path / "secrets.toml"
    render_secrets({**FULL, "APP_BASE_URL": "https://example.com/"}, dest)
    parsed = tomllib.loads(dest.read_text(encoding="utf-8"))
    assert parsed["auth"]["redirect_uri"] == "https://example.com/oauth2callback"


def test_returns_false_when_auth_is_not_configured(tmp_path):
    """Local dev has no Auth0. Writing a half-populated [auth] section is worse
    than writing none: st.user.is_logged_in appears and every visitor is
    locked out of their own machine."""
    dest = tmp_path / "secrets.toml"
    assert render_secrets({"APP_BASE_URL": "http://localhost:8501"}, dest) is False
    assert not dest.exists()


@pytest.mark.parametrize("missing", sorted(FULL))
def test_a_single_missing_variable_means_not_configured(tmp_path, missing):
    """Partial configuration is the dangerous state, so it is treated as none."""
    env = {k: v for k, v in FULL.items() if k != missing}
    dest = tmp_path / "secrets.toml"
    if missing == "APP_BASE_URL":
        pytest.skip("APP_BASE_URL has its own default; covered below")
    assert render_secrets(env, dest) is False


def test_an_existing_file_is_not_clobbered(tmp_path):
    """A developer's own secrets.toml may hold unrelated keys."""
    dest = tmp_path / "secrets.toml"
    dest.write_text('[mine]\nkey = "value"\n', encoding="utf-8")
    render_secrets(FULL, dest)
    parsed = tomllib.loads(dest.read_text(encoding="utf-8"))
    assert parsed["mine"]["key"] == "value", "an unrelated section was destroyed"
    assert parsed["auth"]["client_id"] == "abc123", "auth was not added"


def test_the_file_is_gitignored():
    import pathlib
    root = pathlib.Path(__file__).resolve().parents[1]
    ignore = (root / ".gitignore").read_text(encoding="utf-8")
    assert ".streamlit/secrets.toml" in ignore, (
        "the generated file holds the Auth0 client secret")
```

- [ ] **Step 2: Run them and watch them fail**

Run: `LLM_MODE=mock python -m pytest tests/test_streamlit_secrets.py -q`
Expected: FAIL, `ModuleNotFoundError: No module named 'scripts.render_streamlit_secrets'`

- [ ] **Step 3: Implement**

```python
"""Write .streamlit/secrets.toml from environment variables, then exit.

st.login() takes its OIDC configuration from an [auth] section in
secrets.toml. Streamlit's secrets manager writes top-level scalars *into*
os.environ and never reads [auth] back out of it, so there is no environment
route to configuring login — and Render supplies environment variables and no
files. This bridges the two, and runs from the start command before Streamlit
does.

Absent or incomplete configuration writes nothing and exits 0: local
development has no Auth0, and the app falls open there exactly as it did with
an unset ADMIN_PASSWORD.
"""

from __future__ import annotations

import os
import pathlib
import sys
from typing import Mapping

REQUIRED = ("AUTH_CLIENT_ID", "AUTH_CLIENT_SECRET",
            "AUTH_SERVER_METADATA_URL", "AUTH_COOKIE_SECRET")

DEST = pathlib.Path(__file__).resolve().parents[1] / ".streamlit" / "secrets.toml"


def _toml_string(value: str) -> str:
    """A TOML basic string. Auth0 secrets contain quotes and backslashes.

    Naive interpolation yields a file that either fails to parse or, worse,
    parses to a truncated secret — which surfaces as an authentication failure
    with no hint that the file is at fault.
    """
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return '"%s"' % escaped


def render_secrets(env: Mapping[str, str], dest: pathlib.Path) -> bool:
    """Write the [auth] section. Returns False when auth is not configured."""
    if any(not (env.get(k) or "").strip() for k in REQUIRED):
        return False

    base = (env.get("APP_BASE_URL") or "http://localhost:8501").rstrip("/")
    section = {
        "redirect_uri": base + "/oauth2callback",
        "cookie_secret": env["AUTH_COOKIE_SECRET"],
        "client_id": env["AUTH_CLIENT_ID"],
        "client_secret": env["AUTH_CLIENT_SECRET"],
        "server_metadata_url": env["AUTH_SERVER_METADATA_URL"],
    }

    # Preserve anything already in the file — a developer's own sections are
    # not ours to destroy — but replace any [auth] we previously wrote.
    existing = ""
    if dest.exists():
        kept, skipping = [], False
        for line in dest.read_text(encoding="utf-8").splitlines():
            if line.strip().startswith("["):
                skipping = line.strip() == "[auth]"
            if not skipping:
                kept.append(line)
        existing = "\n".join(kept).rstrip() + "\n\n" if kept else ""

    body = "".join("%s = %s\n" % (k, _toml_string(v))
                   for k, v in section.items())
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(existing + "[auth]\n" + body, encoding="utf-8")
    return True


def main() -> int:
    if render_secrets(os.environ, DEST):
        print("wrote %s" % DEST)
    else:
        print("auth not configured (%s); login disabled"
              % ", ".join(REQUIRED))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run the tests, then add the gitignore entry**

```bash
LLM_MODE=mock python -m pytest tests/test_streamlit_secrets.py -q
printf '\n# Generated at startup from AUTH_* env vars; holds the Auth0 client secret\n.streamlit/secrets.toml\n' >> .gitignore
```

- [ ] **Step 5: Commit**

```bash
git add scripts/render_streamlit_secrets.py tests/test_streamlit_secrets.py .gitignore
git commit -m "Render Streamlit's auth secrets from the environment"
```

---

## Task 3: The gate

**Files:**
- Create: `auth.py`
- Test: `tests/test_auth_gate.py`
- Modify: `app.py` (`_admin_unlocked`, `_render_admin_login_sidebar`, `main`)

**Interfaces:**
- Consumes: `database.is_allowed`, `database.is_admin` (Task 1)
- Produces: `gate_decision(auth_configured: bool, logged_in: bool, email: str | None, allowed: bool) -> str`, returning `"open"`, `"login"` or `"denied"`

The decision is a pure function because `st.login()` cannot be driven from a test — the same reason `resolve_database_url` takes its environment as an argument.

- [ ] **Step 1: Write the failing tests**

```python
"""Who gets in, as a function of four facts.

st.login() redirects a browser to Auth0, so the Streamlit half cannot be
exercised from a test. Everything that decides anything therefore lives here,
where it can be.

The states that matter are not "logged in or not". They are: auth switched
off entirely (local dev), logged out, logged in but not invited, and logged in
and invited. The third is the one worth getting right — it is a real person
who authenticated successfully and still may not enter.
"""

from __future__ import annotations

import pytest

from auth import gate_decision


def test_no_auth_configured_falls_open():
    """Local dev, matching the old unset-ADMIN_PASSWORD behaviour.

    Requiring Auth0 to run the app locally would make every developer
    configure a tenant to see a page.
    """
    assert gate_decision(auth_configured=False, logged_in=False,
                         email=None, allowed=False) == "open"


def test_logged_out_is_asked_to_log_in():
    assert gate_decision(auth_configured=True, logged_in=False,
                         email=None, allowed=False) == "login"


def test_logged_in_and_invited_is_open():
    assert gate_decision(auth_configured=True, logged_in=True,
                         email="reader@example.com", allowed=True) == "open"


def test_logged_in_but_not_invited_is_denied_not_asked_to_log_in_again():
    """The distinction that matters.

    Returning "login" here loops a real user through Auth0 forever: they
    authenticate, come back, are refused, and are asked to authenticate again.
    """
    assert gate_decision(auth_configured=True, logged_in=True,
                         email="stranger@example.com", allowed=False) == "denied"


def test_a_logged_in_user_with_no_email_is_denied():
    """Some OIDC providers can omit email. Failing open on a missing claim
    would admit anyone the provider authenticated."""
    assert gate_decision(auth_configured=True, logged_in=True,
                         email=None, allowed=False) == "denied"


def test_allowed_is_ignored_when_logged_out():
    """`allowed` is computed from an email that does not exist yet; it must
    not be able to open the gate on its own."""
    assert gate_decision(auth_configured=True, logged_in=False,
                         email=None, allowed=True) == "login"


@pytest.mark.parametrize("logged_in,email,allowed", [
    (True, "a@b.c", True), (True, "a@b.c", False),
    (False, None, False), (True, None, True),
])
def test_unconfigured_auth_always_wins(logged_in, email, allowed):
    """One switch, checked first, so a half-configured deploy cannot lock the
    owner out of their own local app."""
    assert gate_decision(False, logged_in, email, allowed) == "open"
```

- [ ] **Step 2: Run them and watch them fail**

Run: `LLM_MODE=mock python -m pytest tests/test_auth_gate.py -q`
Expected: FAIL, `ModuleNotFoundError: No module named 'auth'`

- [ ] **Step 3: Implement `auth.py`**

```python
"""The access decision, kept away from Streamlit so it can be tested.

st.login() redirects a browser to Auth0; nothing about that is reachable from
a test. So the Streamlit layer does I/O only — read st.user, call st.login() —
and every judgement lives in gate_decision.
"""

from __future__ import annotations

OPEN = "open"
LOGIN = "login"
DENIED = "denied"


def gate_decision(auth_configured: bool, logged_in: bool,
                  email: str | None, allowed: bool) -> str:
    """One of OPEN, LOGIN or DENIED.

    DENIED and LOGIN are deliberately distinct. A logged-in user who is not on
    the invite list has authenticated successfully and simply has no access;
    sending them back to LOGIN loops them through Auth0 forever, arriving each
    time at the same refusal.
    """
    if not auth_configured:
        return OPEN
    if not logged_in:
        return LOGIN
    if not email:
        return DENIED
    return OPEN if allowed else DENIED
```

- [ ] **Step 4: Wire `app.py`**

Replace `_admin_unlocked` and `_render_admin_login_sidebar` with:

```python
def _auth_configured() -> bool:
    """True when secrets.toml carries an [auth] section.

    st.user.is_logged_in only exists when auth is configured, so this has to be
    checked before touching it.

    The try/except is not defensive padding: with no secrets.toml at all —
    every local checkout — `"auth" in st.secrets` *raises*
    StreamlitSecretNotFoundError rather than returning False, so a bare
    membership test crashes the app on startup.
    """
    from streamlit.errors import StreamlitSecretNotFoundError
    try:
        return "auth" in st.secrets
    except StreamlitSecretNotFoundError:
        return False


def _is_logged_in() -> bool:
    """Deliberately separate from _current_email().

    Collapsing the two would make a logged-in user whose provider omitted the
    email claim indistinguishable from a logged-out one, so they would be sent
    back to Auth0 forever instead of being told they cannot enter.
    """
    if not _auth_configured():
        return False
    try:
        return bool(st.user.is_logged_in)
    except Exception:            # noqa: BLE001 — treated as logged out
        return False


def _current_email() -> str | None:
    if not _is_logged_in():
        return None
    try:
        return st.user.get("email")
    except Exception:            # noqa: BLE001
        return None


def _admin_unlocked() -> bool:
    """True if the Archive / Drafts / Admin tabs should be visible.

    Fail-open with auth unconfigured, matching the old unset-ADMIN_PASSWORD
    behaviour so local development needs no Auth0 tenant.
    """
    if not _auth_configured():
        return True
    return database.is_admin(get_db_session(), _current_email())


def _render_account_sidebar(email: str | None) -> None:
    with st.sidebar:
        st.caption("Signed in as %s" % email)
        if st.button("Sign out", use_container_width=True):
            st.logout()
```

Then, in `main()`, place the gate **after** the three token-bearing subscriber
flows and **before** everything else:

```python
    # These three carry their own tokens and arrive from email links. Gating
    # them would mean demanding a login to unsubscribe, which is both hostile
    # and wrong: the recipient may not be an invited user at all.
    confirm_param = st.query_params.get("confirm")
    if confirm_param:
        page_subscriber_confirm(confirm_param)
        return

    unsub_param = st.query_params.get("unsub")
    if unsub_param:
        page_subscriber_unsubscribe(unsub_param)
        return

    prefs_param = st.query_params.get("prefs")
    if prefs_param:
        page_subscriber_preferences(prefs_param)
        return

    # Everything below is invited-users-only, including the doc / plan /
    # cafr_plan deep links, which are content.
    email = _current_email()
    decision = gate_decision(
        _auth_configured(),
        _is_logged_in(),
        email,
        database.is_allowed(get_db_session(), email) if email else False,
    )
    if decision == LOGIN:
        st.title("Pension Plan Intelligence")
        st.write("This site is available to invited users.")
        if st.button("Sign in with email"):
            st.login()
        return
    if decision == DENIED:
        st.title("Pension Plan Intelligence")
        st.warning(
            "%s is not on the invite list. Contact james@walsh.nu for access."
            % (email or "This account"))
        if st.button("Sign out"):
            st.logout()
        return

    if email:
        _render_account_sidebar(email)
```

The `doc` / `cafr_plan` / `plan` early returns move **below** the gate. Import
`from auth import gate_decision, LOGIN, DENIED` at the top.

- [ ] **Step 5: Add the placement test**

```python
def test_subscriber_links_are_reachable_without_logging_in():
    """Unsubscribe must never require a login.

    Asserted on source order because the gate's position in main() is the
    whole of the behaviour, and main() cannot be driven far enough to observe
    it without a live Auth0.
    """
    import pathlib
    src = (pathlib.Path(__file__).resolve().parents[1] / "app.py").read_text(
        encoding="utf-8")
    gate = src.index("decision = gate_decision(")
    for token in ('st.query_params.get("confirm")',
                  'st.query_params.get("unsub")',
                  'st.query_params.get("prefs")'):
        assert src.index(token) < gate, (
            "%s is gated — an email recipient would be asked to log in to "
            "unsubscribe" % token)
    for token in ('st.query_params.get("doc")',
                  'st.query_params.get("plan")'):
        assert src.index(token) > gate, "%s is content and must be gated" % token
```

- [ ] **Step 6: Run everything**

```bash
LLM_MODE=mock python -m pytest tests/test_auth_gate.py -q
LLM_MODE=mock INSIGHTS_MODE=mock python -m pytest tests/ -q --ignore=tests/postgres
```

- [ ] **Step 7: Verify the app still renders with auth unconfigured**

```bash
python scratch_apptest.py sqlite   # or the AppTest fingerprint used in step 4
```
Expected: 23 tabs, zero exceptions — local dev is unchanged.

- [ ] **Step 8: Commit**

```bash
git add auth.py app.py tests/test_auth_gate.py
git commit -m "Gate the app on Auth0 identity plus the invite list"
```

---

## Task 4: Managing users from the Admin tab

**Files:**
- Modify: `app.py` (`page_admin` — one more sub-tab)
- Test: `tests/test_allowed_emails.py` (extend)

- [ ] **Step 1: Add the failing test**

```python
def test_the_last_admin_cannot_revoke_themselves(session):
    """Otherwise the app becomes unadministrable and only a DB edit recovers it."""
    database.grant(session, "boss@example.com", added_by="self", is_admin=True)
    with pytest.raises(ValueError, match="last admin"):
        database.revoke_admin_safe(session, "boss@example.com")


def test_revoking_one_of_two_admins_is_fine(session):
    database.grant(session, "a@example.com", added_by="self", is_admin=True)
    database.grant(session, "b@example.com", added_by="self", is_admin=True)
    assert database.revoke_admin_safe(session, "a@example.com") is True
```

- [ ] **Step 2: Implement `revoke_admin_safe` in `database.py`**

```python
def revoke_admin_safe(session: Session, email: str) -> bool:
    """revoke(), refusing to remove the last remaining admin.

    Without this the app can be locked into a state where nobody can reach the
    Admin tab and only a direct database edit restores it.
    """
    row = _active_grant(session, email)
    if row is not None and row.is_admin:
        remaining = (session.query(AllowedEmail)
                     .filter(AllowedEmail.is_admin.is_(True),
                             AllowedEmail.revoked_at.is_(None),
                             AllowedEmail.id != row.id)
                     .count())
        if remaining == 0:
            raise ValueError("refusing to revoke the last admin")
    return revoke(session, email)
```

- [ ] **Step 3: Add the `Access` sub-tab to `page_admin`**

`page_admin` currently unpacks seven tabs (`app.py:2723`). Make it eight:

```python
    (tab_runs, tab_coverage, tab_backlog, tab_failed,
     tab_cafr, tab_cafr_refreshes, tab_subscribers, tab_access) = st.tabs(
        ["Recent Runs", "Plan Coverage", "Pipeline Backlog",
         "Failed Docs", "CAFR Coverage", "CAFR Refreshes",
         "Subscribers", "Access"]
    )
```

Then, after the existing sub-tab bodies:

```python
    with tab_access:
        st.subheader("Who can sign in")
        session = get_db_session()

        rows = (session.query(database.AllowedEmail)
                .order_by(database.AllowedEmail.email).all())
        st.dataframe(
            [{"email": r.email,
              "admin": r.is_admin,
              "added": r.added_at,
              "added by": r.added_by,
              "revoked": r.revoked_at} for r in rows],
            use_container_width=True, hide_index=True)

        with st.form("grant_access", clear_on_submit=True):
            new_email = st.text_input("Email to invite")
            make_admin = st.checkbox("Admin (can manage this list)")
            if st.form_submit_button("Invite"):
                try:
                    database.grant(session, new_email,
                                   added_by=_current_email() or "unknown",
                                   is_admin=make_admin)
                    st.success("Invited %s" % new_email.strip().lower())
                    st.rerun()
                except ValueError as exc:
                    st.error(str(exc))

        active = [r.email for r in rows if r.revoked_at is None]
        if active:
            target = st.selectbox("Revoke access for", active)
            if st.button("Revoke"):
                try:
                    # revoke_admin_safe, not revoke: removing the last admin
                    # leaves the app unadministrable.
                    database.revoke_admin_safe(session, target)
                    st.success("Revoked %s" % target)
                    st.rerun()
                except ValueError as exc:
                    st.error(str(exc))
```

- [ ] **Step 4: Run the suite and commit**

```bash
LLM_MODE=mock python -m pytest tests/test_allowed_emails.py -q
git add database.py app.py tests/test_allowed_emails.py
git commit -m "Manage the invite list from the Admin tab"
```

---

## Task 5: Cut over

**Files:**
- Modify: `render.yaml`, `CLAUDE.md`, `nextsteps.md`, `tests/test_deployment_config.py`

- [ ] **Step 1: `render.yaml` — start command and env vars**

```yaml
    startCommand: >-
      python scripts/render_streamlit_secrets.py &&
      streamlit run app.py --server.port $PORT --server.address 0.0.0.0
    envVars:
      - key: AUTH_CLIENT_ID
        sync: false
      - key: AUTH_CLIENT_SECRET
        sync: false
      - key: AUTH_SERVER_METADATA_URL
        sync: false
      - key: AUTH_COOKIE_SECRET
        sync: false
```

Remove `ADMIN_PASSWORD`.

- [ ] **Step 2: Extend `tests/test_deployment_config.py`**

```python
def test_render_generates_the_auth_secrets_before_starting():
    render = yaml.safe_load((ROOT / "render.yaml").read_text(encoding="utf-8"))
    web = render["services"][0]
    assert "render_streamlit_secrets" in web["startCommand"], (
        "st.login() reads secrets.toml, which Render has no way to supply as "
        "a file — it must be generated at startup")
    keys = {e["key"] for e in web["envVars"]}
    assert {"AUTH_CLIENT_ID", "AUTH_CLIENT_SECRET",
            "AUTH_SERVER_METADATA_URL", "AUTH_COOKIE_SECRET"} <= keys
    assert "ADMIN_PASSWORD" not in keys, "replaced by the is_admin flag"


def test_no_auth_secret_is_committed():
    render = yaml.safe_load((ROOT / "render.yaml").read_text(encoding="utf-8"))
    for entry in render["services"][0]["envVars"]:
        if entry["key"].startswith("AUTH_"):
            assert entry.get("sync") is False and "value" not in entry, entry
```

- [ ] **Step 3: Seed the first admin**

Before the first deploy, against Neon:

```bash
python -c "
import database
s = database.get_session()
database.grant(s, 'james@walsh.nu', added_by='bootstrap', is_admin=True)
print(database.is_admin(s, 'james@walsh.nu'))
s.close()"
```

Without this nobody can enter, including the person who would add themselves.

- [ ] **Step 4: Update the docs**

`CLAUDE.md`'s "Archive / Drafts / Admin password gate" section describes a
mechanism that no longer exists. Replace it with the two-layer model, note
that `_admin_unlocked()` now consults `allowed_emails.is_admin`, and record
that the three subscriber query-param flows are deliberately ungated. Mark
step 6 done in `nextsteps.md`.

- [ ] **Step 5: Full verification and commit**

```bash
LLM_MODE=mock INSIGHTS_MODE=mock python -m pytest tests/ -q --ignore=tests/postgres
TEST_POSTGRES_URL="$(cat .test_pg_url)" LLM_MODE=mock python -m pytest tests/postgres/ -q
git add render.yaml CLAUDE.md nextsteps.md tests/test_deployment_config.py
git commit -m "Cut over to invite-only access"
```

---

## Verification summary

| Claim | Evidence |
|---|---|
| Permission is ours, not Auth0's | Task 1 — revocation is immediate, no token wait |
| Revocation keeps an audit trail | Task 1 — `revoked_at` stamped, row retained |
| Case and whitespace cannot lock out a real user | Task 1 |
| `secrets.toml` survives quotes and backslashes | Task 2 — parsed back with `tomllib` |
| Partial config never half-enables login | Task 2 — one missing variable means "not configured" |
| A logged-in stranger is refused, not looped | Task 3 — `DENIED` distinct from `LOGIN` |
| Unsubscribe never requires a login | Task 3 Step 5 — gate placement asserted |
| Local dev needs no Auth0 | Task 3 — unconfigured always returns `OPEN` |
| The app cannot become unadministrable | Task 4 — last admin cannot self-revoke |
| Render generates secrets before Streamlit starts | Task 5 — asserted in the config tests |

## What this plan deliberately does not do

- **Add public signup.** Invite-only is the decision; opening it reopens the phase-2 static-site port at the same time, as one decision (portal spec §1).
- **Move the subscriber flows behind auth.** Digest recipients are not necessarily invited users, and unsubscribe must work for anyone holding the link.
- **Add Google or GitHub sign-in.** Auth0 configuration when wanted, not code.
- **Test `st.login()` itself.** It redirects a browser to Auth0. Everything that decides anything is in `gate_decision`, which is fully covered; the Streamlit layer is I/O only.
