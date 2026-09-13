"""Fail the job loudly when the Anthropic balance is empty.

    python -m scripts.preflight_credit [--job NAME]

Why. The API key is prepaid, and when the balance runs out nothing else
here says so: the summariser writes only dedup rows, extractors log a 400
per document and continue, the daily pipeline goes green. It happened on
2026-09-01, again from 09-05 to 09-09, and again on 09-11 and 09-12; each
time the first sign was someone noticing thin output days later.

What. One Haiku call with max_tokens=1, before any real work. Two answers
are fatal and exit 1, turning the workflow red, with an email to the
approval recipient so the lapse is seen the same day:

  * a 400 naming the credit balance  -> top up
  * a 401                            -> the key is wrong or revoked

Anything else the probe hits (a timeout, a 529, a 500) is printed as a
warning and exits 0: an API blip is not a reason to skip a day's fetch,
and the job's own error handling covers a persistent outage.

The email is sent with the briefings' mailer, so it needs RESEND_API_KEY
and the APPROVAL_EMAIL_* variables in the job's env; if sending fails the
step still exits 1, because the red job is the primary signal and the
email is the second.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone

import anthropic

PROBE_MODEL = "claude-haiku-4-5-20251001"


def _send(subject: str, text: str) -> None:
    from insights.approval import ApprovalEmail, send_email
    send_email(ApprovalEmail(subject=subject, html=f"<pre>{text}</pre>", text=text,
                             pdf_attachment=None, pdf_filename=None))


def _fail(reason: str, job: str, detail: str) -> int:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    text = (f"{reason}\n\nJob: {job}\nWhen: {now}\n\n{detail}\n\n"
            "The job has been stopped before it did any work. Nothing is "
            "lost: the next scheduled run picks up where this one would have. "
            "Top up at https://console.anthropic.com/settings/billing, then "
            "either wait for the next run or dispatch the workflow by hand.")
    print(f"::error::{reason} ({job})")
    print(text)
    try:
        _send(f"[PensionGraph] {reason}", text)
        print("Alert email sent.")
    except Exception as exc:  # noqa: BLE001 - the red job is the primary signal
        print(f"Alert email failed: {exc}")
    return 1


def check(client=None, job: str | None = None) -> int:
    """Return the exit code: 0 to proceed, 1 to stop the job."""
    job = job or os.environ.get("GITHUB_WORKFLOW") or "local"
    if client is None:
        from dotenv import load_dotenv
        load_dotenv()  # a local run reads the key from .env, as database.py does
        try:
            client = anthropic.Anthropic()
        except TypeError as exc:  # the SDK's "could not resolve authentication"
            return _fail("Anthropic API key missing", job, str(exc))
    try:
        client.messages.create(model=PROBE_MODEL, max_tokens=1,
                               messages=[{"role": "user", "content": "hi"}])
    except anthropic.AuthenticationError as exc:
        return _fail("Anthropic API key rejected", job, str(exc))
    except TypeError as exc:
        # The SDK resolves credentials lazily, at the first request: with no
        # key anywhere this is where "Could not resolve authentication
        # method" surfaces.
        if "authentication" in str(exc).lower():
            return _fail("Anthropic API key missing", job, str(exc))
        print(f"warning: preflight probe failed ({exc}); continuing")
        return 0
    except anthropic.BadRequestError as exc:
        if "credit balance" in str(exc).lower():
            return _fail("Anthropic credit balance exhausted", job, str(exc))
        print(f"warning: preflight probe got an unexpected 400, continuing: {exc}")
        return 0
    except Exception as exc:  # noqa: BLE001 - transient; the job's own handling covers a real outage
        print(f"warning: preflight probe failed ({type(exc).__name__}: {exc}); continuing")
        return 0
    print(f"Anthropic credit OK ({job})")
    return 0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--job", help="name to put in the alert; defaults to $GITHUB_WORKFLOW")
    args = ap.parse_args()
    sys.exit(check(job=args.job))


if __name__ == "__main__":
    main()
