# CIO Insights Automation — Decisions

This file records non-obvious choices made while building the
publication automation layer (`insights/` package). For background,
see the build spec in the conversation that produced this PR.

## 1. Spec/codebase mismatches confirmed before build

The build spec assumed several capabilities that did not exist in the
repo. After surfacing the gaps, the founder confirmed the following:

| # | Spec assumption | Actual state | Decision |
|---|---|---|---|
| A | `publish_notes.py` emails subscribers | It does `git add notes/ && commit && push`; Render auto-deploys. No SMTP, no list. | Approval email goes **only to the founder**. "Publish" = the existing git-push-to-Render flow. Subscriber email is out of scope. |
| B | `summarizer.py` accepts a list of prior summaries | It is per-`Document` JSON extraction; narrative composition lives in `generate_notes.py` and reads from DB tables. | Cascade composition (monthly-from-4-weeklies, annual-from-12-monthlies) is **net-new** prompt + logic in `insights/compose.py`. This is the only place new editorial work was unavoidable; the founder is asked to review the cascade prompts before live mode. |
| C | PDF generation lives in `publish_notes.py` | It lives in `app.py:_markdown_to_pdf_bytes`. | Extract that function into `insights/render.py` and re-import from `app.py`. Pure refactor — no behavior change. |
| D | Migrate `_cafr_*.json` / `_extract_todo.txt` scratch files | Only `_cafr_overrides.json` exists, and it is intentional manual config (commit `6fb9dd9`), not run-state. | No migration. Patterns added to `.gitignore` defensively. `_cafr_overrides.json` left alone. |

## 2. Scheduler host — Render cron

`render.yaml` already deploys this repo to Render and Render supports
cron jobs as a service type. Adding cron jobs to the existing
`render.yaml` keeps deployment in one file. GitHub Actions would have
required mirroring secrets (Anthropic key, Resend key, DB) into a
second place.

The four scheduled jobs reuse the existing build/start environment via
the `pension-plan-intelligence` build, so they share the persistent
disk at `/data` where the SQLite DB lives.

## 3. Approval HTTP surface — Streamlit query-param routing

The spec offered "FastAPI service alongside Streamlit OR Streamlit's
own routing." Chose Streamlit-only:

- No second service to deploy or share state with.
- The DB session and existing `render_sidebar` / page model already
  handle `?doc=<id>` style deep-links (see `app.py:1030`).
- Approval endpoints are simply two more query params:
  `?approve=<token>` and `?reject=<token>`.

Trade-off: the response is a Streamlit page, not a plain HTTP
endpoint. Acceptable — clicking a link from email already opens a
browser; a Streamlit confirmation page is the same UX.

## 4. Email delivery — Resend

The repo had no email path. Picked [Resend](https://resend.com):

- Single environment variable (`RESEND_API_KEY`), single REST call,
  no SMTP plumbing, no AWS account setup.
- Founder-only recipient means we don't need the more advanced
  list-management features that justify SES.
- Live mode just sends; mock mode writes the rendered email to
  `tmp/sent_emails/<timestamp>_<token>.eml` so integration tests can
  assert on what would have been sent without API keys.

If the founder later wants to send to a subscriber list, Resend's
list APIs cover that — but that is a separate task.

## 5. Composition cascade

Three composition paths in `insights/compose.py`:

1. **Weekly** (`compose_weekly`) — calls
   `generate_notes.gather_highlights_data(session, days=7)` +
   `build_highlights_prompt` + `generate_note`. This **reuses** the
   existing per-document corpus pipeline so the weekly digest is
   exactly what `python generate_notes.py --highlights-only`
   produces today.
2. **Monthly** (`compose_monthly`) — accepts the four most recent
   approved weekly `Publication.draft_markdown` strings, builds a
   new prompt instructing Claude to synthesize across them, calls
   the same `generate_note` wrapper to keep prompt-cache and retry
   behavior consistent. **New prompt, ~60 lines.**
3. **Annual** (`compose_annual`) — same shape as monthly but takes
   12 monthlies and asks for a year-in-review.

The new prompts live in `insights/compose.py` and are flagged with
`# EDITORIAL: review with founder before live mode`. They follow the
same grounding rules as the existing CIO Insights prompt.

## 6. Idempotency

Every cycle starts with a "find or create the Publication row for
this `(cadence, period_start)` pair" step. The `UniqueConstraint` on
`publications` makes the second insert raise; the cycle catches that
and reuses the existing row. So `python -m insights.scheduler weekly
--period 2026-04-19` can be re-run safely.

When a Publication is in status `awaiting_approval` and the cycle is
re-run for the same period, the cycle leaves the row alone (does not
re-compose, does not re-send the email). To force a re-compose pass
`--force` (which moves the existing row to `expired` and creates a
new one).

## 7. Failure handling

Every scheduled run is wrapped in a try/except that:

1. Sets the `Publication.status = "failed"` and stores the traceback
   in `error_message`.
2. Calls `insights.notify.alert_failure()` which posts to
   `SLACK_WEBHOOK_URL`. Posts are best-effort — if Slack also fails,
   the error is logged but the failure handler does not re-raise.

For weekly runs that fail mid-way, the `WeeklyRunPlan` rows preserve
which plans completed; the next run continues from `pending`.

## 8. Token security

Tokens are 32-byte URL-safe random strings. Only the SHA-256 hash is
stored in `approval_tokens.token_hash`. The raw token only appears in
the email body and the URL clicked — never in logs.

Approve and reject get separate tokens (one row per action). Both are
single-use (`consumed_at` set atomically with the publication status
change).

## 9. What was deliberately NOT changed

Verified via `git diff` after each commit — none of these files were
modified:

- `summarizer.py`
- `generate_notes.py`
- `publish_notes.py`
- `pipeline.py` / `fetcher.py` / `extractor.py`
- All CAFR-related modules

`app.py` is the one exception: `_markdown_to_pdf_bytes` was lifted
out into `insights/render.py` (decision C above), and two new tabs
(`Drafts`, `Insights`) plus the approval query-param handler were
added. Existing tabs are unchanged.

## 10. Things to revisit later

- The cascade prompts in `insights/compose.py` are the only piece of
  net-new editorial work in this PR. The founder agreed to a review
  pass before flipping `INSIGHTS_MODE=live` for the first monthly.
- Subscriber email distribution. Not in scope for this PR.
- Postgres migration. Stay on SQLite as the spec demanded; the
  approval flow is single-writer so SQLite's serialization is fine.
- A "send me a one-off ad-hoc Insight" command. The current scheduler
  supports `--period <date>` for backfill but always tries to fit the
  cadence boundaries; an off-cadence Insight would need a fourth
  cycle type.
