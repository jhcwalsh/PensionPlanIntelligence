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


---

# DECISIONS — RFP Alerts Pipeline

This file documents non-obvious choices made while building the RFP
extraction pipeline layered on top of the existing PensionPlanIntelligence
Streamlit app. If the spec said one thing and the code does another, the
reason is here.

## Stack and infrastructure

### SQLite, not Postgres
The original task spec called for Postgres + Alembic. The user chose to
**layer onto the existing infrastructure**, which is SQLite (`db/pension.db`,
148 plans / 2,998 docs / 2,442 summaries). Substitutions:
- UUIDs become 32-char hex strings (`uuid.uuid4().hex`)
- JSONB becomes `Text` containing JSON strings, parsed at the API layer
- `numeric(3,2)` becomes `Float`
- `timestamptz` remains a timezone-aware `DateTime` stored by SQLAlchemy
- Postgres' `gen_random_uuid()` → `uuid.uuid4().hex` default at the ORM layer

### No Alembic
The existing app uses `Base.metadata.create_all(engine)` (idempotent), so
adding new tables is a one-liner that doesn't require introducing migration
infrastructure over an already-populated database. Re-running `init_db()`
creates the new tables and is safe on existing data. If this project ever
adds destructive schema changes, Alembic should be introduced then.

### No Docker / MinIO
PDFs live on the Render persistent disk via `Document.local_path`. The
diagnostic opens the local PDF when present; otherwise it falls back to
splitting the cached `extracted_text` on the `[Page N]` markers that
`extractor.py` inserts. This keeps the dev/test loop fast and avoids
introducing object-storage or container infra for one extra stage.

### FastAPI as a second Render service
The existing service runs Streamlit; a second `pension-rfp-api` web service
runs uvicorn against the same persistent disk and SQLite file. The cron
runs as a third (`type: cron`) service. All three share the disk so the
same `pension.db` is the system of record. SQLite handles concurrent
readers fine; the cron is the only writer.

## Schema and IDs

### Deterministic `rfp_id`
Computed as `sha256(plan_id || rfp_type || anchor_date || normalize(title))[:16]`
where `anchor_date` is the first non-null of (`release_date`,
`response_due_date`, `award_date`) and `normalize(title)` lowercases,
strips punctuation, and collapses whitespace. This means re-running the
pipeline against the same document **upserts** rather than duplicates.
Adding new dates or status transitions while keeping the original anchor
date stable preserves the id across the RFP's lifecycle.

### `incumbent_manager_id` is `null` in v1
Manager entity normalization is deferred. We store the raw firm name as
it appears in the document. The schema requires `incumbent_manager_id`
but constrains it to `null` so future reconciliation is a non-breaking
add (replace the constraint with a proper `string|null`).

### Schema additions beyond the spec
- `prompt_version` is on `RFPRecord` but not on the public schema body, on
  the principle that internal versioning is operational metadata.
- `awarded_manager` was added to capture post-award winners (the spec only
  named `incumbent_manager` and `shortlisted_managers`).

## Prompt versioning

Prompts live at `rfp/prompts/{version}.md`. The constant
`RFP_PROMPT_VERSION` in `database.py` controls which version a run uses.
Both `RFPRecord.prompt_version` and `DocumentHealth.prompt_version`
record the version. Bumping `RFP_PROMPT_VERSION` makes
`get_documents_pending_rfp_extraction` return everything again so the
whole corpus is re-processed under the new prompt; old records remain in
the table for regression analysis.

## LLM mode

### Cache key
`sha256(prompt || plan_id || document_id || chunk_text)[:16]`. plan_id and
document_id are included so identical chunk text from two unrelated
documents (boilerplate copies between board packets are common) doesn't
collide.

### Mock mode
`LLM_MODE=mock` reads from `fixtures/llm_responses/{key}.json`. A missing
file is treated as `{"rfps": []}` so adding a new fixture document
without writing a "no RFPs" file for every chunk is fine. The cache key
is computed identically in real and mock modes.

### Model and tool use
Real mode uses `claude-sonnet-4-6` with **tool use forced** via
`tool_choice={"type":"tool","name":"report_rfps"}`. The tool's
`input_schema` wraps `lib/rfp_schema.json` in `{rfps: [<schema>]}`. This
is the cleanest way to guarantee schema-valid JSON without parsing free
text. Confidence-driven re-runs against a stronger model are deferred.

## Idempotency and re-runs

A document is "pending" if it has no `DocumentHealth` row at the current
prompt version. `DocumentHealth` is the sole "this document was processed"
marker — a doc that produces zero records (e.g. a governance packet) is
still marked processed so the orchestrator doesn't re-extract on every
run. Re-extraction is triggered explicitly by bumping the prompt version.

`URL` uniqueness on `Document` and the deterministic `rfp_id` upsert
together cover the open-question idempotency rule from the spec
(same plan + same content = same record across re-runs).

## Year filter fallback

`/api/v1/rfps?year=YYYY` matches if any of (release_date,
response_due_date, award_date) falls in YYYY. For records whose three
dates are all null (common for in-flight Manager searches where dates
aren't announced yet), we fall back to `extracted_at.year`. Without this
fallback, dateless records would disappear from the default frontend
view.

## Backfill on new plans

When a new plan is added to `data/known_plans.json` and synced into the
DB, the existing fetcher will discover its documents and the existing
extractor will populate `extracted_text`. The next RFP-orchestrator run
picks them up automatically — `get_documents_pending_rfp_extraction`
just looks at extraction status and the absence of a `DocumentHealth`
row, with no concept of "new vs. backfill". This means new plans get
their full historical document set processed on first run.

## Observability

`structlog` JSON logs with one line per document and one summary line per
run, both correlated by `run_id`. Slack alerts via webhook on (a) a
failed run or (b) >20% of processed docs producing zero records — the
latter is a fast canary on "the prompt is broken".

## CI

- **test.yml** runs pytest in `LLM_MODE=mock` on every push/PR. No API
  key needed.
- **nightly_eval.yml** runs the orchestrator against fixture documents,
  scores against `fixtures/golden_set.jsonl`, fails the job on >2pp
  regression from `fixtures/eval_baseline.json`, and otherwise opens a
  PR refreshing the baseline via `peter-evans/create-pull-request`.

## Tests deferred

- `pyright --strict` and `ruff`/`black` are not enforced in CI yet (the
  existing codebase isn't strictly typed). New code uses
  `from __future__ import annotations` and full type hints, but the
  type-check gate is a follow-up.
- `actionlint` is not added; the workflows are short and reviewed
  manually.
- An OCR fallback for fully-scanned PDFs is described in the spec but
  the current corpus has very few such docs; deferred until we see real
  failures.
