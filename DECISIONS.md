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
