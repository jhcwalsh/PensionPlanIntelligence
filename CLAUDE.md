# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

Three layered systems sharing one SQLite database (`db/pension.db`, ~42 MB, tracked in git):

1. **Meeting-document pipeline** (`pipeline.py`, `fetcher.py`, `extractor.py`, `summarizer.py`) — fetches board materials and CAFRs from ~148 U.S. public pension plans, extracts text, summarizes with Claude per-document. Hybrid: GHA cron handles 137 of 148 plans daily; local Windows Task Scheduler handles the 11 WAF-blocked plans (see `data/local_only_plans.json`).
2. **CIO Insights automation** (`insights/` package) — composes weekly / monthly / annual editorial briefings from the existing summaries, gated on a magic-link approval email to the founder. GHA cron-triggered (weekly Sundays 11:00 UTC, monthly 1st of month 18:00 UTC).
3. **RFP alerts pipeline** (`rfp/`, `lib/`, `api/`, `scripts/`) — structured extraction of RFP records from already-fetched documents into `rfp_records` / `document_health` / `pipeline_runs`, served via FastAPI. RFP backfill runs locally via Windows Task Scheduler weekly; FastAPI lives on Render.

The Streamlit app (`app.py`) reads from the same DB and surfaces all three layers as tabs.

## Common commands

```bash
# Tests — all three layers' tests use the same conftest. Mock both LLM modes.
LLM_MODE=mock pytest tests/ -q
LLM_MODE=mock pytest tests/test_weekly_e2e_mock.py -q          # one insights file
LLM_MODE=mock pytest tests/unit/test_relevance.py -q           # one RFP file
LLM_MODE=mock pytest tests/ -k token                            # by name pattern

# DB schema management — no Alembic; init_db() is idempotent
python -c "import database; database.init_db()"

# Pipeline (local; uses Playwright)
python pipeline.py                              # all plans, full fetch+extract+summarize
python pipeline.py calpers --extract-only       # one plan, skip fetch
python pipeline.py --status                     # read-only summary

# CAFR refresh + investment extraction
python refresh_cafrs.py
python extract_cafr_investments.py

# Insights cycles (manual / backfill)
INSIGHTS_MODE=mock python -m insights.scheduler weekly --skip-scrape         # writes to tmp/sent_emails/
INSIGHTS_MODE=live python -m insights.scheduler weekly --skip-scrape         # real send via Resend
python -m insights.scheduler weekly --period 2026-04-19 --skip-scrape --force # force re-compose

# RFP pipeline (against fixtures in mock mode; against pension.db in live)
LLM_MODE=mock python -m scripts.run_rfp_extraction
LLM_MODE=mock python -m scripts.run_eval

# Streamlit and FastAPI services
streamlit run app.py
uvicorn api.main:app --reload --port 8000
```

## Architecture you have to internalize before editing

### The DB IS the deploy mechanism for data
`db/pension.db` is committed. Pushing to `master` is how new data lands on Render. The Streamlit web service and FastAPI service on Render mount the persistent disk at `/data` but read from the deployed `db/pension.db` until something writes back. Three things now write back to master:
1. **GHA daily-pipeline** (~11:00 UTC) — fetches/extracts/summarizes 137 plans, commits the DB.
2. **Local Windows Task Scheduler daily** — same pipeline scoped to the 11 WAF-blocked plans via `--local-only`, commits the DB.
3. **GHA weekly-insights** (Sundays 11:00 UTC) and **GHA monthly-insights** (1st of month 18:00 UTC) — compose digests, send approval email, commit the DB + `notes/` (+ `cafr_summaries/` for monthly).

Local Task Scheduler still owns the WAF-blocked subset of monthly CAFR refresh (5 plans via `--local-only`) — a `git push`-back operation on the same master branch. Each writer runs at a distinct time; conflicts haven't been observed but a `git pull --rebase` would be the next defensive step if they appear.

GitHub's hard 100 MB single-file limit is the ceiling on this model. The DB started bumping into it once `documents.extracted_text` accumulated, which forced the gzip wrapper (next section). When the DB approaches ~80 MB again, plan a real fix (Git LFS, or moving the DB out of git onto Render's persistent disk via a separate sync) rather than another column-level workaround.

### `documents.extracted_text` is gzipped on disk
The full extracted PDF text is the bulk of the DB by 10× over everything else. To stay under GitHub's size limit, `Document.extracted_text` uses a `GzippedText` `TypeDecorator` (`database.py`): callers see plain `str` both ways, but on disk values are gzipped UTF-8 bytes (`impl=LargeBinary`). Legacy uncompressed `str` rows are returned as-is, so the model change was safe to land before the data migration. Implications:
- Don't run raw SQL like `SELECT extracted_text FROM documents` — you'll get gzip bytes. Always go through the SQLAlchemy ORM, or `gzip.decompress(row[0])` yourself.
- Aggregate queries like `LENGTH(extracted_text)` measure compressed bytes, not text length.
- `scripts/migrate_compress_extracted_text.py` is the one-shot migration; idempotent on the gzip magic header. Re-running it is safe.

### Three layered packages, one DB, idempotent schema
`database.py` defines all 15 tables for all three subsystems in one module. There is no migration framework. `init_db()` calls `Base.metadata.create_all(engine)` — adding a new model class and re-running `init_db()` on an existing DB just creates the missing tables. **Never write SQL ALTER TABLE migrations**; just add the SQLAlchemy class and call `init_db()`. Existing-row backfill is a one-off script.

### Two independent mock flags
`INSIGHTS_MODE=mock` (insights package) and `LLM_MODE=mock` (RFP pipeline) are unrelated. Tests' `conftest.py` sets both as autouse fixtures; production sets neither. When debugging an unexpected real-API call, check both env vars.

### Test DB isolation does NOT reload the database module
`tests/conftest.py` rebinds `database.engine` and `database.SessionLocal` per-test using `monkeypatch.setattr`. Reloading the module would orphan the ORM classes and break SQLAlchemy's mapper registry. If you write a new test that needs DB isolation, follow this pattern — use the existing `_isolated_environment` (insights-style) or `tmp_db` (RFP-style) fixture rather than instantiating your own engine.

### IPS pipeline is content-hash versioned (not FY-keyed like CAFRs)
`refresh_ips.py` runs locally only (Windows Task Scheduler, monthly). Unlike CAFRs which are FY-tagged, IPS is versioned by content hash: `IpsDocument` has `UNIQUE(plan_id, content_hash)`, so a plan accumulates a row each time the board publishes a new IPS, while same-content re-fetches dedupe silently. Discovery is fully automated — no manual URL curation in `known_plans.json` required: `fetch_ips.discover_ips_urls()` mines existing extracted documents for embedded IPS URLs, then site-crawls seed paths under `plan.website`. Each candidate is gated by a Haiku 4.5 verification call (`verify_is_ips()`) so adjacent policy docs (proxy voting, securities lending) don't pollute the table. `IPS_MODE=mock` short-circuits the LLM call for tests; production hits Anthropic at ~$1-2/cycle total.

### Approval flow is Streamlit-query-param-based
Magic-link emails contain `?approve=<token>` and `?reject=<token>`. The Streamlit app's `main()` checks `st.query_params` before rendering tabs and dispatches to `page_document_detail`, `page_cafr_plan_detail`, or the approval consumer. Tokens are SHA-256-hashed in `approval_tokens`; raw values exist only in the email body. To add a new deep-link route, follow the same pattern in `app.py`'s `main()`.

### Idempotency keys for cycles
- `Publication` is unique on `(cadence, period_start)`. `find_or_create_publication()` returns the existing row or creates a new one with `status="generating"`.
- `finalize_for_approval()` raises if status isn't `"generating"`. So once a publication is `awaiting_approval`, the cycle won't resend its email.
- To force a re-send, expire the existing publication first (or use `--force` on the scheduler CLI). Setting it back to `"generating"` directly works but bypasses the audit trail.
- The same idempotency pattern applies to `WeeklyRun` (unique on `period_start`) and the RFP `rfp_id` (deterministic from `sha256(plan_id + doc_id + chunk_id + record_index)`).

### Where each cadence runs
Render hosts only two web services now: Streamlit (`pension-plan-intelligence`) and FastAPI (`pension-rfp-api`), both mounting the persistent disk `pension-db` at `/data`. All cron-style work moved off Render — partly to GHA, partly to local Windows Task Scheduler:

| Cadence | Trigger | Where | Workflow / .bat |
|---|---|---|---|
| Daily document pipeline (137 plans) | cron 11:00 UTC | GHA | `.github/workflows/daily-pipeline.yml` |
| Daily document pipeline (11 WAF-blocked plans) | Task Scheduler | local Windows | `scripts/run_daily.bat` |
| Weekly CIO Insights composition + email | cron Sundays 11:00 UTC | GHA | `.github/workflows/weekly-insights.yml` |
| Weekly RFP backfill (`--limit 100`) | cron Sundays 11:30 UTC | GHA | `.github/workflows/weekly-rfp.yml` |
| Monthly CAFR refresh (~92 plans) | cron 1st of month 15:00 UTC | GHA | `.github/workflows/monthly-cafr-refresh.yml` |
| Monthly CAFR refresh (5 WAF-blocked plans) | Task Scheduler | local Windows | `scripts/run_monthly.bat` |
| Monthly IPS refresh (all 148 plans, auto-discover + verify via Haiku 4.5) | Task Scheduler | local Windows | `scripts/run_ips.bat` |
| Monthly CAFR extraction + insights composition + email | cron 1st of month 18:00 UTC | GHA | `.github/workflows/monthly-insights.yml` |
| Quarterly insights composition + email | cron 1st of Jan/Apr/Jul/Oct 19:00 UTC | GHA | `.github/workflows/quarterly-insights.yml` |

GHA secrets that must exist for the cron entries to work: `ANTHROPIC_API_KEY`, `RESEND_API_KEY`, `APPROVAL_EMAIL_RECIPIENT`, `APPROVAL_EMAIL_FROM`. Local cron uses the same names from `.env`. Schedules are UTC; ET drifts one hour between EDT and EST. The 1st-of-month sequence is deliberate: GHA CAFR refresh @ 15:00 UTC → local CAFR refresh runs early ET → GHA monthly-insights @ 18:00 UTC pulls a DB that already has both runs' new CAFRs.

## Conventions worth knowing

- **Don't run `git add .`** — dozens of untracked scratch files at the repo root (`_cafr_*.json`, `*.log`, `data/known_plans.json.bak*`, screenshots, an empty stray `pension.db` at the repo root) are intentionally left out. Stage by name or path.
- **CAFR overrides** live in `_cafr_overrides.json` (committed) — manual `{plan_id: pdf_url}` map for plans where URL templates fail. Treat as config, not run-state.
- **Plan registry** is `data/known_plans.json` (committed). Optional fields: `cafr_url_template` (with `{year}`), `cafr_landing`, `cafr_url`, `playwright_wait_selector`, `sub_page_pattern`. The DB `plans` table doesn't store these CAFR fields; `refresh_cafrs.py` reads them from JSON at runtime.
- **Two distinct DB files** look similar: `db/pension.db` (the real ~42 MB DB, tracked) and an empty `pension.db` at the repo root (stray, untracked, ignore). `DB_PATH` env var defaults to the former.
- **Notes vs. publications**: `notes/` directory holds approved markdown briefings (committed, served by Streamlit); `tmp/sent_emails/` holds mock-mode email artifacts (gitignored).

## CI

`.github/workflows/test.yml` runs `pytest tests/ -q` on every push/PR with `LLM_MODE=mock`. `.github/workflows/nightly_eval.yml` runs `scripts/run_eval.py` daily and opens a PR if `fixtures/eval_baseline.json` drifted (auto-merged or reviewed manually).
