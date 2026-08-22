# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

Two layered systems sharing one SQLite database (`db/pension.db`, ~64 MB, tracked in git):

1. **Meeting-document pipeline** (`pipeline.py`, `fetcher.py`, `extractor.py`, `summarizer.py`) — fetches board materials and CAFRs from ~148 U.S. public pension plans, extracts text, summarizes with Claude per-document. Cloud-only: GHA cron handles 137 of 148 plans daily. The 14 WAF-blocked plans in `data/waf_blocked_plans.json` / `data/waf_blocked_cafr_plans.json` are skipped everywhere (no runner can reach them) — 8.5% of tracked AUM, though 4 of the 14 had no documents anyway.
2. **Insights automation** (`insights/` package) — composes monthly / quarterly / annual editorial briefings from the existing summaries, plus a daily digest. All auto-publish and email a copy; nothing waits on approval. The weekly cadence still runs but is **silent** (no email, no notes file) because monthly composes from weekly publications — see the cadence-cascade note below.

The Streamlit app (`app.py`) reads from the same DB and surfaces both layers as tabs.

A third layer, the RFP alerts pipeline (`rfp/`, `lib/`, `api/`), was removed on
2026-08-16 together with the FastAPI service that served it. The `rfp_records`
table and its 189 rows are deliberately retained, frozen: `twin_builder`
and `scripts/build_manager_roster` still read them for the `rfp_state` facet and
for consultant/custodian/actuary relationships. Nothing refreshes them, so the
twins' freshness dates on those facets stop advancing. See
`docs/superpowers/specs/2026-08-16-low-maintenance-app-design.md`.

## Common commands

```bash
# Tests — both layers share the same conftest. Mock both LLM modes.
LLM_MODE=mock pytest tests/ -q
LLM_MODE=mock pytest tests/test_weekly_e2e_mock.py -q          # one insights file
LLM_MODE=mock pytest tests/ -k token                            # by name pattern

# DB schema management — no Alembic; init_db() is idempotent
python -c "import database; database.init_db()"

# Which backend am I on? (DATABASE_URL decides; .env is loaded by database.py)
python -c "import database; print(database.engine.dialect.name)"

# Compare every read in queries.py across two backends (read-only both sides)
python scripts/compare_backends.py db/pension.db "$DATABASE_URL"

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

# Daily Pension Digest (runs from GitHub Actions, not local Task Scheduler)
INSIGHTS_MODE=mock python -m insights.scheduler daily          # dry-run; writes to tmp/sent_emails/
INSIGHTS_MODE=live python -m insights.scheduler daily          # real send via Resend
python -m insights.scheduler daily --force                     # re-send today's digest

# Streamlit app
streamlit run app.py
```

## Architecture you have to internalize before editing

### The database is Neon Postgres, reached by `DATABASE_URL`

Cut over on 2026-08-21. `database.py` resolves the backend at import:
`DATABASE_URL` when set, otherwise the historical SQLite file at `DB_PATH`.
Every writer — the GHA crons, Render's Streamlit, the one local job — talks to
the same Neon database over the network. There is no deploy step for data; a
row is visible everywhere the moment it is committed.

Three consequences worth holding on to:

- **`db/pension.db` is no longer in git.** It is gitignored, still on disk
  locally, and preserved in history: `git show e0d6f45:db/pension.db > db/pension.db`
  recovers the exact file the migration was taken from. The 100 MB single-file
  ceiling, the pack bloat, and the multi-writer commit contention all went with
  it — as did `scripts/db_sync.py`, every DB-commit workflow step, and the
  `boto3`/`moto` dependencies that existed only for the abandoned R2 route.
- **`database.py` loads `.env` itself**, before resolving the URL, with
  `override=False`. Every entry point calls `load_dotenv()` *after* importing
  `database`, so a `DATABASE_URL` living only in `.env` used to be invisible —
  `import app` came up on SQLite with the variable sitting right there and
  nothing raised. Real environment variables still win, which is what GitHub
  Actions and Render supply.
- **An unset or empty `DATABASE_URL` silently means SQLite.** On a runner or a
  diskless Render service that is an empty file: the job reads nothing, writes
  nothing, and exits zero. Every DB-touching workflow declares the secret at
  *job* level so a step added later cannot miss it, and
  `tests/test_deployment_config.py` asserts that. If a deployment looks like
  total data loss, check this variable first.

Workflows still commit `notes/`, `cafr_summaries/` and
`data/asset_class_mappings.json` — those are files, not data, and the
`!cancelled()` guards on those steps survive for them. The guards' original
rationale (irreversible work living only on the runner until the commit
recorded it) is gone: rows are durable when written. What they still buy is
that one plan's data quirk, which makes an extractor exit 1, does not leave the
derived-data builds a day stale.

Local Task Scheduler owns exactly one job: the weekly meeting-recordings
catalogue (`scripts/run_recordings.bat`). It no longer commits or pushes
anything, which is what removed its conflict-avoidance time slot.

**Re-extraction is no longer size-gated.** `MAX_STORED_CHARS` is 2,000,000 and
450 documents were truncated at the old 150k cap. Re-extracting them would have
added ~20.5 MB to a 68 MB file 11 MB from GitHub's hard limit; against Postgres
that constraint does not exist. What still gates it is the source PDFs, which
are never kept — see the portal spec §2.3 on the R2 PDF store.

### `documents.extracted_text` is gzipped on disk
The full extracted PDF text is the bulk of the database by 10× over everything else. `Document.extracted_text` uses a `GzippedText` `TypeDecorator` (`database.py`): callers see plain `str` both ways, but stored values are gzipped UTF-8 bytes (`impl=LargeBinary`, which is `BYTEA` on Postgres). It was introduced to stay under GitHub's file-size limit; that reason is gone, but it still cuts storage and transfer roughly tenfold over the network, so it stays. Legacy uncompressed `str` rows are returned as-is, so the model change was safe to land before the data migration. Implications:
- Don't run raw SQL like `SELECT extracted_text FROM documents` — you'll get gzip bytes. Always go through the SQLAlchemy ORM, or `gzip.decompress(row[0])` yourself.
- Aggregate queries like `LENGTH(extracted_text)` measure compressed bytes, not text length.
- `scripts/migrate_compress_extracted_text.py` is the one-shot migration; idempotent on the gzip magic header. Re-running it is safe.

### Layered packages, one DB, idempotent schema
`database.py` defines all 15 tables in one module. There is no migration framework. `init_db()` calls `Base.metadata.create_all(engine)` — adding a new model class and re-running `init_db()` on an existing DB just creates the missing tables. **Never write SQL ALTER TABLE migrations**; just add the SQLAlchemy class and call `init_db()`. Existing-row backfill is a one-off script.

### Two independent mock flags
`INSIGHTS_MODE=mock` (insights package) and `LLM_MODE=mock` (document/CAFR extraction) are unrelated. Tests' `conftest.py` sets both as autouse fixtures; production sets neither. When debugging an unexpected real-API call, check both env vars.

### Test DB isolation does NOT reload the database module
`tests/conftest.py` rebinds `database.engine` and `database.SessionLocal` per-test using `monkeypatch.setattr`. Reloading the module would orphan the ORM classes and break SQLAlchemy's mapper registry. If you write a new test that needs DB isolation, follow this pattern — use the existing `_isolated_environment` (insights-style) or `tmp_db` fixture rather than instantiating your own engine.

### IPS pipeline is content-hash versioned (not FY-keyed like CAFRs)
`refresh_ips.py` runs locally only (Windows Task Scheduler, monthly). Unlike CAFRs which are FY-tagged, IPS is versioned by content hash: `IpsDocument` has `UNIQUE(plan_id, content_hash)`, so a plan accumulates a row each time the board publishes a new IPS, while same-content re-fetches dedupe silently. Discovery is fully automated — no manual URL curation in `known_plans.json` required: `fetch_ips.discover_ips_urls()` mines existing extracted documents for embedded IPS URLs, then site-crawls seed paths under `plan.website`. Each candidate is gated by a Haiku 4.5 verification call (`verify_is_ips()`) so adjacent policy docs (proxy voting, securities lending) don't pollute the table. `IPS_MODE=mock` short-circuits the LLM call for tests; production hits Anthropic at ~$1-2/cycle total.

### Cadences auto-publish, and they cascade
Since 2026-08-16 every cadence uses `cycle_common.finalize_and_send`:
compose → render PDF → (optionally write `notes/`) → email → `published`.
There is no approval gate, no magic link, no `publish-approved` workflow and
no reminders job. `finalize_for_approval`, `insights/reminders.py`,
`scripts/publish_pending.py` and `approval.issue_tokens`/`consume_token` are
all gone. The `approval_tokens` table remains in the schema, unused, as an
audit trail of past approvals.

Two flags on `finalize_and_send` carry the differences:
- `notify=False` — compose silently. **Only weekly uses this.** Weekly
  briefings go to nobody, but the weekly `Publication` row is what
  `monthly._gather_approved_weeklies` reads, and monthly raises outright
  when it finds none. So the weekly cron must keep running.
- `archive=True` — write the canonical `notes/` file that the app serves.
  Monthly/quarterly/annual set it; daily and weekly don't.

**The cascade is the thing to remember**: daily is standalone, weekly feeds
monthly, and monthly feeds both quarterly and annual. Deleting a cadence
breaks the one above it a period later, not immediately.

### Archive / Drafts / Admin password gate
The Archive, Drafts, and Admin tabs are hidden from the tab strip entirely until the user enters the password set in the `ADMIN_PASSWORD` env var. The login form is a sidebar expander rendered by `_render_admin_login_sidebar()`; the predicate `_admin_unlocked()` drives whether the three gated tabs are appended to `main()`'s `tab_specs` list. Single shared password, session-state-sticky for the browser tab. Leave the env var unset for local dev (fail-open — tabs always present, no login UI). Set on Render to keep internal tooling, pre-editorial drafts, and the back-catalogue archive off the public site.

### Idempotency keys for cycles
- `Publication` is unique on `(cadence, period_start)`. `find_or_create_publication()` returns the existing row or creates a new one with `status="generating"`.
- `finalize_and_send()` raises if status isn't `"generating"`, so a re-run won't resend.
- To force a re-send, expire the existing publication first (or use `--force` on the scheduler CLI). Setting it back to `"generating"` directly works but bypasses the audit trail.
- The same idempotency pattern applies to `WeeklyRun` (unique on `period_start`).

### Daily Pension Digest
The `daily` cadence auto-sends every day. Lookback state lives in the
`daily_runs` table (anchored on `MAX(sent_at)`); the GitHub Actions cron at
`.github/workflows/daily-digest.yml` runs at 13:00 UTC daily and commits
`db/pension.db` back after each successful send.

Triggers (volume / keyword / reappearing-plan) used to route a busy day
through an approval email. Since 2026-08-16 they only *annotate* the digest
and are recorded in `daily_runs.triggers`; `daily_runs.approval_gated` is
now always False and kept solely so historical rows stay readable. Three env
vars still tune the rules: `DAILY_APPROVAL_DOC_THRESHOLD` (default 10),
`DAILY_APPROVAL_KEYWORDS` (default `"RFP,manager,search,investment policy"`),
`DAILY_REAPPEAR_DAYS` (default 30).

Note there used to be a *second* daily digest — `scripts/send_daily_digest.py`,
invoked from `daily-pipeline.yml` — which sent a separate email with regex
RFP alerts. It was removed with the RFP subsystem; `daily-digest.yml` is the
only digest now.

### Where each cadence runs
Render hosts one web service: Streamlit (`pension-plan-intelligence`), reading Neon over the network. The persistent disk is gone — it existed only to hold the committed SQLite file. All cron-style work runs off Render — mostly GHA, one local Windows Task Scheduler job:

| Cadence | Trigger | Where | Workflow / .bat |
|---|---|---|---|
| Daily document pipeline (137 plans) | cron 11:00 UTC | GHA | `.github/workflows/daily-pipeline.yml` |
| Weekly Insights composition (silent — feeds monthly) | cron Sundays 12:30 UTC | GHA | `.github/workflows/weekly-insights.yml` |
| Monthly CAFR refresh + structured extraction (~92 plans) | cron 1st of month 15:00 UTC | GHA | `.github/workflows/monthly-cafr-refresh.yml` |
| Monthly IPS refresh (auto-discover + verify via Haiku 4.5) | cron 1st of month 16:00 UTC | GHA | `.github/workflows/monthly-ips.yml` |
| Weekly meeting-recordings catalogue (discover sources → poll via yt-dlp → email digest; no video downloads) | Task Scheduler Sat 08:00 local | local Windows | `scripts/run_recordings.bat --no-downloads` |
| Monthly insights composition + auto-publish | cron 1st of month 18:00 UTC | GHA | `.github/workflows/monthly-insights.yml` |
| Quarterly insights composition + auto-publish | cron 1st of Jan/Apr/Jul/Oct 19:00 UTC | GHA | `.github/workflows/quarterly-insights.yml` |
| Annual insights composition + auto-publish | cron Jan 5 19:00 UTC | GHA | `.github/workflows/annual-insights.yml` |

GHA secrets that must exist for the cron entries to work: `DATABASE_URL`, `ANTHROPIC_API_KEY`, `RESEND_API_KEY`, `APPROVAL_EMAIL_RECIPIENT`, `APPROVAL_EMAIL_FROM`. Local runs read the same names from `.env`. Schedules are UTC; ET drifts one hour between EDT and EST. The 1st-of-month sequence is still deliberate — CAFR refresh @ 15:00 UTC, IPS @ 16:00, monthly-insights @ 18:00 — but now only so monthly composes from fresh CAFRs, not because one job's commit has to reach another. They share a database.

### `!cancelled()` on the derived-data and file-commit steps

The workflows guard their later steps with `if: ${{ !cancelled() && ... }}`
rather than letting them default to `success()`. The original reason is gone:
irreversible work used to live only on the runner until a commit recorded it,
so skipping the commit discarded a whole day. Rows are durable when written
now.

What the guard still buys is worth keeping. The extractors exit `1` when *any
single item* fails — one Claude error out of ~138 documents is enough — and
without the guard that one quirk would skip the manager-roster and twin-snapshot
rebuilds, leaving the derived data a day stale, and skip the `notes/` /
`cafr_summaries/` commits, losing a briefing's published file even though the
`Publication` row says it went out. `!cancelled()` rather than
`continue-on-error` so the job still goes red and the failure stays visible;
only the discarding is removed.

Deliberate exception: `daily-pipeline.yml`'s digest email stays on `success()`,
to avoid emailing a digest for a failed run.

## Conventions worth knowing

- **Don't run `git add .`** — dozens of untracked scratch files at the repo root (`_cafr_*.json`, `*.log`, `data/known_plans.json.bak*`, screenshots, an empty stray `pension.db` at the repo root) are intentionally left out. Stage by name or path.
- **CAFR overrides** live in `_cafr_overrides.json` (committed) — manual `{plan_id: pdf_url}` map for plans where URL templates fail. Treat as config, not run-state.
- **Plan registry** is `data/known_plans.json` (committed). Optional fields: `cafr_url_template` (with `{year}`), `cafr_landing`, `cafr_url`, `playwright_wait_selector`, `sub_page_pattern`. The DB `plans` table doesn't store these CAFR fields; `refresh_cafrs.py` reads them from JSON at runtime.
- **`db/pension.db` is a local leftover, not the database.** Since the 2026-08-21 cutover it is gitignored and nothing reads it unless `DATABASE_URL` is unset. The stray empty `pension.db` at the repo root is a separate, older accident — ignore both. The live data is in Neon.
- **Notes vs. publications**: `notes/` directory holds approved markdown briefings (committed, served by Streamlit); `tmp/sent_emails/` holds mock-mode email artifacts (gitignored).

## CI

`.github/workflows/test.yml` runs `pytest tests/ -q` on every push/PR with `LLM_MODE=mock`.
