# Move `db/pension.db` out of git — Cloudflare R2 as source of truth

**Date:** 2026-07-08
**Status:** Approved (design review with founder, 2026-07-07/08)

## Problem

`db/pension.db` (53 MB and growing) is committed to git; pushing to master is
the data-deploy mechanism. Three pressures force a change:

1. **GitHub's 100 MB hard limit** — every push already warns at 50 MB.
2. **Write collisions** — GHA daily-pipeline, GHA daily-digest, GHA Sunday
   workflows, and four local Task Scheduler jobs all commit the DB. Two
   binary-conflict incidents in the two days before this design.
3. **Ephemeral Streamlit writes** — subscriber sign-ups and approval-link
   clicks write to whichever DB file the Render service reads; if that is the
   deployed repo copy, those writes are lost on redeploy.

Constraint that shapes the design: **a Render persistent disk attaches to
exactly one service.** The disk cannot be the shared source of truth for
FastAPI, GHA runners, and the local machine. Something must be the sync bus.

Decision (founder, 2026-07-07): keep SQLite; use object storage as the bus.
Postgres was considered and rejected for now — FTS5 search, the `GzippedText`
decorator, test fixtures, and raw-SQL scripts are all SQLite-shaped, and
nothing today demands a hosted DB. Cloudflare R2 chosen over S3/B2 (free
tier, zero egress) and over FastAPI-hosted sync endpoints (bespoke code,
single point of failure, weak backup story).

Note: R2 does **not** support S3-style object versioning. Backup history
comes from explicit dated snapshots with a lifecycle expiry rule.

## Architecture

One R2 bucket (`pension-db`):

```
current/pension.db        # the live database
current/manifest.json     # {generation, sha256, size, uploaded_by, uploaded_at}
snapshots/YYYY-MM-DD.db   # daily snapshot, expired after 30 days (lifecycle rule)
```

New module `db_sync.py` (boto3 against the R2 S3-compatible endpoint):

- `pull(dest)` — download `current/pension.db` to `dest` iff the remote
  manifest generation differs from the locally recorded one. Records the
  generation alongside the file (e.g. `dest + ".manifest"`).
- `push(src, uploaded_by)` — upload DB, then conditional-PUT the manifest
  (If-Match on the manifest ETag captured at pull time). On mismatch:
  **fail loudly** — never clobber. Caller re-pulls and re-runs.
- `snapshot()` — server-side copy of `current/pension.db` to today's
  `snapshots/` key. Invoked by the GHA daily-pipeline after its push.

Credentials via env vars `R2_ENDPOINT`, `R2_ACCESS_KEY_ID`,
`R2_SECRET_ACCESS_KEY`, `R2_BUCKET` — set in GHA repo secrets, local `.env`,
and both Render service dashboards.

## Writer flow (GHA workflows + local .bat tasks)

`pull → run job → push`. Git continues to carry code, `notes/`, and
`cafr_summaries/` — workflows and `.bat`s keep their git steps for those but
stop committing the DB. On push conflict (another writer won the race):
re-pull and re-run; every job is already idempotent by repo contract. R2
unreachable → abort via the existing `scripts/notify_failure.py` path
without touching the DB.

Writers: daily-pipeline (GHA), daily-digest (GHA), weekly-rfp (GHA),
weekly-insights (GHA), weekly-rfp-brief (GHA), monthly-cafr-refresh (GHA),
monthly/quarterly/annual-insights (GHA), run_daily.bat, run_monthly.bat,
run_ips.bat, run_recordings.bat (local). (`nightly_eval` only commits
`fixtures/eval_baseline.json` — not a DB writer.)

## Render services

- **Streamlit** (`pension-plan-intelligence`): pull to `/data/pension.db` on
  startup; a TTL-cached freshness check (5 min) compares remote manifest
  generation and on change re-pulls and disposes the SQLAlchemy engine. Its
  own writes (subscribers, approval consumption, admin actions) push
  immediately after commit, marked `uploaded_by=streamlit`.
- **FastAPI** (`pension-rfp-api`): stays diskless; pull to a local path on
  startup, refresh on the same 5-minute manifest check (async task). 53 MB
  re-pull on cold start is acceptable.

## Migration sequence (each step independently reversible)

1. Create bucket + API token; configure lifecycle rule on `snapshots/`;
   seed `current/` from the current DB; verify pull/push round-trip locally.
2. Land `db_sync.py` + tests (moto-mocked S3). The conditional-PUT conflict
   path gets an explicit test. CLI: `python -m scripts.db_sync pull|push|snapshot`.
3. Switch GHA workflows and `.bat`s to pull/push. **Dual-write week**: they
   also still commit the DB to git as fallback.
4. Flip Render services to pull-on-start. Remove whatever dashboard hook
   currently populates `/data` (dashboard access required — do together with
   founder; the mechanism is not in the repo and must be discovered there).
5. After one clean week: `git rm --cached db/pension.db`, add to
   `.gitignore` (no history rewrite), remove dual-write steps, update
   CLAUDE.md + render.yaml comments.

Rollback at any step = revert the flag/step that switched that writer. The
frozen git copy of the DB remains an emergency restore indefinitely.

## Testing

- Unit: `db_sync` pull/push/snapshot against moto; conflict path (stale
  ETag → exception, no overwrite); pull no-op when generation matches.
- Integration (manual, step 1): round-trip against the real bucket.
- Cutover verification: after step 4, confirm a local `.bat` run's data is
  visible on the live site within 5 minutes without any git push.

## Success criteria

- No DB blob in git pushes; repo growth flatlines.
- Any writer's push visible on the live site within ~5 minutes.
- Silent clobber impossible (conditional PUT), collisions become retries.
- Streamlit-side writes survive redeploys.
- 30 days of daily snapshots restorable from R2.
