# Low-maintenance app: design

**Date:** 2026-08-16
**Status:** approved. **Step 1 implemented 2026-08-16** — see the
corrections marked CORRECTED below, which were found while implementing it.
**Supersedes:** the hybrid GHA + local Windows Task Scheduler model described in CLAUDE.md

## Goal

Turn Pension Plan Intelligence into an app that runs itself, is useful to James
and to a small set of invited people, and can open to a wider audience later
without a rewrite. The binding constraint is **James's time**, not money.

Success looks like: nothing routine requires James's attention, no machine of
his is in the path of the app working, and adding a user is one row.

## Decisions taken

| Question | Decision |
|---|---|
| Audience | Invited now (real per-person identity), able to open later. No billing, no public signup. |
| First-class scope | Plan digital twins; raw document pipeline + archive. |
| Briefings | Monthly, quarterly, annual. **No weekly.** Daily digest kept. |
| RFP / manager-search subsystem | **Cut entirely.** |
| Work needing James's PC | Move to cloud, except the recordings catalogue (below). |
| Budget | ~$20–50/month for hosting and data. |
| Publishing | Auto-publish, with an FYI email to James after the fact. |
| Access | Email allowlist with magic-link sign-in. |
| Port or stay | **Stay on Streamlit now**; static-site port as a costed phase 2 with an explicit trigger. |

## Forcing function

`db/pension.db` is committed to git and is the deploy mechanism. It is 64 MB
against GitHub's hard 100 MB single-file limit, three writers commit it daily,
and every revision is a full ~60 MB blob in the pack — the last push emitted
`GH001: Large files detected`. CLAUDE.md says to plan a real fix at ~80 MB.

This has a deadline measured in months and would force architectural change on
its own. It anchors the plan.

---

## 1. Target architecture

```
GitHub Actions (scheduler)  ──▶  Neon Postgres  ◀──  Streamlit on Render
        │                       (managed, PITR)      (one service, always-on)
        └──▶ Cloudflare R2 (PDF store)
                                     ▲
                  local Windows ─────┘  (recordings catalogue only, best-effort)
```

Two managed services, one repo, one optional local side job.

**Postgres replaces DB-in-git.** Removes the size ceiling, the pack bloat, the
`git pull --rebase` contention between writers, and the coupling where "deploy"
means "commit a binary". `database.py` is already pure SQLAlchemy, so this is
largely a connection string plus dialect fixes.

**PDFs move to R2.** Today they exist only on the runner, which is why CLAUDE.md
requires structured extraction to run in the same job as the fetch. That
constraint caused two separate defects found during the 2026-08-15 review pass
(the monthly workflow discarding a whole refresh, and the local `.bat` missing
the actuarial step). Storing PDFs — single-digit GB, ~$0.015/GB — decouples
fetch from extract and makes re-extraction free rather than a re-download.

**GHA remains the only scheduler.** No Render cron, no Task Scheduler in the
critical path.

## 2. Data-layer discipline

Rule: **`app.py` contains no queries.** Every read goes through a named function
in the data layer returning plain dicts.

`database.get_twin_index` already demonstrates the pattern and the payoff. This
is what makes phase 2 a front-end-only job: the static site or an API consumes
the same functions.

Deliberately unchanged:
- The `GzippedText` `TypeDecorator` works on Postgres as `BYTEA`. Keep it.
- `init_db()` / `create_all` stays. Alembic is maintenance this size of project
  does not need; new tables still appear by adding a model class.

## 3. Scope changes

### Deleted

| Deleted | Reason |
|---|---|
| `rfp/`, `lib/`, `scripts/run_rfp_extraction.py`, `scripts/run_eval.py`, RFP tab | Subsystem cut |
| FastAPI service (`api/`) on Render | Existed mainly to serve RFP records |
| `weekly-rfp.yml`, `weekly-rfp-brief.yml`, `nightly_eval.yml` | Cut subsystem |
| `publish-approved.yml`, `approval_tokens`, magic-link approval, reminders job | Auto-publish replaces the gate |
| `scripts/run_daily.bat`, `run_monthly.bat`, `run_ips.bat`, `run_weekly.bat`, `run_quarterly.bat` | Fully cloud |
| `scripts/db_sync.py` and every DB-commit step in every workflow | Postgres removes the mechanism |

**CORRECTED — `rfp_records` is NOT inert.** `twin_builder.build_rfp_facets`
and `scripts/build_manager_roster._apply_rfp_entries` both read it, so dropping
the data would degrade the digital twins, which are first-class scope. Measured
before deleting anything: 189 records across 36 of 148 plans feed the
`rfp_state` facet, and 31 governance roster rows across 21 plans (23
consultant, 6 actuary, 2 custodian).

The resolution (agreed 2026-08-16): **cut the code, freeze the data.** The
table and its rows stay; nothing refreshes them, so those facets' freshness
dates stop advancing and the app shows them honestly as stale. The extraction
code remains recoverable from git history if fresh consultant data is ever
wanted.

**CORRECTED — `weekly-insights.yml` must NOT be deleted.** The cadences are a
cascade: `insights/monthly.py` composes from weekly publications and raises
outright when it finds none for its period, and quarterly and annual compose
from monthlies in turn. Deleting the weekly cron would have broken the monthly
briefing a month later, silently. What was actually wanted was no weekly
briefing *in the inbox*, so weekly now composes **silently**
(`notify=False, archive=False`) purely to feed monthly.

Note: the `!cancelled()` guards added on 2026-08-15 protect commit steps that
this design deletes. They remain correct until step 5 of the migration, and are
removed with the steps they guard.

### Kept, unchanged in spirit

Document pipeline, twins, archive, CAFR/IPS extraction, daily digest,
monthly/quarterly/annual briefings.

### Added

Postgres, R2 PDF store, OIDC auth, an `allowed_emails` table, per-run cost
logging.

## 4. Jobs and cadences

Ten scheduled workflows become **eight**: three are deleted (`weekly-rfp`,
`weekly-rfp-brief`, `nightly_eval`) and one is new — IPS, arriving from local
Task Scheduler. `weekly-insights` survives per the correction above, and
`daily-digest` survives because the daily digest was explicitly kept.

| Job | When | Does |
|---|---|---|
| `daily-pipeline` | 11:00 UTC | Fetch/extract/summarise 137 plans → Postgres, PDFs → R2; rebuild rosters and twins |
| `daily-digest` | 13:00 UTC | Auto-send digest of new documents; `daily_runs` anchors lookback |
| `weekly-insights` | Sundays 12:30 UTC | **Silent** — no email, no `notes/` file. Exists only so monthly has weeklies to synthesize |
| `monthly-cafr` | 1st, 15:00 UTC | CAFR refresh, investment + actuarial extraction, asset-class normalisation |
| `monthly-ips` | 1st, 16:00 UTC | IPS discovery + Haiku verification. **Moves from local Task Scheduler to GHA** (~$1–2/cycle). Slotted between the CAFR refresh and monthly-insights so a single monthly DB state feeds the briefing |
| `monthly-insights` | 1st, 18:00 UTC | Compose → auto-publish → FYI email |
| `quarterly-insights` | 1st Jan/Apr/Jul/Oct | as above |
| `annual-insights` | Jan 5 | as above |

Plus `test.yml` on push. `probe-pipeline.yml` and `test-email.yml` become
manual-dispatch dev tools.

### The one deliberate local exception

The **meeting-recordings catalogue** stays on local Windows Task Scheduler
(`scripts/run_recordings.bat --no-downloads`, weekly). It is a side dataset, not
part of the app's core promise.

With Postgres this gets *simpler* than today: the local job writes directly to
the shared database over the network, so it no longer needs `db_sync`, a git
push-back, or a conflict-avoidance time slot. It is explicitly **best-effort** —
if James's machine is off for a month, nothing else degrades and no cloud job
depends on its output.

## 5. Access and auth

**Mechanism:** Streamlit's native `st.login()` (OIDC) against Auth0 configured
for passwordless email.

This delivers the chosen magic-link *experience* without hand-rolling session
security. Streamlit has no cookie API, so a DIY magic link means either losing
the session on every page refresh or hand-rolling signed cookies — the one place
in this plan where DIY is a bad trade. Auth0 is free to 25,000 MAU.

**Authorisation:** a new `allowed_emails` table (`email`, `added_at`, `added_by`,
`revoked_at`). After OIDC returns a verified email, the app checks it against
that table. Adding a user is one row from the Admin tab; revoking is one update.

**Consequences:**
- Per-person identity, individual revocation, and usage visibility, none of
  which the current shared `ADMIN_PASSWORD` provides.
- Adding Google or GitHub sign-in later is Auth0 configuration, not code.
- `ADMIN_PASSWORD` is replaced by an `is_admin` flag on `allowed_emails`.

## 6. Cost

**Infrastructure (~$20–50/mo):** Neon Postgres (free tier initially, ~$19 at
scale), Render always-on Streamlit (~$7–25), R2 (a few dollars).

**Claude API** is the real recurring cost and nothing above reduces it. Four
controls:

1. **Haiku 4.5 for first-pass document summarisation**, escalating to Sonnet only
   for CAFRs and long or complex documents. The largest single saving.
2. **Hard per-run document caps**, so a backlog cannot spike a bill.
3. **Prompt caching** on system prompts — already done in the CAFR extractors,
   extend to the summariser.
4. **Per-run spend logged to a table** and surfaced in the monthly briefing, so
   cost is visible without checking a dashboard.

**GitHub Actions:** a private repo gets 2,000 free minutes/month, and a daily
Playwright pipeline can approach that. Making the repo public — the underlying
data is all public-source — makes Actions free and removes the ceiling.

## 7. Coverage: 148 → 137 plans

Dropping the WAF-blocked plans affects **14 plans** (the materials block list of
11 and the CAFR block list of 5 overlap only on `asrs` and `strs_ohio`),
totalling **$486B of $5,689B AUM — 8.5%**.

The headline overstates the real loss:

- **Four are already effectively uncovered:** MCERA and PGCERS have zero
  documents; FRS ($210B, the largest name on the list) and LASERS have one each.
  The local runner is not delivering these today.
- **Three lose only their annual CAFR**, not board materials: ACRS, PBPR, FWERF
  keep their document flow from GHA.

Genuine ongoing loss concentrates in **six plans** — ASRS, KPERS, NV PERS,
NM PERA, CORP AZ, STRS Ohio — roughly $245B and ~155 documents of history that
stops growing.

**Mitigation:** the app must show per-plan "last updated" honestly, so staleness
is visible rather than silent.

**Revisit path (explicitly agreed):** route the blocked fetches through a
residential/datacentre proxy from GHA, typically $10–75/month. This restores full
coverage with no PC dependency and is a configuration change to the fetcher, not
an architectural one. Revisit when the missing six plans matter to a user, or if
the app opens more widely.

## 8. Phase 2 trigger

Port the read surface to a static site (Astro or Next on Cloudflare Pages, with
Cloudflare Access) when **any** of:

- more than ~50 weekly active users;
- Streamlit p95 page load exceeds 3 seconds;
- genuinely public, unauthenticated access is wanted;
- Render cost exceeds the Postgres bill.

Because `app.py` holds no queries, this is a front-end-only port consuming the
same data-layer functions. Running cost afterwards drops toward $0–5/month.

## 9. Migration sequence

Ordered so the system is never in a broken state, and so the largest maintenance
win lands first with zero migration risk.

1. ~~**Delete, still on SQLite.**~~ **DONE 2026-08-16** (commits `e742197`,
   `4a5b561`, `4512e04`, `67c842b`). RFP subsystem and FastAPI service removed;
   every cadence auto-publishes; all local jobs except recordings retired.
   Suite green at 246 tests. Two corrections above were found doing it.
   Also fixed on the way: a second redundant daily digest, and a bug this step
   introduced where mock-mode cycles overwrote committed briefings in `notes/`.
2. ~~**Move remaining queries out of `app.py`** into the data layer.~~
   **DONE 2026-08-16.** All 41 call sites moved to `queries.py`; app.py holds
   zero. Verified by a characterization harness (0 of 14 data-function outputs
   differ vs HEAD) and guarded by two tests: app.py must contain no `.query(`,
   and importing `queries` must not pull in streamlit.
3. **Stand up Neon.** One-shot SQLite→Postgres migration preserving ids. Verify
   by comparing every plan's twin `_canonical_hash` before and after, plus row
   counts per table.
4. **Dual-run.** Staging Streamlit on Postgres beside prod on SQLite; compare
   pages.
5. **Cut workflows over.** Delete `db_sync` and every DB-commit step, and the
   `!cancelled()` guards that protect them. Freeze the final SQLite file in R2.
6. **Add auth last**, once everything else is stable.
7. **Point the local recordings job at Postgres**; drop its git push-back.

## 10. Risks

**Datetime handling is the sharp edge.** SQLite silently discards timezone
information — this is why the naive/aware `extracted_at` mixture found on
2026-08-15 was harmless in practice. **Postgres will not discard it.** That
latent bug becomes real on migration. Step 3 requires an audit of every datetime
write, not just the two extractors already fixed. Known remaining naive
`utcnow()` call sites include `twin_builder.py`, `insights/daily.py` and
`scripts/build_manager_roster.py`.

**CORRECTED 2026-08-19 — there was no mixture, and the scope was ~27× larger.**
The audit (`docs/superpowers/plans/2026-08-19-datetime-audit.md`) found **no
column holding both naive and aware values**. All 45 populated `DateTime`
columns were 100% naive, *including* the 17 whose default is the timezone-aware
`database._utcnow` — because SQLAlchemy's SQLite `DATETIME` format has no
timezone field and strips the offset on write.

Two consequences the paragraph above gets wrong:

- **SQLite ignores `DateTime(timezone=True)` entirely** — verified empirically. Every test in the suite runs on SQLite, so *no test could distinguish a correct fix from a broken one*. This promotes §11's Postgres CI container from "most valuable addition" to a hard prerequisite; it was built first for that reason.
- **"Three known call sites" was 81 sites across 39 files**, plus all 58 `DateTime` columns. Also found: four functions named `_utcnow` with two opposite meanings (`database`'s is aware; the three recordings modules' stripped the offset).

One risk turned out not to exist: no writer has ever used local time, so every
stored value was genuinely UTC and the backfill could stamp all 40,820 of them
wholesale.

**Other dialect risks:** `LENGTH()` over gzipped `BYTEA` measures compressed
bytes (already true on SQLite, but worth re-checking any aggregate query);
JSON-in-Text columns are unaffected; autoincrement id preservation must be
explicit in the migration script.

**CORRECTED 2026-08-19 — search is a fourth dialect risk, and the worst kind.**
Full-text search is SQLite **FTS5**, which has no Postgres equivalent, and this
spec never mentions it. Worse, it degraded *silently*: `_init_fts` and
`search_summaries` each wrapped their failure in a bare `except Exception`
written for "this SQLite build lacks FTS5", which would have swallowed "this is
not SQLite" identically — `init_db()` succeeding on Neon while ranked search
quietly became a substring scan. Fixed 2026-08-19 with explicit dialect
detection. The replacement index is specified in
`docs/superpowers/specs/2026-08-19-portal-readiness-design.md` §2.

**Single point of failure moves, not disappears.** Today it is James's PC;
afterwards it is Neon plus Render. Both are managed with backups, which is the
point, but the app should fail visibly rather than silently when the DB is
unreachable.

**Auth0 is a new third-party dependency.** Accepted deliberately, because the
alternative is hand-rolled session security.

## 11. Testing

- Keep the existing suite on SQLite for speed; `tests/conftest.py` continues to
  rebind `database.engine` / `SessionLocal` per test rather than reloading the
  module.
- **Add one CI job running the full suite against a throwaway Postgres service
  container**, to catch dialect drift. This is the single most valuable testing
  addition in this design.
- The migration script gets its own test against a fixture SQLite file.
- Deleting the RFP subsystem removes `tests/unit/test_relevance.py` and the eval
  fixtures; the remaining suite must stay green throughout step 1.
