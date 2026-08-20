# Next steps

**As of 2026-08-19.** Working doc for the low-maintenance app migration.
Design: `docs/superpowers/specs/2026-08-16-low-maintenance-app-design.md`,
plus `docs/superpowers/specs/2026-08-19-portal-readiness-design.md` (portal).
Work so far is on branch `fix/cafr-actuarial-extraction`, **not yet pushed**.

## Where things stand

| Step | Status |
|---|---|
| 1. Delete (RFP, approval gate, local jobs) — still on SQLite | **Done** — `e742197`, `4a5b561`, `4512e04`, `67c842b` |
| 2. Move queries out of `app.py` into the read layer | **Done** — `785e4f3`, `d75cf22`, `9a04e6a` |
| 3. Stand up Neon; migrate SQLite → Postgres | Not started |
| 4. Dual-run staging on Postgres beside prod on SQLite | Not started |
| 5. Cut workflows over; delete `db_sync` and the DB-commit steps | Not started |
| 6. Add auth | Not started |
| 7. Point the local recordings job at Postgres | Not started |

Net so far: 10 scheduled workflows → 8, two Render services → one, five local
Task Scheduler jobs → one, no approval clicks, one daily email instead of two,
and `app.py` holds zero queries. Nothing routine needs attention and no machine
of James's is in the path.

**The forcing function is still live, and moved.** `db/pension.db` is now
**68 MB** against GitHub's hard 100 MB limit (up from 64 MB after merging 18
days of master on 2026-08-19), committed daily. Step 5 is where that clears.

---

## Before step 3 — one prerequisite left

### 1. The datetime audit — **DONE 2026-08-19**

Findings and plan: `docs/superpowers/plans/2026-08-19-datetime-audit.md`.

It overturned the premise. There was **no naive/aware mixture**: all 45
populated columns were 100% naive, because SQLite strips the offset on write —
and ignores `DateTime(timezone=True)` entirely, so the suite could never have
caught this. Scope was 81 call sites across 39 files and all 58 columns, not
"three known sites". No writer ever used local time, so the backfill could
stamp all 40,820 values as UTC wholesale.

**Decided: `TIMESTAMPTZ`.** Landed on the branch:

| Task | Status |
|---|---|
| 1. Ratchet test freezing the 39 offender files | Done — `9c67390` |
| 2. Single `database.utcnow()`; 3 shadowing `_utcnow` deleted | Done — `2144b73` |
| 3. Postgres CI job (service container) | Done — `a9f7b7c` |
| 4. All 58 columns `timezone=True` | Done — `fd621da` |
| 5. Convert the 81 call sites | **Not started** |
| 6. One-shot UTC backfill script | Done — `e27ef41` |
| 7. Correct the docs | Done |

Task 5 is the remaining one, and the plan says not to split it: ~20 cutoff
sites compare against DB reads, so a half-converted codebase raises TypeError.
It is also the commit most likely to break a running pipeline, so it should
land after CI has gone green on Task 4.

**Nothing here is verified yet.** SQLite cannot test any of it; the Postgres
job only runs on a push, which has not happened.

### 2. Two decisions needed from James

- **Auth0.** The spec (§5) overrides the "email allowlist + magic link" answer
  with Auth0 passwordless, because Streamlit has no cookie API and the
  alternative is hand-rolling signed-cookie session security. Same user
  experience, no DIY crypto, free to 25k MAU — but it is a new third-party
  identity dependency and needs an explicit yes.
- **Make the repo public?** A private repo caps GitHub Actions at 2,000
  minutes/month and the daily Playwright pipeline can approach that. The
  underlying data is all public record, so going public makes Actions free —
  but it publishes the code and commit history.

---

## The migration sequence

Ordered so the system is never in a broken state.

**Step 3 — Neon Postgres.** One-shot migration preserving ids. Verify by
comparing every plan's twin `_canonical_hash` before and after, plus per-table
row counts. This is the first step that touches *data* rather than code.
Watch: datetimes (above), autoincrement id preservation, and `GzippedText`
landing as `BYTEA`.

**Step 4 — Dual-run.** Staging Streamlit on Postgres beside prod on SQLite;
compare pages. Low risk, read-only. The characterization harness pattern used
in step 2 applies directly here.

**Step 5 — Cut over.** Delete `scripts/db_sync.py`, every DB-commit step, and
the `!cancelled()` guards that exist to protect those commit steps. Freeze the
final SQLite file in R2 as a backup. **This is where the 100 MB ceiling, the
pack bloat and the multi-writer contention all disappear.**

**Step 6 — Auth.** Last, so it lands on an otherwise stable app.
`allowed_emails` table; adding a user is one row, revoking is one update.

**Step 7 — Recordings job.** Point it at Postgres and drop its git push-back.
This gets *simpler* than today: no `db_sync`, no conflict-avoidance time slot.

---

## Independent of the migration

**Claude API cost controls (spec §6).** The real recurring cost, untouched by
any of the above and doable at any time:

1. Haiku 4.5 for first-pass document summarisation, escalating to Sonnet only
   for CAFRs and long documents — the single largest saving.
2. Hard per-run document caps so a backlog cannot spike a bill.
3. Extend prompt caching beyond the CAFR extractors to the summariser.
4. Log spend per run into a table, surfaced in the monthly briefing, so cost
   is visible without checking a dashboard.

---

## Loose ends

- **`db/pension.db` is uncommitted.** After the 2026-08-19 merge it holds
  master's 18 days of pipeline data plus the local delta re-applied losslessly
  (3 `cafr_actuarial` rows, 150 `ips_refresh_log` rows; the tables were
  disjoint). Verified: 36 of 38 tables byte-identical to remote. Still
  uncommitted by choice — needs a decision. `stash@{0}` holds the pre-merge
  state.
- **The branch is not pushed.** 31 commits ahead of `origin/master`, which
  moves ~2 commits/day. Nothing in the datetime or search work is verified
  until the Postgres CI job runs, and that needs a push.
- **Mock mode writing to committed data has happened twice** —
  `data/asset_class_mappings.json` and `notes/`. Both fixed with the same
  frozen-path guard. Worth treating as a repo-wide hazard and searching for a
  third instance rather than waiting for it to appear.
- **`twin_builder.build_rfp_facets` dedupes on raw manager names** — the same
  weakness fixed in the roster, deliberately left because fixing it would
  rename displayed relationships across every twin. Lower stakes since
  2026-08-19: the RFP-derived relationships are no longer displayed.
- **The 150k extraction cap truncates 444 documents (10.5%)** —
  `extractor.py:32`. Harmless for summarisation, which is what it was for, but
  it caps full-text search at roughly the first 35 pages of the largest board
  packets. Fixing it retroactively needs R2, since the PDFs are not kept. See
  the portal spec §2.3.
- **`or_pers` reads GASB-basis figures** from the Financial Section because its
  Actuarial Section is scanned images. Labelled in `notes`, but OCR is the real
  fix. Same applies to any other image-only CAFR.
- **The Drafts tab is vestigial.** Nothing can enter `awaiting_approval` now
  that every cadence auto-publishes; it shows historical rows only.
- **Coverage is 137 of 148 plans.** The 14 WAF-blocked plans (8.5% of AUM, but
  4 of them had no documents anyway) are skipped everywhere. Reopening path is
  a residential proxy at $10–75/month — see the spec's coverage section.
- **`scripts/normalize_managers.py`** shares the write-to-committed-config
  shape but has no mock branch, so it is not vulnerable today. Watch it if one
  is ever added.

---

## Recommendation

**Push the branch.** Everything landed since 2026-08-19 — 58 timezone-aware
columns, the search dialect fix, the backfill script — is unverified by
anything stronger than a SQLite suite that is structurally blind to all of it.
The Postgres CI job exists and has never run. One push converts a pile of
plausible work into evidence, and stops the branch drifting further from a
master that moves twice a day.

Then **datetime Task 5** (the 81 call sites), which the plan deliberately keeps
as one commit, and which is the change most likely to break a running pipeline
— so it wants a green CI behind it.
