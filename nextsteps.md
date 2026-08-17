# Next steps

**As of 2026-08-16.** Working doc for the low-maintenance app migration.
Design: `docs/superpowers/specs/2026-08-16-low-maintenance-app-design.md`.
Work so far is on branch `fix/cafr-actuarial-extraction`.

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

**The forcing function is still live.** `db/pension.db` is 64 MB against
GitHub's hard 100 MB limit, committed daily. Step 5 is where that clears.

---

## Before step 3 — two prerequisites

### 1. The datetime audit (do this first)

The sharpest migration hazard, and the only part of step 3 that can be done
and verified while still safely on SQLite.

SQLite silently discards timezone information — that is why the naive/aware
`extracted_at` mixture found on 2026-08-15 was harmless in practice. **Postgres
will not discard it.** The latent bug becomes real at migration.

Known naive `utcnow()` call sites: `twin_builder.py`, `insights/daily.py`,
`scripts/build_manager_roster.py`. The audit should sweep *every* datetime
write, not just those three — `database._utcnow` is the tz-aware helper to
standardise on.

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

- **`db/pension.db` is uncommitted.** Carries the 3 actuarial rows from the
  2026-08-16 live verification on top of a pre-existing delta. Deliberately
  untouched — needs a decision.
- **Mock mode writing to committed data has happened twice** —
  `data/asset_class_mappings.json` and `notes/`. Both fixed with the same
  frozen-path guard. Worth treating as a repo-wide hazard and searching for a
  third instance rather than waiting for it to appear.
- **`twin_builder.build_rfp_facets` dedupes on raw manager names** — the same
  weakness fixed in the roster, deliberately left because fixing it would
  rename displayed relationships across every twin.
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

Do **the datetime audit** next. It is small, it is a hard prerequisite for
step 3, and it is the only piece that can be completed and proven correct
while still on SQLite — everything after it carries migration risk.
