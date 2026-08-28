# Next steps

**As of 2026-08-28.** Working doc. Supersedes the 2026-08-19 migration
edition (in git history at `e66d56d`) — that migration is complete.

Specs: `docs/superpowers/specs/2026-08-16-low-maintenance-app-design.md`,
`docs/superpowers/specs/2026-08-19-portal-readiness-design.md`.

---

## Done (2026-08-21 → 2026-08-28)

| | |
|---|---|
| Neon Postgres migration | 33 tables, verified, `DATABASE_URL` everywhere |
| Product renamed to PensionGraph | domain live, TLS, branding |
| Neon quota outage | root-caused, fixed, upgraded to Launch |
| `extracted_text` deferred (#31) | was 5.2 MB/call for a coverage count |
| `APP_BASE_URL` → pensiongraph.com (#32) | 5 hardcoded sites, one only the test found |
| Email defaults (#33) | `pensionintel.com` was a domain nobody owns |
| Subscriber rollback (#10) | merged after 3.5 months |
| Idle-in-transaction fault (#34) | **verified against the symptom**, not just the mechanism |
| Resend on `mail.pensiongraph.com` | real delivery proven end to end |
| Back-catalogue links | 98 publications + 1 note rewritten |
| API spend instrumented | ~$10–15/month; Batch API thread closed as not worth it |

---

## A. Needs a decision from James

### A1. July monthly backfill — **has a deadline**

Must exist before the **2026-10-01** quarterly run, or Q3 composes from
August alone. Two things to approve together:

- **Prerequisite:** flip weeklies `id=80` (07-12, `expired`, 7,472 chars) and
  `id=88` (07-19, `awaiting_approval`, 9,217 chars) to `published`. Both hold
  real content; both are stranded by the removed approval gate. Reversible.
- **Side effect:** `insights.scheduler monthly` has no `--no-email` flag, so
  it composes (~$0.10–0.30), writes `notes/`, **emails the briefing**, and
  publishes.

July will be thin regardless: the `07-05` week never composed (that cron
failed), and `07-26..08-01` spans the month boundary so the date filter
excludes it from **both** July and August.

### A2. Auth0 — plan written, not started

`docs/superpowers/plans/2026-08-22-auth0-invite-only.md`. Needs a tenant and
four `AUTH_*` values. Free to 25k MAU, but a new identity dependency.

### A3. Make the repo public?

Private caps Actions at 2,000 min/month and the Playwright pipeline
approaches it. Data is all public record. Going public makes Actions free but
publishes code and history.

### A4. WAF proxy for the 14 blocked plans?

8.5% of tracked AUM (4 of the 14 had no documents anyway). Residential proxy
is $10–75/month.

---

## B. Ready to build — diagnosed, not started

### B1. Weekly Insights tab is frozen on May

It globs `notes/7day_highlights_*.md`. Nothing writes those any more —
`insights/weekly.py:206` sets `archive=False` because weekly composes silently
to feed monthly. Newest file is `2026-05-24`; meanwhile **17 weekly
publications sit in the database** with full `draft_markdown`.

Fix: read `publications` where `cadence='weekly'`, newest first. Open
question: show all 17, or only `approved`/`published`? Monthly's gather counts
only the latter, so matching it keeps the tab honest about what actually feeds
downstream — but hides four composed weeks.

Not to be confused with `notes/weekly_consultant_rfps_*.md` — a different,
dead product from the RFP subsystem removed on 2026-08-16.

### B2. `www.pensiongraph.com` still points at the retired host

`CNAME www -> pensionplanintelligence.onrender.com`. Works today; it is the
same dependency PR #32 removed from the code.

---

## C. Verification outstanding

### C1. Neon transfer meter — nobody has looked

Three days of post-fix data. The egress fix eliminated the largest *measured*
consumer, but Neon's usage dashboard was never available while diagnosing, so
a second contributor cannot be ruled out. **Only James can see this.**

---

## D. New requests (James, 2026-08-28)

### D1. "Filter by Plan" dropdown

**Not dead — partially wired.** `main()` passes `plan_id` to 4 of 9 tabs:
Activity, Search, Investment Actions, Meeting Recordings. The other five
(Insights, Managers, CAFR, Asset Allocation, Plans, Subscribe) ignore it while
the control stays visible in the sidebar, so it reads as broken.

Decide: hide it on tabs that ignore it, or wire the remaining tabs up. Removing
it outright would lose working behaviour on four tabs.

### D2. Monthly for August + Year-to-date for August

August monthly currently gathers **3 weeklies** (`08-02`, `08-09`, `08-16`); a
4th arrives when the `08-30` run auto-publishes `08-23..08-29`. So the
scheduled 2026-09-01 run will work unaided — this item is about producing it
now, plus the YTD view.

### D3. CAFR — pending extractions and a coverage table

- **Investigate the "7 pending".** All 140 CAFR *documents* are
  `extraction_status='done'`, so the page is counting something else: 13 CAFR
  documents have no `CafrExtract` row (127 of 140 do). Reconcile 7 vs 13 —
  the page's definition differs from both.
- **Add to the CAFR page:** count of CAFRs by latest fiscal year (2025, 2024,
  2023 …) with **change since the prior month**.

### D4. Performance Reports tab

New tab, table shaped like the CAFR one:

| Plan | Latest performance quarter | Total plan | Private equity | Private credit | Real assets | Real estate | Source link | Source date |

Must be backed by a **data table**, not assembled ad hoc, so charts and other
views can be built on it later. Needs a schema decision first — likely a new
model in `database.py` plus an extractor, following the `CafrExtract` pattern.

### D5. Download recordings behind the Admin login

Add a download option for meeting recordings, gated by `_admin_unlocked()`.
Note the weekly recordings job runs with `--no-downloads` today, so the media
may not exist locally — check what the job stores before designing this.

---

## E. Carried-forward loose ends

- **The Drafts tab is vestigial.** Nothing can enter `awaiting_approval` now
  that every cadence auto-publishes; it shows historical rows only.
- **450 documents truncated at the old 150k cap.** `MAX_STORED_CHARS` is now
  2,000,000 and Postgres removes the size constraint, but re-extraction needs
  the source PDFs, which are never kept. See portal spec §2.3 on the R2 store.
- **`or_pers` reads GASB-basis figures** because its Actuarial Section is
  scanned images. Labelled in `notes`; OCR is the real fix. Applies to any
  image-only CAFR.
- **Coverage is 137 of 148 plans** — see A4.
- **Mock mode writing to committed data has happened twice**
  (`data/asset_class_mappings.json`, `notes/`). Both fixed with a frozen-path
  guard. Worth searching for a third instance rather than waiting for it.
- **`twin_builder.build_rfp_facets` dedupes on raw manager names** — the
  weakness fixed in the roster, left alone because fixing it renames displayed
  relationships across every twin. Lower stakes now the RFP facets are frozen.
- **`scripts/normalize_managers.py`** shares the write-to-committed-config
  shape but has no mock branch, so it is not vulnerable today.

---

## Suggested order

1. **A1 July backfill** — the only item with a real deadline.
2. **C1 transfer meter** — one dashboard glance, closes the last unverified claim.
3. **D2 August monthly + YTD** — small, and the inputs are already there.
4. **D1 Filter by Plan** — small, visible, decide-then-do.
5. **D3 CAFR** — investigation then a UI addition.
6. **B1 Weekly tab** — restores three months of missing briefings.
7. **D4 Performance Reports** — the largest; needs a schema decision first.
8. **D5 recordings download**, **B2 www**, then **A2–A4** at leisure.
