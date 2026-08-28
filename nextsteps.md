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

### A1. July monthly backfill — **DONE 2026-08-28**

Publication 125, published and emailed (delivery `4f4c9217`). Weeklies `id=80`
and `id=88` were released from the removed approval gate first.

Q3's quarterly (due 2026-10-01) now gathers 1 monthly, up from 0. It reaches 2
once August composes — see D2.

Thin by necessity: the `07-05` week never composed because that cron failed,
and `07-26..08-01` spans the month boundary so the date filter excludes it
from both July and August.

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

### B1. Weekly Insights tab — **DONE** (PR #35)

Was frozen on 2026-05-24 for three months: it globbed
`notes/7day_highlights_*.md`, and `insights/weekly.py:206` sets
`archive=False` so nothing writes those any more. Now reads `publications`,
defaulting to `approved`/`published` to match what monthly gathers. Opens on
the newest week.

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

### D1. "Filter by Plan" dropdown — **DONE** (PR #35)

**Not dead — partially wired.** `main()` passes `plan_id` to Activity, Search,
Investment Actions and Meeting Recordings; the other five tabs ignore it while
the control stays visible, so it read as broken on more than half the app.

Captioned with the tabs it drives rather than removed — removing it would have
destroyed working behaviour on four tabs.

### D2. Monthly for August + Year-to-date for August

August monthly currently gathers **3 weeklies** (`08-02`, `08-09`, `08-16`); a
4th arrives when the `08-30` run auto-publishes `08-23..08-29`. So the
scheduled 2026-09-01 run will work unaided — this item is about producing it
now, plus the YTD view.

### D3. CAFR — **answered**, and it is not a CAFR problem

Coverage-by-reporting-year table shipped (PR #35).

The "7 pending" root cause, from a diagnostic run on 2026-08-28 that cost
**$0.00** — none of the seven reached Claude:

```
missing_file   5    the source PDF is gone from disk
no_section     1    NIC FY2024 — Investment Section not found
too_short      1    WV IMB FY2024 — 296 chars, likely scanned images
```

**Five of seven are one known problem.** The structured extractor reads the
local PDF, not the stored text, and only **45 of 140** CAFR PDFs still exist
on disk. Those five cannot be fixed by re-running anything — they need the
CAFR re-fetched (the URLs are still in the database) or a PDF store.

See E1: this is the PDF-retention gap surfacing, and it will keep producing
new "pending" rows as more PDFs age off disk.

The other two are genuine per-document problems and want individual attention.

### D4. Performance Reports tab — **DONE, annual** (PR #35)

83 plans, headline return by asset class, CSV download, backed by a data
structure so charts can be built on the same query.

**Open question.** These are **fiscal-year** returns, not the calendar
quarters requested — CAFRs are annual, and `cafr_performance` already held
2,690 rows covering exactly the classes asked for. True quarterly data lives
in the 48 `doc_type='performance'` documents, which have no structured
extraction at all: a new model plus a new Claude extractor, larger than the
tab itself, with recurring API cost.

Decide: keep the annual view, or build the quarterly extractor.
`performance_report_rows()` is documented as where a quarterly source merges in.

### D5. Download recordings behind the Admin login

Add a download option for meeting recordings, gated by `_admin_unlocked()`.
Note the weekly recordings job runs with `--no-downloads` today, so the media
may not exist locally — check what the job stores before designing this.

---

## E. Carried-forward loose ends

### E1. PDF retention — the root cause behind several symptoms

Source PDFs are never kept. Only 45 of 140 CAFR PDFs remain on disk. This is
not one bug but the common cause of at least three:

- 5 of the 7 CAFRs stuck at "pending extract" (D3)
- the 450 documents truncated at the old 150k cap, which cannot be
  re-extracted retroactively
- any future structured extraction that needs the original document

The fix is the R2 PDF store in portal spec §2.3. Worth treating as one piece
of work rather than repeatedly re-diagnosing its symptoms.

### Others

- **The Drafts tab is vestigial.** Nothing can enter `awaiting_approval` now
  that every cadence auto-publishes; it shows historical rows only.
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
