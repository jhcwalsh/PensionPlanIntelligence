# Session notes

## 2026-10-12 — Catch-up after the month-long cron outage

**Done**
- Neon `plans.fiscal_year_end` set to match the registry for the 24 corrected plans; twins rebuilt.
- After the first green daily pipeline since 13 Sep (GHA, 21:35 UTC), ran the monthly-cafr-refresh steps
  locally against Neon: 76 FY2025 CAFRs saved, 71 investment extracts, asset-class mappings (+329), twins.
- Q3 2026 quarterly composed from the July and August monthlies, published and emailed.

**Why**
- A weekly reads "downloaded in the last 7 days" from when it runs, whatever period it is labelled with, so
  missed weeklies cannot be backfilled: five would all hold today's catch-up. James chose to skip the
  September monthly; September's meetings reach readers via the 18 Oct weekly and the October monthly.

**Broken / unsure**
- Fixed 2026-10-13: DPERS/ACERA crash. The model sometimes sends an array as a JSON string (on DPERS the
  whole result inside `asset_allocation`); `_normalize_payload` decodes it. The crash also leaked a
  row-less extract into the next commit, blocking retries; both extractors now roll back. Both re-extracted.
- Fixed 2026-10-13: the text-search locator took the contents page as the Investment Section. It now
  skips pages naming 2+ sections and prefers "Investment Section" over a bare "Investments" label.
  13 empty extracts re-extracted, all with rows now; 22 others with rows get tighter ranges on a --force run.
  Still wide: lacers (29-103, no real header) and pers_phoenix (18-82, an org chart names the section).
- Asset-class mappings refreshed for the re-extracted rows (+74), twins rebuilt for the 15 plans.
- Re-extracted the 21 CAFRs whose old range was cut short: performance rows up sharply (lacera 2 to 92,
  ri_ers 4 to 74). epcp_tx came back empty (its exhibits are images) and was restored from a backup.
  +52 mappings; twins rebuilt.
- ct_trs holds its 2025 actuarial valuation tagged as an FY2026 CAFR.

**Next**
1. Make `upsert_plan` update registry fields, so registry fixes reach the DB.
2. ct_trs: its FY2026 "CAFR" is an actuarial valuation; retag or delete.

## 2026-10-11 — Every cron but weekly-watch stopped on 13 Sep; CAFR refresh saved nothing

**Done**
- 7c4b000 (pushed): `ANTHROPIC_API_KEY` at job level in seven workflows. The credit preflight ran
  before any step that set the key, failed "key missing" every run since 13 Sep, and skipped the work.
- 6666794 (pushed): `refresh_cafrs.refresh_plan` falls back to the prior FY after a failed download.
- b828324 (pushed): curated FY2025 URLs, below.
- `cafr_url_by_year` in `data/known_plans.json`, checked first by `fetch_cafr.resolve_cafr_url_for_year`;
  74 plans given a curated FY2025 URL. A curated URL is saved as its curated year even when the cover parser disagrees.

**Why**
- 1 Sep refresh saved 0 of 148. Templates bake in upload-date folders, and URLs carry `/2026/03/` folders or
  "FY25" that `year_from_url` misreads; the cover parser reads "2025 and 2024" covers as 2024 and image covers
  as None. Curating the URL per year sidesteps all three. Dry run against scratch SQLite: 74/74 saved as FY2025.

**Broken / unsure**
- `fiscal_year_end` fixed in `known_plans.json` for 24 plans (20 to 12-31, ers_texas/trs_texas 08-31,
  dcrb/jaxpf_fl 09-30), each from the report cover. Re-ran the refresh dry run as of 1 Nov: still FY2025.
  **Neon `plans.fiscal_year_end` still says 06-30** for all 24: `upsert_plan` only inserts, so the JSON
  never reaches it. The app's FYE column and twin as-of dates read the DB copy. Unsure: nic_ne, ocers_fl.
- No FY2025 found: gmebs_ga, ri_ers (Cloudflare), pgcers_md, trs_texas (Imperva), sdrs_sd, wperp (not out),
  ers_hi (not out), cccera (Issuu only), rcera_ca and ocers_fl (registry entries look wrong), atlanta_ers, ucrp.
- `_cafr_overrides.json` is not read by the monthly refresh at all.

**Next**
1. Dispatch monthly-cafr-refresh, then the missed weekly → monthly → quarterly runs.
2. Update the 24 `fiscal_year_end` values in Neon (needs James's OK), then rebuild twins.
3. Each year, add the new FY's URLs to `cafr_url_by_year` (or make landing scraping year-aware).
