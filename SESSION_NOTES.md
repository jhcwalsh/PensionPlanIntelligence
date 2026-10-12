# Session notes

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
