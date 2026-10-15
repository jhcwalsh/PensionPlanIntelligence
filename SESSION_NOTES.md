# Session notes

## 2026-10-15 — CAFR outline locator: flat, stitched and scrambled bookmarks

**Done**
- `extract_cafr_investments._locate_via_toc` (also used by the actuarial extractor): a bookmark naming a
  section ("Investment Section") now wins over looser matches; the earliest one not on a contents page is
  taken, and the range ends where the next other section starts. Page-less bookmarks (-1, PDFs stitched from
  separate files) take their children's first page (`_toc_page`). 3 new tests; the ipopif_il fixture now
  has a real contents page and a next section, and expects 56-73 as the real PDF gives.
- lacers 29-103 -> 84-99 (allocation 13 -> 7: the 6 dropped rows were actuarial-note duplicates; returns
  unchanged). pers_phoenix 18-82 -> 61-84 (allocation 15 -> 21, performance 26 -> 97). Backup in scratchpad.
  +42 asset-class mappings; both twins rebuilt. Actuarial extracts were already on the right pages.

**Why**
- pers_phoenix's flat outline put "Consultant's Report" at the section's own level and page, so the range
  was empty and text search took an org chart. lacers's section bookmarks had no page at all.

**Broken / unsure**
- Old vs new over all 137 latest CAFRs: 12 change, every one checked against its dividers and right.
  Not yet re-extracted: ipopif_il, nmerb, vrs (0 rows), nv_pers (0/2), kpers_ks (no returns) all sat on
  1-3 page ranges; bcers_md, hpops, or_pers, pera_colorado have rows but wide ranges. Actuarial extracts
  for the changed plans are already on the right pages.

**Next**
1. Re-extract those 9 investment sections (about $3).

## 2026-10-14 — ct_trs's "FY2026 CAFR" was its actuarial valuation

**Done**
- Neon document 3128 (`sr_actval25.pdf`) retagged from `cafr` FY2026 to `actuarial_valuation`, fiscal year
  cleared. Its summary and section read are kept. ct_trs now shows "Missing CAFR" on the coverage page.
- `data/known_plans.json`: dropped ct_trs's `cafr_url` (it pointed at the valuation) and noted why.
- `database.NON_MEETING_DOC_TYPES` replaces three copies of `["cafr", "performance"]` (meeting feed,
  Activity tab, notes count) and includes the new type; `queries.BOARD_MATERIAL_EXCLUDED_TYPES` too.
  New test in `tests/test_queries.py`. Performance view rebuilt.

**Why**
- CT TRB publishes no standalone annual report; TRS sits in the State of Connecticut ACFR. The static
  `cafr_url` re-claimed the valuation each month as "already have FY2026", so the gap was invisible.

**Broken / unsure**
- The valuation's FY2024 total return (11.49%) still feeds the performance view; it is a real figure.

**Next**
1. Tighten the lacers and pers_phoenix Investment Section ranges.

**Later the same day: registry edits now reach the database.** `database.upsert_plan` updated nothing on an
existing row, so Neon had drifted: 53 materials URLs, 12 websites and SamCERA's abbreviation were stale. It
now writes every registry key present in the entry (absent keys left alone); new `tests/test_upsert_plan.py`.
Neon synced once by hand (0 fields differ) and 13 twins rewritten. The daily pipeline keeps cloud plans in
step from now on; WAF-blocked plans only sync when the Mac mini job runs, which is still not loaded.

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
- ct_trs holds its 2025 actuarial valuation tagged as an FY2026 CAFR (fixed 2026-10-14).

**Next**
1. Make `upsert_plan` update registry fields, so registry fixes reach the DB.

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
