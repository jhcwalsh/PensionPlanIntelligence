# Session notes

## 2026-10-17 — CAFR actuarial gaps filled; perf-row losses diagnosed; zero-coverage probe

**Done**
- Actuarial extraction for the 91 latest-per-FY CAFRs with no `cafr_actuarial` row: 68 sent in one batch,
  all saved ($3.19). 20 had no locatable section and 3 had no PDF.
  - Latest CAFR per plan with a funded ratio: 50 → 111 of 138. A sample of values looks plausible, with no outliers.
- Backups and one-off scripts from 16–17 Oct moved to `backups/2026-10-16-fulltext-resummary/`.
  `backups/` is now in `.gitignore`.
- Today's daily pipeline was green (13:59 UTC), so the roster has picked up the merged manager names.
  Credit preflight is OK for Sunday's weekly.

**Broken / unsure**
- The re-summary's perf-row drops are real, not duplicates. On 272 docs, rows fell 3,672 → 2,435.
  - The full-text summaries keep headline rows and drop manager-level and secondary-period rows
    (e.g. pmpf_pa manager returns, mcera FYTD/YTD).
  - Proposed fix, no spend: old rows plus new rows for asset classes the old data lacked (+958),
    about 4,630 rows. Not applied yet.
- Zero-coverage probe (top 10 by AUM, read-only):
  - sf_sfers is registry-only: point it at the full-board page.
  - psprs_az needs `scan_embedded_urls` plus `MIN_DATE` applied before the 50-link cap.
  - sbcera (Legistar) and vcera (PrimeGov) need fetcher patterns.
  - nycppf/fdny are blocked by Akamai's headless check, even from a residential IP.
  - mn_pera, wv_imb and surs_il publish nothing fetchable for 2026.

**Next**
1. Apply the perf-row merge (James's OK).
2. sf_sfers registry fix; then the psprs_az cap fix.
3. Check Sunday's weekly published (first since 30 Aug).

## 2026-10-16 — Board-material catch-up, and full-text re-summaries of 892 long packs

**Done** (Neon only; no repo code changed)
- Freshness review: the daily pipeline is healthy, and 310 docs arrived in the catch-up after the outage.
- Ran `pipeline.py --extract-only --retry-failed`, then re-downloaded failed docs that had no copy anywhere
  into R2 and re-extracted them. 83 recovered; $2.49.
- 141 still fail: 113 dead links, 13 not PDFs, 6 scanned board packs (OCR is off for that type),
  and the rest WAF or timeouts.
- Checked that the Sunday 18 Oct weekly will run now that the key fix (7c4b000) is in place and credit
  is live. It covers 11–17 Oct.
- Re-summarised 892 of 904 long board packs, agendas and minutes on Sonnet 5 in batch mode. Each now
  reads up to 400k chars instead of 50k. $87.11.
  - Investment actions 2,192 → 3,359 (+53%), decisions 4,571 → 5,558 (+22%), performance rows 6,506 → 7,343 (+13%).
  - 13 `dedup:` copies were refreshed to match.
- Performance reports: nothing to spend on. All 23 unextracted ones are excluded on purpose (DC menus,
  mistagged docs).

**Why**
- API credit was due to expire, and the 50k excerpt cut most packs before the investment sections.
- James chose this and the perf-report check, not the CAFR actuarial gaps or manager normalisation.

**Broken / unsure**
- 272 docs now have fewer perf rows than before; not checked. Probably duplicate tables in the old excerpts.
- 2 docs hit JSON truncation and fell back to a 20k excerpt, which may be worse than the old summary.
- 12 kept their old summaries (filename skips or errored results).
- The old summaries are backed up only in the session scratchpad (`summaries_backup_before_fulltext.json`),
  next to the one-off `resummarize_full.py`. Neither is committed.

- Later: ran `scripts.normalize_managers` ($0.69, 23 calls): mapped the 1,325 new manager names
  (1,061 managers, 264 rejected as funds, placeholders and similar) in `data/manager_mappings.json`.
  Distinct canonicals went from 581 to 1,239; the 1,156 old entries are unchanged.
  - Each batch is canonicalised on its own, so near-duplicates such as "State Street" vs "State Street
    Global Advisors" survive, and a few combined strings ("Baillie Gifford; State Street...") are kept whole.
  - The nightly roster build and the app read the committed file, so nothing changes in production until it is pushed.
- Later: cross-batch cleanup. A single Sonnet call over all 1,239 canonicals proposed 122 merges.
  - Vetoed 6 wrong ones (Meadow, Lightstone, NB Eagle, Clearwater Advisors, and two combined strings).
  - Pointed Stonepeak and Stellex at the firm rather than a fund, and flipped Callan Associates to "Callan".
    `test_build_manager_roster` reads the live file and expects "Callan".
  - Canonicals went from 1,239 to 1,123 (166 raw entries rewritten). The one-off script, merge list and
    pre-merge copy are in the session scratchpad.

**Next**
1. Spot-check 3–4 of the docs that lost perf rows; check that Sunday's weekly published.
2. Push so the roster picks up the new manager mappings.
3. Probe the top zero-coverage plans (SFERS, NYCPPF, MN PERA, SURS, WV IMB, SBCERA, FDNY, PSPRS).

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

- Later: those 9 re-extracted on their latest CAFR only ($0.71 with the mappings). ipopif_il 0 -> 22/8,
  nmerb 0 -> 19/94, vrs 0 -> 10/31, nv_pers 0/2 -> 6/33, kpers_ks 8/0 -> 8/43 (allocation/performance);
  the four wide ones about level. +49 mappings, 9 twins rebuilt. Backup in scratchpad.

- Later: FY2024 redone for nv_pers (0/1 -> 5/29), or_pers (pages 5-120 -> 107-120, 8/11 -> 8/30) and vrs
  (0/0 -> 10/111). +21 mappings. ipopif_il has no FY2024: its second "FY2025" document (id 3109) is the
  same report at another URL, with an empty extract. The app uses the good one (4925).

- Later: ipopif_il duplicate document 3109 deleted (James's OK) with its extract, summary, section reads,
  derived performance rows and 15 `performance_observation` rows (a table only the unmerged pdf-retention
  branch writes; all 15 values also sit on 4925). Its 3 refresh-log rows kept, unlinked. Backup in scratchpad.
  The PDF file and R2 object are 4925's too (same hash) and were left alone. Performance view rebuilt.

- Later: ipopif_il's `cafr_url_template` replaced by `cafr_url_by_year` (2024, 2025). Each report has its
  own /Resources/<id>/ and the path after it is ignored, so the template fetched the current report for any
  year. Neon document 4925's URL corrected from the made-up ".../2024-2026/" to the real 2024-2025 link.
- Later: full CAFR refresh run locally (monthly workflow steps): 0 new, 133 already have, 10 no URL, 2 failed
  (ers_hi, trs_texas, as before); FY2026 links all 404 as expected before December. Investment step found
  only the 15 long-standing skips; actuarial re-read 3 (lacers, bcers_md, pers_ms). $1.21.

**Next**
1. Add ipopif_il's FY2026 link to `cafr_url_by_year` when the report appears (with the other curated plans).

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
