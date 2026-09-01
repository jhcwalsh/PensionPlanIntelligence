# Next steps

**As of 2026-09-01.** Working doc. Supersedes the 2026-08-19 migration
edition (in git history at `e66d56d`) — that migration is complete.

Specs: `docs/superpowers/specs/2026-08-16-low-maintenance-app-design.md`,
`docs/superpowers/specs/2026-08-19-portal-readiness-design.md`,
`docs/superpowers/specs/2026-08-29-pdf-retention-design.md`.

---

## Done (2026-08-21 → 2026-09-01)

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
| August monthly + YTD (D2) | Publication 126, published and emailed |
| Quarterly performance extractor (D4) | scoped to the one plan with real data; see D4 |
| CAFR locator bug (D3) | plan's-own-name TOC collision fixed; hit 4-5 plans, not 2 |
| `NameError: 'pd' not defined` on CAFR Coverage + Performance tabs | PR #38 |
| `mboi_mt` CAFR standalone-format fix (D3) | 0 rows → 17 alloc + 8 perf rows; PR #39 |
| `www.pensiongraph.com` CNAME (B2) | repointed off retired Render slug |
| Recordings download, admin-gated (D5) | PR #40, verified live end to end |
| **Targeted section read (D8)** | 951 windows, 510 docs, $1.0980; +14% asset-class cells |
| **`horizon_of` label repair (D7)** | ~3,350 rows reclaimed from `unclear` |
| **Per-asset-class view (D6)** | 2,437 cells, 126 plans, 13 classes |
| **Scraper + extraction repairs (D9)** | OCR backlog, `not_a_pdf`, Playwright, new domain |
| **PDF retention code (E1)** | R2 store built and wired — **but inert, see E1** |
| **Dollar amounts read as LaTeX** | every weekly briefing's figures rendered as italics |
| **Period end + Performance sub-tabs (D10)** | which quarter a figure is, and a page per table |

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

### A4. WAF proxy — now a 7-plan question, and a weaker one

Probed 2026-09-01 (see A5). A residential IP recovers 7 of the 14, so a proxy
would only ever buy the other 7 — and for 4 of those (`asrs`, `corp_az`,
`acrs_pa`, `strs_ohio`) it probably would not, because their listing pages
already render fine and it is the **PDF path** that 403s, to every technique
tried including Playwright's own browser request context. A proxy changes the
IP, which is not the axis those sites are refusing on.

The honest residue a proxy might actually fix is `frs` and `pgcers_md`, whose
listing pages 403. `scers_suffolk` is not a proxy question at all — its
selector is stale.

So: **weaker than it looked.** $10–75/month for a plausible 2 plans.

### A5. Mac mini — Stage 1 built, schedule installed but not loaded

`docs/superpowers/plans/2026-08-30-mac-mini-waf-plans.md`. Tasks 1–6 done on
2026-09-01: the probe, the arm64 container, `scripts/waf_blocked_ids.py`,
`scripts/run_waf_plans.sh`, and the launchd agent. The mini has the repo, a
least-privilege `.env` (five values; no Render key), a working Docker image,
and both runner guards verified.

**It is worth 5 materials plans and 2 CAFRs, not 14** — the plan was written
expecting 14, the first probe suggested 11, and testing downloads rather than
just discovery brought it to 7. Materials coverage 137 → 142 when it runs.

**The agent is installed and deliberately not loaded**, because loading it
schedules a nightly fetch of the plans nothing else can re-fetch, with
retention off. One command once E1 lands:

```
launchctl load ~/Library/LaunchAgents/com.pensiongraph.wafplans.plist
```

### A6. `scers_suffolk`'s stale selector — cheap, and not a Mac mini job

Misfiled as a WAF block for months. The site answers HTTP 200 with 108
anchors from anywhere, GitHub Actions included; the discovery selector just
finds no document links. Fixing it and removing the id from
`data/waf_blocked_plans.json` is +1 plan of coverage for an hour's work and
no infrastructure. Better value than anything in A4.

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

### B2. `www.pensiongraph.com` still points at the retired host — **DONE 2026-08-29**

Was a real risk, not just cosmetic: Render routes by verified custom domain
(`www.pensiongraph.com` was already verified on the right service), so it
worked today regardless of the CNAME's literal target — but pointing at a
retired, unhyphenated `pensionplanintelligence.onrender.com` slug is a
dangling-CNAME subdomain-takeover risk if that name ever got released.
James repointed it (Namecheap, `dns1/dns2.registrar-servers.com`) to
`pension-plan-intelligence.onrender.com`; verified resolving correctly.

---

## C. Verification outstanding

### C1. Neon transfer meter — nobody has looked

The egress fix eliminated the largest *measured* consumer, but Neon's usage
dashboard was never available while diagnosing, so a second contributor cannot
be ruled out. **Only James can see this.** Now a week of post-fix data.

### C2. The Streamlit app hangs mid-rerun locally — not root-caused

Clicking through to the Performance tab locally blanks the main pane and never
repaints; the script status stays "Stop" indefinitely. Ruled out: the new
queries (all under 0.9s against Neon), and the admin-gated tabs (it still hangs
with `ADMIN_PASSWORD` set, which reproduces production's tab set). Does **not**
reproduce on Render, where the same page renders fine — so the live site is
unaffected and this is a local-development annoyance, not an outage. Left open
rather than guessed at. Suspicion, unproven: a rerun triggered while the
previous one is still in a network read holds the shared
`@st.cache_resource` Session, and the two block each other — the same shape as
the `IllegalStateChangeError` seen when two browser tabs were open at once.

---

## D. Feature requests

### D1. "Filter by Plan" dropdown — **DONE** (PR #35)

**Not dead — partially wired.** `main()` passes `plan_id` to Activity, Search,
Investment Actions and Meeting Recordings; the other five tabs ignore it while
the control stays visible, so it read as broken on more than half the app.

Captioned with the tabs it drives rather than removed — removing it would have
destroyed working behaviour on four tabs.

### D2. Monthly for August + Year-to-date for August — **DONE 2026-08-28**

Publication 126, published and emailed. The legacy YTD note
(`notes/2026_cio_insights.md`, `generate_notes.py --insights-ytd-only`) was
also regenerated and committed — 118 plans, ~$4.8T AUM, validated 97%+
against the source corpus. It's a fallback only: the Year-to-date tab
prefers the newest `quarterly_cio_insights_*.md`/`annual_cio_insights_*.md`
(see `app.py::_find_latest_insights`), currently `quarterly_cio_insights_
2026-04-01.md` — that won't refresh until Q3 closes (2026-10-01).

### D3. CAFR — **answered, and fixed** — bigger than the two flagged docs

The original "7 pending" diagnostic (2026-08-28) found `no_section`
(NIC FY2024) and `too_short` (WV IMB FY2024) as the two "genuine
per-document problems." Investigating them turned up a real, general bug,
not two isolated ones:

- **The TOC locator matched a plan's own name.** For plans whose own name
  contains "Investment" (WSIB, WV IMB, IPOPIF, FPIF, Montana Board of
  Investments), `_locate_via_toc`'s lenient "contains investment" pattern
  matches the title-page TOC entry, and the level-preference tie-break
  never displaces that false match with a later, correct one — the plan
  silently gets a 1-7 page range and **zero data saved, no error raised**.
  Fixed in `extract_cafr_investments.py` (PR #36) by excluding TOC entries
  containing the plan's own name. Verified against the real PDFs for
  `wv_imb` and `ipopif_il`. `mboi_mt`'s false match ("BOARD OF INVESTMENTS
  STAFF") only shares a name *fragment*, not the full name — see below.
- **NIC and `mboi_mt` aren't GASB CAFRs at all** — short standalone
  investment-council annual reports with no TOC/section structure to
  locate. Both given `cafr_format="standalone"` (PR #36, PR #39) to feed
  the whole document instead. `mboi_mt`: 0 rows → 17 allocation + 8
  performance rows, re-verified live 2026-08-29.
- **`wsib` is not this bug. Still open.** Its "INVESTMENTS" section (pages
  9-15) is *correctly* located — the document is structured INTRODUCTION/
  INVESTMENTS/FINANCIALS, and the returns table lives inside FINANCIALS →
  Retirement Funds (pages 18-63), not co-located with the allocation
  targets. Needs either a second section search or a merge across two
  locations. Low priority: it fails safe, showing fewer rows rather than
  wrong ones.

### D4. Performance Reports tab — **DONE, both annual and quarterly** (PR #35, #36, #37)

83 plans, annual headline return by asset class (from CAFRs), CSV download.

**Quarterly decided: build it, scoped down.** Of the 48 `doc_type=
'performance'` documents assumed to be "true quarterly reports with no
extraction," only **30 (`nycrs_comptroller`)** actually are. `mn_msrs` and
`pera_colorado` hold DC-plan investment-menu returns; `calpers` holds a
governance-committee meeting transcript; `dcrb` holds a blank vendor form —
all mistagged `doc_type='performance'`. Extracting the other four as if
they were fund performance would have put wrong or DC-plan data into a
table that claims to be the pension fund's return, so
`extract_performance_reports.py` only ever processes `nycrs_comptroller`
(`ALLOWED_PLAN_IDS`, deliberately not silent). Surfaced as a second table
in the Performance tab, one row per constituent NYC system (NYCERS/TRS/
POLICE/FIRE/BERS) since there's no single "total fund" figure across them.

Shipped with a real bug of its own: `MAX_OUTPUT_TOKENS=4096` silently
truncated every tool call before it reached the `returns` array — all 30
documents "saved" with **zero rows**, no error. Fixed (16384, PR #37),
re-ran live: all 30 now carry real per-fund quarterly returns.

### D5. Download recordings behind the Admin login — **DONE 2026-08-29** (PR #40)

The `--no-downloads` weekly job leaves `download_status='pending'` for
almost everything (3402 of 3435), but 12 real files exist on this
machine's D: drive from earlier manual runs — confirmed before designing
anything. Added a selectbox + download button below the Recordings table,
scoped to those 12, admin-gated. Use case confirmed with James first:
grabbing a file from another device on the LAN while the app runs locally
(never useful on Render — files "never go onto Render's persistent disk"
by design). Verified live end to end: downloaded a real 2,080,074-byte
file, matched the UI's displayed size exactly.

### D6. Per-asset-class performance across plans — **DONE 2026-08-31**

One asset class, every plan, across quarter / 1y / 3y / 5y / 10y — the mirror
of the existing table, which fixes the plan and shows every class.

Needed a second derived table, `plan_asset_class_horizon`, because it could not
be read off the first one: `pick_latest` prunes each plan to at most two rows
from a single document, which discards precisely the horizons this view is made
of. Uniqueness on `(plan, asset class, horizon_key)` **is** the selection rule.
Derived rather than queried live for the same reason as its neighbour — parsing
2.2 MB of summary JSON behind a Streamlit cache is the read shape that exhausted
Neon's quota in August.

Selection is best-available-per-cell (James's call): the most recent reading
wins each cell, ties break toward the targeted read. A row may therefore mix
documents across columns, which the existing view forbids — acceptable here
because within one asset class the comparison runs across plans, so the row
never claims to be a portfolio. The UI says so in a caption and carries **As
of** and **Sources**.

Live: **2,437 cells across 126 plans and 13 asset classes.** For real estate,
69 plans have an annual figure, 27 a 3-year, 27 a 10-year, 26 a 5-year, 23 a
quarter.

### D7. `horizon_of` label repair — **DONE 2026-08-31**

Thousands of stored figures landed in `unclear`, which no horizon-keyed view
can use. Repaired in two passes, ~3,350 rows reclaimed. The cheapest coverage
anywhere on this list: no fetching, no extraction, no spend.

Three of the fixes were found by measuring rather than guessing:

- 572 rows said `1 Yr` and were filed as multi-year, because `_MULTI`'s
  exclusion spelled out "1 year" and missed the abbreviation. Wrong in the
  existing view too.
- 50 rows give a period as two dates. A range states both ends, so its length
  is arithmetic — unlike a bare `12/31/24`, which says only when something
  stopped and deliberately stays `unclear`. The rule refuses to round: a real
  six-month range has no bucket in this vocabulary and stays `unclear` rather
  than being called annual.
- 131 `ITD IRR%` rows are inception-to-date; 113 headed just `Month` are
  monthly. The bare-noun rule is anchored to the whole label so it cannot
  swallow `3 Month`.

`Long-Term Expected Real Rate of Return` (240 rows) is an actuarial assumption
in the period field, not a return, and is now dropped rather than classified.

### D8. Targeted section read — **DONE 2026-08-31**

`docs/superpowers/plans/2026-08-30-targeted-read.md`. The summariser fills a
~50,000-character budget from the front of a document, chosen to write a good
summary rather than to find a table; on a real board pack the performance
headings begin 31% in. `section_finder.py` scores 30,000-character windows on
heading clutter and numeric density, and `targeted_extract.py` reads the best
ones through DeepSeek V4 Flash (`llm_openrouter.py`).

**951 windows across all 510 candidate documents, for $1.0980.** All 195
verification figures appear verbatim in the window they were read from — zero
hallucinated numbers.

Two honest notes. The run came in **32% over** the ~$0.83 estimate, because
that estimate assumed 800 output tokens and real windows returned up to 127
rows. And the coverage gain is smaller than the plan implied: plans with
asset-class detail went 110 → 116, cells 597 → 683 (+14%), zero plans lost
detail. Extraction is no longer the constraint — the targeted read yields
7,236 canonicalisable rows against the summariser's 4,069. What caps the view
is `pick_latest`'s one-document-per-plan-per-horizon rule, which is deliberate.

### D9. Scraper and extraction repairs — **DONE 2026-08-31**

A batch of separate faults, each found while chasing the one before:

- 30 documents deferred for OCR were cleared; a failed OCR run had rewritten
  them to `ocr_empty`, silently dropping them from the priced backlog. Added
  `ocr_unavailable` and `OCR_OWED_REASONS` so a failure can no longer erase
  the debt.
- 13 `not_a_pdf` documents refetched; 32 more fixed via Playwright.
- Plan config updated to the new domain, discovery re-run.
- `refetch_missing_files` scoped to `extracted_text IS NULL` — as first written
  it would have proposed re-downloading 2,557 perfectly good documents.

137 documents remain `failed` (130 with no text at all), out of 5,095.

### D10. Period end + Performance sub-tabs — **DONE 2026-09-01**

**Which quarter is this figure?** Neither stored column said. `Period` is
verbatim and inconsistent (`FY2025`, `1 Yr.`, `12 months ended March 31, 2026`)
and 54% of the corpus states no date in the label at all; `As of` is the
*document's* date, so a pack presented 14 May 2026 reporting through 31 March
filed a 2026Q1 return under 2026Q2. `queries.period_end` resolves both into one
period-end date and `quarter_label` buckets it — what a label states beats what
a document date implies, and where a label says only `1 Year` the fallback
rounds the document's date back to the last closed quarter, never forward.

Computed at read time, not stored: both derived tables already carry
`period_label` and `as_of_date`, and the alternative was a schema change to two
tables for a display concern.

Running it over the live corpus caught three parser bugs, each of which had
dated rows into a quarter that had not happened — abbreviated months
(`Mar 31, 2026`), `1Q 2026` as well as `Q1 2026`, and YTD labels read as whole
years. **Thirteen future-dated rows before, zero now.**

Filtering differs between the two tables because the tables differ. The
collated view filters per row (every figure in a row is from one document); the
per-asset-class view filters **cell by cell**, since a row mixes documents
across columns by design. Pick 2026Q1 and every number on screen is a 2026Q1
figure; a plan with nothing left drops out rather than showing an all-blank row
that reads as a bad quarter. On real estate that is 8 plans of 81.

The four tables also stopped sharing one page — they read as one long table
with three interruptions, and each one's filters sat screens above its own
rows. Now a sub-tab each, with the quarterly tab appearing only when it has
rows.

**Known limitation:** `FY2025` is bucketed to 30 June, the convention
`collect_from_cafr` already assumes. Plans with a different fiscal year end are
a quarter or two off. The verbatim `Period` column sits beside it so a reader
can check.

---

## E. Carried-forward loose ends

### E1. PDF retention — **built, and currently doing nothing**

The R2 PDF store is written and wired: `pdf_store.py` (content-addressed by
SHA-256), `documents.content_sha256` / `r2_uploaded_at` / `retention_status`,
`scripts/backfill_pdf_store.py` for the existing corpus, and
`pdf_store.document_pdf` now wrapping both extractors so they read from R2 when
the local file is gone. Spec:
`docs/superpowers/specs/2026-08-29-pdf-retention-design.md`.

**It has never retained a single file.** `retention_status` is NULL for all
5,095 documents and `content_sha256` for all 5,095. The four `R2_*` values do
not exist in `.env` and, on the evidence, not in the `daily-pipeline.yml`
secrets either — and when they are missing, retention is a **silent no-op**:
the pipeline fetches, extracts, goes green, and keeps nothing. `fetcher.py`
prints one line per run saying whether retention is on; that line is the thing
to check.

**This is the top item on the list, and only James can start it:** create the
Cloudflare R2 bucket, then set `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`,
`R2_SECRET_ACCESS_KEY` and `R2_BUCKET` in `.env` and as GHA secrets on
`daily-pipeline.yml`. Everything downstream is already written and tested.

What stays broken until then, unchanged from when this was first diagnosed:

- 5 of the 7 CAFRs stuck at "pending extract" (D3)
- the 450 documents truncated at the old 150k cap — no longer size-gated
  (`MAX_STORED_CHARS` is 2,000,000 against Postgres), only source-gated
- any future structured extraction that needs the original document
- ~4 GB of archivable material accumulating and being discarded daily

### Others

- **The Drafts tab is vestigial.** Nothing can enter `awaiting_approval` now
  that every cadence auto-publishes; it shows historical rows only.
- **`or_pers` reads GASB-basis figures** because its Actuarial Section is
  scanned images. Labelled in `notes`; OCR is the real fix. Applies to any
  image-only CAFR.
- **Coverage is 137 of 148 plans** — see A4, A5.
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

Sections A1, B1, B2 and D1–D10 are done. What's left:

1. **E1 R2 credentials** — the only item where the code is finished and the
   work is a config change. Until it lands, every PDF the pipeline downloads is
   thrown away, and the archive this was all building toward does not exist.
   **Only James can do this.**
2. **E1 backfill**, immediately after — `scripts/backfill_pdf_store.py` over
   the existing corpus, ~4 GB. Cheap, and it stops the loss compounding.
3. **C1 transfer meter** — one dashboard glance, closes the last unverified
   claim from the August outage. **Only James can see this.**
4. **C2 local Streamlit hang** — development friction only, does not affect
   the live site. Worth an hour with a thread dump rather than more guessing.
5. **A5's one command** — `launchctl load ...wafplans.plist` on the mini, the
   moment R2 is on. Everything else for it is built and verified; leaving it
   unloaded is the only thing standing between the mini and 7 more plans.
6. **A6 `scers_suffolk` selector** — +1 plan, an hour, no infrastructure, and
   it lands in the cloud pipeline rather than on a machine in your house.
7. **`wsib`'s missing performance data** (D3) — needs a second section search
   or a cross-section merge. Not urgent: it fails safe.
8. **A2 Auth0**, **A3 public repo**, **A4 WAF proxy** — decisions only James
   can make, no urgency on any of them. A4 is now worth less than it looks;
   see the entry.

Worth saying plainly: with D6, D7, D8 and D10 shipped, the performance data is
in better shape than the extraction pipeline that feeds it. The binding
constraint has moved from "can we read the numbers" to "are we keeping the
documents" — which is E1.
