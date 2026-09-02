# Next steps

**As of 2026-09-02.** Working doc. Supersedes the 2026-08-19 migration
edition (in git history at `e66d56d`) — that migration is complete.

Specs: `docs/superpowers/specs/2026-08-16-low-maintenance-app-design.md`,
`docs/superpowers/specs/2026-08-19-portal-readiness-design.md`,
`docs/superpowers/specs/2026-08-29-pdf-retention-design.md`.

---

## Done (2026-08-21 → 2026-09-02)

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
| **Horizon table keeps history (D11)** | 6,196 cells; 2025Q4 PE 17 → 22 plans |
| **Neon transfer verified (C1)** | 0.68 GB against a 500 GB allowance |
| **Scraper + extraction repairs (D9)** | OCR backlog, `not_a_pdf`, Playwright, new domain |
| **PDF retention live (E1)** | 4,882 of 5,109 PDFs in R2; 172 lost to link rot |
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
listing pages 403. `scers_suffolk` is not a proxy question at all — it
publishes nothing (A6).

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

**Loaded 2026-09-02**, once E1 made retention real — the agent was held back
deliberately until then, because scheduling a nightly fetch of the plans
nothing else can re-fetch, with retention off, is the most expensive way to
look busy. First run 07:30 local. R2 was verified from inside the mini's
pipeline container, not merely configured.

### A6. `scers_suffolk` — **dissolved 2026-09-02, there was no task**

Written up as "a stale selector, +1 plan for an hour's work". Wrong. The board
page renders fine from anywhere and has nothing on it: 92 anchors, every one
site navigation, and "agenda", "minutes" and "meeting" appear zero times. The
only PDF is an unrelated social-services form.

`known_plans.json` has said so since 2026-08-29 — *"publishes no board
materials... nothing to scrape until Suffolk starts publishing"*. The A6
entry was created by reading "HTTP 200, 108 anchors, we find no documents" as
a scraper bug, without checking what the anchors were, and without reading the
note already on the plan.

Reclassified `blocked_by: "no_materials"`. It stays on the block list so the
daily pipeline does not spend a Playwright run on a directory page. Nothing to
do until Suffolk publishes something; the ACFR still arrives from osc.ny.gov.

**Coverage arithmetic, corrected again:** materials tops out at 142 + `frs` +
`pgcers_md` = 144 of 148, not 148. Four plans have no reachable materials at
all and one of them has none to reach.

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

### C1. Neon transfer meter — **CLOSED 2026-09-02, with a number**

0.68 GB network transfer against **500 GB per project per month** on Launch —
0.14% of the allowance. Storage 133.82 MB (~$0.05/month).

The August outage blew the *free* tier's 5 GB. Launch is 100× that, so the
failure is structurally out of reach at anything like current usage: it would
take a sustained 16 GB/day for a month, and the worst bug ever measured in
this codebase managed 1.5 GB/day.

Worth recording that the 0.68 GB briefly looked alarming when read against the
old 5 GB ceiling, and that it was measured on the heaviest day this database
has ever had — a table rebuilt several times, the R2 backfill reading every
document row, and a dozen full-corpus probes. The allowance is what settles
it, not the usage.

No second-contributor hunt needed. Even if one exists it has no consequence at
this headroom.

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

**Superseded on 2026-09-01 — see D11.** The key gained `period_end`, so the
table holds history rather than a snapshot, and the paragraph above describes
what happens *within* one quarter rather than across all of them.

### D11. The horizon table keeps every quarter — **DONE 2026-09-01**

Prompted by James: a sweep of 2025Q4 private equity showed 17 plans and that
looked low. It was. Keyed on `(plan, asset class, horizon_key)`, the table
kept one reading per cell — the latest — so a plan with both a 2025Q4 and a
2026Q1 figure kept only 2026Q1 and vanished from any question about 2025Q4. A
snapshot being asked history questions.

`period_end` joined the key. **2,437 cells → 6,196**, and 2025Q4 private
equity went **17 → 22 plans**. Not the 26 that reported one: the other four
report that quarter only on horizons the view has no column for (inception,
20-year, monthly, part-year) — a separate and smaller question, and the
cheapest remaining coverage win on this table.

Two readings of the *same* quarter still collapse to one, so a figure
restated in a later pack supersedes the earlier printing. History, not
duplicates.

The read layer keeps both questions: no quarter chosen means "how is everyone
doing now" (latest per cell, the old behaviour); choosing quarters sweeps
them, including plans that have since reported something newer.

Two faults fixed alongside it, both worth remembering rather than
rediscovering:

- **A document cannot report a period ending after it was written.** Nine rows
  sat in 2026Q4, a quarter that had not happened — "Calendar Year 2026" in a
  May pack, "Fiscal 2026", "1-year ending Feb-2026", each arriving by a
  different route through the year rules. One cap catches all of them.
- **A keyed widget dies when its options change under an open tab.** Streamlit
  raises rather than ignoring a stored selection that is no longer offered,
  and the quarter list changes for ordinary reasons. `_drop_stale_selection`
  reconciles session state before the widget renders.

Also: figures before `queries.EARLIEST_PERIOD_END` (2025Q1) are no longer
shown — the corpus reached back to 1994Q4 and a 2014 return sat in a column
beside a 2026 one. A display rule, not a build rule; nothing was deleted.

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

### E1. PDF retention — **DONE 2026-09-02. The archive exists.**

Built on 2026-08-29 and inert until today: the four `R2_*` values did not
exist, and a missing credential makes retention a silent no-op — fetch,
extract, go green, keep nothing. `retention_status` was NULL for all 5,095
documents, which is what "built" was worth.

James created the bucket and the account-scoped token. Retention is now on in
three places, each verified rather than assumed:

- **`.env`** — round-tripped a test object: write, read back with matching
  bytes, delete.
- **GitHub Actions** — four secrets set via `gh secret set` over stdin;
  `daily-pipeline.yml` already declared them at job level.
- **The Mac mini** — verified *from inside the pipeline container*, not just
  by checking the variables were present. That distinction is the whole point:
  a green run that keeps nothing looks exactly like a green run that works.

**Backfill: 4,882 of 5,109 documents retained (95.6%).**

| | |
|---|---|
| `stored_local` | 2,431 — PDF still on disk, free |
| `stored_refetch` | 2,411 — re-downloaded from source |
| `unrecoverable` | **172 — source link dead, gone permanently** |
| `skipped_waf` | 55 — the Mac mini reaches 7 of those plans from 07:30 |

**The 172 are the price of not doing this sooner.** Their text survives; the
PDFs do not, and the links have rotted. When this was first diagnosed a
20-URL sample suggested 19 of 20 were still fetchable; the real figure was
93% of 2,583. Better than the sample implied, and every day of delay moved it
the wrong way.

Consequences now unblocked:

- The 449 documents truncated at the old 150k cap — see D12.
- Structured extraction that needs the original document, at any point in the
  future, on any machine: `pdf_store.document_pdf` means the extractors no
  longer have to run on the machine that did the fetching.
- The 5 CAFRs stuck at "pending extract" (D3), for whichever of them was a
  `missing_file`.

### D12. Re-extracting the truncated 449 — driver written 2026-09-02

`scripts/reextract_truncated.py`. `MAX_STORED_CHARS` was 150,000 while the
database was a SQLite file in git; it is 2,000,000 now and the PDFs are
retained, so the text is simply read again. **No model, no cost** — PyMuPDF's
text layer, locally.

Sampled ten documents: 4,207,781 characters recovered, `smcera` 150,000 →
1,301,665, `me_pers` → 1,348,495. Across all 449 that suggests 150–200M
characters, against a corpus currently holding 157M.

It never shrinks a document: a shorter or non-`done` re-read keeps the stored
text and reports why. A truncated 150,000 characters is worth more than a
complete 400, and a PDF that re-reads short means a bad retained copy or a
changed source — something to investigate, never to overwrite.

What this does **not** change: summaries. The summariser reads the first
~50,000 characters, so existing summaries are unaffected. The gain is search,
and the targeted read (D8), which scans the whole document and is where the
extra text actually gets used.

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

Sections A1, A6, B1, B2, C1, D1–D12 and E1 are done. Everything that was
blocking something else has landed. What's left is genuinely optional, which
is a different position from any previous edition of this document.

1. **The 130 failed extractions** — the last real backlog. Their failure
   reasons were never recorded, so nobody knows how many are image-only
   (needing OCR, which costs) versus broken downloads (free to retry). Triage
   first, then price. Recording a reason on failure is worth doing anyway.
2. **OCR is the only expensive model path left.** `extractor.py:211` pins
   vision OCR to Sonnet. Haiku 4.5 has vision and is far cheaper, but OCR is
   exactly where a cheaper model degrades quietly — worth an A/B on a few
   pages before switching, not a blind swap. Everything else is already on the
   cheap option: the targeted read runs DeepSeek V4 Flash, the summariser
   routes to Haiku by default.
3. **Re-run the targeted read (D8)** over the documents D12 grew. The summariser
   only ever reads the first ~50k characters, so the recovered text is
   currently invisible to everything except search; the targeted read is what
   consumes it. ~$1 last time, and it should be quoted again first.
4. **The horizon view's missing columns** — four plans report 2025Q4 private
   equity only as since-inception / 20-year / monthly / part-year, which have
   no column. Small, and it is the remainder of the question that produced D11.
5. **C2 local Streamlit hang** — development friction only, does not affect the
   live site. Worth an hour with a thread dump rather than more guessing.
6. **`wsib`'s missing performance data** (D3) — needs a second section search
   or a cross-section merge. Not urgent: it fails safe, showing fewer rows
   rather than wrong ones.
7. **A2 Auth0**, **A3 public repo**, **A4 WAF proxy** — decisions only James
   can make. A4 is now worth about two plans; see the entry.

Worth saying plainly: the constraint that shaped this whole document — "we are
throwing away the documents" — is gone. 95.6% of the corpus is archived, every
future fetch is retained, and extraction no longer has to happen on the machine
that did the fetching. What remains is improvement rather than repair.
