# Next steps

**As of 2026-09-03.** Working doc. Supersedes the 2026-08-19 migration
edition (in git history at `e66d56d`) — that migration is complete.

Specs: `docs/superpowers/specs/2026-08-16-low-maintenance-app-design.md`,
`docs/superpowers/specs/2026-08-19-portal-readiness-design.md`,
`docs/superpowers/specs/2026-08-29-pdf-retention-design.md`,
`docs/superpowers/specs/2026-09-02-lazyeconomist-style-design.md`.

> **Read entries as dated observations, not current state.** A3 sat here for
> weeks asking whether to make the repo public; it had already been public
> since April, and one `gh repo view` would have said so at any point. Before
> acting on an entry, check the claim it rests on still holds.

---

## Done (2026-08-21 → 2026-09-03)

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
| **Failed extractions triaged (D13)** | 104 lost for good, 7 fixed, no OCR bill |
| **Scraper + extraction repairs (D9)** | OCR backlog, `not_a_pdf`, Playwright, new domain |
| **PDF retention live (E1)** | 4,882 of 5,109 PDFs in R2; 172 lost to link rot |
| **Dollar amounts read as LaTeX** | every weekly briefing's figures rendered as italics |
| **Period end + Performance sub-tabs (D10)** | which quarter a figure is, and a page per table |
| **Recovered text read (D16)** | 587 windows, $0.78; 2025Q4 PE 22 → 33 plans |
| **SDCERS unblocked (D14)** | the "403" was a spinner; 1 → 61 documents |
| **Coverage review vs PPD (D15)** | 99.5% of the top 200 by assets; `aum_billions` ~12% low |
| **Horizon columns** | 48 hidden cells recovered; a plan with no column had no row |
| **UCRP + APERS added (D17)** | 150 plans; UC was the largest omission at $110.8B |
| **Restyle (D18)** | paper/Fraunces/one accent, almost all supported config |

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

### A3. Repo visibility — **DECIDED 2026-09-03: go private.**

**It has been public since 2026-04-05.** `gh repo view` says
`isPrivate: false`. The entry asking whether to *go* public was stale for
weeks and nobody checked, which is why the warning at the top of this document
now exists.

So the Actions argument it rested on is moot, and measurement kills it anyway.
**1,609 minutes over the 30 days to 2026-09-03, against the 2,000 a private
repo gets free:**

| workflow | runs | minutes |
|---|---|---|
| Daily pipeline | 33 | 1,076 |
| tests | 137 | 378 |
| daily-digest | 41 | 65 |
| everything else | 32 | 90 |

It fits at 80%, and Linux overage is **$0.008/min** — 400 minutes over is
$3.20/month. Actions cost is not a reason to keep anything public. Note the
two growing terms: the daily pipeline scales with plans tracked (150 now,
two added 2026-09-02), and `tests` scales with development activity, so 1,609
is a busy month rather than a floor.

**The real argument for going private is the registry, not the code.**
`data/known_plans.json` is 150 plans with working `materials_url` configs,
WAF classifications, CAFR templates and the notes that took months to earn —
the SDCERS spinner, the OnBase `dropid` presets, which hosts need a
residential IP. The scraper is replaceable; that file is not.

**Nothing here is security-sensitive.** `.env` is gitignored, `db/pension.db`
left history in August, and every source document is public record.

**Two things to know before flipping it.** Going private is forward-looking
only — the history has been public since April and cannot be unpublished;
the mitigating fact is **0 forks, 0 stars, 0 watchers**, so nothing has been
taken and no orphaned forks are created. And Render deploys from this repo:
its GitHub grant usually survives a visibility change but sometimes needs
re-authorising, so confirm the service still builds immediately afterwards
rather than finding out at the next deploy.

**The plan.** James flips the switch — repository visibility is an account
setting and cannot be changed from here.

1. **Merge this branch to master first.** Doing it in the other order means
   re-authorising Render and then immediately testing a deploy of unmerged
   work, which confuses two failure modes.
2. **Settings → General → Danger Zone → Change repository visibility →
   Private.** GitHub asks for the repository name to confirm.
3. **Check Render still builds.** Trigger a manual deploy and watch it clone.
   If the grant lapsed, reconnect the repository in Render's settings — the
   symptom is a clone failure, not a build failure, so read the first lines of
   the log rather than the last.
4. **Check the next scheduled GHA run goes green.** The daily pipeline at
   11:00 UTC is the one that matters; Actions on a private repo start
   consuming the 2,000-minute allowance from that point.
5. **Watch the allowance for one month** at Settings → Billing → Plans and
   usage. Expect ~1,600 minutes. If a heavy development month pushes it over,
   the overage is pennies — but the two levers if it ever matters are running
   `tests` on pull requests only rather than every push, and dropping the
   daily pipeline to weekdays.

**Not blocking anything.** Nothing in this document depends on it, and it can
happen whenever the merge lands.

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

**Sharpened 2026-09-03** while reviewing the restyle, which is the first time
it has been pinned to an interaction rather than a page:

- **The first render always succeeds.** Insights renders fully, sidebar
  metrics and all. It is the *second* render that never arrives.
- **A tab click never completes its rerun.** Performance and Plans both.
  Sometimes the tab underline moves and the pane blanks; sometimes the click
  does nothing at all and the previous tab stays put.
- **Nothing reaches the server log.** No traceback, no warning — the log ends
  at the FTS5 notice from startup.
- **Not the theme.** Reproduced on the same build with
  `.streamlit/config.toml` removed, so it predates the restyle.

That "renders once, then never again" shape fits the existing suspicion — a
session held by the first render's network read — better than anything
page-specific, and it says the next probe is the *rerun*, not the queries.
Worth an hour with `py-spy dump` against the Streamlit process while it is
wedged; a thread stuck in psycopg would settle it either way.

**Cost so far:** it made the visual review of the restyle incomplete. The
Performance tab's wide horizon table — the change most at risk from a
restyle — has still not been looked at locally.

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

### D16. Reading the recovered text — **DONE 2026-09-02**

D12 recovered 122,839,578 characters, and every document holding them had
already been read. `backlog_documents` excluded read documents outright, so
the new material was invisible: a priced run returned 41 documents and $0.03,
all of them genuinely new files. The first quote was therefore meaningless,
and quoting it was the mistake — the worklist was not the set in question.

`--reread` moves the unit from the document to the window. `read_offsets()`
loads every `(document_id, offset)` already recorded and the worklist filters
candidates against it **before** `--top` slices, so a document is bought only
at offsets nobody has read. Filtering after the slice takes the best window,
finds it read, and leaves the document with nothing — backwards, since the
second-best window is the one that only exists now the text runs past the cap.
The outer join is dropped rather than merely unfiltered: a document with three
recorded reads would otherwise be priced and bought three times.

**587 of 590 windows, 3 failures, $0.7827** against a $1.00 ceiling. Reads
951 → 1,538; return rows 29,274 → 53,474; horizon cells 6,196 → 7,913. 242
windows (41%) came back empty, which is the targeted read confirming there is
no returns grid rather than a failure.

**2025Q4 private equity: 17 → 22 → 33 plans** — the question that started D11.
2025Q4 now covers 70 plans and 1,174 cells. Real estate 34, US public equity
38, private credit 25.

Two observations, neither acted on. **2026Q3 exists with 7 cells across 3
plans** — a quarter that has not ended, from documents stating a period end in
progress; legitimate, thin, and worth suspicion. **The 3 failures** are
individual call errors and re-runnable for pennies, since `--reread` skips
what is already bought.

Also fixed: the credential check ran *after* the corpus scan, so a missing
`OPENROUTER_API_KEY` cost minutes of free ranking before surfacing 590 copies
of one message. `llm_openrouter.have_key()` is now a preflight under
`--approve` only, exiting 2 so "not configured" is distinguishable from
"nothing to read". The key itself was never missing — `.env` is gitignored, so
this worktree carried a stale private copy of it. Worth remembering for every
future worktree, and for `ANTHROPIC_API_KEY` and the four `R2_*` values, whose
absence is a silent no-op rather than an error.

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

### D13. The 137 failed extractions, triaged 2026-09-02

Every one already carried a reason in `extraction_details`. An earlier note
here claimed they did not; that was written after querying `document_skips`,
a different table, and is corrected rather than deleted because the same
mistake is easy to repeat -- the reasons live in **`ExtractionDetail`**.

| Reason | n | PDF | Outcome |
|---|---|---|---|
| `file_missing` | **104** | unrecoverable | **Lost permanently.** Same root cause as the backfill's 172. |
| `not_a_pdf` | 12 | in R2 | Not PDFs, and re-fetching does not help — see below |
| `file_missing` | 8 | not attempted | WAF-blocked; the Mac mini reaches these |
| `file_missing` | 7 | in R2 | **Fixed.** 245,169 characters recovered |
| `ocr_gate_doc_type` | 5 | in R2 | Image-only, blocked by policy not cost |
| `unsupported_format` | 1 | in R2 | Also not a PDF |

**No OCR spend is pending anywhere.** The `ocr_deferred` queue — the only
reason in the vocabulary that is a funding decision — is empty. An earlier
note calling this "an unpriced OCR backlog" was wrong: there is no bill.

The 5 `ocr_gate_doc_type` documents are the only place OCR cost could enter,
and five documents is small either way.

**The 12 `not_a_pdf` were probed before downloading: none now serves a PDF.**
Eleven are `sdcers_ca` returning `text/html` at those exact URLs, one timed
out. So the retained bytes are faithful — the fetcher archived what it was
given — and the URLs themselves are wrong. Re-fetching would re-archive the
same HTML. The fix is in discovery for that one plan, and it is worth doing:
11 documents is most of what `sdcers_ca` has failed on.

### D14. `sdcers_ca` OnBase — **DONE 2026-09-02. It was never blocked.**

**The 1,435-byte stub was a spinner, not a 403.** Four probes recorded its
size; none decoded it. It is a "Downloading, please wait…" interstitial holding
one line of jQuery that rewrites its own address from `DownloadFile` to
`DownloadFileBytes`. That route serves the PDF — 171,442 bytes, `%PDF-`,
`application/pdf`. Weeks of "no host we have serves these", and the answer was
in the bytes we already had.

`fetcher._js_redirect_target` reads the rewrite **from the page** rather than
hardcoding OnBase's route names, so the next vendor's interstitial works
without a code change and a page offering no such instruction is refused rather
than guessed at. It mirrors the page's own `indexOf` guard, because
`DownloadFileBytes` contains `DownloadFile` and a naive second pass yields
`DownloadFileBytesBytes`; a test caught that before it shipped.

TLS is a separate, real fault: the host serves its leaf without the
intermediate. `fetcher.TLS_INCOMPLETE_CHAIN_HOSTS` is an **exact hostname set**
checked against `urlparse().hostname` — a suffix match would pass
`board.sdcers.gov.evil.test`, and there is a test for that.

**Result: 1 → 61 documents extracted.** 50 of the 80 failures recovered; the
other 30 are `www.sdcers.org` URLs stored before the migration, which soft-404
to the .gov homepage and are gone from this source.

**Only one plan uses OnBase** — 90 URLs, all `sdcers_ca`. The "vendor platform,
so the scraper serves many plans" argument this entry rested on was wrong, and
checking it first would have cost five minutes. It was still worth doing, but
on the merits of one $12B plan, not a platform.

---

**Original entry, kept because the diagnosis it records was half right:**

### D14 (original). `sdcers_ca` OnBase — discovery fixed, downloading blocked

**Half done, and the half that is done is the half that was unknown.**

San Diego migrated `sdcers.org` -> `sdcers.gov` in 2026 and moved agendas into
OnBase at `board.sdcers.gov`. The 30 stored `.org` URLs soft-404 to the .gov
homepage, so the fetcher archived 81,038 bytes of HTML under a `.pdf` name --
that is the whole of D13's `not_a_pdf` bucket, and re-fetching them is
pointless (re-probed 2026-09-02: still `text/html`).

**Discovery solved.** The OnBase landing page shows nothing useful — four
future meetings with empty Links cells — but `Meetings/Search` takes a
date-range preset as `?dropid=N`:

| dropid | Meetings | Document links |
|---|---|---|
| **1** ("Last Year") | 29 | **174** |
| **7** ("This Year") | 23 | **114** |
| 0, 2–6, 8 | 0–4 | 0 |

Those two are now the plan's `materials_url` and `extra_pages`.

**Downloading not solved, and it is the remaining blocker.** Every
`DownloadFile` href returns an identical **1,435-byte stub** beginning
`b'




'` — via `requests`, via `requests` with `verify=False`, and via
Playwright's own request context carrying the page's cookies. Clicking the
anchor times out because it is not visible until its row is expanded, so the
page does something beyond following the href. Reproducing that interaction is
what a fix has to do.

**TLS, separately:** `board.sdcers.gov` omits its intermediate certificate.
`requests` raises `SSLError`; Playwright raises "unable to verify the first
certificate"; `ignore_https_errors=True` works. Any fix needs a **host-scoped**
tolerance, never a global one.

Worth doing because OnBase Agenda Online is a vendor platform, not a
one-off — before building it, check how many other tracked plans use it, since
the same scraper would serve all of them.

### D15. Review the top 200 US public pension plans — **DONE 2026-09-02**

**Full review: `docs/superpowers/notes/2026-09-02-ppd-coverage-review.md`.**
Reproduce with `python -m scripts.ppd_coverage` (`--states` for the
side-by-side).

**Benchmark chosen: the Public Plans Database**, not the P&I Top 200. "Top
200" normally means P&I's, which is mostly *corporate* DB plans — GM, Boeing,
IBM — so scoring against it would count plans this project deliberately does
not track. PPD (Boston College CRR) is 248 state and local plans holding ~95%
of US state/local assets, FY2025 for 186 of them: the universe this registry
is trying to be a subset of.

**PPD $5,856B / 248 plans against our $5,689B / 148.** Every state and DC has
at least one tracked plan; there is no state-shaped hole.

**Three findings.**

1. **One large genuine gap: the University of California Retirement Plan,
   $110.8B.** It would rank #11 nationally, above Oregon PERS and Arizona SRS,
   both tracked. The only omission that changes the shape of the corpus.
2. **The rest is ~$60B across ~50 municipal funds**, none above $12B —
   Arkansas PERS $11.9B, the four Chicago city funds $12.5B, three Louisiana
   funds $9.5B, the Kansas City and St. Louis funds $8B, then a long tail of
   city police-and-fire plans. Cheap to add, cheap to skip: the case for them
   is *board-materials* coverage, not assets.
3. **`aum_billions` is systematically ~12% low.** Median ratio 0.881 across 32
   hand-verified pairs, 26 of 32 understated, one-directional — a vintage lag,
   not rounding. Philadelphia −37%, Connecticut Teachers −35%, CalPERS −11%.
   **This makes CLAUDE.md's "8.5% of tracked AUM" approximate**, since that
   percentage is computed from this column and the lag differs per plan.

**Name matching cannot do this, and two attempts proving it are in git
history.** PPD's unit is the plan, ours is the investing entity, so six
"Washington … Plan 1/2/3" rows are one WSIB portfolio. Stripping boilerplate
called four tracked Houston and Austin funds missing; stripping the state name
instead scored *University of California* as covered by CalPERS on the shared
word "California" — the $110.8B finding, hidden by a tokeniser. So
`scripts/ppd_coverage.py` prints state totals and a side-by-side and refuses to
classify; the verdicts were read by hand.

**Recommended next:** add UC; build a `plan_id` → `ppd_id` map and refresh
`aum_billions` from it; leave the municipal tail unless document coverage is
the goal — that one is James's call, not a default.

---

**Original brief, kept for the reasoning:**

The corpus grew by accretion: 148 plans, added when someone noticed them.
Nobody has ever checked that list against an external ranking, so the honest
answer to "do we cover the largest US plans?" is that we do not know. The
Performance and Allocation views invite exactly that reading — a reader
comparing real-estate returns across 126 plans will assume the set is the
market, and nothing on the page says otherwise.

The work:

1. **Get a defensible ranking.** Public Plans Data (Boston College CRR) and
   the P&I 1000 are the usual sources; the NASRA public-fund listing is
   another. Pick one, record which and as of when, because the ranking moves.
2. **Match it against `data/known_plans.json`.** Matching on name is the
   hard part -- "Teacher Retirement System of Texas" against `trs_texas`,
   and the several systems that share a sponsor. Expect to do this
   semi-manually and to store the mapping, so the comparison is repeatable
   rather than a one-off spreadsheet.
3. **Produce three numbers**: how many of the top 200 we track, what share of
   their combined AUM that is, and the list of the largest plans we miss.
4. **Then decide what to add.** A missing plan is not free -- each one needs
   a `known_plans.json` entry with a working `materials_url`, and the WAF and
   OnBase work above shows what "working" can cost.

Two things to be careful about. `plans.aum_billions` is already in the
registry but was hand-entered and is not sourced or dated, so it should be
checked against the external ranking rather than trusted as the basis for it.
And coverage should be reported by **AUM**, not plan count: 148 of 200 sounds
adequate while missing the largest five would not be.

Worth doing before any further per-plan scraper work. It is the question that
tells you whether `sdcers_ca` ($12B) is worth a day, or whether that day
belongs to a plan we do not have at all.

### D17. UCRP and APERS added — **DONE 2026-09-03**

The two fillable gaps D15 found. Probing all 19 uncovered top-200 plans showed
only these two publish board materials at a findable URL; the rest serve member
forms and handbooks where board packets would be, which is the `scers_suffolk`
lesson again — absent from the ranking is not the same as addable. Arlington
County, Kansas City MO and Hartford CT returned 403 from a datacentre IP and
are Mac-mini candidates if they ever seem worth ~$6B combined.

**Registry is 150 plans.** Top-200 coverage 97.4% → **99.5% by assets**.

**APERS: 16 documents, 61 horizon cells across 9 asset classes and 4 quarters.**
Board packets with allocation tables — the ideal shape.

**UCRP: 84 documents, 11 horizon cells across 5 classes.** Two sources, because
one was not enough. Pointing `materials_url` at the Regents minutes index —
richer, and matching the other 148 — was wrong for this plan: 40 minutes
produced *one* cell, because minutes record that a committee reviewed
performance, not what the numbers were. Adding UC Investments' annual reports
as `extra_pages` fixed it as far as the source allows.

**The source's ceiling, so nobody re-litigates it:** UC publishes allocation by
asset class (market value and weight) but returns only per *pool* — Endowment,
Blue and Gold, GEP, Pension, Working Capital — where UCRP is "Pension".
Per-asset-class returns do not appear to be published at all, and the figures
that exist sit in chart labels that extract as jumbled number runs. A thin
per-class row for UC is the source, not a scraper bug.

**Cost:** $1.79 for the first 56 documents, $3.36 for the UC follow-up. The
second overran its estimate because 26 of 56 documents escalated to Sonnet at
~30× Haiku — 46% of the documents carrying 94% of the bill. **UC Regents
minutes are long multi-committee documents, which is exactly the shape that
escalates**; quoting a blended Haiku rate for them was the estimating error.

**Operational trap worth keeping:** `fetcher.run_fetcher` slices
`doc_links[:max_docs_per_plan]`, so a low `--max-docs` is consumed by whichever
page is discovered first. Here the already-known minutes ate the whole budget
and the run reported "0 new documents" against 88 links found.

### D18. Restyle to match lazyeconomist.com — **DONE 2026-09-03**

Spec and an "as built" section:
`docs/superpowers/specs/2026-09-02-lazyeconomist-style-design.md`.

Paper ground (`#fbfaf7`), Fraunces headings, Inter Tight body, JetBrains Mono
labels, one burnt-orange accent (`#b8410e`). **Almost all of it is supported
`.streamlit/config.toml` configuration**, not CSS — Streamlit 1.58 exposes
`font`, `headingFont`, `codeFont`, `fontFaces`, `borderColor`, `baseRadius`,
`headingFontSizes` and `chartCategoricalColors`. The CSS left in `app.py` is
four rules over two `data-testid` selectors, so nothing depends on
`.st-emotion-cache-*` names that break silently on upgrade.

**Two silent failures in one step, both now guarded by tests.**
`theme.fontFaces` takes a `.woff2`, not the `fonts.googleapis.com/css2`
stylesheet — the spec caught that one. It missed the second: **Google splits
each family by `unicode-range` and the largest file is usually Cyrillic**, so
picking by size ships a font with no Latin glyphs and the browser quietly falls
back. Caught only because Inter Tight shrank 89,800 → 44,872 bytes when
selection changed to "the block containing `U+0000`".

Fonts are vendored (143,608 bytes, three faces, all SIL OFL) rather than
CDN-loaded, so a rotated `fonts.gstatic.com` URL cannot change how the app
renders. `tests/test_theme_config.py` asserts every key is a real Streamlit
option, every declared font file exists and starts with `wOF2`, and no
`theme.dark` block exists — light-only is deliberate, since `#b8410e` on
near-black is muddy.

**Render must be RESTARTED, not just redeployed:** `config.toml` is read at
process start.

**Not fully reviewed:** Performance and Plans render nothing locally — C2, not
the restyle, confirmed by reproducing it with the config removed. Those two
tabs want looking at on Render once this is merged.

---

### D19. Are the numbers right? — checks, not hope

**Requested by James, 2026-09-03. Not started.** Nothing currently validates a
single extracted figure. Three LLM paths write numbers — the targeted read
(DeepSeek V4 Flash), the summariser (Haiku, escalating to Sonnet) and the CAFR
extractor — and whatever they return is stored, built into
`plan_asset_class_horizon`, and shown. A wrong number looks exactly like a
right one.

**This is worth doing before adding more plans.** Coverage is now 99.5% of the
top 200 by assets; accuracy is unmeasured.

#### The check that already works

Prototyped 2026-09-03 against the live table. Public plans holding the same
asset class over the same quarter earn *similar* returns — dispersion is real
but bounded — so a robust z-score against the peer median finds parse errors
without needing a ground truth.

Group by `(asset_class, horizon_key, period_end)`, keep groups of 8+ plans,
flag on `|x − median| / (1.4826 × MAD) > 6`. **203 groups qualify; 82 of ~7,999
cells flag — about 1%, a reviewable queue.** The top of it is unambiguous:

| flagged | value | peer median | why it is wrong |
|---|---|---|---|
| `persi_id` total 5y 2023Q4 | **110.00%** | 8.23% | not a return |
| `hfrrf` opportunistic 1y 2024Q2 | **94.80%** | 7.70% | not a return |
| `dpfp` private equity 1y 2025Q4 | **68.80%** | 7.83% | implausible, and repeats in 2026Q1 |
| `sers_oh` cash 1y 2024Q4 | **−11.60%** | 5.30% | cash does not lose 11.6% |

Cheap, needs no API calls, and every hit is traceable to a document.

#### The free check being thrown away

**The extractors capture `benchmark_pct` and the build discards it.** Of 66,041
raw observations, **17,959 (27%) carry a benchmark** — 12,790 of 53,474 from
targeted reads, 5,169 of 12,567 from the summariser — and
`plan_asset_class_horizon` has no column for it.

A return beside its own benchmark is the strongest sanity check available and
it costs nothing: a plan reporting +12.4% against a benchmark of +12.0% is
almost certainly read correctly, while +110% against +8% is not. It also makes
excess return displayable, which is what an investment reader actually wants.
Adding the column is part of D20.

#### The rest of the ladder, cheapest first

1. **Range gates.** A quarterly return outside ±40%, an annual outside ±80%, a
   10-year annualised outside ±25%, an allocation weight outside 0–100. These
   catch index levels, market values and basis points misread as percentages.
2. **Allocation weights should sum to ~100%** per plan and date. A plan summing
   to 60% or 180% has had rows dropped or double-counted, and neither is
   visible today.
3. **Temporal continuity.** A plan's 10-year annualised figure cannot move 20pp
   in a quarter — the window barely changed. Large jumps in long horizons are
   parse errors nearly every time.
4. **Cross-source agreement.** Where two of the three sources cover the same
   `(plan, asset_class, horizon, period_end)`, disagreement beyond ~0.5pp is a
   flag. `pick_best_per_cell` silently prefers one today; the disagreement it
   resolves is evidence and should be recorded, not dropped.
5. **CAFR as ground truth.** CAFRs are audited. Where a targeted read and a
   CAFR cover the same plan-year, the CAFR wins and the delta measures how much
   the LLM path can be trusted — the only place a real error *rate* can be
   computed rather than an anomaly count.
6. **A sampled human audit.** Twenty random cells per quarter, opened at the
   stored `DocumentSectionRead.offset` in the retained PDF and checked by eye.
   **PDF retention makes this possible for the first time** — every figure is
   now traceable to a byte range of a document we still hold.

#### Where it should live

A `scripts/check_performance.py` that runs the whole ladder and writes results
to a table, plus an Admin tab showing the queue newest-first. Not a test:
these are data findings, not code failures, and a red CI run is the wrong
channel for "one plan's private equity looks odd this quarter". Wire it into
the daily pipeline **after** the derived-data rebuild, on the same
`!cancelled()` footing as the other post-steps.

Two rules worth fixing now so this stays honest. **Never auto-delete a flagged
figure** — flag, show, let a human decide; a silent delete is the same failure
as a silent wrong number, minus the evidence. And **record the check that
fired**, so a cell cleared once is not re-flagged forever.

---

### D20. Make the data usable — by charts, and by an agent

**Requested by James, 2026-09-03. Not started.** Closely tied to D19: *you
cannot check what you cannot query*, and today the atomic facts are not
queryable at all.

#### What is actually wrong

**The atomic fact has nowhere to live.** One observation — a plan, an asset
class, a horizon, a period, a number, a source, and the document window it came
from — is currently spread across:

- `summaries.performance_data` — **TEXT**, a JSON array
- `document_section_read.returns_json` — **TEXT**, a JSON array
- `cafr_performance` / `cafr_allocation` — properly tabular, but CAFR-only

TEXT, not JSONB, so **none of it is queryable in SQL**. Every consumer parses
JSON in Python. `scripts/build_performance_view.py` reads all three, maps asset
classes, classifies horizons, picks a winner per cell, and writes
`plan_asset_class_horizon` — which is then **dropped and recreated** on every
shape change.

The result: **66,041 raw observations collapse to 7,999 horizon cells.** Much
of that loss is legitimate — superseded documents, `pick_best_per_cell`, the
2025 staleness cutoff, asset classes with no canonical mapping — but *none of
it is recoverable*, because the losers were never stored as rows. The only
durable record is a JSON blob nobody can query and a derived table that gets
dropped.

That is why D19 is hard, why `benchmark_pct` was discardable without anyone
noticing, and why an agent has nothing to talk to.

#### The proposal: an observation fact table

One immutable row per extracted number. Long format, never dropped:

```
performance_observation
  id, plan_id, document_id
  asset_class_raw          what the document said
  asset_class              canonical, nullable when unmapped
  horizon_key, period_label, period_end, as_of_date
  return_pct, benchmark_pct
  source                   targeted_read | summariser | cafr
  offset                   window in documents.extracted_text, nullable
  model, extracted_at
  UNIQUE(document_id, source, asset_class_raw, period_label)
```

Four things follow, and they are the whole point:

- **D19's checks become SQL.** Peer dispersion, range gates, cross-source
  agreement and weight sums are all one query over one table instead of a
  Python pass over parsed blobs.
- **`benchmark_pct` survives**, so excess return is displayable and the best
  free check is available.
- **`plan_asset_class_horizon` keeps being derived and droppable** — that
  design is right for a view — but the facts underneath stop being destroyed
  with it. Rebuilding stops being lossy.
- **Unmapped asset classes become visible.** `asset_class_raw` alongside the
  canonical value turns "13 classes" into a measurable mapping gap rather than
  silent loss.

Build it the project's way: **add the model class and run `init_db()`** — no
`ALTER TABLE`, per CLAUDE.md — then a one-off backfill script that parses the
existing blobs. The blobs stay as provenance; nothing is deleted.

An allocation equivalent (`allocation_observation`, with `weight_pct` and
`target_pct`) is the same shape and the same argument.

#### Then, for agents

Only worth doing **after** the fact table exists; an agent over three TEXT
blobs and a droppable view is a worse version of the same problem.

- **A read-only MCP server** over Neon is the natural fit, exposing a handful
  of intentional tools — `plans`, `performance(plan, asset_class, horizon,
  period)`, `allocation`, `search_documents`, `document_window(document_id,
  offset)` — rather than arbitrary SQL. The last one matters: it lets an agent
  quote the source text behind a number instead of asserting it.
- **Not a general SQL endpoint.** `documents.extracted_text` is deferred and
  gzipped precisely because loading it in bulk exhausted Neon's transfer quota
  on 2026-08-25. An agent with `SELECT *` would repeat that within a day.
- **A tidy export** — one Parquet or CSV of the fact table, rebuilt daily and
  published — covers most analytical uses with no service to run, and is the
  cheapest thing that makes the data reusable. Worth doing first.
- The **FastAPI service was removed on 2026-08-16** as unmaintained. Anything
  new here should justify why it will not go the same way; the export needs no
  uptime, which is the argument for starting there.

#### Suggested split

1. `performance_observation` + backfill + `benchmark_pct` carried through.
2. D19's checks as SQL over it, plus the Admin queue.
3. Daily Parquet/CSV export.
4. MCP server, only if an agent is actually going to consume it.

---

## Suggested order

Everything that was blocking something else has landed. A, B, C1, D1–D18 and
E1 are done or decided; what follows is improvement, and the first item is the
only one holding anything back.

### 0. Merge to master and restart Render

**Ten commits sit on `worktree-pdf-retention` and none of it is live.** That is
the restyle (D18), UCRP and APERS (D17), the SDCERS fix (D14), the horizon
columns, `--reread` and the credential preflight (D16).

**Restart, do not merely redeploy** — `.streamlit/config.toml` is read at
process start, so a redeploy into the same container will not pick up the
theme. Then look at the Performance and Plans tabs on Render: C2 made that
review impossible locally, and the wide horizon table is the change most at
risk from a restyle.

Do this before A3's visibility change, so a Render clone failure cannot be
confused with a bad deploy.

### 1. The observation fact table (D20, part 1)

**Do this before D19, and before adding plans.** One immutable row per
extracted number, replacing three TEXT blobs nothing can query. It is the
prerequisite for every check in D19, it stops `benchmark_pct` — present on 27%
of 66,041 observations — being discarded at build time, and it means rebuilding
the derived view stops destroying the facts underneath it.

Add the model class, run `init_db()`, backfill from the existing blobs. No
`ALTER TABLE`. No API calls.

### 2. The checks (D19)

Once the facts are queryable the checks are SQL. Start with the two that are
already proven or free:

- **Peer dispersion** — prototyped 2026-09-03, flags 82 of ~7,999 cells at
  robust-z > 6, and the top of the queue is unambiguous garbage (a 110% 5-year
  return, cash losing 11.6% in a year).
- **Return against its own benchmark** — free, and available the moment the
  fact table carries it.

Then range gates, weight sums, temporal continuity, cross-source agreement, and
a sampled human audit against the retained PDFs. Flag and show; never
auto-delete.

### 3. Refresh `aum_billions` from PPD

The highest-value item left, because it is **silently wrong everywhere** rather
than missing in one place. D15 measured a median ratio of 0.881 across 32
hand-verified pairs — 26 of 32 understated, one-directional, a vintage lag
rather than rounding. Philadelphia −37%, Connecticut Teachers −35%,
CalPERS −11%.

**CLAUDE.md's "8.5% of tracked AUM" is computed from this column**, so it
inherits the error, unevenly. Every AUM-weighted claim in the docs is
approximate until this is fixed.

The work: a hand-checked `plan_id` → `ppd_id` map stored in the registry, then
`scripts/ppd_coverage.py` refreshes the figure on every run. **Deliberately not
automated** — three matcher attempts failed in one afternoon, one of them
hiding a $110.8B plan behind a shared word. Free, no API calls.

### 4. C2, the local hang

Now pinned to an interaction rather than a page: renders once, then a tab click
never completes its rerun, nothing in the log. See the entry. Next probe is
`py-spy dump` against the wedged process — a thread parked in psycopg would
confirm or kill the held-session theory in one shot. Development friction only,
but it has now cost a review.

### 5. OCR model A/B

`extractor.py:211` pins vision OCR to Sonnet, and it is **the only expensive
model path left** — the targeted read runs DeepSeek V4 Flash and the summariser
routes to Haiku. Haiku 4.5 has vision and is far cheaper, but OCR is exactly
where a cheap model degrades quietly, so this wants a 5-document comparison,
not a swap. Only ~5 documents are affected, so the prize is small and the
downside of getting it wrong is silent corruption of the archive.

### 6. `wsib`'s missing performance data (D3)

Needs a second section search or a cross-section merge. Fails safe — fewer
rows, not wrong ones.

### 7. Small change

- **Three windows failed in the D16 re-read.** Re-runnable for pennies;
  `--reread` skips everything already bought.
- **~50 municipal plans remain uncovered** (D15/D17). They mostly publish
  member forms rather than board packets, and are worth ~1% of assets. Only
  worth it if *document* coverage becomes the goal.
- **Arlington County, Kansas City MO, Hartford CT** 403 from a datacentre IP —
  Mac-mini candidates, ~$6B combined.

### Waiting on James

**A2 Auth0** (needs a tenant and four `AUTH_*` values) and **A4 WAF proxy**
(now worth about two plans, and probably not those two — see the entry).
**A3 is decided**: go private, per the plan in that entry, after the merge.

---

Worth saying plainly: the constraint that shaped this whole document — "we are
throwing away the documents" — is gone. 95.6% of the corpus is archived, every
future fetch is retained, and extraction no longer has to happen on the machine
that did the fetching. What remains is improvement rather than repair.

And the newer lesson, earned twice in one day: **this document's entries are
dated observations.** A3 asked for weeks whether to go public when it already
was; D14 recorded SDCERS as blocked when the "block" was a spinner nobody had
decoded. Both were one command away from being checked. Re-verify before acting.
