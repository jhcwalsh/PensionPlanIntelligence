# Performance data coverage: design

**Date:** 2026-08-29
**Status:** draft, pending review.
**Context:** The Performance Reports tab (PR #35, #36, #37) currently shows
annual fiscal-year returns from CAFRs for 83 plans, plus true periodic
returns for exactly **one** plan. This spec is about closing that gap.

## Goal

Every U.S. public pension plan publishes its investment performance
somewhere. Today PensionGraph surfaces periodic (non-annual) performance for
one of 148 plans. This spec designs a pipeline that finds it for the rest —
first by mining documents already in the corpus, then, only where that
fails, by discovering it on the plan's own website.

## Decisions taken

| Question | Decision |
|---|---|
| Sequencing | **Mine first, crawl as fallback.** Site-crawl only plans mining can't resolve. |
| Cadence | **Recurring, local.** Windows Task Scheduler, like IPS refresh. |
| Repeat-run cost | **Learn the source.** A plan whose source is known and unchanged costs no LLM calls. |
| Discovery pattern | **Reuse `fetch_ips.py`'s** mine-then-crawl-then-verify shape rather than inventing one. |
| Storage | **Reuse `performance_report_extract`/`_return`.** No new table for the data itself. |
| Truncated documents | **Recover on TOC evidence,** not blanket re-extraction. |
| Computer time | **Not a constraint.** LLM calls are; crawling and re-fetching are free. |
| Build order | **R2 PDF retention lands first.** See §5.1. |

---

## 1. Why the current coverage is one plan

The quarterly extractor (`extract_performance_reports.py`) processes exactly
one plan because that is how many of the 48 `doc_type='performance'`
documents are genuinely periodic fund-performance reports:

| Plan | Docs | What they actually are |
|---|---|---|
| `nycrs_comptroller` | 30 | Genuine — monthly performance reviews, per constituent system |
| `mn_msrs` | 10 | A 457/deferred-comp plan's **investment menu** returns |
| `pera_colorado` | 6 | Capital Accumulation Plans (DC overlay) — same problem |
| `calpers` | 1 | A *Performance, Compensation & Talent Management* **meeting transcript** |
| `dcrb` | 1 | A blank vendor **evaluation form** |

Four of five plans are mistagged. `ALLOWED_PLAN_IDS` exists in that module to
make excluding them a visible, deliberate choice rather than a silent filter.

**The lesson that shapes this spec:** `doc_type` is not trustworthy evidence
of what a document contains. Every stage below verifies content rather than
trusting a label — which is why Stage B gates every candidate through an
LLM check before extraction, and why the extraction prompt itself is allowed
to answer "this isn't a performance report."

## 2. What mining the corpus can actually reach

Reconnaissance on 2026-08-29, sampling 15 plans' most recent `board_pack`
documents:

- **63 of 148 plans (43%) have any `board_pack` document at all.** The rest
  have only `agenda`/`minutes`, which sampled consistently thin — topic lists
  with no embedded figures. This, not 148, is Stage A's realistic ceiling.
- **Of 7 sampled plans with a real board pack, 4 had extractable performance
  figures** — but one of those (SERS PA) turned out to be a CAFR mistagged
  as `board_pack`, already covered by the CAFR extractor. So **3 of 7 are
  genuine new coverage**: PERS-OR, TRS Texas, TRS-GA. That ~43% hit rate on
  plans-with-board-packs is what §9's estimate is built from, and the
  mistagged CAFR is a reminder that some apparent gaps are data-hygiene
  issues rather than real ones.
- **The 3 misses were legitimate** — a meeting notice, a procurement
  announcement, a calendar of meeting dates. Nothing to extract.

Two findings drove design decisions below:

1. **The newest board pack is often not the useful one.** Several plans'
   latest `board_pack` was a notice or calendar while an older one carried
   the data. Checking only the newest would produce false "not found" results
   and push plans into a needless crawl. Stage A therefore checks the most
   recent N.
2. **PERS-OR's board pack is stored truncated at the old 150k cap**, so its
   table may sit past the stored text. See §5.

## 3. Architecture

```
For each plan (148):

  if a known source exists (performance_source row):
      re-fetch that exact source          # free: no LLM
      if content hash unchanged and already extracted:
          done — 0 LLM calls
      else:
          extract (1 call)
          still finds data → update last_verified_at, done
          now finds nothing → mark source stale, fall through

  Stage A — mine the existing corpus:
      take the most recent N board_pack/agenda docs (N=5)
      regex pre-filter each                # free: no LLM
      first passing doc → extract (1 call)
      empty result → try next candidate (cap 3 calls/plan/cycle)
      found → record source_type='board_pack_mining'

  Stage B — fallback discovery (only if Stage A found nothing):
      mine extracted docs for embedded report URLs
      site-crawl seed paths under plan.website
      Haiku-verify each candidate (1 cheap call each)
      qualifying → download, tag doc_type='performance', extract
      found → record source_type='site_url', source_detail=<url>

  neither → no source recorded; retried next cycle
```

### 3.1 Storage

Both stages write into the existing `performance_report_extract` /
`performance_report_return` tables. No new schema for the data itself.

Each row is labelled with the period the source actually reports (`fy`,
`1y`, `3mo`, `fytd`, `fy_<YYYY>`), never a period assumed from context —
the same honesty the Performance tab's two tables already practise by
showing "Period" and "As of" rather than claiming everything is quarterly.
`fund_scope` stays available for multi-fund plans (NYC's five systems);
null elsewhere.

### 3.2 The learned-source registry

One new table, `performance_source` — one row per plan:

| Column | Meaning |
|---|---|
| `plan_id` | PK, one row per plan |
| `source_type` | `'board_pack_mining'` or `'site_url'` |
| `source_detail` | the discovered URL for `site_url`; null for mining |
| `requires_full_refetch` | true when the stored text was truncated (§5) |
| `content_hash` | hash of the last successfully extracted content |
| `last_verified_at`, `last_extract_id` | provenance |

This is what makes repeat runs nearly free, and it is self-learned — the
`site_url` case is functionally what `cafr_url_template` is for CAFRs, except
discovered automatically rather than hand-curated. That distinction matters:
curating 148 URLs by hand is exactly the maintenance burden the
low-maintenance design exists to avoid, and is why IPS discovery was built
this way in the first place.

A source going stale (extraction stops finding data — a plan redesigns its
site, changes its board-pack format) demotes the plan back to full discovery
on the next cycle rather than failing permanently.

## 4. Stage A — mining

**Selection.** The most recent 5 `board_pack`/`agenda` documents, newest
first (see §2, finding 1).

**Regex pre-filter.** Patterns validated during reconnaissance:
`Total Fund|Total Plan|Total Portfolio` near a percentage; `Net/Gross
Return`; `FYTD|QTD|1-Year`; `rate of return`; `Benchmark` near a percentage.

This is **purely a cost filter**. It decides which documents reach Claude,
never what the answer is. Documents with zero hits are skipped without an
LLM call. In reconnaissance it cleanly separated real hits from genuine
non-candidates.

**Extraction.** The first passing document goes to the existing extraction
call shape. That prompt already returns an empty array with a stated reason
when a document isn't a fund-performance report — so a regex false positive
costs one call and self-corrects rather than writing junk data. On empty,
try the next candidate, capped at **3 extraction calls per plan per cycle**
so one messy plan cannot run away with cost. A plan that exhausts the cap
without a result falls through to Stage B.

## 5. Truncated documents

450 documents were extracted under the old 150k `MAX_STORED_CHARS` cap. The
cap is now 2,000,000 (raised once Postgres removed the file-size constraint),
but those rows still hold truncated text — and a board pack's performance
table is exactly the kind of content that sits past 150k.

A board pack's opening pages are its own agenda/table of contents, and that
survives truncation because it is at the front. Stage A uses it to tell
*"no performance data here"* apart from *"data exists, past the cut."*

**Detection.** A document is a truncation suspect when:
- its stored text is at/near the 150k cap (i.e. it was truncated), **and**
- its leading ~3-5k chars contain a performance-agenda-item pattern
  (`Investment Performance`, `Performance Report`, `Quarterly Performance`,
  `Investment Report`), **and**
- the stored body yields no actual figures.

**Response.** Retrieve the full document and re-extract at the current cap,
then re-run Stage A over the full text — reading the stored object from R2,
per §5.1. The plan does **not** fall through to Stage B; it would be
searching for a document already in hand. `requires_full_refetch` is recorded
so future cycles retrieve the full document rather than trusting the stored
text.

**Why evidence-driven.** Only documents whose own TOC says the data is there
get re-extracted, rather than blanket re-processing all 450. Fails safe: if
re-extraction still finds nothing, the plan continues to Stage B as normal.

### 5.1 Sequencing: R2 lands first

This section originally specced re-fetching from the source URL, as a
self-contained workaround, on the reasoning that this spec should not balloon
into the PDF retention project. That reasoning produced the wrong call, and
the decision here supersedes it: **PDF retention (E1) is built first, and §5
above is written assuming it exists.**

Portal spec §2.3 had already reached this conclusion — *"R2 is therefore a
prerequisite for retroactive body search, not a parallel workstream"* — and
its argument applies almost unchanged here. Two weaknesses in the
re-fetch-from-source approach were under-weighted when this spec was drafted:

- **It depends on the source URL still being alive.** A document truncated
  in 2024 whose board has since reorganised its site is simply unrecoverable.
  With R2, re-extraction is a local operation over a stored object.
- **It re-downloads the same PDF indefinitely** — per feature, per cycle,
  forever. Retention pays that cost once.

This is also the second time in two sessions the missing-PDF gap has
surfaced: the five `missing_file` CAFRs in D3 were the first. Two independent
features hitting the same wall is the signal that the sequencing is wrong,
not that each needs its own workaround.

**What this changed above:** §5's detection logic is unaffected — TOC evidence
is still what distinguishes "no data here" from "data past the cut." Only the
*response* changed: read the stored object and re-extract locally, rather
than re-fetching from a source URL that may no longer resolve.

**If R2 slips.** Should retention be deferred after all, §5's response
reverts to source re-fetch and the feature still works — for documents whose
source URL is still alive. That is the fallback, not the plan.

**Scope boundary (still stands).** Even with R2 first, this spec does not own
PDF retention. It consumes it. The retention design — what is stored, where,
lifecycle, cost — belongs in its own spec.

## 6. Stage B — fallback discovery

Mirrors `fetch_ips.py`'s proven pattern rather than inventing one:

1. **Mine for URLs** — scan the plan's already-extracted documents for
   embedded links matching performance-report shapes (`*performance*`,
   `*investment-report*`, `*quarterly*` + `.pdf`), as `discover_ips_urls()`
   does.
2. **Site-crawl** — seed paths under `plan.website` (`/investments`,
   `/performance`, `/financial-reports`, `/investment-reports`), collecting
   PDF candidates. Computer time is free, so this can be thorough.
3. **Verify with Haiku** — a cheap per-candidate gate, like
   `verify_is_ips()`: *is this a fund performance report for this plan?*
   This is what stops a CAFR, an actuarial valuation, or a DC-plan menu from
   being mistaken for one — precisely the mistagging that made four of five
   plans unusable in §1.
4. **Download and tag correctly** as `doc_type='performance'`, then run
   Stage A's extractor over it.
5. **Record the winning URL** so future cycles skip steps 1-3 entirely.

WAF-blocked plans (`data/waf_blocked_plans.json`) are skipped here as
everywhere else — no runner can reach them.

## 7. Cost

The design's central cost property: **the first cycle is the expensive one;
steady state is near zero.**

| Situation | LLM calls |
|---|---|
| Known source, content unchanged | 0 |
| Known source, new content | 1 |
| Stage A resolves it | 1-3 (capped at 3) |
| Stage B needed | 1 verify per candidate + 1 extract |
| Nothing found | Stage A's cap + Stage B's candidates |

Computer time — crawling, re-fetching, hashing — is unconstrained by
decision, and is deliberately used to avoid LLM calls wherever it can
substitute for one.

## 8. Error handling and testing

Failures are **per-plan and non-fatal**: one plan's dead site or malformed
PDF must not stop the run, matching how the CAFR and IPS extractors already
behave.

`PERF_MODE=mock` short-circuits LLM calls for tests, following the existing
`IPS_MODE` / `LLM_MODE` / `INSIGHTS_MODE` convention. Note the codebase
already has two independent mock flags for unrelated subsystems; this adds a
third rather than overloading an existing one.

Tests cover:
- the regex pre-filter's separation of real documents from junk (using the
  actual reconnaissance samples — a procurement notice and a meeting
  calendar must not pass);
- source-registry transitions: learn → reuse → go stale → re-discover;
- "extraction returns empty → try next candidate → fall through to Stage B";
- truncation detection: TOC evidence present vs. absent, and that absent
  evidence does *not* trigger a re-fetch.

Live verification against a handful of real plans before wiring any
scheduler.

## 9. Expected outcome

Honest estimate, from §2's reconnaissance: Stage A can plausibly reach
**25-35% of plans** (those with substantive board packs containing a
locatable table). Stage B's reach is genuinely unknown until built — the
premise that every plan publishes performance *somewhere* is credible, but
whether it is reliably discoverable by crawl-plus-verify is exactly what
Stage B tests.

This is worth stating plainly: the target is broad coverage, but the design
should be judged after the first full cycle reports real per-plan outcomes,
not assumed to reach 148. The `performance_source` table makes that
measurable — every plan either has a working source or doesn't, visibly.

## 10. Out of scope

- **The R2 PDF store / general PDF retention** (E1, portal spec §2.3) — a
  **dependency, not a parallel item**: it is built first and this spec
  consumes it (§5.1). Its own design — what is stored, where, lifecycle,
  cost — belongs in its own spec.
- **`wsib`'s missing performance data** — its returns live in a *different
  section* of a document already correctly located, needing a cross-section
  merge in the CAFR extractor. Unrelated mechanism; tracked separately.
- **Re-tagging the four mistagged `doc_type='performance'` plans.** They stay
  excluded via `ALLOWED_PLAN_IDS`. If Stage B later finds real reports for
  `mn_msrs` or `pera_colorado`, those arrive as new, correctly-tagged
  documents rather than by reinterpreting the existing bad ones.
