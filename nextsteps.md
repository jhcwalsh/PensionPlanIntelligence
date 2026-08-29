# Next steps

**As of 2026-08-29.** Working doc. Supersedes the 2026-08-19 migration
edition (in git history at `e66d56d`) — that migration is complete.

Specs: `docs/superpowers/specs/2026-08-16-low-maintenance-app-design.md`,
`docs/superpowers/specs/2026-08-19-portal-readiness-design.md`.

---

## Done (2026-08-21 → 2026-08-29)

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
- **`wsib` is not this bug.** Its "INVESTMENTS" section (pages 9-15) is
  *correctly* located — the document is structured INTRODUCTION/
  INVESTMENTS/FINANCIALS, and the returns table lives inside FINANCIALS →
  Retirement Funds (pages 18-63), not co-located with the allocation
  targets. Real, separate gap, unfixed: needs either a second section
  search or a merge across two locations. Low priority — currently just
  shows fewer rows (5 allocation, 0 performance), not wrong ones.

The 5 `missing_file` CAFRs (PDF gone from disk) are still the E1
PDF-retention gap — unrelated, unfixed, will recur.

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

Everything in sections A1, B1, B2, D1-D5 above is done as of 2026-08-29.
The D3 TOC-collision bug was also checked against `extract_ips.py` (not
exposed — no TOC/section search at all, sends the whole already-short
document) and `extract_cafr_actuarial.py` (shares `_locate_via_toc` but
uses a strict `\bactuarial\s+section\b` pattern; no plan name contains
"actuarial section", confirmed against the DB) — neither is vulnerable.
What's left:

1. **C1 transfer meter** — one dashboard glance, closes the last unverified
   claim. **Only James can see this.**
2. **E1 PDF retention (R2 PDF store)** — the root cause behind the 5
   `missing_file` CAFRs and the 450 truncated-at-150k documents. Worth
   doing once rather than re-diagnosing its symptoms again next time a
   PDF ages off disk.
3. **wsib's missing performance data** (new, D3) — low priority, needs a
   second section search or a cross-section merge, not urgent since it
   fails safe (fewer rows, not wrong ones).
4. **A2 Auth0**, **A3 public repo**, **A4 WAF proxy** — decisions only
   James can make, no urgency on any of them.
