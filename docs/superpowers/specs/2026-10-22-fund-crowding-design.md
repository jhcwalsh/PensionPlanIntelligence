# Fund crowding tool: "who else is in this fund?"

Date: 2026-10-22. Status: design, awaiting James's approval.

## Intent

James plans to build an **AI investment-committee (IC) member**: an agent that
reviews a proposed commitment for a pension plan. This design covers one tool
it will call, not the IC member itself.

The question the tool answers: **who else is in this fund?** Given a fund (for
example "GTCR Fund XV"), which other U.S. public pension plans have committed to
it, for how much, when, and on what evidence. The IC member uses this to spot
**crowding**: many plans piling into the same GPs and vintages share
concentration risk and compete for the same deals and liquidity.

A second, lesser question: **which funds tend to be held together?** That is,
for a given fund, which other funds show up in the same plans' commitments.

## What exists today (recon, 2026-10-22)

| Source | What it holds | Size |
|---|---|---|
| `summaries.investment_actions` (JSON list per document) | `action` (hire/fire/commitment/…), `manager`, free-text `description`, `asset_class`, `amount_millions` | 9,508 actions from 2,593 non-dedup summaries, 2.7 MB |
| …`action == "commitment"` | Usually a named fund: "up to $100M in Audax Direct Lending Solutions Fund III-A, L.P." | 3,091 actions, 98 plans, 84% with an amount |
| `plan_manager_roster` | Plan ↔ **manager** (not fund), status, first/last seen | 3,347 manager rows, 123 plans |
| `data/manager_mappings.json` | Raw manager string → canonical manager (2,481 entries, LLM-built, in git) | |
| `plans.aum_billions` | Plan size | |
| CAFR tables | Asset-class allocation and returns only, no fund holdings | |

The fund name is lost everywhere except the free-text `description` (and
sometimes `manager`). The manager normaliser collapses funds into managers on
purpose ("2024 Golden Bay, L.P." → "Golden Bay"). Some descriptions bundle
several funds into one action ("KKR GII V ($258M), ASF IX ($248M), Actis 6
($165.6M)…").

The overlap is real. A crude regex with no AI found 151 fund names held by 2+
plans, e.g. Francisco Partners VIII (~9 plans across two spellings), GTCR Fund
XV (7), KKR North America Fund XIV (6), TPG Partners X (5), KKR Global
Infrastructure Investors V (5).

Coverage is recent: of ~1,560 dated documents with commitments, ~880 are 2026
meetings and ~190 are 2025. That suits current fundraises and misses older
holdings.

## Goals

1. A durable **plan ↔ fund table** built from the commitments we already have,
   kept fresh as new documents are summarised.
2. A Python function `who_else_is_in(fund, exclude_plan=None)` returning the
   other plans in that fund, with evidence.
3. A Python function `funds_held_alongside(fund, exclude_plan=None)` returning
   funds that co-occur with it across plans.
4. A thin **MCP server** exposing both, so any Claude-based agent (including the
   future IC member) can call them as tools. (James approved this approach.)

## Non-goals (v1)

- Building the IC member.
- A Streamlit tab. Could follow later from the same functions.
- Mining CAFR schedules of private-markets partnerships for legacy holdings.
- Public-markets mandates (these are manager-level and the roster covers them).
- Any judgement of whether something is "crowded". The tool returns facts with
  evidence, and the IC member judges. *(Builder's note: my call.)*
- A new hosted web service. The FastAPI layer was removed on 2026-08-16 for
  maintenance reasons and should not come back for this.

## Anti-goals

- **A false match is worse than a miss** (James, explicitly). Telling the IC
  member a plan is in a fund when it isn't would corrupt a vote. Every design
  choice below leans to precision: when unsure, don't merge, and say so.
- Reporting "nobody else is in this fund" as if it were proof. Absence in our
  data is weak evidence, and every answer must say what was searched.
- Re-reading PDFs or full extracted text to build this. It is unnecessary
  spend and a Neon transfer risk (see CLAUDE.md, 2026-08-25 quota outage).

## Design

### 1. Table: `fund_commitments`

New SQLAlchemy model in `database.py`, created by `init_db()` (no ALTERs, per
repo convention). One row per **(plan, canonical fund)**, the deduplicated
relationship. A separate evidence list records every action/document that
mentions it, because the same commitment typically appears in an agenda, a
board pack and the minutes.

Fields (names are the builder's):
- `plan_id`, `fund_id` (FK to a funds table or a canonical key), unique together
- `fund_name_raw` (most informative raw spelling seen)
- `manager_canonical` (via `manager_mappings.json`)
- `fund_series` (e.g. "North America Fund"), `fund_number` (e.g. 14), `vehicle`
  (e.g. "A", "Cayman", "Parallel") where present
- `asset_class` (canonicalised as the roster does)
- `amount_millions`: latest **approved** amount if any, else latest proposed
- `status`: `approved` (a board vote or a recorded closing) | `proposed` (on an
  agenda for approval, or a staff recommendation to this board) |
  `pipeline` (consultant pipeline, due-diligence list, "under consideration")
  | `unknown` | `declined`. Only `approved` rows count as holders (see §4).
  *(Builder's note: my call, following the precision anti-goal.)*
- `first_seen`, `last_seen` (meeting dates)
- `evidence`: JSON list of `{document_id, meeting_date, doc_type, url, quote}`
- `match_confidence`: how the fund identity was established (see §3)

Plus a `funds` table (or equivalent) holding one row per canonical fund:
canonical name, manager, series, number, asset class.

### 2. Filling it: extraction from existing action text

A new script (e.g. `scripts/build_fund_commitments.py`):

- Reads `summaries.investment_actions` where `action` is `commitment` (and
  `hire` where the description names a fund), skipping `dedup:` summaries.
  Never reads `documents.extracted_text`.
- Sends descriptions in batches to **Haiku 4.5** with a schema-constrained
  prompt that splits bundled lines into one item per fund and returns raw fund
  name, manager, series, number, vehicle, amount, status. Actions with no
  identifiable fund ("repeat commitment to real estate partnership with
  Almanac") are kept as **manager-only** rows, never guessed into a fund.
- Idempotent and incremental: keyed on the summary/action, so the nightly run
  only processes new summaries. Initial backfill is ~3,100 actions, about $1.
- Respects `LLM_MODE=mock` for tests.
- Runs in `daily-pipeline.yml` after summarisation, behind the existing
  `scripts.preflight_credit` step and with the `!cancelled()` guard used by the
  other derived-data builds.

Forward fix: add a `fund_name` field to the summariser's `investment_actions`
schema so new summaries carry it directly. The extraction step still runs over
everything (it does the splitting and the structured fields), but uses
`fund_name` when present.

### 3. Fund identity (the precision-critical part)

Two raw names are the **same fund** only when all of these agree:
- canonical manager (via the existing `manager_mappings.json`),
- fund series after normalisation (case, punctuation, "Fund", "L.P.", "LP"),
- fund number (roman or arabic, normalised).

Rules:
- **No number never merges into a number.** A raw name without a fund number
  never merges into a numbered fund.
- **Unnumbered funds merge only within 12 months** (James, 2026-10-22). Two
  unnumbered names (open-ended credit, real estate and infrastructure vehicles,
  e.g. "Wellington Emerging Markets Fund") are the same fund when the canonical
  manager and normalised name match exactly **and** the mentions fall within 12
  months of each other. Stored under one manager + name key, and the window is
  applied at query time, anchored on the asking plan's own commitment date (or
  today when there is no asking plan or it has no row). It is anchored rather
  than chained, so A (Jan 2024) and C (Nov 2025) don't link through B
  (Dec 2024). Such matches carry `match_confidence = "unnumbered"` and are
  shown as such. *(The anchoring is my reading of James's rule.)*
- **Different numbers are different funds, always.** KKR NA XIII ≠ XIV.
- **A vintage year counts as the fund number** ("2024 Golden Bay", "Fund
  2025"). Year-labelled series are distinct vintages exactly as numbered ones
  are. *(Builder's note: my call.)*
- **Parallel vehicles** ("Fund III-A", "Fund III (Cayman)", "Parallel Fund III")
  roll up to the main fund for crowding purposes, since they invest pari passu
  in the same deals. The vehicle is kept on the row and shown in results.
  *(Builder's note: my call.)*
- Ambiguous series names (e.g. "Francisco Partners VIII" vs "Francisco Partners
  Agility IV", which are different products from one manager) are decided by a
  committed mappings file, `data/fund_mappings.json`, in the same style as
  `manager_mappings.json`. It is LLM-proposed and human-correctable, and a bad
  merge is fixed by editing one line in git.
- `match_confidence` records whether a row matched on an exact normalised key,
  through the mappings file, as an unnumbered windowed match, or not at all.

**Keeping the mappings up to date** (James, 2026-10-22):
- The nightly run merges **only on an exact normalised key**. Anything else
  stays a separate fund. That is precision-safe, and the cost is a temporary
  miss.
- A manual script (like `scripts/normalize_managers.py`) proposes fuzzy fund
  merges, and new manager spellings for `manager_mappings.json`, as edits to the
  JSON. James reviews the git diff and commits. No LLM-proposed merge reaches
  the IC member unreviewed, and the nightly workflow needs no commit step for
  mappings.

### 4. `who_else_is_in(fund, exclude_plan=None)`

Input is free text. Resolution:
- Exact or mapped match → that fund.
- Several plausible funds (e.g. "KKR North America" with no number) →
  **return the candidate list and no holders.** Never pick one silently.
- No match → empty result plus coverage block.

Returns structured JSON:
- `fund`: canonical name, manager, series, number, asset class
- `holders`: one entry per other plan: plan id/name, plan AUM, amount, % of
  plan AUM, status, first/last seen, vehicle, evidence (document links + short
  quote). **Approved rows only.**
- `pending`: plans where the fund is `proposed` or `unknown`, in their own
  section, flagged. `pipeline` and `declined` rows are not returned.
- `same_family_other_vintages`: the same manager + series at other fund
  numbers, with holders. A **separate, clearly labelled section**, never mixed
  into `holders`. *(Builder's note: my call. Useful crowding context,
  precision-safe because it is labelled.)*
- `manager_only_mentions`: plans that committed to this manager with no fund
  named, in their own section, flagged as unresolved.
- `coverage`: number of plans whose commitments were searched, earliest and
  latest meeting date, `as_of`, and a one-line caveat that absence is not proof.

### 5. `funds_held_alongside(fund, exclude_plan=None)`

For the plans holding the fund, count the other funds those plans also hold.
Return funds held by at least 2 of those plans, ranked by count, with the
co-holding plans listed. Same coverage block. Builder may add a lift-style score
if cheap. Secondary to §4: keep it simple.

### 6. MCP server

A small stdio MCP server exposing the two functions as tools, with descriptions
written for an agent caller (what the tool is for, the precision stance, how to
read `coverage`). It runs locally against Neon via `DATABASE_URL` (loaded by
`database.py`). Queries touch only the new small tables, never deferred text.

The `mcp` SDK is a new dependency. It must **not** go in `requirements.txt`
(that is what Render builds the public web service from). It belongs in
`requirements-pipeline.txt` or a separate requirements file.

## Rollout and sign-off (James, 2026-10-22)

1. Create the new tables and run the backfill straight into Neon. This is
   purely additive: no existing table or row changes.
2. Claude checks a random sample of at least 50 multi-plan matches against
   their evidence quotes and writes a review sheet (a CSV or markdown file with
   fund, plans, and quotes).
3. James skims the sheet. **The nightly step is not added to
   `daily-pipeline.yml` until James OKs it**, because a push to master puts it
   live.

## Success criteria

- Backfill produces fund rows for the large majority of the 3,091 commitments.
  The builder reports the count of fund rows, manager-only rows and dropped
  actions.
- **Precision check before shipping:** hand-review a random sample of at least
  50 multi-plan matches against their evidence quotes, with zero false merges
  allowed. Any false merge is fixed via the mappings or rules and the sample
  re-drawn.
- The known overlaps above (GTCR XV, Francisco Partners VIII, KKR NA XIV,
  KKR GII V…) come back from `who_else_is_in` with the plans listed, and
  Francisco Partners VIII and Francisco Partners Agility IV stay separate.
- Tests pass under `LLM_MODE=mock`, covering identity rules (number mismatch,
  missing number, parallel vehicle, ambiguous input → candidates), dedupe of
  the same commitment across agenda/minutes, and the coverage block.
- The MCP server can be connected to Claude Code locally, and both tools answer
  a real query.

## Left to the builder

Table and column names, the module layout (e.g. a `funds/` package), prompt
wording, batch size, how the funds table and mappings file interact, the MCP
SDK version, and whether co-occurrence uses a lift score.
