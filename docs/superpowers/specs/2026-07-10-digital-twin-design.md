# Digital Twin of a Pension Plan — Spec & Approach

## Context

The app already ingests the primary documents that describe a public pension plan — CAFRs (structured extracts for 116/148 plans), IPS documents (37 plans, raw text), board materials (3,574 LLM-summarized docs with structured `investment_actions`), RFP records (32 plans), and meeting-recording metadata (63 plans) — but this knowledge is scattered across tabs and tables. A **digital twin** assembles it into a per-plan, versioned, queryable structured state model. Chosen direction (founder, 2026-07-10): structured state model first (no prediction/persona in v1), aimed at asset managers and consultants as sales intelligence — extending the RFP-alerts value proposition. Provenance and staleness honesty are the product: every fact carries its as-of date and source document.

## The twin: nine facets, each fact provenance-wrapped

Fact envelope: `{"v": <value>, "as_of": "2024-06-30", "src": {doc_id, table, row_id, url}}`

| Facet | v0 (existing data, no LLM spend) | v1 (new extraction) |
|---|---|---|
| identity | `plans` row | — |
| policy | `cafr_extract.investment_policy_text` + CAFR targets/ranges | structured IPS parse |
| allocation (+ drift vs targets) | `cafr_allocation` (89 plans) | IPS targets where fresher |
| performance | `cafr_performance` grid (83 plans) | — |
| manager_roster | derived: `aggregate_managers()` (plan-filtered) + `manager_mappings.json` + RFP incumbents/awards | reconciliation table |
| activity_timeline | merged `Summary.investment_actions` + `decisions`, source-linked | — |
| rfp_state | `rfp_records` JSON grouped by lifecycle | — |
| governance_people | weak (consultant mentions) | IPS governance + actuary firm |
| funding_actuarial | empty ("not captured") | CAFR actuarial extraction |

Snapshot metadata: per-facet **completeness score** and **freshness** (coverage is uneven and must be shown, not hidden).

## New tables (SQLAlchemy classes in `database.py`; idempotent `init_db()`, never ALTER)

1. **`twin_snapshots`** — plan_id, built_at, schema_version, `facets` (reuse `GzippedText`), facets_hash, `changed_facets` (diff vs previous — the future alert feed), completeness. Insert **only when hash changes**; prune to last 8 + first-of-month. ~10–20 KB gzipped per snapshot → single-digit MB/yr.
2. **`twin_build_runs`** — mirrors `PipelineRun` bookkeeping.
3. **`ips_extract`** + child **`ips_allocation`** — structured IPS: objectives (target return, risk tolerance), allocation targets/ranges, rebalancing policy, permitted/prohibited assets, governance (consultant name/role, delegation), manager structure, ESG/divestment, effective date.
4. **`cafr_actuarial`** — scalar columns for cross-plan SQL: funded_ratio_pct, UAAL, net_pension_liability, discount_rate_pct, assumed_return_pct, contribution rates, ADC & % contributed, membership counts, actuary_firm, valuation_date.
5. **`plan_manager_roster`** — rebuilt per plan: canonical_name, role (manager/consultant/custodian/actuary), asset_class (raw + canonical), status (current/terminated/unknown), first/last seen, evidence (doc_ids + quotes), confidence. Presented as "observed activity", never authoritative.

All new tables are rebuildable from sources (house pattern: `save_extract` replace-on-reextract).

## Asset-class normalization

Clone the manager pattern exactly: committed `data/asset_class_mappings.json` (mirrors `data/manager_mappings.json`, 1,156 entries) + `scripts/normalize_asset_classes.py` (modeled on `scripts/normalize_managers.py`, Haiku-assisted, idempotent). Canonical taxonomy ~12 classes as a `database.py` constant. **Raw label always kept beside canonical; unmapped surfaces as `unmapped`, never dropped.** Applied at twin-build time, not written into source tables. Cost: <$1.

## New extractors (clone `extract_cafr_investments.py`'s tool-forced-JSON pattern: cached system prompt, `tool_choice`, tenacity, text_hash + prompt_version skip)

- **`extract_ips.py`** — parses `IpsDocument.extracted_text` (gate: `verification_verdict == "yes"`, latest hash per plan). ~$5–10 one-time (37 plans), <$1/mo.
- **`extract_cafr_actuarial.py`** — reuses `locate_investment_section`'s TOC machinery with ACTUARIAL SECTION patterns; GASB 67/68 fallback. ~$10–25 one-time (116 CAFRs), ~$2/mo appended to monthly CAFR workflow.
- **`scripts/build_manager_roster.py`** — mostly deterministic (aggregate_managers + mappings + RFP join; status heuristics); optional Haiku adjudication ~$3–8.

**v1 total: ~$20–45 one-time, $3–5/mo.**

## Assembly & versioning

Materialized snapshots via a new **`twin_builder.py`** (repo root; pure Python, no LLM), one `build_<facet>()` per facet; extract `_cafr_plan_detail_data`'s logic (app.py:2304) into the builder and make the Streamlit page consume snapshots. Two time axes kept distinct: `built_at` (when we knew) vs per-fact `as_of` (source effective date) — the gap is surfaced as staleness. **The builder runs as a step inside the existing GHA daily-pipeline job** (and monthly CAFR job) before the DB push — no new independent DB writer, so no new `db_sync.SyncConflict` surface while the R2 migration is in flight. Rejected alternative: pure views (can't answer "what did we know on date X"; expensive per-request reassembly on Render).

## Serving

- **Streamlit**: `page_plan_twin` via `?plan=<id>` query param (same dispatch pattern as `page_cafr_plan_detail` in `main()`); the thin Plans tab becomes an index (AUM, funded ratio, completeness, freshness) linking to twin pages. Every section shows as-of badges + source deep-links to `page_document_detail`. Rendering-only in app.py.
- **FastAPI**: `api/routes/twins.py` cloned from `api/routes/rfps.py`: `GET /api/v1/twins`, `GET /api/v1/twin/{plan_id}[?as_of=]`, later `/changes?since=`.
- **Alerts (v2)**: classify `changed_facets` (target change, manager termination, funded-ratio move >2pts, RFP status change, new IPS) → extend `insights/daily.py`'s existing trigger machinery first; optional weekly twin brief later via `cycle_common`.

## Phasing (each independently shippable)

| Phase | Scope | Effort | LLM cost |
|---|---|---|---|
| **v0** | snapshots + builder over existing data (funding facet explicitly empty), GHA hook, twin page + Plans index, API endpoints, tests (existing `tmp_db` fixtures) | 3–5 days | ~$0 |
| **v1** | IPS + actuarial extractors, asset-class normalization, manager roster; wire into monthly CAFR workflow + local IPS .bat | 1–2 weeks | $20–45 once, $3–5/mo |
| **v2** | change-alert classification into daily digest; `/changes` endpoint | 3–5 days | ~$0 |

## Top risks

1. **Staleness misrepresentation** — CAFR data is ~1yr old at publication; as-of badges and freshness scoring are non-negotiable UI elements.
2. **Asset-class mapping errors corrupt cross-plan drift** — raw labels retained, human-reviewable committed mapping, visible `unmapped`.
3. **Roster inference noise** — evidence quotes + confidence tiers; "observed activity" framing.
4. **DB size / writer races** — gzip + hash-skip + prune; builder inside existing writer slots only.
5. **app.py bloat (3,900+ lines)** — assembly lives in `twin_builder.py`, app.py renders only.

## Verification

- Unit: builder facets against seeded fixtures (existing `tmp_db` isolation pattern); snapshot hash-skip and prune; extractor schemas in mock mode (`LLM_MODE`-style env gates, matching `IPS_MODE` precedent).
- Integration: build twins for 2–3 real plans locally (`python twin_builder.py calpers`), inspect JSON; render the twin page via `streamlit run app.py`; `GET /api/v1/twin/calpers` round-trip.
- E2E after v1: one plan with all nine facets populated end-to-end (e.g. a plan with CAFR + IPS + RFPs), verify every fact's source link resolves.

## Immediate next step on approval

This is a spec/approach deliverable: commit it as `docs/superpowers/specs/2026-07-10-digital-twin-design.md`, then (when you're ready to build) run the superpowers writing-plans → subagent-driven-development flow for v0 — same machinery as the R2 migration currently awaiting your Task 6 credentials.
