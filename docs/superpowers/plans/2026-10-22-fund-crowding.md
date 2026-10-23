# Fund Crowding Tool Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A plan ↔ fund table built from existing board commitments, two query functions ("who else is in this fund?", "what is held alongside it?"), and a thin MCP server so an AI IC member can call them.

**Architecture:** A new `funds/` package. Haiku splits each commitment action's text into per-fund items, cached by action content hash (`fund_action_extracts`). A deterministic rebuild turns the cache plus summaries into `funds` and `fund_commitments`, applying strict identity rules and a git-reviewed alias file. Query functions read only those two tables plus `plans`. `funds/mcp_server.py` wraps them for stdio MCP.

**Tech Stack:** Python 3.12, SQLAlchemy (Neon Postgres / SQLite in tests), anthropic 0.125.0 (Haiku 4.5 via `summarizer._get_client()`), `mcp` SDK (FastMCP), pytest with `LLM_MODE=mock`.

**Spec:** `docs/superpowers/specs/2026-10-22-fund-crowding-design.md`

## Global Constraints

- A false match is worse than a miss. When unsure, don't merge.
- Never read `documents.extracted_text`. Source is `summaries.investment_actions` only.
- New tables via SQLAlchemy models + `init_db()`; no ALTER TABLE.
- Only `approved` rows are holders. `proposed`/`unknown` → `pending`. `pipeline`/`declined` never returned.
- Different fund numbers are different funds. A vintage year counts as the number. No number never merges into a number.
- Unnumbered funds match only when the mention is within 365 days of the anchor (the asking plan's own `last_seen` for that fund, else today). Anchored, not chained. Undated unnumbered rows are never holders.
- Parallel vehicles (letter suffix, Cayman, Parallel, Offshore, Onshore, Feeder, Delaware, Lux, SCSp) roll up to the main fund; vehicle kept and shown.
- Nightly merges only on exact normalised key. Fuzzy merges come only from `scripts/propose_fund_mappings.py`, edited into `data/fund_mappings.json` for James to review in git.
- `mcp` must not go in `requirements.txt` (Render builds from it). Use `requirements-mcp.txt`.
- The tool states facts and coverage. It never labels anything "crowded".
- Model: `summarizer.MODEL_HAIKU` (`claude-haiku-4-5-20251001`), via the instrumented `summarizer._get_client()` so spend lands in `api_usage`.

## Review Focus

1. The same commitment appears in an agenda ("up to $100M", proposed) and in the minutes ("$75M", approved): expect one holder row, `approved`, amount 75, both documents in evidence. → Task 4 `test_rebuild_dedupes_across_documents`.
2. Missing or non-manager manager ("Various", null) with a generic fund name ("Fund III"): must never merge across plans. → Task 1 `test_fund_key_none_without_manager`, Task 4 `test_rebuild_unknown_manager_never_merges`.
3. One action bundling several funds with a single total: per-fund amounts only, the total never copied onto each fund. → Task 4 `test_rebuild_bundle_does_not_copy_total`.
4. Query without a number ("KKR North America") when numbered funds exist: return candidates, no holders. → Task 5 `test_resolve_ambiguous_returns_candidates`.
5. Plan with null `aum_billions`: `pct_of_plan_aum` is null, no crash. → Task 5 `test_holder_pct_null_when_aum_missing`.

---

### Task 1: Fund-name parsing and identity keys

**Files:**
- Create: `funds/__init__.py` (empty), `funds/identity.py`
- Test: `tests/test_fund_identity.py`

**Interfaces:**
- Produces:
  - `norm_tokens(s: str | None) -> list[str]`: lowercase, `&`→`and`, strip punctuation except hyphen inside tokens, drop parentheticals' brackets but keep their words, split on whitespace.
  - `norm_manager(name: str | None) -> str`: `norm_tokens` joined by space with legal suffixes (`inc`, `llc`, `lp`, `l.p.`, `ltd`, `co`, `corp`, `corporation`, `company`, `group`, `management`, `capital management`, `asset management`, `advisors`, `advisers`, `partners lp`) removed from the end. Returns `""` for None/blank.
  - `@dataclass(frozen=True) ParsedFund: series: str; number: int | None; vehicle: str | None`
  - `parse_fund_name(raw: str, manager: str | None) -> ParsedFund`
  - `fund_key(manager_norm: str, parsed: ParsedFund) -> str | None`: `f"{manager_norm}|{parsed.series}|{parsed.number or ''}"`, or `None` when `manager_norm == ""` or (`parsed.series == ""` and `parsed.number is None`).
  - `roman_to_int(tok: str) -> int | None`: I/V/X/L only, valid form, 1–50.

`parse_fund_name` rules:
1. Tokens via `norm_tokens`. Split tokens of the form `iii-a`/`15-b` into number + vehicle letter.
2. Remove a leading manager prefix: the longest leading run of tokens equal to the leading tokens of `norm_tokens(manager)` (try also with the legal-suffix-stripped manager).
3. Drop stopwords anywhere: `fund`, `funds`, `the`, `lp`, `l.p.`, `llc`, `scsp`, `l.p`, `inc`, `ltd`.
4. Number = the **last** token that is a year 1990–2039, an arabic integer 1–99, or a roman numeral (`roman_to_int`). A leading year also counts if no later number exists.
5. Tokens after the number: if all are vehicle words (single letter a–h, `cayman`, `parallel`, `offshore`, `onshore`, `feeder`, `delaware`, `lux`, `luxembourg`, `us`, `non-us`, `aiv`), they form `vehicle` (joined by space). Otherwise they stay in series, after a `#` marker token.
6. Vehicle words before the number (`parallel`, `offshore`, `cayman`, `feeder`) move into `vehicle` too.
7. `series` = remaining tokens joined by single spaces.

- [ ] **Step 1: Write the failing tests**

```python
from funds.identity import parse_fund_name, fund_key, norm_manager, ParsedFund

def test_manager_prefix_and_stopwords_removed():
    assert parse_fund_name("GTCR Fund XV, L.P.", "GTCR") == ParsedFund("", 15, None)
    assert parse_fund_name("KKR North America Fund XIV", "KKR") == ParsedFund("north america", 14, None)

def test_spellings_of_same_fund_share_key():
    m = norm_manager("Francisco Partners")
    a = fund_key(m, parse_fund_name("Francisco Partners VIII", "Francisco Partners"))
    b = fund_key(m, parse_fund_name("Francisco Partners Fund VIII, L.P.", "Francisco Partners"))
    assert a == b

def test_agility_is_a_different_series():
    m = norm_manager("Francisco Partners")
    a = fund_key(m, parse_fund_name("Francisco Partners VIII", "Francisco Partners"))
    b = fund_key(m, parse_fund_name("Francisco Partners Agility IV", "Francisco Partners"))
    assert a != b

def test_different_numbers_differ():
    assert parse_fund_name("KKR North America Fund XIII", "KKR").number == 13

def test_vintage_year_is_number():
    assert parse_fund_name("2024 Golden Bay, L.P.", "Golden Bay").number == 2024

def test_parallel_vehicles_roll_up():
    p = parse_fund_name("Audax Direct Lending Solutions Fund III-A, L.P.", "Audax")
    assert (p.series, p.number, p.vehicle) == ("direct lending solutions", 3, "a")
    assert parse_fund_name("Advent GPE XI (Cayman)", "Advent").vehicle == "cayman"

def test_trailing_non_vehicle_words_stay_in_series():
    p = parse_fund_name("ASF IX Infrastructure B", "Ardian")
    assert p.number == 9 and "infrastructure" in p.series

def test_unnumbered_fund():
    assert parse_fund_name("Wellington Emerging Markets Fund", "Wellington") == ParsedFund("emerging markets", None, None)

def test_fund_key_none_without_manager():
    assert fund_key("", parse_fund_name("Fund III", None)) is None
    assert fund_key("acme", ParsedFund("", None, None)) is None
```

- [ ] **Step 2: Run** `LLM_MODE=mock pytest tests/test_fund_identity.py -q`. Expected: FAIL (module missing).
- [ ] **Step 3: Implement `funds/identity.py`** per the rules above.
- [ ] **Step 4: Run tests.** Expected: PASS.
- [ ] **Step 5: Commit** `funds/__init__.py funds/identity.py tests/test_fund_identity.py` (+ SESSION_NOTES.md one-liner).

---

### Task 2: Tables

**Files:**
- Modify: `database.py` (add three models after `PlanManagerRoster`; update the "15 tables" count only if a docstring states it)
- Test: `tests/test_fund_models.py`

**Interfaces:**
- Produces:
  - `FundActionExtract` (`fund_action_extracts`): `action_hash` String(40) PK, `items_json` Text (JSON list), `model` String, `created_at` DateTime(tz).
  - `Fund` (`funds`): `id` Integer PK, `fund_key` String unique not null, `canonical_name` String, `manager_canonical` String, `series` String, `number` Integer null, `asset_class` String.
  - `FundCommitment` (`fund_commitments`): `id` PK, `plan_id` FK plans, `fund_id` FK funds **nullable** (null = manager-only row), `manager_canonical` String, `fund_name_raw` String, `vehicle` String, `asset_class` String, `amount_millions` Float, `status` String not null, `first_seen` String(10), `last_seen` String(10), `evidence` Text (JSON list of `{document_id, meeting_date, doc_type, url, quote}`), `match_confidence` String (`exact` | `alias` | `unnumbered` | `manager_only`), `derived_at` DateTime(tz). Indexes on `plan_id`, `fund_id`, `manager_canonical`. `UniqueConstraint(plan_id, fund_id)`.

- [ ] **Step 1: Test** `test_fund_tables_created`: after the autouse fixture's `init_db()`, `inspect(database.engine).get_table_names()` contains all three, and a `FundCommitment` with `fund_id=None` can be inserted twice for one plan.
- [ ] **Step 2: Run, expect FAIL. Step 3: add models. Step 4: run, expect PASS.**
- [ ] **Step 5: Commit.**

---

### Task 3: Haiku extraction, and `fund_name` in the summariser schema

**Files:**
- Create: `funds/extract.py`
- Modify: `summarizer.py` (`build_extraction_prompt`: add `"fund_name": "the specific fund or vehicle if one is named, e.g. GTCR Fund XV, else null"` to the `investment_actions` item)
- Test: `tests/test_fund_extract.py`

**Interfaces:**
- Produces:
  - `@dataclass ActionInput: action_hash: str; action: dict; doc_type: str | None; meeting_date: str | None`
  - `action_hash(action: dict) -> str`: sha1 hex of `json.dumps(action, sort_keys=True, ensure_ascii=False)`.
  - `extract_batch(client, batch: list[ActionInput]) -> dict[str, list[dict]]`: keyed by `action_hash`. Each item: `{fund_name: str|None, manager: str|None, amount_millions: float|None, status: "approved"|"proposed"|"pipeline"|"declined"|"unknown", asset_class: str|None}`.
  - `MODEL = summarizer.MODEL_HAIKU`, `BATCH_SIZE = 40`.

Prompt (system, cached): one item per distinct fund named; split bundled lists, giving each fund its own amount if stated, else null; `fund_name` exactly as written or null if no specific fund is named (never invent one); `manager` is the firm managing that fund; status definitions copied from spec §1 (approved = board vote or recorded closing; proposed = on agenda for approval or staff recommendation to this board; pipeline = consultant pipeline, due-diligence list, under consideration; declined; unknown). Doc type and meeting date are passed as context (minutes usually record approvals; agendas usually proposals). Output a JSON object `{"<index>": [items]}`, no prose. `temperature=0`, `max_tokens=8000`. Tolerate a code fence. Unknown indices are ignored, and missing ones are absent from the result (retried next run).

Mock mode (`os.environ.get("LLM_MODE") == "mock"`): no client call. Each action → one item: `fund_name = action.get("fund_name") or action.get("description")`, `manager = action.get("manager")`, `amount_millions = action.get("amount_millions")`, `status = action.get("_mock_status", "approved")`, `asset_class = action.get("asset_class")`.

- [ ] **Step 1: Tests:**
  - `test_mock_mode_one_item_per_action`: two actions → two keys, `fund_name` from description, status approved.
  - `test_action_hash_stable_and_order_insensitive`: dict key order doesn't change the hash.
  - `test_live_response_parsed_by_index`: fake client whose `messages.create` returns content text `'```json\n{"0":[{"fund_name":"A Fund I","manager":"A","amount_millions":5,"status":"approved","asset_class":null}],"7":[]}\n```'` with `LLM_MODE` unset (monkeypatch.delenv). Result has only the hash for index 0.
  - `test_summarizer_prompt_asks_for_fund_name`: `"fund_name"` is in the output of `build_extraction_prompt`.
- [ ] **Step 2: Run, FAIL. Step 3: implement. Step 4: run, PASS.** Also run `LLM_MODE=mock pytest tests/ -q -k summar` to catch prompt snapshot tests.
- [ ] **Step 5: Commit.**

---

### Task 4: Build: collect, extract, rebuild

**Files:**
- Create: `funds/build.py`, `scripts/build_fund_commitments.py`, `data/fund_mappings.json` (`{"aliases": {}, "separate": []}`)
- Test: `tests/test_fund_build.py`

**Interfaces:**
- Consumes: Task 1 (`norm_manager`, `parse_fund_name`, `fund_key`), Task 2 models, Task 3 (`ActionInput`, `action_hash`, `extract_batch`, `BATCH_SIZE`), `twin_builder._load_manager_mappings`, `load_asset_class_mappings`, `canonical_asset_class`, `_parse_json_list`.
- Produces:
  - `collect_actions(session) -> list[tuple[ActionInput, dict]]`: every `commitment` or `hire` action in every summary (dedup summaries included). The dict is `{document_id, plan_id, meeting_date, doc_type, url}`. Selects only the needed columns: `Summary.document_id, Summary.investment_actions, Document.plan_id, Document.doc_type, Document.url, Document.meeting_date, Meeting.meeting_date` (outer join). Date = doc date, else meeting date, as `YYYY-MM-DD` or None.
  - `extract_pending(session, client, limit: int | None = None) -> tuple[int, int]`: distinct hashes not in `fund_action_extracts`, in batches of `BATCH_SIZE`. Writes one cache row per returned hash and commits per batch. Returns `(extracted, failed_batches)`. A failed batch is logged and skipped.
  - `rebuild(session) -> dict`: returns counts `{funds, commitments, manager_only, dropped}`.
  - `load_fund_mappings() -> dict` and `resolve_alias(key: str, aliases: dict) -> tuple[str, bool]` (follows up to 5 hops; bool = alias used).
  - CLI `python -m scripts.build_fund_commitments [--limit N] [--rebuild-only] [--dry-run]`: extract (unless `--rebuild-only`), then rebuild, print counts. Exit 1 if any batch failed (after still rebuilding).

`rebuild` algorithm:
1. For each `(ActionInput, ctx)` with a cache row, for each item:
   - `hire` items with null `fund_name` → dropped.
   - Amount: item amount; if null and the action produced exactly one item, the action's `amount_millions`; else null.
   - Manager raw = item manager or action manager → canonical via manager mappings (raw → canonical; an entry whose `is_manager` is false means no manager) → `norm_manager`.
   - `fund_name` null → manager-only row keyed `(plan, manager_norm)`, `match_confidence="manager_only"`, skipped if `manager_norm == ""`.
   - Else `key = fund_key(manager_norm, parse_fund_name(fund_name, manager_raw))`. `None` → dropped. Then `resolve_alias`.
2. Group by `(plan_id, key)`. Status: `approved` if any evidence approved, else the status of the latest-dated evidence (undated sorts first). Amount: latest-dated approved amount, else latest non-null. `first_seen`/`last_seen` from dated evidence. Evidence de-duplicated on `document_id`, `quote` = the action's description truncated to 300 chars. `vehicle` = most common non-null. `match_confidence` = `alias` if an alias was used, `unnumbered` if number is None, else `exact`.
3. `Fund` per key: `canonical_name` = most common raw `fund_name` (tie → longest), `manager_canonical` = most common canonical manager display name, `series`/`number` from the key, `asset_class` = most common canonical asset class.
4. In one transaction: delete all `FundCommitment` then all `Fund`, insert new. Tests run it twice to show idempotence.

- [ ] **Step 1: Tests** (seed `Plan`, `Document`, `Summary` rows directly; mock-mode extraction):
  - `test_rebuild_dedupes_across_documents`: plan P, agenda (2026-03-01, `_mock_status: proposed`, amount 100) + minutes (2026-03-20, approved, amount 75), both "GTCR Fund XV" with manager GTCR → one commitment, status approved, amount 75, evidence has 2 docs, first_seen 2026-03-01, last_seen 2026-03-20.
  - `test_two_plans_same_fund_two_spellings`: P1 "Francisco Partners VIII", P2 "Francisco Partners Fund VIII, L.P." → one `Fund`, two commitments.
  - `test_rebuild_unknown_manager_never_merges`: P1 and P2 both "Fund III" with manager "Various" (mapping `is_manager: false`; monkeypatch the mapping loader) → zero `Fund` rows.
  - `test_rebuild_bundle_does_not_copy_total`: fake cache row for one action with two items with null amounts, action amount 500 → both commitments have amount None.
  - `test_hire_without_fund_dropped_commitment_without_fund_manager_only`.
  - `test_alias_applied`: `fund_mappings.json` aliases key A → key B (monkeypatch `load_fund_mappings`) → one fund, `match_confidence == "alias"`.
  - `test_extract_pending_skips_cached_and_dedup_text`: two summaries with an identical action → one cache row; a second run extracts 0.
  - `test_rebuild_idempotent`: two rebuilds → same counts.
- [ ] **Step 2: Run, FAIL. Step 3: implement. Step 4: run, PASS.**
- [ ] **Step 5: Commit.**

---

### Task 5: Query functions

**Files:**
- Create: `funds/query.py`
- Test: `tests/test_fund_query.py`

**Interfaces:**
- Consumes: Task 1 `norm_tokens`, `roman_to_int`; Task 2 models; `Plan`.
- Produces:
  - `resolve_fund(session, query: str) -> tuple[Fund | None, list[Fund]]`
  - `who_else_is_in(fund: str, exclude_plan: str | None = None, session=None, today: date | None = None) -> dict`
  - `funds_held_alongside(fund: str, exclude_plan: str | None = None, session=None, today: date | None = None, limit: int = 25) -> dict`
  - `coverage(session) -> dict`: `{plans_searched, earliest_meeting, latest_meeting, as_of, caveat}`; `caveat` = "Built from board commitments in meeting documents we hold. Absence from this list is not evidence a plan is not invested."
  - When `session` is None, functions open and close their own `database.get_session()`.

Resolution: the search string for a fund is `norm_tokens(manager_canonical + " " + canonical_name)` with every number token converted to arabic. Query tokens are converted the same way.
1. Case-insensitive exact `canonical_name` match, or an exact `fund_key` match → that fund.
2. Else candidates = funds whose search tokens ⊇ query tokens, where the query's number token (if any) must equal `number`. Exactly one candidate and the query has a number → that fund. Exactly one candidate and the query has no number and the candidate has none → that fund. Otherwise return `(None, candidates[:10])`.

`who_else_is_in` result:
```
{"query", "fund": {name, manager, series, number, asset_class} | None,
 "candidates": [{name, manager, number}],     # non-empty only when unresolved
 "holders": [...], "pending": [...],
 "same_family_other_vintages": [{"fund": {...}, "holders": [...]}],
 "manager_only_mentions": [...],
 "coverage": {...}}
```
Holder entry: `{plan_id, plan_name, plan_aum_billions, amount_millions, pct_of_plan_aum, status, first_seen, last_seen, vehicle, match_confidence, evidence}`. `pct_of_plan_aum = round(amount / (aum*1000) * 100, 3)`, or None if either is missing.
- holders: `status == "approved"`, `plan_id != exclude_plan`. For an unnumbered fund, only rows whose `[first_seen, last_seen]` interval is within 365 days of the anchor (`exclude_plan`'s own row `last_seen` for this fund, else `today`). Undated rows are moved to `pending` with `"reason": "undated"`.
- pending: `proposed`/`unknown` rows (plus the undated case above), excluding `exclude_plan`.
- same_family_other_vintages: only when `fund.number` is not None; funds with the same `manager_canonical` and `series` and a different non-null number; approved holders only, minus `exclude_plan`.
- manager_only_mentions: `fund_id IS NULL`, same `manager_canonical`, status not in (`pipeline`, `declined`), minus `exclude_plan`.
- Sort holders by `last_seen` desc.

`funds_held_alongside`: H = approved holder plan ids per the same rules (minus `exclude_plan`). For every other fund with approved commitments in H: count distinct plans; keep count ≥ 2; sort by count desc, then name. Each: `{fund: {...}, plans: [...], count, share_of_holders: round(count/len(H), 3)}`. Returns `{query, fund, holders_considered: len(H), alongside: [...], coverage}`.

- [ ] **Step 1: Tests** (seed `Fund`/`FundCommitment`/`Plan` directly):
  - `test_holders_exclude_asking_plan_and_pending_split`
  - `test_resolve_ambiguous_returns_candidates`: funds KKR NA XIII and XIV; query "KKR North America" → fund None, 2 candidates, holders [].
  - `test_resolve_roman_and_arabic_equivalent`: "KKR North America Fund 14" resolves to XIV.
  - `test_other_vintages_separate`: asking about XIV, a holder of XIII appears only under `same_family_other_vintages`.
  - `test_unnumbered_window_anchored`: asking plan's row last_seen 2026-06-01; others at 2025-07-01 (in), 2025-01-01 (out), 2027-05-01 (in); an undated one goes to pending with reason undated.
  - `test_unnumbered_anchor_defaults_to_today`
  - `test_holder_pct_null_when_aum_missing`
  - `test_pipeline_and_declined_never_returned`
  - `test_funds_held_alongside_counts_cooccurrence`: 3 plans in X; Y held by 2 of them; Z by 1 → only Y, count 2, share 0.667.
  - `test_coverage_block_present_and_counts_plans`
- [ ] **Step 2: Run, FAIL. Step 3: implement. Step 4: run, PASS.**
- [ ] **Step 5: Commit.**

---

### Task 6: MCP server

**Files:**
- Create: `funds/mcp_server.py`, `requirements-mcp.txt` (`-r requirements.txt` + pinned `mcp==<version installed>`)
- Modify: `CLAUDE.md` (short "Fund crowding tool" section: what it is, build command, MCP register command, precision stance, review flow for `fund_mappings.json`)

**Interfaces:**
- Consumes: Task 5 `who_else_is_in`, `funds_held_alongside`.
- Produces: `python funds/mcp_server.py` (stdio). Tools `who_else_is_in(fund: str, exclude_plan: str | None = None) -> dict` and `funds_held_alongside(fund: str, exclude_plan: str | None = None) -> dict`, with descriptions written for an agent: purpose, approved-only holders, separate sections, candidates mean "re-ask with an exact name", read `coverage` before treating absence as meaningful. The file inserts the repo root on `sys.path` so it runs from any cwd.

- [ ] **Step 1:** `pip install mcp` into `.venv` and record the version in `requirements-mcp.txt`.
- [ ] **Step 2:** Implement with `from mcp.server.fastmcp import FastMCP` (or the installed SDK's equivalent).
- [ ] **Step 3: Verify** with a short stdio client script (scratchpad) that lists tools and calls `who_else_is_in("GTCR Fund XV")` against Neon after Task 8's backfill. Expected: 2 tools listed and a JSON result with holders.
- [ ] **Step 4: Commit.**

---

### Task 7: Mapping proposals (the human-reviewed path)

**Files:**
- Create: `scripts/propose_fund_mappings.py`
- Test: `tests/test_propose_fund_mappings.py`

**Interfaces:**
- Consumes: Task 4 `load_fund_mappings`, the cache, `scripts.normalize_managers._classify_batch` / `_load_existing` / `_save`.
- Produces: CLI `python -m scripts.propose_fund_mappings [--dry-run]`. It changes **only** the JSON files and never the DB.
  1. Manager strings seen in cache items but absent from `manager_mappings.json` → classified via `normalize_managers._classify_batch` in batches of 60 and merged.
  2. Candidate fund pairs: keys with the same manager and number but different series, where one series' tokens are a subset of the other's. Excluded: pairs already in `aliases` or `separate`. Each pair goes to Haiku: "Are these the same fund?" → `{"same": bool, "confidence": "high"|"low"}`. Only `same && high` → `aliases[longer_key] = shorter_key`. Everything else → `separate`.
  - `candidate_pairs(keys: list[str], mappings: dict) -> list[tuple[str, str]]`
  - Mock mode: no calls, and every pair → `separate`.

- [ ] **Step 1: Tests:** `test_candidate_pairs_same_manager_number_subset_series`, `test_candidate_pairs_skip_known`, `test_mock_mode_writes_separate_only` (monkeypatch the paths to tmp).
- [ ] **Step 2–4: FAIL → implement → PASS. Step 5: Commit.**

---

### Task 8: Live backfill, precision check, review sheet

**Files:**
- Create: `scripts/fund_review_sheet.py`, output `docs/superpowers/notes/2026-10-22-fund-crowding-review.md`
- Modify: `SESSION_NOTES.md`

- [ ] **Step 1:** `python -c "import database; database.init_db()"` (against Neon; adds three tables).
- [ ] **Step 2:** `python -m scripts.build_fund_commitments --limit 80`, then inspect 10 cache rows by eye against the descriptions.
- [ ] **Step 3:** Full run: `python -m scripts.build_fund_commitments`. Record extracted count, failures, `api_usage` cost, and rebuild counts.
- [ ] **Step 4:** `scripts/fund_review_sheet.py --n 50 --seed 1` samples funds with ≥2 approved holders and writes, per fund, each holder's plan, `fund_name_raw`, amount, date, quote and url.
- [ ] **Step 5:** Claude reviews all 50 against the quotes. Any false merge → fix the parse rule or alias (add a regression test to Task 1/4), rebuild with `--rebuild-only`, redraw with a new seed, and repeat until 50 are clean. Record the outcome at the top of the sheet.
- [ ] **Step 6:** Spot-check the spec's known overlaps (GTCR XV, Francisco Partners VIII vs Agility IV, KKR NA XIV, KKR GII V, TPG Partners X) via `who_else_is_in`.
- [ ] **Step 7:** Run `python -m scripts.propose_fund_mappings` once and leave the JSON diff for James (commit separately so it's reviewable).
- [ ] **Step 8:** Full test suite `LLM_MODE=mock pytest tests/ -q`, then commit the sheet + notes.

---

### Task 9: Nightly wiring (separate commit, not pushed)

**Files:**
- Modify: `.github/workflows/daily-pipeline.yml` (after "Rebuild manager rosters")

```yaml
      - name: Build fund commitments
        if: ${{ !cancelled() }}
        env:
          ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
        run: python -m scripts.build_fund_commitments
```

- [ ] **Step 1:** Add the step and run `LLM_MODE=mock pytest tests/test_deployment_config.py -q`. Expected: PASS.
- [ ] **Step 2:** Commit on its own. **Do not push**: a push to master makes it live, and pushing is James's call.
