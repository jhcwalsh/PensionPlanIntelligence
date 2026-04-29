# RFP Alerts Pipeline

A structured-extraction stage layered on top of the existing
PensionPlanIntelligence pipeline (`fetcher.py` → `extractor.py` →
`summarizer.py`). For every document already in the corpus, it produces
schema-conformant RFP records with provenance back to source page +
verbatim quote, and serves them through a FastAPI read API.

## Architecture

```
                 (existing)                              (new)
fetcher.py  →  extractor.py  →  pension.db  →  rfp/orchestrator.py  →  pension.db
                                                  │   ├── lib/pipeline_diagnostic.py
                                                  │   ├── rfp/relevance.py
                                                  │   ├── rfp/llm.py  (Claude tool-use)
                                                  │   └── lib/schema_validator.py
                                                  ▼
                                              api/main.py  (FastAPI /api/v1/rfps)
```

Tables added to `db/pension.db`:
- `rfp_records` — one row per extracted RFP, JSON body + confidence
- `document_health` — one row per (document, prompt_version)
- `pipeline_runs` — one row per orchestrator invocation

Key invariants:
- Every record validates against `lib/rfp_schema.json`
- `rfp_id` is a deterministic hash → re-running upserts, never duplicates
- `extraction_confidence < 0.70` records exist in DB but are hidden from
  the default API response (use `?include_review=true` to see them)
- A document is processed once per prompt version; bumping
  `RFP_PROMPT_VERSION` re-runs the entire corpus

## Local dev

```bash
# Install deps (existing pipeline + new RFP+API additions)
pip install -r requirements-pipeline.txt

# Initialize / migrate tables (idempotent — adds the new ones)
python -c "import database; database.init_db()"

# Run the RFP pipeline against fixture documents in mock LLM mode
LLM_MODE=mock python -m scripts.run_rfp_extraction

# Serve the API
uvicorn api.main:app --reload --port 8000
curl 'http://localhost:8000/api/v1/rfps' | jq
curl 'http://localhost:8000/api/v1/rfps?year=2024&rfp_type=Consultant' | jq
curl 'http://localhost:8000/api/v1/rfps/stats?year=2024' | jq
```

OpenAPI docs at http://localhost:8000/docs.

## Tests

```bash
LLM_MODE=mock pytest tests/ -q
```

Three layers:
- `tests/unit/` — schema validator, ids, diagnostic, relevance, LLM mock,
  eval harness, database models
- `tests/integration/test_pipeline_e2e.py` — runs the orchestrator
  end-to-end against three fixture text "documents" and asserts the
  resulting DB state (3 records, deterministic ids, schema-valid,
  governance doc → 0 records, idempotent re-run)
- `tests/api/` — FastAPI endpoints via `TestClient`

## Running against real PDFs in production

The Render cron service `pension-rfp-pipeline` runs
`scripts/run_rfp_extraction.py` four times per day. It reads documents
from `pension.db`, opens the PDFs at `Document.local_path` (on the
shared persistent disk), runs the diagnostic, and extracts via the real
Anthropic API (set `ANTHROPIC_API_KEY` in the Render dashboard).

## Adding a new plan

1. Add the plan to `data/known_plans.json` (id, name, materials_url, etc.)
2. `python update_plan_url.py --plan <id> ...` to sync to the DB
3. Run `python pipeline.py <id>` to fetch + extract its documents
4. The next scheduled (or manual) `python -m scripts.run_rfp_extraction`
   processes the new docs automatically

## Eval and CI

- `python -m scripts.run_eval` runs the orchestrator on fixture documents
  and scores against `fixtures/golden_set.jsonl`. Exit non-zero on >2pp
  regression vs. `fixtures/eval_baseline.json`.
- `.github/workflows/test.yml` runs the test suite on every push/PR.
- `.github/workflows/nightly_eval.yml` runs the eval at 02:00 UTC and
  opens a baseline-refresh PR on success.

## Files

| Path | Purpose |
|---|---|
| `lib/rfp_schema.json` | JSON Schema v7 contract for RFP records |
| `lib/schema_validator.py` | jsonschema-based validator |
| `lib/pipeline_diagnostic.py` | Stage-1 PDF quality verdict |
| `lib/eval_harness.py` | Golden-set scorer with field tolerances |
| `rfp/ids.py` | Deterministic rfp_id derivation |
| `rfp/relevance.py` | Page splitter + keyword filter + chunker |
| `rfp/llm.py` | Claude tool-use wrapper + mock mode |
| `rfp/prompts/rfp_v1.md` | Versioned extraction prompt |
| `rfp/orchestrator.py` | Ties the stages together |
| `rfp/alerting.py` | Slack webhook on failures |
| `rfp/logging_setup.py` | structlog JSON config |
| `api/main.py` | FastAPI app entry |
| `api/routes/rfps.py` | `/api/v1/rfps` and `/stats` |
| `api/schemas.py` | pydantic response models |
| `scripts/run_rfp_extraction.py` | cron entry point |
| `scripts/run_eval.py` | nightly eval CI entry point |
| `scripts/seed_llm_fixtures.py` | regenerate canned LLM responses |
| `fixtures/documents/*.txt` | test fixture "documents" |
| `fixtures/llm_responses/*.json` | canned LLM responses keyed by hash |
| `fixtures/golden_set.jsonl` | hand-verified expected records |
| `fixtures/eval_baseline.json` | last-good accuracy snapshot |

See `DECISIONS.md` for the rationale behind every non-obvious choice.
