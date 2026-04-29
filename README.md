# Pension Plan Intelligence

A data platform that aggregates board materials and CAFRs from U.S. public
pension plans, summarizes them with Claude, and publishes a searchable
Streamlit site plus scheduled CIO Insights briefings.

## Architecture in one paragraph

The **pipeline** (run locally) scrapes plan websites with Playwright,
extracts text from PDFs/Word docs, and asks Claude to produce per-document
summaries. The **Streamlit app** ([app.py](app.py)) serves a public site at
[pensionplanintelligence.onrender.com](https://pensionplanintelligence.onrender.com)
backed by the SQLite DB at `db/pension.db`. The **insights/** package
([DECISIONS.md](DECISIONS.md), [insights/scheduler.py](insights/scheduler.py))
runs scheduled CIO Insights publications — weekly digest, monthly synthesis,
annual year-in-review — gated on a magic-link approval email to the founder.

## Two workflows you need to understand

This repo has two separate cadences. They share the same SQLite DB but run
in different places:

| Workflow | Where it runs | What it does | Cadence |
|---|---|---|---|
| **Pipeline** (`pipeline.py`) | Your local machine | Fetch new docs → extract text → summarize | Whenever you want fresh data |
| **Insights cron** (Render) | Render cron services | Compose / email / approve / publish CIO Insights | Sun / 1st-of-month / Jan 5 / daily |

The insights cron does **not** run the pipeline — it composes from
whatever is already in `db/pension.db`. So you need to run the pipeline
locally and push the updated DB before each weekly cycle, or the digest
will be stale.

---

## Local setup (one-time)

You need Python 3.12+ and git.

```bash
git clone https://github.com/jhcwalsh/PensionPlanIntelligence.git
cd PensionPlanIntelligence

# Create a venv
python -m venv .venv
# Windows:
.venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate

# Install full pipeline dependencies (includes Playwright, pdfplumber, anthropic)
pip install -r requirements-pipeline.txt

# Install the Chromium binary Playwright drives (~150 MB download)
playwright install chromium

# Copy and fill in the env file
cp .env.example .env
# Open .env and at minimum set ANTHROPIC_API_KEY
```

That's it for the pipeline. The other env vars in `.env.example` only
matter if you're running the insights scheduler locally (rare — it runs
on Render).

---

## How the pipeline works

`python pipeline.py` is a thin orchestrator ([pipeline.py](pipeline.py)
→ `run_pipeline`) that drives three independent stages against the
same SQLite DB. Each stage reads what it needs and writes back into
DB rows; if any stage fails, the next run picks up exactly where
the last one left off — there's no in-memory state that has to
survive between stages.

### Stage 1 — Fetch ([fetcher.py](fetcher.py) → `run_fetcher`)

For every plan in [data/known_plans.json](data/known_plans.json):

1. **Load the materials page.** Plans flagged `materials_type:
   "playwright"` are rendered in headless Chromium so JavaScript
   fires before link extraction; everything else is plain `requests`
   + `BeautifulSoup`. The plan's `materials_url` is the entry point.
2. **Discover candidate documents.** All `<a>` links on the page
   are collected, plus any nested pages that match
   `RELEVANT_KEYWORDS` (`agenda`, `minutes`, `board`, `investment`,
   …). URLs that look like document downloads but lack a file
   extension (e.g. CalPERS' `/documents/<id>/download`) are
   recognised via `DOC_URL_PATTERNS`.
3. **Filter to the investment focus.** `INVESTMENT_FOCUS` keeps
   only investment / portfolio committee material; `EXCLUDE_COMMITTEES`
   drops audit, finance, benefits, governance, real-estate, etc.
   This is what keeps the corpus signal-rich rather than every
   board document the plan publishes.
4. **Skip already-known URLs.** `document_exists(session, url)` short-
   circuits — every URL we've ever downloaded is already a row in
   the `documents` table.
5. **Download new docs** to `downloads/<plan_id>/` (or
   `/data/downloads/<plan_id>/` on Render's persistent disk) and
   insert one row in `documents` per file: URL, filename, plan,
   doc_type (`agenda` / `board_pack` / `minutes` / `performance` /
   …), parsed meeting date, file size, `extraction_status="pending"`.

Pipeline-wide, only docs newer than `--min-year` (default 2026)
are fetched. There's no global rate limit; the fetcher just walks
plans sequentially.

### Stage 2 — Extract ([extractor.py](extractor.py) → `run_extractor`)

Walks every `documents` row whose `extraction_status="pending"`:

1. **PDF: pdfplumber first** — preserves layout and pulls tables as
   pipe-delimited rows alongside body text. Most board packs come
   out clean.
2. **PDF: PyMuPDF (`fitz`) fallback** — plain text only, used when
   pdfplumber raises (encrypted PDFs, malformed layouts, etc.).
3. **PDF: OCR fallback** — only triggered when both above produce
   no text and `--retry-failed` is passed. Renders each page with
   PyMuPDF and runs `pytesseract`. Slow, so off by default.
4. **DOCX:** `python-docx` paragraph + table walk.
5. **Cap.** Whatever the extractor produces is truncated to
   `MAX_TEXT_CHARS=150_000` before persisting.

Each row's `extracted_text` and `page_count` are filled in;
`extraction_status` flips to `done` (or `failed` if every
extractor returned empty).

### Stage 3 — Summarize ([summarizer.py](summarizer.py) → `run_summarizer`)

Walks every `documents` row that is `extracted_text` ready but
has no `summaries` row yet:

1. **Skip non-substantive docs.** Filenames matching
   `attendance | building map | calendar | bio | parking | …`
   are dropped — pure logistics, no investment signal.
2. **Hash dedup.** MD5 the extracted text. If another summary in
   the DB has the same hash, copy it into a thin pointer row
   (with `model_used="dedup:..."`) instead of paying for the
   API call. This catches identical board packs republished
   verbatim and the same agenda being posted twice.
3. **Smart truncate.** `smart_truncate()` builds a ~50 k character
   excerpt: first 20 k chars (agenda, exec summary), keyword-
   selected windows from the middle (manager hires, allocation
   changes, performance), last 10 k chars (decisions, votes).
   A 200-page board pack fits comfortably in one Claude call
   without losing the parts we care about.
4. **Model routing** (`choose_model`): Haiku for short docs and
   simple agendas; Sonnet only when the doc is large *and* its
   first 5 k chars contain investment keywords. Sonnet is ~4×
   the cost — reserved for documents that earn it.
5. **Structured JSON output.** The Claude prompt asks for a
   strict JSON schema: `summary`, `key_topics`, `decisions`
   (with vote tallies), `investment_actions` (hires / fires /
   commitments with dollar amounts), `performance_data`
   (returns vs benchmarks), `notable_items`. The response is
   parsed and persisted as a `summaries` row alongside
   `text_hash`, `model_used`, `generated_at`.

Per-document cost typically lands at \$0.001 (Haiku) – \$0.05
(Sonnet on a 200-page board pack). A weekly run across all
plans usually costs \$1–\$5.

### What ends up in the DB

```
plans         148 rows  (fixed registry from data/known_plans.json)
documents   3,000+ rows (one per downloaded file, growing weekly)
summaries   2,500+ rows (one per substantive document)
cafr_*      separate cadence — see refresh_cafrs.py
```

Everything downstream (the Streamlit app, the insights cron,
the analyst notes generator) reads only from `documents` and
`summaries`. The pipeline never reads back from anything it
wrote — each stage is one-way.

### Resumability

Because every stage is keyed by a status column (`extraction_status`
on documents, presence/absence of a `summaries` row), interrupted
runs are safe to retry. If `pipeline.py` dies during summarization,
re-running it skips the already-done docs and resumes on the
unsummarized ones automatically.

---

## Running the pipeline (local, weekly)

The pipeline is what produces the data the website and CIO Insights
briefings draw from. Run it before each Sunday so the weekly digest has
fresh content.

### The standard refresh

```bash
python pipeline.py
```

End-to-end fetch → extract → summarize, as described in *How the
pipeline works* above. Typical run: 15–60 minutes depending on how
many new documents were posted that week. Cost: \$1–\$5.

### Useful flags

```bash
# Just one plan (for debugging or after fixing a plan's URL)
python pipeline.py calpers

# Skip the slow fetch step — useful if you want to re-summarize without
# re-downloading
python pipeline.py --extract-only        # extract + summarize
python pipeline.py --summarize-only      # summarize only

# See what's in the DB without running anything
python pipeline.py --status

# See new meetings detected in the last 14 days
python pipeline.py --updates

# Retry documents that previously failed extraction (uses OCR fallback)
python pipeline.py --retry-failed
```

### Pushing the refreshed DB

The Streamlit site and the Render cron services both read
`db/pension.db`. After a pipeline run, commit and push:

```bash
git add db/pension.db
git commit -m "Refresh pipeline (YYYY-MM-DD)"
git push
```

Render auto-deploys on push to `master`. The new DB becomes available
to the cron services immediately.

> **Heads up:** `db/pension.db` is ~90 MB and exceeds GitHub's 50 MB
> recommendation. It works, but if it grows much further, consider
> migrating to Git LFS.

---

## CAFR refresh (monthly, optional)

CAFRs/ACFRs are the annual financial reports — separate cadence from board
materials.

```bash
python refresh_cafrs.py            # check every plan for a new CAFR
python extract_cafr_investments.py # extract investment-section data into the DB
```

These can run anytime; they're not gated to the weekly cycle.

---

## Running the Streamlit site locally

```bash
streamlit run app.py
```

Visit http://localhost:8501. Tabs include Notes, Summary, Updates,
Search, Browse Recent, Investment Actions, Plans, Drafts (CIO Insights
awaiting approval), Insights (approved publications), and Admin.

---

## CIO Insights automation

This is fully described in [DECISIONS.md](DECISIONS.md). Short version:

- Render runs four cron services. Their schedules and `INSIGHTS_MODE=live`
  are wired in [render.yaml](render.yaml).
- The weekly cron runs `python -m insights.scheduler weekly --skip-scrape`
  every Sunday 02:00 ET. It composes the digest from documents already
  in `db/pension.db`. It does **not** scrape — that's why you need to
  run the local pipeline first.
- The cron emails the founder with an Approve/Reject magic link via
  Resend. Clicking Approve triggers `git commit` + `git push` of the
  approved markdown into `notes/`, which Render's web service auto-deploys.
- Monthly composes from the four most recent approved weeklies; annual
  from the twelve approved monthlies. They never re-read raw documents.

### Render env vars to set before the first live run

Set these on the Render dashboard for each cron service (and the
web service, which uses the same approval base URL):

```
ANTHROPIC_API_KEY=sk-ant-…
RESEND_API_KEY=re_…
APPROVAL_EMAIL_RECIPIENT=james@walsh.nu
APPROVAL_EMAIL_FROM=insights@yourdomain.com    # must be a Resend-verified domain
SLACK_WEBHOOK_URL=https://hooks.slack.com/…    # optional but recommended
```

`APPROVAL_BASE_URL`, `DB_PATH`, `INSIGHTS_MODE` are already set in
[render.yaml](render.yaml).

### Manual / backfill runs

You can run any cycle locally for testing or to backfill a missed window:

```bash
# Mock mode — no email sent, no LLM calls, writes to tmp/sent_emails/
INSIGHTS_MODE=mock python -m insights.scheduler weekly --period 2026-04-19 --skip-scrape

# Live mode — real Claude call, real email
INSIGHTS_MODE=live python -m insights.scheduler weekly --period 2026-04-19 --skip-scrape

# Backfill the monthly for March 2026
python -m insights.scheduler monthly --period 2026-03

# Force-recompose if a draft is stuck
python -m insights.scheduler weekly --period 2026-04-19 --skip-scrape --force
```

---

## Tests

```bash
python -m pytest tests/
```

34 tests cover token lifecycle, idempotency, status transitions, weekly
resumability, weekly e2e, monthly cascade, reminders + expiry, failure
alerting, and existing-component delegation. They all run under
`INSIGHTS_MODE=mock` — no API keys needed.

---

## Repo layout

```
.
├── app.py                       Streamlit web app (deployed to Render)
├── pipeline.py                  Local: fetch → extract → summarize
├── fetcher.py                   Playwright-driven document downloader
├── extractor.py                 PDF/DOCX text extraction
├── summarizer.py                Claude per-document summarization
├── generate_notes.py            Claude analytical briefings (7-day, CIO Insights)
├── publish_notes.py             Local: regenerate notes + git push (legacy path)
├── fetch_cafr.py                CAFR/ACFR discovery and download
├── extract_cafr_investments.py  Structured CAFR investment data extraction
├── refresh_cafrs.py             Monthly CAFR refresh entry point
├── database.py                  SQLAlchemy models + DB helpers
├── data/known_plans.json        ~148 plans with URLs, AUM, fiscal year ends
├── db/pension.db                SQLite corpus (committed)
├── notes/                       Published markdown briefings
├── insights/                    CIO Insights publishing automation
│   ├── scheduler.py             python -m insights.scheduler {weekly,monthly,annual,reminders}
│   ├── weekly.py                Weekly cycle (compose only — scrape is local)
│   ├── monthly.py               Monthly cycle (composes from approved weeklies)
│   ├── annual.py                Annual cycle (composes from approved monthlies)
│   ├── compose.py               Calls existing summarizer / cascades for monthly+annual
│   ├── approval.py              Token lifecycle + Resend email
│   ├── publish.py               git commit + push of approved markdown
│   ├── render.py                Markdown → PDF (shared with app.py)
│   ├── notify.py                Slack failure alerts
│   ├── reminders.py             Daily 72h reminder + 7d expiry sweep
│   └── cycle_common.py          Status transitions, idempotent CRUD
├── tests/                       pytest suite — runs under INSIGHTS_MODE=mock
├── DECISIONS.md                 Architectural choices for the insights package
├── render.yaml                  Render web service + four cron services
├── requirements.txt             Web service deps (lean)
├── requirements-pipeline.txt    Local-only deps (Playwright, pdfplumber, anthropic)
├── .env.example                 Required env vars
└── packages.txt                 OS packages (libsqlite3-dev) for Render builds
```

---

## Common tasks

### "I want a fresh weekly digest tomorrow"

```bash
python pipeline.py
git add db/pension.db && git commit -m "Refresh pipeline" && git push
# Wait for Sunday 02:00 ET, or trigger the cron manually from Render dashboard.
```

### "A plan's materials URL changed"

```bash
python update_plan_url.py
# Or edit data/known_plans.json directly.
```

### "The weekly cycle failed and I need to re-run it"

```bash
python -m insights.scheduler weekly --period 2026-04-19 --skip-scrape --force
```

`--force` expires the stuck draft and re-composes from scratch. The
founder gets a fresh approval email.

### "I want to test the approval email without sending it"

```bash
INSIGHTS_MODE=mock python -m insights.scheduler weekly --skip-scrape
ls tmp/sent_emails/      # the rendered email lands here
```
