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

## Running the pipeline (local, weekly)

The pipeline is what produces the data the website and CIO Insights
briefings draw from. Run it before each Sunday so the weekly digest has
fresh content.

### The standard refresh

```bash
python pipeline.py
```

That runs all three steps end-to-end:

1. **Fetch** — for each plan in [data/known_plans.json](data/known_plans.json),
   visit its materials URL and download new agendas, board packs, and
   minutes (with Playwright for JS-rendered sites).
2. **Extract** — pull text out of every newly downloaded PDF/DOCX
   (pdfplumber → PyMuPDF fallback).
3. **Summarize** — ask Claude (Haiku for short docs, Sonnet for
   investment-heavy packs) to produce a structured summary per document.

A typical run takes 15–60 minutes depending on how many new documents
were posted that week. Cost is usually \$1–\$5 per run.

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
