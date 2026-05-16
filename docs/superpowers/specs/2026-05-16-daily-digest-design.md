# Daily Pension Digest — Design

**Date:** 2026-05-16
**Status:** Approved design, ready for implementation plan

## Context

The repository has weekly / monthly / annual editorial briefings in the `insights/` package, but no daily-cadence email. The local pipeline (`pipeline.py`) fetches new board materials each day; today the only way to see what landed is to open Streamlit or scan `logs/daily.log`.

We want a **daily what's-new digest** delivered to the founder's inbox — a factual, grouped-by-plan listing of documents that first appeared in `db/pension.db` since the last digest, with a one-paragraph factual summary per plan synthesized from the day's documents.

Constraints from the user:
- **No surprises.** Editorial tone is factual, not judgemental.
- **Runs without the laptop on.** GitHub Actions cron, not Windows Task Scheduler.
- **Approval only when content is unusual** (volume / keyword / reappearing-plan triggers). Auto-send on normal days.
- **Quiet-day heartbeat.** Empty days send a one-line "nothing today" note so the absence of an email never silently means "the cron broke."

The cadence slots into the existing `insights/` package as a fourth product (alongside `weekly`, `rfp_weekly`, `monthly`, `annual`).

## Architecture

**Where it runs:** new `.github/workflows/daily-digest.yml`. GitHub Actions scheduled cron, no laptop dependency. Pattern mirrors the existing `nightly_eval.yml`.

**Schedule:** `0 13 * * *` (13:00 UTC ≈ 9am ET EST / 8am ET EDT). Tunable.

**Workflow steps:** checkout → install `requirements-pipeline.txt` → `python -m insights.scheduler daily` → commit `db/pension.db` back if `daily_runs` advanced.

**State (`last_sent_at`)** lives in a new `daily_runs` table inside `db/pension.db`. Reuses the existing "DB IS the deploy mechanism for data" pattern — the workflow commits the DB back after a successful send. No external store.

**Trigger flow inside `insights.scheduler daily`:**

```
1. select_new_docs(since=last_daily_run_ts)   → list[Document]
2. apply_triggers(docs)                       → list[reason]
3. compose_daily(docs)                        → markdown
4. find_or_create_publication("daily", today)
5. if triggers: finalize_for_approval(pub)    # existing magic-link flow
   else:        finalize_and_send(pub)        # new helper, no token
6. record_daily_run(now)                      → daily_runs table
```

**Behavior note:** if `pipeline.py` doesn't run locally one day, the cron still fires and sends "nothing today" (because `Document.first_seen` hasn't advanced). Heartbeat preserved; pipeline-skipped days look the same as quiet days. Distinguishing them is a separable future feature.

## Components

### `insights/daily.py` — the only substantive new module

Public surface:

```python
def run_cycle(force: bool = False, now: datetime | None = None) -> RunResult
def select_new_docs(since: datetime, session: Session) -> list[Document]
def apply_triggers(docs: list[Document]) -> list[TriggerReason]
def compose_daily(docs: list[Document], triggers: list[TriggerReason]) -> str
def record_daily_run(sent_at: datetime, publication_id: int, session: Session) -> None
```

`run_cycle` is the orchestrator the scheduler calls. Everything else is pure-ish and individually testable.

#### `select_new_docs(since)`

```python
session.query(Document)
    .filter(Document.downloaded_at.isnot(None))
    .filter(Document.downloaded_at > since)
    .filter(Document.downloaded_at < now_utc)   # exclude future-dated rows from clock skew
    .order_by(Document.plan_id, Document.meeting_date.desc().nullslast())
    .all()
```

Uses `Document.downloaded_at` — when the PDF was actually fetched locally. (`Document` has no `first_seen` or `created_at` column.) Filtering on `IS NOT NULL` excludes URLs the fetcher discovered but never downloaded. If `last_sent_at` is null (first ever run), fall back to `now - 24h`. Strict `>` on the boundary so a doc with `downloaded_at == last_sent_at` isn't double-counted.

#### `apply_triggers(docs)` — returns reasons; empty list → auto-send

Three rules, ORed:

1. **Volume:** `len(docs) > DAILY_APPROVAL_DOC_THRESHOLD` (default 10) → reason `"volume:{n}"`.
2. **Keyword:** any doc's `title` (case-insensitive) contains a keyword from `DAILY_APPROVAL_KEYWORDS` → reason `"keyword:{kw}"`. Default keywords: `"RFP,manager,search,investment policy"`.
3. **Reappearing plan:** for each `plan_id` in `docs`, look up the plan's *prior* most-recent `Document.first_seen`. If that timestamp is older than `DAILY_REAPPEAR_DAYS` (default 30), reason `"reappear:{plan_slug}"`. One SQL per plan, cheap.

Reasons are rendered into the email header so you can see *why* a day went to approval.

#### `compose_daily(docs, triggers)` — LLM synthesis, factual-only

- Group docs by `plan_id`. For each plan, build a tight prompt: plan name + the docs' titles, dates, types, and existing per-doc summaries.
- Single Claude call per plan, `temperature=0`, system prompt:

  > *"Produce one factual paragraph describing what these documents are. Do not editorialize. Do not infer significance. Do not recommend. State only what the documents are and what they cover. ≤3 sentences."*

- Output markdown:

  ```markdown
  # Pension Plans — Daily Digest — 2026-05-16

  Triggers: keyword:RFP, volume:14   ← only present when triggers fired

  ## CalPERS
  <factual paragraph>
  - [Investment Committee Minutes — 2026-05-12](https://…/document/123)
  - [Board Agenda — 2026-05-15](https://…/document/124)

  ## Texas TRS
  …
  ```

- Doc links use `APPROVAL_BASE_URL` + `/?document=<id>` (the Streamlit deep-link pattern).
- **Quiet-day path:** if `docs` is empty, skip the LLM entirely; body is one line: *"No new documents fetched in the last 24 hours."*

#### `record_daily_run(sent_at, publication_id, session)`

Inserts a row into `daily_runs`. Called after a successful send (auto-send branch) or after `finalize_for_approval` returns (approval branch). The lookback timestamp advances even on approval-gated days — see "Approval flow tradeoff" below.

### `finalize_and_send(session, pub)` — new helper in `insights/cycle_common.py`

Sibling of the existing `finalize_for_approval(session, pub, ...)` in `cycle_common.py`. It:

- Skips `approval_tokens` row creation.
- Renders an `ApprovalEmail`-shaped object whose HTML omits the magic-link buttons (subject/body wording differs too — informational, not actionable).
- Calls the existing `insights.approval.send_email(email, to=...)` for delivery — same Resend POST in live mode, same `tmp/sent_emails/` artifact in mock mode. No new HTTP plumbing.
- Transitions `Publication.status: generating → published` directly (skips `awaiting_approval`).

The `ApprovalEmail` dataclass is reused as the email envelope; only the rendering helper is new. PDF generation (`insights/render.py`) is reused unchanged.

### `daily_runs` table — minimal, in `database.py`

```python
class DailyRun(Base):
    __tablename__ = "daily_runs"
    id              = Column(Integer, primary_key=True)
    sent_at         = Column(DateTime, nullable=False, index=True)
    publication_id  = Column(Integer, ForeignKey("publications.id"), nullable=False)
    docs_count      = Column(Integer, nullable=False)
    triggers        = Column(JSON, nullable=False, default=list)   # ["volume:14", "keyword:RFP"]
    approval_gated  = Column(Boolean, nullable=False, default=False)
```

`last_sent_at` is `SELECT MAX(sent_at) FROM daily_runs`. Idempotency for re-runs comes from `Publication`'s existing `(cadence, period_start)` unique key — `period_start` is `date(now_utc)`.

### `insights/scheduler.py` — new subcommand

Adds `daily` parallel to `weekly` / `monthly` / `annual` / `reminders`. Same `--period`, `--force`, mock-mode handling. Body just calls `insights.daily.run_cycle(force=...)`.

### `insights/config.py` — additions

- New entry in `_CADENCE_DISPLAY`:
  ```python
  "daily": ("Daily", "Pension Digest", "daily_digest"),
  ```
- Three new env-var readers:
  ```python
  DAILY_APPROVAL_DOC_THRESHOLD = int(os.environ.get("DAILY_APPROVAL_DOC_THRESHOLD", "10"))
  DAILY_APPROVAL_KEYWORDS      = [k.strip() for k in os.environ.get(
      "DAILY_APPROVAL_KEYWORDS", "RFP,manager,search,investment policy"
  ).split(",") if k.strip()]
  DAILY_REAPPEAR_DAYS          = int(os.environ.get("DAILY_REAPPEAR_DAYS", "30"))
  ```

### `.github/workflows/daily-digest.yml`

```yaml
name: daily-digest
on:
  schedule: [{ cron: "0 13 * * *" }]
  workflow_dispatch:
jobs:
  send:
    runs-on: ubuntu-latest
    timeout-minutes: 10
    permissions: { contents: write }
    env:
      INSIGHTS_MODE: live
      ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
      RESEND_API_KEY:    ${{ secrets.RESEND_API_KEY }}
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.12", cache: pip }
      - run: pip install -r requirements-pipeline.txt
      - name: Send daily digest
        run: python -m insights.scheduler daily
      - name: Commit daily_runs update
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
          git add db/pension.db
          git diff --cached --quiet || git commit -m "Daily digest run $(date -u +%F)"
          git push
```

## Error handling and idempotency

### Idempotency — three layers

1. **`Publication.(cadence="daily", period_start=date_utc)` unique key** — `find_or_create_publication()` returns the existing row; if its status isn't `"generating"`, the scheduler exits. Re-running on the same UTC day is safe.
2. **`daily_runs.sent_at`** — `select_new_docs(since=max(sent_at))` defines "new" relative to the *last successful send*, not the last cron *attempt*. A mid-cycle crash leaves docs eligible for the next run; layer 1 prevents double-publication.
3. **`--force`** — manual escape hatch on the scheduler CLI. Same pattern as weekly/monthly. Not wired into the workflow.

### Failure surfaces

| Failure | Behavior |
|---|---|
| Claude API call fails during `compose_daily` for one plan | Caught per-plan. Section falls back to `"<N> document(s) fetched today; LLM synthesis failed."` Other plans still synthesize. |
| All Claude calls fail | Email still sends with fallback per plan and a banner: `"LLM synthesis unavailable — showing document list only."` |
| Resend API fails | `Publication` stays `generating`. Workflow step fails; GitHub Actions surfaces a red X. Next day's run picks up the same docs and republishes. |
| `select_new_docs` returns empty on a non-quiet day | Indistinguishable from a real quiet day without per-run logging. Sends the "nothing today" note. Accepted limitation. |
| `git push` of `db/pension.db` fails after a successful send | DB on Render is stale by a day. Next day's run pushes both days' `daily_runs` rows together. No re-send because of layer 1. |
| Workflow doesn't fire (GitHub Actions outage) | No email. Next day's run looks back through both days' `first_seen` and sends one combined digest. Volume trigger likely fires → routes through approval. Correct behavior. |

### Approval-flow tradeoff (worth flagging)

On triggered days, the digest goes through `finalize_for_approval()` and does **not** auto-send to subscribers; it sits awaiting approval. The `daily_runs` row is still written immediately, with `approval_gated=True`, so the next day's lookback window is correctly anchored.

Consequence: if a triggered day expires (7d) without approval, those docs never reach the digest stream — but the lookback already advanced. They'd only surface via Streamlit or a weekly briefing.

Alternative ("carry-forward window" where unapproved days bleed into the next digest) is **explicitly deferred** as YAGNI.

### Security

- **Secrets:** `ANTHROPIC_API_KEY`, `RESEND_API_KEY` in GitHub Actions secrets. Already the pattern (`nightly_eval.yml`). No new secret types.
- **`db/pension.db` is committed and assumed public-ish.** Already true; daily digest doesn't change it.
- **Repo write permission:** `permissions: { contents: write }`. Already used by `nightly_eval.yml`.
- **Approval tokens:** unchanged. Same SHA-256-hashed-in-DB pattern from `approval.py`.
- **Email-injection from doc titles:** titles come from untrusted PDF metadata. HTML-escape in HTML body (existing weekly does this); plain-text body has no risk; PDF rendering goes through the existing markdown path.
- **No new outbound surface** beyond Anthropic + Resend.

## Testing

Follows the existing layout: `tests/test_<feature>_e2e_mock.py` for full cycles, `tests/unit/test_<module>_*.py` for the pieces. Reuses `tmp_db` / `_isolated_environment` conftest fixtures.

### Unit tests — `tests/unit/test_daily_*.py`

**`test_daily_select.py`**
- `last_sent_at` null → falls back to `now - 24h`.
- Doc with `first_seen == last_sent_at` → excluded (strict `>`).
- Doc with `first_seen` just before → excluded.
- Future-dated `first_seen` (clock skew) → excluded by `< now_utc`.
- Docs ordered by `(plan_id, document_date desc)`.

**`test_daily_triggers.py`**
- Empty docs → empty reasons.
- 9 docs vs threshold 10 → no volume reason; 11 → `"volume:11"`.
- Title `"Search for Investment Consultant"` with keyword `"search"` → `"keyword:search"`. Case-insensitive.
- Plan with prior most-recent doc 40 days ago vs `DAILY_REAPPEAR_DAYS=30` → `"reappear:<slug>"`. Same plan with prior doc 5 days ago → no reappear.
- All three rules co-fire → all three reasons in the list.

**`test_daily_compose.py`** (LLM mocked via `INSIGHTS_MODE=mock`)
- Two plans, 3 docs → markdown has two `## <plan name>` sections, each with a paragraph and a doc-link list.
- Per-plan Claude call fails → fallback string in that section; other section synthesizes.
- All Claude calls fail → banner present.
- Quiet day (`docs=[]`) → no LLM call (assert mock not invoked); body is the "nothing today" line.
- Triggers list rendered into header when non-empty; omitted when empty.
- Doc links use `APPROVAL_BASE_URL` + `/?document=<id>`.

**`test_daily_publish.py`**
- `finalize_and_send()` transitions `generating → published` without creating an `approval_tokens` row.
- Mock mode writes `.eml` + `.pdf` + `.json` to `tmp/sent_emails/`.
- Subject prefix comes from `_CADENCE_DISPLAY["daily"]`.

**`test_daily_runs_table.py`**
- `init_db()` creates the `daily_runs` table.
- `record_daily_run()` inserts with correct `publication_id`, `docs_count`, `triggers`, `approval_gated`.
- `last_sent_at` query returns `MAX(sent_at)`.

### End-to-end — `tests/test_daily_e2e_mock.py`

Mirrors `tests/test_weekly_e2e_mock.py`. One test per branch:

1. **Quiet day, auto-send.** Empty `documents` → "nothing today" email in `tmp/sent_emails/`. `Publication.status == "published"`. `daily_runs.docs_count=0`, `approval_gated=False`.
2. **Normal day, auto-send.** 3 docs across 2 plans, no triggers → email contains both plan sections, no magic link. `approval_gated=False`. No `approval_tokens` row.
3. **Triggered day, approval-gated.** 12 docs (volume trigger) → magic-link approval email. `Publication.status == "awaiting_approval"`. `approval_tokens` row exists. `daily_runs.approval_gated=True`.
4. **Idempotency.** Run twice on the same UTC date → second run sees the non-`generating` `Publication` and exits cleanly. `tmp/sent_emails/` count unchanged.
5. **Crash-recovery.** First run records `Publication` as `generating` then raises before `finalize_and_send`. Second run completes it. Single email artifact in `tmp/`.
6. **`--force` re-send.** Same UTC day, `--force` → second send happens; both publications visible; both emails in `tmp/`.

### CI

`.github/workflows/test.yml` already runs `pytest tests/ -q` with `LLM_MODE=mock`. `INSIGHTS_MODE=mock` is autouse in `conftest.py`. No CI changes needed.

## Verification

Before turning the workflow live:

1. `INSIGHTS_MODE=mock python -m insights.scheduler daily --period 2026-05-16` → inspect `.eml` + PDF in `tmp/sent_emails/`.
2. `INSIGHTS_MODE=mock python -m insights.scheduler daily --period 2026-05-16 --force` → second artifact.
3. Trigger the workflow once via `workflow_dispatch` (add a `workflow_dispatch` input to override `INSIGHTS_MODE` to `mock` for the test run) before letting the scheduled cron go live. Inspect the workflow log; confirm the DB-commit step is a no-op when nothing changed.
4. Watch the first live scheduled run end-to-end: workflow green, email received, `db/pension.db` commit lands on master, `daily_runs` table reflects the run.

## File-level change list

**New files**
- `insights/daily.py` — selector, triggers, composer, orchestrator (~250 lines)
- `tests/unit/test_daily_select.py`
- `tests/unit/test_daily_triggers.py`
- `tests/unit/test_daily_compose.py`
- `tests/unit/test_daily_publish.py`
- `tests/unit/test_daily_runs_table.py`
- `tests/test_daily_e2e_mock.py`
- `.github/workflows/daily-digest.yml`

**Modified files**
- `database.py` — add `DailyRun` model. No migration; `init_db()` is idempotent.
- `insights/config.py` — add `"daily"` row to `_CADENCE_DISPLAY`; add `DAILY_APPROVAL_DOC_THRESHOLD`, `DAILY_APPROVAL_KEYWORDS`, `DAILY_REAPPEAR_DAYS` env-var readers.
- `insights/scheduler.py` — register `daily` subcommand calling `insights.daily.run_cycle()`.
- `insights/cycle_common.py` — add `finalize_and_send(session, pub)` sibling to `finalize_for_approval()`.
- `CLAUDE.md` — append the `daily` cadence to the architecture section, list the new env vars, mention the GitHub Actions cron.

**Not modified** — `approval.py`, `compose.py`, `publish.py`, `app.py`, `pipeline.py`, `scripts/run_daily.bat`, `render.yaml`. Daily digest is self-contained.

## Critical reuse — existing functions to call rather than duplicate

- `insights.cycle_common.find_or_create_publication(session, cadence=..., period_start=...)` — Publication idempotency. Pattern modeled after `weekly.py:163`, `monthly.py:69`, `annual.py:58`, `rfp_weekly.py:54`.
- `insights.cycle_common.finalize_for_approval(session, publication, ...)` — triggered-day branch. Used by all existing cadences; `cycle_common.py:87`.
- `insights.approval.send_email(email, to=...)` — `approval.py:287`. Generic; takes an `ApprovalEmail` dataclass. Works for both branches (auto-send and approval-gated).
- `insights.approval.ApprovalEmail` dataclass — reuse as the envelope; auto-send branch renders different HTML body content.
- `insights.config.cadence_display(cadence)` — `config.py:78`. Subject prefix, product name, slug.
- `insights.config.is_mock()` — `config.py:55`. Mock/live switch.
- `insights.render.*` — PDF rendering for the email attachment.
- `database.init_db()` — `Base.metadata.create_all`. Creates the new table when `DailyRun` is added. No migrations.
- `database.SessionLocal()` — DB session factory.
- `Document.downloaded_at` — the "newness" timestamp. (`Document` has no `first_seen` or `created_at`.)
