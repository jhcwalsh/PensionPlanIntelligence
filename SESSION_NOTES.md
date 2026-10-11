# Session notes

## 2026-10-11 — Every cron but weekly-watch stopped on 13 Sep

**Done**
- Put `ANTHROPIC_API_KEY` in the job-level `env` of seven workflows (daily-pipeline, weekly-insights,
  monthly-cafr-refresh, monthly-ips, monthly-insights, quarterly-insights, annual-insights).
- `tests/test_preflight_credit.py` now asserts the preflight step can see the key.

**Why**
- The credit preflight (4b87202, 13 Sep) runs before any step that had the key in its own `env`, so it
  failed "Anthropic API key missing" on every run and skipped the real work. Only weekly-watch had the
  key at job level. The old test checked the mail variables but not the key itself.
- Damage: daily pipeline has fetched nothing since 13 Sep; no weekly publications since then, so the
  1 Oct monthly and quarterly briefings failed; the 1 Oct CAFR refresh never ran.

**Broken / unsure**
- Not pushed, so nothing is fixed on GHA until it is.
- The missed runs need re-running once pushed: daily pipeline (one run catches up), the weekly
  composes for the missed Sundays, then monthly (Sep period) and quarterly (Q3), then the CAFR refresh.
- Separately, the CAFR refresh itself saves nothing: 1 Sep run was 143 no_strategy, 46 url_failed, 0 saved.
  `refresh_cafrs.refresh_plan` returned on a failed download of the newest FY instead of trying the
  prior one (fixed, `tests/test_refresh_cafrs.py`). But most `cafr_url_template`s bake in an upload-date
  folder (`/2025/01/`), so 26 of 29 still 404 for FY2025 — they need `cafr_landing` pages instead.
  `_cafr_overrides.json` is not read by the monthly refresh at all.

**Next**
1. Push, then dispatch the daily pipeline and confirm it gets past the preflight.
2. Backfill the weekly → monthly → quarterly cascade in order.
3. Find FY2025 CAFR URLs for the ~82 plans missing one.
