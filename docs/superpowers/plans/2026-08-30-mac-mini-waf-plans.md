# Stage 1: WAF-blocked plans from the Mac Mini — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fetch the 14 WAF-blocked pension plans nightly from the Mac Mini's
residential IP, writing to the same Neon Postgres everything else reads, so
coverage goes from 137 of 148 plans back to 148.

**Architecture:** No change to the pipeline's logic. `pipeline.py` and
`refresh_cafrs.py` already bypass their block lists when plan IDs are named
explicitly on the CLI — that escape hatch is documented in both docstrings and
is the entire mechanism. This plan adds three things around it: a helper that
derives the ID lists from the existing JSON block lists so nothing is
hardcoded twice, a shell wrapper that runs the two commands inside a one-shot
Docker container and emails on failure, and a launchd agent that fires it
daily. The Mini writes to Neon over the network; Render and GitHub Actions are
untouched.

**Tech Stack:** Python 3.12, Playwright 1.49.0 (Chromium), Docker via OrbStack
on macOS arm64, launchd, Neon Postgres, Resend (failure email).

**Spec:** `docs/superpowers/specs/2026-08-30-mac-mini-migration-design.md`
(Stage 1 in §8; rationale in §3 and §4; risks in §7). The host is described in
`docs/mac-mini-hosting-runbook2.md`.

## Global Constraints

- **`DATABASE_URL` must be set and non-empty on every invocation.**
  `database.resolve_database_url()` treats unset or empty as "use `DB_PATH`",
  which in a fresh container is an empty SQLite file — the job reads nothing,
  writes nothing, and exits zero. This is the single most likely way for this
  stage to look like it works while doing nothing.
- **All four `R2_*` values must be set on every invocation.** If any one is
  missing, PDF retention is a silent no-op (CLAUDE.md): the run fetches,
  extracts, goes green, and retains nothing. The second-most-likely way for
  this stage to look like it works while throwing away the thing it exists to
  collect — and worse than the `DATABASE_URL` case above, because these 14
  plans are precisely the ones no other machine can re-fetch. `fetcher.py`
  prints retention on/off at the start of every run; that line is the check.
- **`LLM_MODE` and `INSIGHTS_MODE` must be unset in production.** They are
  independent flags; mock mode writing to committed data has already happened
  twice in this repo.
- **Playwright is pinned to `playwright==1.49.0`** in
  `requirements-pipeline.txt`. Do not bump it as part of this work.
- **13 of the 14 plans are `materials_type: "playwright"`.** Playwright
  Chromium working on linux/arm64 is not a side risk — it is the critical
  path. See Task 2.
- **The 14 plan IDs are never to be hardcoded.** They live in
  `data/waf_blocked_plans.json` (11 ids) and `data/waf_blocked_cafr_plans.json`
  (5 ids), overlapping on `asrs` and `strs_ohio`. Anything needing the list
  derives it.
- **Python 3.12**, matching `actions/setup-python` in the workflows.
- **This is a one-shot job, not a service.** The runbook's
  `restart: unless-stopped` pattern is for the Streamlit-style long-running
  containers and is wrong here.

---

## The 14 plans

Derived from the two block lists; reproduced here for the reader, **not** to be
copied into code.

| Plan | Materials | CAFR | `materials_type` | Block reason |
|---|---|---|---|---|
| `acrs_pa` | | ✓ | playwright | Generic HTTP 403 |
| `asrs` | ✓ | ✓ | playwright | Cloudflare "Just a moment" |
| `corp_az` | ✓ | | playwright | Cloudflare "Just a moment" |
| `frs` | ✓ | | playwright | Generic HTTP 403 |
| `fwerf_tx` | | ✓ | playwright | Generic HTTP 403 |
| `kpers_ks` | ✓ | | playwright | Cloudflare "Just a moment" |
| `lasers_la` | ✓ | | playwright | Cloudflare "Just a moment" |
| `mcera` | ✓ | | playwright | Cloudflare "Attention Required" |
| `nmpera` | ✓ | | playwright | Cloudflare "Just a moment" |
| `nv_pers` | ✓ | | playwright | Cloudflare "Just a moment" |
| `pbpr_pa` | | ✓ | playwright | Generic HTTP 403 |
| `pgcers_md` | ✓ | | playwright | Generic HTTP 403 |
| `scers_suffolk` | ✓ | | playwright | Cloudflare "Just a moment" |
| `strs_ohio` | ✓ | ✓ | html_links | Cloudflare "Just a moment" |

Two groups behave differently and are verified differently:

- **403 group** (`frs`, `pgcers_md`, `pbpr_pa`, `fwerf_tx`, `acrs_pa`) — a
  plain HTTP request is refused by source IP. A `requests`-based probe from the
  Mini settles these outright.
- **Cloudflare-challenge group** (the other nine) — the server returns
  challenge HTML that must be executed. **A `requests` probe can still fail
  from a residential IP and prove nothing**, because `scripts/probe_scrape.py`
  uses `requests`, not a browser. These are settled only by a real Playwright
  fetch (Task 2).

Getting that distinction wrong is the most likely way to abandon this stage on
a false negative.

## File Structure

| File | Responsibility |
|---|---|
| `scripts/waf_blocked_ids.py` | **Create.** Single source of truth for the ID lists. Reads both JSON block lists, exposes `materials_ids()` / `cafr_ids()` / `all_ids()`, and prints space-separated IDs for shell consumption. |
| `tests/test_waf_blocked_ids.py` | **Create.** Anti-drift tests: the helper's lists must equal what `pipeline.py` and `refresh_cafrs.py` actually subtract, and every ID must exist in the registry. |
| `Dockerfile.pipeline` | **Create.** Pipeline image with Playwright Chromium baked in at `/ms-playwright`, outside the bind-mounted `/app`. |
| `docker-compose.pipeline.yml` | **Create.** One-shot service, repo bind-mounted so a `git pull` needs no rebuild. No restart policy. |
| `scripts/run_waf_plans.sh` | **Create.** The runner: pull, fetch materials, refresh CAFRs, email on failure. macOS counterpart to `scripts/run_recordings.bat`. |
| `tests/test_run_waf_plans_script.py` | **Create.** Static assertions on the runner — no hardcoded plan IDs, both ID lists sourced from the helper, every step guarded. |
| `docs/mac-mini/com.pensiongraph.wafplans.plist` | **Create.** launchd agent, committed so the schedule is in git rather than only on the box. |
| `CLAUDE.md` | **Modify.** Cadence table + the "cloud-only" claim in "What this repo is". |
| `nextsteps.md` | **Modify.** Close A4. |
| `data/waf_blocked_plans.json`, `data/waf_blocked_cafr_plans.json` | **Modify** (`_doc` / `_how_it_works` strings only). They currently say these plans run nowhere. |

Deliberately **not** touched: `pipeline.py`, `refresh_cafrs.py`, `fetcher.py`,
`fetch_cafr.py`, any workflow, `render.yaml`. If this stage needs a change in
any of them, something has been misunderstood — stop and re-read the spec.

---

### Task 1: Prove the residential IP actually changes anything

The whole stage rests on an assumption that has never been tested against all
14 plans. This task costs twenty minutes and can kill the plan before any code
is written. **No code is produced.** Its deliverable is a recorded result and a
go/no-go.

`scripts/probe_scrape.py` already exists for exactly this question — its
docstring opens: *"Probe whether GitHub Actions runners can scrape
pension-plan board-materials pages without being blocked by cloud-IP-aware bot
mitigation."* It is read-only, touches no database, and needs only `requests`.

**Files:**
- Create: none
- Modify: none
- Test: none (manual verification task)

**Interfaces:**
- Consumes: nothing.
- Produces: a go/no-go recorded in this document, and the knowledge of which
  of the two groups (above) actually needed a browser. Task 2 consumes that.

- [ ] **Step 1: Get the repo onto the Mini with a probe-only environment**

On the Mini, over SSH from Windows (`ssh jameswalsh@JHCW-mini.local`):

```bash
mkdir -p ~/apps/pensiongraph && cd ~/apps/pensiongraph
git clone git@github.com:jhcwalsh/PensionPlanIntelligence.git .
python3 -m venv .probe-venv
./.probe-venv/bin/pip install requests python-dotenv
```

This venv is throwaway — it exists so Task 1 can run before any Docker work.
Delete it at the end of Task 5.

- [ ] **Step 2: Probe the 403 group — the decisive one**

```bash
cd ~/apps/pensiongraph
./.probe-venv/bin/python -m scripts.probe_scrape frs pgcers_md
./.probe-venv/bin/python -m scripts.probe_scrape --cafr pbpr_pa fwerf_tx acrs_pa
```

Expected if the premise holds: `✓` and an HTTP 200 for each, in place of the
403 that put them on the block list.

Expected if the premise fails: `✗` with HTTP 403 again — meaning these sites
block on something other than IP reputation (a User-Agent rule, a geo rule, a
missing referer). **If all five still 403, stop and report.** The residential
IP is not the missing ingredient and this stage should not be built.

- [ ] **Step 3: Probe the Cloudflare group, and read the result correctly**

```bash
cd ~/apps/pensiongraph
./.probe-venv/bin/python -m scripts.probe_scrape \
    asrs corp_az kpers_ks lasers_la mcera nmpera nv_pers scers_suffolk strs_ohio
```

**A failure here is not a stop condition.** `probe_scrape` uses `requests`; a
Cloudflare interstitial is expected to defeat it regardless of source IP. Record
the outcome and carry it into Task 2. A `✓` for `strs_ohio` (the one
`html_links` plan) is the most informative single result, because that plan
does not get a browser in production either.

- [ ] **Step 4: Record the findings in this plan**

Append a short table under this task: plan, probe result, HTTP status. Commit
it, so the next reader knows what was true on the day rather than re-running
everything.

```bash
git add docs/superpowers/plans/2026-08-30-mac-mini-waf-plans.md
git commit -m "Stage 1 Task 1: record WAF probe results from the Mini"
```

**Gate:** the 403 group must pass. The Cloudflare group is allowed to fail
here.

#### Result (2026-09-01): **GO**, with two plans dropped from scope

Run from the Windows desktop at `10.0.0.181` rather than from the Mini at
`10.0.0.42` — same LAN, therefore the same residential public IP, which is the
only property under test. Method was stronger than this task specifies: rather
than `probe_scrape`'s plain `requests`, the *production fetch path*
(`pipeline.py --fetch-only --max-docs 1 --min-year 2027`, Playwright where the
plan is configured for it), with a `requests` follow-up on the three that
returned no links. `--min-year 2027` makes it read-only — discovery runs, but
nothing qualifies for download, which matters because R2 retention is still
off and these documents are irreplaceable.

| Plan | Links found | Verdict |
|---|---|---|
| `nmpera` | 223 | ✓ |
| `lasers_la` | 213 | ✓ |
| `mcera` | 80 (40 sub-pages) | ✓ |
| `kpers_ks` | 68 | ✓ |
| `acrs_pa` | 63 | ✓ |
| `asrs` | 40 | ✓ |
| `nv_pers` | 38 | ✓ |
| `corp_az` | 33 | ✓ |
| `pbpr_pa` | 21 | ✓ |
| `fwerf_tx` | 18 | ✓ |
| `strs_ohio` | 1 | ✓ — `requests` 403, Playwright fallback succeeded |
| `scers_suffolk` | 0 | **Not blocked, and empty.** HTTP 200; the page is a department directory with no agendas or minutes on it (re-verified 2026-09-02). |
| `frs` | 0 | ✗ HTTP 403 from residential IP |
| `pgcers_md` | 0 | ✗ HTTP 403 from residential IP |

**Not one Cloudflare challenge fired**, on any of the eight plans listed as
"Just a moment" or "Attention Required" — including from plain `requests` in
the `strs_ohio` case. Whether the residential IP defeats them or the sites'
WAF configuration changed since August is unresolved and does not matter to
this decision: they are reachable.

Three consequences for the rest of this plan:

1. **Proceed.** Three of the five in the 403 group pass (`pbpr_pa`,
   `fwerf_tx`, `acrs_pa`), so the stop condition — all five still 403 — is not
   met.
2. **`frs` and `pgcers_md` leave Stage 1's scope.** They block on something
   other than IP reputation, so the Mini does not help them and shipping them
   in the runner's list would produce a job that fails two plans every night.
   They stay on the block lists with their reason corrected; A4's proxy
   question survives for exactly these two.
3. **`scers_suffolk` has nothing to fetch**, and was misfiled on
   the block list. It is reachable from anywhere, including GitHub Actions —
   fixing its selector restores it to the *cloud* pipeline, no Mini required.
   Tracked separately; do not fold it into this plan.

#### Correction, same day: the probe above measured the wrong thing

Everything above is about **rendering the listing page**. It says nothing
about **downloading the PDFs that page links to**, and those are independent:
a plan is only useful to the Mini if both work. The table's ticks were awarded
for "Found N document links", which reads as success and is not one.

Caught when Task 2's container ran the real command on the Mini. `asrs`
discovered 40 links and then 403'd on every download. Re-tested from Windows:
403 there too. So this was never a container or arm64 artefact — the first
probe used `--min-year 2027` to stay read-only, which meant it never attempted
a single download.

Downloads, tested against known-good PDF URLs already in the corpus:

| Plan | Listing | PDF download | Net |
|---|---|---|---|
| `kpers_ks`, `lasers_la`, `mcera`, `nmpera`, `nv_pers` | ✓ | ✓ 200 | **Stage 1** |
| `pbpr_pa`, `fwerf_tx` | ✓ | ✓ 200 | **Stage 1** (CAFR) |
| `asrs`, `corp_az`, `acrs_pa`, `strs_ohio` | ✓ | ✗ 403 | lists, cannot fetch |
| `frs` | ✗ 403 | ✓ 200 | cannot discover |
| `pgcers_md` | ✗ 403 | — | blocked |
| `scers_suffolk` | ✓ 200 | ✓ 200 | page is empty of materials |

The 403 on the four is not a cookie problem, which was the obvious hypothesis
and is worth recording as ruled out: it survives a `requests` call carrying
the browser's cookie jar (only `__cf_bm` is issued, never `cf_clearance`), a
`Referer` matching the listing page, and Playwright's own
`APIRequestContext`, which shares the browser's TLS fingerprint and cookies.
The PDF path has its own rule.

**Revised arithmetic.** Stage 1 is worth **5 materials plans and 2 CAFRs**,
not 14 and not 11. Materials coverage goes **137 → 142**. The residue for A4
is seven plans, not two.

*Corrected again 2026-09-02:* an earlier revision of this paragraph promised a
sixth plan from fixing `scers_suffolk`'s selector. There is no selector to
fix — its page carries no materials at all, which `known_plans.json` had
recorded on 2026-08-29 and this table's author had not read. Materials
coverage tops out at **144 of 148**, not 148: `frs` and `pgcers_md` need
something A4 might buy, `scers_suffolk` needs Suffolk to start publishing, and
the four `download_403` plans need something nobody has.

Whether that is still worth an always-on host and a nightly job is a real
question and is James's to answer — it is a different proposition from the one
this plan was written against.

The runner's lists are still derived from the JSON rather than hardcoded
(Task 3), so these exclusions live in the block lists themselves as
`blocked_by`, not in the runner.

---

### Task 2: A pipeline container that runs Playwright Chromium on arm64

Spec §7 risk 1: `playwright==1.49.0` is pinned, and every workflow installs
Chromium on x86-64 `ubuntu-latest`. arm64 Linux is supported by Playwright but
is the flakiest corner of its matrix, and 13 of the 14 plans need the browser.
This task settles it with a real fetch, not a version-support claim.

**Files:**
- Create: `Dockerfile.pipeline`
- Create: `docker-compose.pipeline.yml`
- Test: manual — a real single-plan fetch against a scratch SQLite database

**Interfaces:**
- Consumes: Task 1's go/no-go.
- Produces: a compose service named `pipeline`, invoked as
  `docker compose -f docker-compose.pipeline.yml run --rm pipeline <cmd...>`,
  with the repo bind-mounted at `/app` and `.env` loaded. Task 4's runner calls
  exactly this.

- [ ] **Step 1: Write the Dockerfile**

Create `Dockerfile.pipeline`:

```dockerfile
# Pipeline image for the Mac Mini's WAF-blocked-plan job.
#
# Not the Streamlit image and not a long-running service: this is built to be
# invoked as `docker compose run --rm`, once a night, and to exit.
#
# Two decisions worth keeping:
#   - PLAYWRIGHT_BROWSERS_PATH puts Chromium at /ms-playwright, OUTSIDE /app.
#     docker-compose.pipeline.yml bind-mounts the repo over /app, which would
#     otherwise mask a browser installed into the default ~/.cache location
#     relative to the working tree.
#   - requirements-pipeline.txt starts with `-r requirements.txt`, so BOTH
#     files must be copied before the install or pip fails on the include.
FROM python:3.12-slim

ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
    PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=utf-8

WORKDIR /app

COPY requirements.txt requirements-pipeline.txt ./
RUN pip install --no-cache-dir -r requirements-pipeline.txt

# --with-deps installs the system libraries Chromium needs. On arm64 this is
# the step that fails first if Playwright has no build for the platform.
RUN python -m playwright install --with-deps chromium

CMD ["python", "pipeline.py", "--status"]
```

- [ ] **Step 2: Write the compose file**

Create `docker-compose.pipeline.yml`:

```yaml
# One-shot pipeline runner for the Mac Mini. Invoked as:
#   docker compose -f docker-compose.pipeline.yml run --rm pipeline python pipeline.py asrs
#
# No `restart:` policy on purpose. The runbook's `restart: unless-stopped` is
# what makes a long-running service survive a reboot; applying it to a batch
# job would restart the pipeline in a loop.
services:
  pipeline:
    build:
      context: .
      dockerfile: Dockerfile.pipeline
    # If Playwright has no working arm64 Chromium, uncomment to build and run
    # the image under Rosetta emulation instead. Correct but markedly slower.
    # platform: linux/amd64
    working_dir: /app
    volumes:
      # Bind-mounted so `git pull` on the host is picked up without a rebuild.
      # Only a dependency change needs `--build`.
      - .:/app
    env_file:
      - .env
```

- [ ] **Step 3: Create the `.env` on the Mini**

Not in git. On the Mini, `~/apps/pensiongraph/.env`:

```
DATABASE_URL=<the same Neon URL the GHA secret holds>
ANTHROPIC_API_KEY=<key>
RESEND_API_KEY=<key>
APPROVAL_EMAIL_FROM=<same value as the GHA secret>
APPROVAL_EMAIL_RECIPIENT=<same value as the GHA secret>
R2_ACCOUNT_ID=<same value as the GHA secret>
R2_ACCESS_KEY_ID=<same value as the GHA secret>
R2_SECRET_ACCESS_KEY=<same value as the GHA secret>
R2_BUCKET=<same value as the GHA secret>
```

`LLM_MODE` and `INSIGHTS_MODE` must be **absent**, not set to `live`.

**The four `R2_*` values are not optional here, though nothing fails without
them.** Per CLAUDE.md, if any one is missing PDF retention is a silent no-op:
the run fetches, extracts, goes green, and retains nothing. These are the 14
plans no other machine can reach, so their PDFs are the ones least likely to
be recoverable later — discarding them is the most expensive version of that
mistake, and it is invisible.

The Mini's own disk is not a substitute. It keeps the file where a GHA runner
would not, but it is one unbacked disk, which is the arrangement spec §5.4
rejected when it declined to move the store here. ("no backups yet" is the
first item on the runbook's own Outstanding list.)

Since 2026-08-31 the extractors read through `pdf_store.document_pdf`, so a
PDF the Mini retains is readable from GHA and Render too. Before that wiring
this would have been write-only bookkeeping; now it is what decouples the
fetch from the extract.

`fetcher.py` prints one line at the start of every run saying whether
retention is on. On the Mini's first run that line is the thing to read: it is
the difference between working and silently discarding the 8.5% of AUM the
machine was bought to reach.

- [ ] **Step 4: Build the image**

```bash
cd ~/apps/pensiongraph
docker compose -f docker-compose.pipeline.yml build
```

Expected: completes. Two distinct ways it can fail, with different fixes —
spec §7 risks 1 and 2:

- **`playwright install --with-deps chromium` reports an unsupported platform
  or downloads nothing.** Uncomment `platform: linux/amd64` in the compose
  file, rebuild, and note it in the commit message.
- **`pip install` starts compiling instead of installing** — watch for
  "Building wheel for PyMuPDF / lxml / psycopg". aarch64 wheels exist for all
  three, so a source build means a version or index problem, not a missing
  wheel. Let it finish (it is slow but works) and record which package it was.

- [ ] **Step 5: Verify the browser actually launches, against a scratch DB**

Do not point the first run at Neon. `database.py` uses `DB_PATH` when
`DATABASE_URL` is empty, which gives a disposable SQLite file:

```bash
cd ~/apps/pensiongraph
docker compose -f docker-compose.pipeline.yml run --rm \
    -e DATABASE_URL= -e DB_PATH=/tmp/probe.db \
    pipeline python pipeline.py asrs --fetch-only --max-docs 2
```

`asrs` is chosen deliberately: it is `materials_type: playwright` and it is in
the Cloudflare-challenge group, so a success here settles both open questions
at once — arm64 Chromium works, *and* the browser gets past the challenge from
this IP.

Expected: documents discovered and fetched, no `Falling back to Playwright...`
loop, no browser-launch traceback.

**If Chromium launches but the challenge still blocks:** that is the real
stop condition for the nine-plan Cloudflare group. The 403 group may still be
worth doing alone — report and ask before continuing.

- [ ] **Step 6: Commit**

```bash
git add Dockerfile.pipeline docker-compose.pipeline.yml
git commit -m "Add one-shot pipeline container for the Mac Mini (arm64 Playwright)"
```

---

### Task 3: `scripts/waf_blocked_ids.py` — one source of truth for the ID lists

The runner needs 11 IDs for `pipeline.py` and 5 for `refresh_cafrs.py`. Writing
them into a shell script would create a second copy of a list that already
exists twice in JSON, and it would rot silently the first time a plan is
unblocked. This helper derives them, and its tests assert it agrees with what
the two entry points actually subtract.

**Files:**
- Create: `scripts/waf_blocked_ids.py`
- Test: `tests/test_waf_blocked_ids.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `materials_ids() -> list[str]` — sorted IDs from `data/waf_blocked_plans.json`
  - `cafr_ids() -> list[str]` — sorted IDs from `data/waf_blocked_cafr_plans.json`
  - `all_ids() -> list[str]` — sorted, deduplicated union
  - `main(argv: list[str] | None = None) -> int` — prints one space-separated
    line; `--materials`, `--cafr`, or neither (union)

  Task 4's runner calls `python -m scripts.waf_blocked_ids --materials` and
  `--cafr`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_waf_blocked_ids.py`:

```python
"""The WAF-blocked plan IDs must have exactly one source of truth.

Two JSON files already list them, and `pipeline.py` and `refresh_cafrs.py`
each subtract their own list on every run. The Mac Mini job (spec
2026-08-30-mac-mini-migration-design.md, Stage 1) needs the same lists a third
time, to pass them back *in* on the CLI.

A hardcoded copy in a shell script would be correct on the day it was written
and wrong the first time a plan is unblocked -- and wrong silently, because
naming a plan that no longer needs naming still works. These tests bind the
helper to the two loaders that actually gate production.
"""

from __future__ import annotations

import json
import pathlib

import pipeline
import refresh_cafrs
from scripts import waf_blocked_ids


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


def test_materials_ids_match_the_pipeline_loader():
    """If these diverge, the Mini fetches a different set than the cloud skips."""
    assert set(waf_blocked_ids.materials_ids()) == set(
        pipeline._load_waf_blocked_ids())


def test_cafr_ids_match_the_refresh_cafrs_loader():
    assert set(waf_blocked_ids.cafr_ids()) == set(
        refresh_cafrs._load_waf_blocked_ids())


def test_all_ids_is_the_deduplicated_union():
    """asrs and strs_ohio are on both lists; the runner must not fetch twice."""
    expected = set(waf_blocked_ids.materials_ids()) | set(
        waf_blocked_ids.cafr_ids())
    result = waf_blocked_ids.all_ids()
    assert set(result) == expected
    assert len(result) == len(set(result)), "all_ids() contains duplicates"


def test_every_blocked_id_exists_in_the_registry():
    """A typo'd id is accepted by the CLI and silently fetches nothing.

    pipeline.py subtracts unknown ids from the registry without complaint, so
    a misspelling on a block list has no symptom today -- it only appears once
    something tries to fetch that id by name, which is what Stage 1 does.
    """
    with open(REPO_ROOT / "data" / "known_plans.json", encoding="utf-8") as f:
        registry = {p["id"] for p in json.load(f)}
    unknown = sorted(set(waf_blocked_ids.all_ids()) - registry)
    assert not unknown, f"block lists name ids absent from known_plans.json: {unknown}"


def test_lists_are_non_empty():
    """Guard on the guards above: an empty list satisfies every set comparison."""
    assert waf_blocked_ids.materials_ids()
    assert waf_blocked_ids.cafr_ids()


def test_cli_prints_space_separated_materials_ids(capsys):
    rc = waf_blocked_ids.main(["--materials"])
    assert rc == 0
    out = capsys.readouterr().out.strip()
    assert out.split() == waf_blocked_ids.materials_ids()


def test_cli_prints_space_separated_cafr_ids(capsys):
    rc = waf_blocked_ids.main(["--cafr"])
    assert rc == 0
    assert capsys.readouterr().out.strip().split() == waf_blocked_ids.cafr_ids()


def test_cli_defaults_to_the_union(capsys):
    rc = waf_blocked_ids.main([])
    assert rc == 0
    assert capsys.readouterr().out.strip().split() == waf_blocked_ids.all_ids()
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
LLM_MODE=mock pytest tests/test_waf_blocked_ids.py -q
```

Expected: collection error — `ModuleNotFoundError: No module named
'scripts.waf_blocked_ids'`.

- [ ] **Step 3: Write the implementation**

Create `scripts/waf_blocked_ids.py`:

```python
"""The WAF-blocked plan IDs, in one place, for the Mac Mini job.

`pipeline.py` and `refresh_cafrs.py` each load their own block list and
subtract it from the registry on every run. The Mini needs the same two lists
in the opposite direction -- to name those plans explicitly on the CLI, which
is the documented bypass:

    "Explicit CLI args win and bypass the block list -- that is how you run a
     blocked plan by hand from a residential IP."
        -- pipeline.py::_resolve_plan_ids

Usage:
    python -m scripts.waf_blocked_ids               # union, space-separated
    python -m scripts.waf_blocked_ids --materials   # for pipeline.py
    python -m scripts.waf_blocked_ids --cafr        # for refresh_cafrs.py

Shell consumers word-split the single output line, so the format is one line
of space-separated ids and nothing else -- no header, no trailing commentary.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
MATERIALS_FILE = REPO_ROOT / "data" / "waf_blocked_plans.json"
CAFR_FILE = REPO_ROOT / "data" / "waf_blocked_cafr_plans.json"


def _load(path: Path) -> list[str]:
    with open(path, encoding="utf-8") as f:
        return sorted(p["id"] for p in json.load(f)["plans"])


def materials_ids() -> list[str]:
    """Plans whose board materials are WAF-blocked (fed to pipeline.py)."""
    return _load(MATERIALS_FILE)


def cafr_ids() -> list[str]:
    """Plans whose CAFR PDF is WAF-blocked (fed to refresh_cafrs.py)."""
    return _load(CAFR_FILE)


def all_ids() -> list[str]:
    """The deduplicated union. asrs and strs_ohio appear on both lists."""
    return sorted(set(materials_ids()) | set(cafr_ids()))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="scripts.waf_blocked_ids")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--materials", action="store_true",
                       help="Only the board-materials block list")
    group.add_argument("--cafr", action="store_true",
                       help="Only the CAFR block list")
    args = parser.parse_args(argv)

    if args.materials:
        ids = materials_ids()
    elif args.cafr:
        ids = cafr_ids()
    else:
        ids = all_ids()

    print(" ".join(ids))
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
LLM_MODE=mock pytest tests/test_waf_blocked_ids.py -q
```

Expected: 8 passed.

- [ ] **Step 5: Run the full suite — the anti-drift tests import production modules**

```bash
LLM_MODE=mock pytest tests/ -q
```

Expected: no new failures. `tests/test_pipeline_cli.py` sweeps every module
containing `ArgumentParser(` and asserts it calls `parse_args()`; the new
script does, so it should pass that sweep on its first run. If it fails there,
the sweep is telling you `main()` is unreachable.

- [ ] **Step 6: Commit**

```bash
git add scripts/waf_blocked_ids.py tests/test_waf_blocked_ids.py
git commit -m "Add scripts/waf_blocked_ids: one source of truth for the blocked plan ids"
```

---

### Task 4: `scripts/run_waf_plans.sh` — the runner

The macOS counterpart to `scripts/run_recordings.bat`, and deliberately built
to the same shape: a log file per task, a `notify_failure` call on every
non-zero step, and a `git pull` first so the block lists and plan registry are
current. Every Python invocation goes through the Task 2 container, so the
Mini needs only `git` and `docker` on its PATH.

**Files:**
- Create: `scripts/run_waf_plans.sh`
- Test: `tests/test_run_waf_plans_script.py`

**Interfaces:**
- Consumes: `scripts/waf_blocked_ids.py` (Task 3), the `pipeline` compose
  service (Task 2), `scripts/notify_failure.py` (already exists — invoked as
  `python -m scripts.notify_failure <task> <step> <log_path> [exit_code]`).
- Produces: an executable at `scripts/run_waf_plans.sh` taking no arguments,
  exiting 0 on success and non-zero on failure. Task 5's launchd agent calls
  it by absolute path.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_run_waf_plans_script.py`:

```python
"""Static guards on the Mac Mini runner script.

The script cannot be exercised in CI -- it needs Docker, a residential IP and
a real Neon URL -- so these tests assert the properties that would otherwise
rot unnoticed. This mirrors the static backstops already in
tests/test_pipeline_cli.py and tests/test_deployment_config.py, and exists for
the same reason: the failure mode is silent.
"""

from __future__ import annotations

import pathlib

from scripts import waf_blocked_ids


SCRIPT = (pathlib.Path(__file__).resolve().parents[1]
          / "scripts" / "run_waf_plans.sh")


def _source() -> str:
    return SCRIPT.read_text(encoding="utf-8")


def test_the_runner_exists():
    assert SCRIPT.exists(), f"{SCRIPT} is missing"


def test_no_plan_id_is_hardcoded():
    """The whole point of scripts/waf_blocked_ids.py.

    A literal id here is correct today and silently wrong the first time a
    plan is unblocked: naming a plan that no longer needs naming still
    succeeds, so nothing fails and the list quietly diverges from the JSON.
    """
    src = _source()
    leaked = [pid for pid in waf_blocked_ids.all_ids() if pid in src]
    assert not leaked, f"hardcoded plan ids in run_waf_plans.sh: {leaked}"


def test_both_id_lists_are_sourced_from_the_helper():
    src = _source()
    assert "scripts.waf_blocked_ids --materials" in src
    assert "scripts.waf_blocked_ids --cafr" in src


def test_both_pipeline_entry_points_are_invoked():
    src = _source()
    assert "pipeline.py" in src, "board materials are never fetched"
    assert "refresh_cafrs.py" in src, "CAFRs are never refreshed"


def test_every_step_notifies_on_failure():
    """run_recordings.bat's pattern: a failed step emails rather than exiting
    quietly into a log nobody opens.

    Counts calls to the script's own `notify` helper rather than occurrences
    of `scripts.notify_failure` -- the helper wraps it, so the module name
    appears exactly once no matter how many steps are guarded.
    """
    src = _source()
    assert "scripts.notify_failure" in src, "no failure notification at all"
    # Lines invoking the helper, excluding its own definition.
    calls = [ln for ln in src.splitlines()
             if ln.strip().startswith("notify ")]
    assert len(calls) >= 4, (
        f"only {len(calls)} guarded steps -- a failure would be silent")


def test_database_url_is_asserted_before_any_work():
    """An unset or empty DATABASE_URL is an empty SQLite file: the job reads
    nothing, writes nothing and exits zero. CLAUDE.md calls this out as the
    first thing to check when a deployment looks like total data loss."""
    assert "DATABASE_URL" in _source()


def test_mock_modes_are_not_set():
    src = _source()
    assert "LLM_MODE=mock" not in src
    assert "INSIGHTS_MODE=mock" not in src
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
LLM_MODE=mock pytest tests/test_run_waf_plans_script.py -q
```

Expected: `test_the_runner_exists` fails; the rest error on the missing file.

- [ ] **Step 3: Write the runner**

Create `scripts/run_waf_plans.sh`:

```bash
#!/bin/bash
# ------------------------------------------------------------------------
# WAF-blocked plans -- Mac Mini only.
#
# The 14 plans in data/waf_blocked_plans.json and
# data/waf_blocked_cafr_plans.json sit behind bot mitigation that refuses
# datacentre IPs, so GitHub Actions subtracts them from every run. This
# machine has a residential IP and fetches exactly those plans, naming them
# explicitly on the CLI -- the documented bypass in both _resolve_plan_ids
# docstrings.
#
# Best-effort by design (spec 2026-08-30 §4). Rows go straight to Neon, which
# Render reads, so there is nothing to commit and nothing to deploy. If this
# machine is off for a month, coverage stops advancing on 14 plans and
# nothing else degrades.
#
# Manual run:  ~/apps/pensiongraph/scripts/run_waf_plans.sh
# Scheduled:   ~/Library/LaunchAgents/com.pensiongraph.wafplans.plist
# ------------------------------------------------------------------------
set -u

APP_DIR="$HOME/apps/pensiongraph"
COMPOSE="$APP_DIR/docker-compose.pipeline.yml"
TASK="waf_plans"
LOG="$APP_DIR/logs/$TASK.log"
LOCK="$APP_DIR/logs/$TASK.lock"

# Absolute paths: launchd and non-interactive SSH do not source .zprofile, so
# a bare `docker` or `git` fails with "command not found". The runbook records
# this biting scripted deploys; it bites scheduled jobs the same way.
GIT=/usr/bin/git
DOCKER=/usr/local/bin/docker

mkdir -p "$APP_DIR/logs"

# mkdir is atomic, and unlike flock it exists on stock macOS. launchd will not
# start a second copy of a running job anyway; this covers a manual run
# overlapping the scheduled one.
if ! mkdir "$LOCK" 2>/dev/null; then
    echo "[$(date -u +%FT%TZ)] $TASK already running, exiting" >> "$LOG"
    exit 0
fi
trap 'rmdir "$LOCK" 2>/dev/null' EXIT

cd "$APP_DIR" || exit 1

{
    echo ""
    echo "=== [$(date -u +%FT%TZ)] Starting $TASK ==="
} >> "$LOG"

# One-shot container. Every Python call goes through it, notify_failure
# included, so the host needs only git and docker.
run() {
    "$DOCKER" compose -f "$COMPOSE" run --rm pipeline "$@"
}

notify() {
    # $1 = step name, $2 = exit code. Best-effort: a failed notification must
    # not mask the failure it is reporting.
    run python -m scripts.notify_failure "$TASK" "$1" "logs/$TASK.log" "$2" \
        >> "$LOG" 2>&1 || true
}

# --- Guard: an unset or empty DATABASE_URL means SQLite ------------------
# database.resolve_database_url() falls back to DB_PATH, which in a fresh
# container is an empty file. The job would read nothing, write nothing and
# exit zero -- the failure this repo has already been bitten by.
if ! run python -c "
import os, sys
url = os.environ.get('DATABASE_URL') or ''
if not url.startswith('postgres'):
    sys.exit('DATABASE_URL is unset, empty, or not a Postgres URL')
" >> "$LOG" 2>&1; then
    notify database_url_guard 1
    exit 1
fi

# --- Keep the block lists and plan registry current ----------------------
echo "[$(date -u +%FT%TZ)] git pull --rebase" >> "$LOG"
if ! "$GIT" pull --rebase origin master >> "$LOG" 2>&1; then
    echo "[$(date -u +%FT%TZ)] pull failed, aborting rebase" >> "$LOG"
    "$GIT" rebase --abort >> "$LOG" 2>&1
    notify git_pull 1
    exit 1
fi

# --- Board materials: fetch + extract + summarize ------------------------
# `docker compose` writes its own progress to stderr, so stdout is just the
# one line the helper prints. 2>/dev/null keeps a compose warning out of the
# captured value.
MATERIALS_IDS=$(run python -m scripts.waf_blocked_ids --materials 2>/dev/null | tr -d '\r')
if [ -z "$MATERIALS_IDS" ]; then
    notify resolve_materials_ids 1
    exit 1
fi

echo "[$(date -u +%FT%TZ)] pipeline.py $MATERIALS_IDS" >> "$LOG"
# Capture the status into a variable rather than testing with `if ! ...`.
# Inside an `if ! cmd; then` body, `$?` is the status of the *negation* --
# always 0 -- so the notification would report success for a failed step.
# shellcheck disable=SC2086
run python pipeline.py $MATERIALS_IDS >> "$LOG" 2>&1
rc=$?
if [ "$rc" -ne 0 ]; then
    notify pipeline "$rc"
    # Not fatal: the CAFR refresh is independent and worth attempting.
    echo "[$(date -u +%FT%TZ)] pipeline exited $rc, continuing to CAFRs" >> "$LOG"
fi

# --- CAFRs ---------------------------------------------------------------
CAFR_IDS=$(run python -m scripts.waf_blocked_ids --cafr 2>/dev/null | tr -d '\r')
if [ -z "$CAFR_IDS" ]; then
    notify resolve_cafr_ids 1
    exit 1
fi

echo "[$(date -u +%FT%TZ)] refresh_cafrs.py $CAFR_IDS" >> "$LOG"
# shellcheck disable=SC2086
run python refresh_cafrs.py $CAFR_IDS >> "$LOG" 2>&1
rc=$?
if [ "$rc" -ne 0 ]; then
    notify refresh_cafrs "$rc"
    exit 1
fi

echo "=== [$(date -u +%FT%TZ)] $TASK completed ===" >> "$LOG"
exit 0
```

- [ ] **Step 4: Make it executable and run the tests**

```bash
git update-index --chmod=+x scripts/run_waf_plans.sh
LLM_MODE=mock pytest tests/test_run_waf_plans_script.py -q
```

Expected: 7 passed.

- [ ] **Step 5: Run it for real on the Mini, once, by hand**

```bash
ssh jameswalsh@JHCW-mini.local "~/apps/pensiongraph/scripts/run_waf_plans.sh"
tail -80 ~/apps/pensiongraph/logs/waf_plans.log
```

Expected: the guard passes, `git pull` succeeds, both commands run with the
plan IDs printed into the log, and the log ends with `waf_plans completed`.
This is the first run that writes to Neon.

- [ ] **Step 6: Confirm the rows landed, from the Windows box**

```powershell
python -c "import database, queries; s=database.get_session(); [print(r.source, r.started_at, r.status) for r in queries.recent_fetch_runs(s, 5)]"
```

Expected: a new row with `source='local'`. `pipeline.py` sets `source` from the
absence of `GITHUB_ACTIONS`, and the Mini is now the only writer using that
value, so it distinguishes Mini runs from GHA runs in the Admin tab's Recent
Runs without a schema change.

- [ ] **Step 7: Commit**

```bash
git add scripts/run_waf_plans.sh tests/test_run_waf_plans_script.py
git commit -m "Add run_waf_plans.sh: nightly WAF-blocked plan fetch for the Mac Mini"
```

---

### Task 5: Schedule it, and prove it survives a reboot

**Files:**
- Create: `docs/mac-mini/com.pensiongraph.wafplans.plist`
- Test: manual — a cold reboot, per the runbook's recovery chain

**Interfaces:**
- Consumes: `scripts/run_waf_plans.sh` (Task 4).
- Produces: a loaded launchd agent named `com.pensiongraph.wafplans`.

- [ ] **Step 1: Write the plist, committed so the schedule lives in git**

Create `docs/mac-mini/com.pensiongraph.wafplans.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!--
  Install to ~/Library/LaunchAgents/com.pensiongraph.wafplans.plist

  A LaunchAgent, not a LaunchDaemon: OrbStack's Docker runs inside the user
  session, so a daemon starting before login would find no docker socket.
  Auto-login is on (runbook, "Host settings applied"), which is what makes a
  user-session agent survive an unattended reboot.

  07:30 local, after the GHA daily pipeline at 11:00 UTC. launchd calendar
  intervals are local time and follow DST; the GHA cron is UTC and does not.
  They do not interact -- both write to the same Postgres and neither reads
  the other's output -- so the drift is harmless.

  If the machine is asleep or off at 07:30, launchd runs the job at the next
  wake. That is the intended best-effort behaviour.
-->
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.pensiongraph.wafplans</string>

    <key>ProgramArguments</key>
    <array>
        <string>/Users/jameswalsh/apps/pensiongraph/scripts/run_waf_plans.sh</string>
    </array>

    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key><integer>7</integer>
        <key>Minute</key><integer>30</integer>
    </dict>

    <key>StandardOutPath</key>
    <string>/Users/jameswalsh/apps/pensiongraph/logs/launchd.out.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/jameswalsh/apps/pensiongraph/logs/launchd.err.log</string>

    <key>RunAtLoad</key>
    <false/>
</dict>
</plist>
```

- [ ] **Step 2: Install and load it on the Mini**

```bash
cp ~/apps/pensiongraph/docs/mac-mini/com.pensiongraph.wafplans.plist \
   ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.pensiongraph.wafplans.plist
launchctl list | grep pensiongraph
```

Expected: one line naming `com.pensiongraph.wafplans`.

- [ ] **Step 3: Fire it manually through launchd**

Running the script directly (Task 4 step 5) proves the script. This proves
launchd's environment, which is the part that differs — no `.zprofile`, hence
the absolute `GIT` and `DOCKER` paths.

```bash
launchctl start com.pensiongraph.wafplans
sleep 30 && tail -40 ~/apps/pensiongraph/logs/waf_plans.log
cat ~/apps/pensiongraph/logs/launchd.err.log
```

Expected: the run starts. A `command not found` in `launchd.err.log` means an
absolute path in the script is wrong for this machine — check with
`which docker` and `which git` and correct the script, not the plist.

- [ ] **Step 4: Cold-reboot test**

The runbook's recovery chain, extended by one step:

```bash
sudo reboot
# wait, then from Windows:
ssh jameswalsh@JHCW-mini.local
who                                    # did auto-login happen?
docker ps -a | head                    # did OrbStack start?
launchctl list | grep pensiongraph     # is the agent loaded?
```

Expected: the agent is loaded without anyone touching the machine. This is the
property the whole host design rests on, and the one that makes the job
trustworthy as a schedule rather than something to remember.

- [ ] **Step 5: Delete the throwaway probe venv from Task 1**

```bash
rm -rf ~/apps/pensiongraph/.probe-venv
```

- [ ] **Step 6: Commit**

```bash
git add docs/mac-mini/com.pensiongraph.wafplans.plist
git commit -m "Add launchd agent for the Mac Mini WAF-blocked plan job"
```

---

### Task 6: Update the documentation that now says the wrong thing

Several documents assert these plans run nowhere. That was true between
2026-08-16 and this change, and is the kind of stale claim that costs an hour
the next time somebody debugs coverage.

**Files:**
- Modify: `CLAUDE.md` — "What this repo is" §1, and the "Where each cadence runs" table
- Modify: `data/waf_blocked_plans.json` — `_doc` and `_how_it_works` strings
- Modify: `data/waf_blocked_cafr_plans.json` — same
- Modify: `nextsteps.md` — close A4
- Modify: `docs/superpowers/specs/2026-08-30-mac-mini-migration-design.md` — Stage 1 status
- Modify: `docs/mac-mini-hosting-runbook2.md` — Outstanding list

**Interfaces:**
- Consumes: the verified behaviour from Tasks 1–5.
- Produces: nothing code-facing.

- [ ] **Step 1: `CLAUDE.md` — the "Cloud-only" claim in §1**

Replace:

```
Cloud-only: GHA cron handles 137 of 148 plans daily. The 14 WAF-blocked plans in
`data/waf_blocked_plans.json` / `data/waf_blocked_cafr_plans.json` are skipped
everywhere (no runner can reach them)
```

with:

```
Two hosts: GHA cron handles 137 of 148 plans daily, and the 14 WAF-blocked
plans in `data/waf_blocked_plans.json` / `data/waf_blocked_cafr_plans.json`
run nightly from the Mac Mini, whose residential IP is the thing the WAFs
accept and a datacentre runner's does not (`scripts/run_waf_plans.sh`, spec
`docs/superpowers/specs/2026-08-30-mac-mini-migration-design.md`). The Mini
job is **best-effort**: if it stops, coverage stops advancing on those 14
plans — 8.5% of tracked AUM, though 4 of the 14 had no documents anyway —
and nothing else degrades.
```

Keep the AUM figures; they now describe what is at stake if the Mini is off
rather than what is permanently missing.

- [ ] **Step 2: `CLAUDE.md` — add a row to the cadence table**

The table under "Where each cadence runs" currently lists one local job. Add:

```
| WAF-blocked plans (14) — materials + CAFR | launchd daily 07:30 local | local Mac Mini | `scripts/run_waf_plans.sh` |
```

and amend the sentence above it that says Task Scheduler owns exactly one job
— there are now two local jobs on two different machines.

- [ ] **Step 3: Correct the block lists' own prose**

Both files' `_doc` says *"These are SKIPPED EVERYWHERE"* and *"nothing runs
from a local Task Scheduler"*. Both `_how_it_works` say the ids are subtracted
"on every run, local or hosted." Update to: subtracted from every *default*
run, and passed back in explicitly by the Mac Mini job. Leave the `plans`
arrays untouched — the tests in Task 3 bind to them.

- [ ] **Step 4: Close A4 in `nextsteps.md`**

A4 currently reads "WAF proxy for the 14 blocked plans? ... Residential proxy
is $10–75/month." Mark it **DONE**, note that the Mini's residential IP
replaced the proxy at no cost, and reference the spec and this plan. Also
correct the "Coverage is 137 of 148 plans — see A4" bullet under E. Others.

- [ ] **Step 5: Update the spec and the runbook**

In `2026-08-30-mac-mini-migration-design.md`, change Status to record Stage 1
as implemented on today's date, and add whatever Task 1 and Task 2 found —
particularly if `platform: linux/amd64` was needed, since spec §7 risk 1 flags
that as the fallback and a future reader should know which branch was taken.

In `docs/mac-mini-hosting-runbook2.md`, add the job to the port/app inventory
and record that PensionGraph now has a non-service, `docker compose run`-style
workload on the box — the runbook currently documents only long-running
`restart: unless-stopped` services.

- [ ] **Step 6: Run the full suite and commit**

```bash
LLM_MODE=mock pytest tests/ -q
git add CLAUDE.md nextsteps.md data/waf_blocked_plans.json \
        data/waf_blocked_cafr_plans.json \
        docs/superpowers/specs/2026-08-30-mac-mini-migration-design.md \
        docs/mac-mini-hosting-runbook2.md
git commit -m "Docs: WAF-blocked plans now run from the Mac Mini; close A4"
```

---

## Done when

Spec §10's verification criteria, restated as a checklist:

- [ ] All 14 plan IDs fetch successfully from the Mini — confirming the
      residential IP is in fact the difference, which had never been tested
      against all 14 before Task 1.
- [ ] Their rows are in Neon and visible in the Streamlit app on Render with no
      deploy step.
- [ ] A `fetch_runs` row records the invocation with `source='local'`, so the
      Admin tab's Recent Runs distinguishes Mini runs from GHA runs.
- [ ] Per-plan "last updated" advances for the six plans carrying the real loss
      — ASRS, KPERS, NV PERS, NM PERA, CORP AZ, STRS Ohio.
- [ ] The host survives a cold reboot with the agent still loaded.
- [ ] `LLM_MODE=mock pytest tests/ -q` passes.

## Out of scope

Named because they are adjacent enough to drift into:

- **Stage 2** (recordings catalogue off Windows), **Stage 3** (`daily-pipeline`
  off GHA), **Stage 4** (app and database). Separate decisions, spec §8.
- **Any change to the 137 GHA-eligible plans.** The block lists' `plans`
  arrays stay exactly as they are; this stage adds a consumer, it does not
  reclassify a single plan.
- **The R2 PDF store** (`2026-08-29-pdf-retention.md`). Independent, and spec
  §5.4 explicitly rejects moving it to the Mini's disk.
- **`insights/` cadences.** Nothing here touches composition, publication or
  email beyond `notify_failure`.
