# Mac Mini hosting: what to migrate, and what not to

**Date:** 2026-08-30
**Status:** draft, pending decision. Nothing implemented.
**Reference:** `docs/mac-mini-hosting-runbook2.md` (the host, built and
reboot-verified 2026-08-30)
**Bears on:** base spec §1 (target architecture), §4 (jobs and cadences),
§6 (cost), §7 (coverage 148 → 137); portal spec §4 (the proxy);
`nextsteps.md` A3, A4, C1.
**Does not supersede** `2026-08-16-low-maintenance-app-design.md`. The
recommendation below is deliberately compatible with it — see §4.

## Goal

A Mac Mini now exists that serves containerised Python apps at subdomains of
`lazyeconomist.com` over Cloudflare Tunnel, with verified unattended reboot
recovery. This spec decides what, if anything, of PensionGraph should move onto
it.

The answer this spec argues for is: **take the one thing the Mini uniquely
offers — a residential IP — and leave the managed services alone.**

## Decisions proposed

| Question | Proposal |
|---|---|
| Streamlit app: Render → Mini | **No.** Trades managed backups, SLA and encrypted-at-rest storage for ~$30/month. |
| Database: Neon → local Postgres | **No**, and it forces the app to move with it. See §2. |
| Scheduled jobs: GHA → Mini | **Only if the Actions ceiling actually bites**, and then only `daily-pipeline`. Try making the repo public first (A3). |
| PDF store: R2 → local disk | **No.** Cost is not the constraint; durability is, and one unbacked disk is worse than R2. |
| WAF-blocked fetches from the Mini | **Yes.** The whole value of the machine, at best-effort risk. See §3. |
| Recordings catalogue: Windows → Mini | **Yes.** Already a sanctioned local exception; the Mini is a strictly better host. |
| Backups on the Mini | **Prerequisite for anything**, per the runbook's own Outstanding list. |

## 1. This is not one decision

"Move the app to the Mini" is four separable moves:

| | Move | Today | Today's cost |
|---|---|---|---|
| **M1** | Streamlit app | Render web service | ~$7–25/mo |
| **M2** | Scheduled jobs (8 GHA crons) | GitHub Actions | 2,000 min/mo cap (private repo) |
| **M3** | Database | Neon Postgres, Launch tier | ~$19/mo |
| **M4** | PDF document store | R2 (planned, not built) | ~$0.11/mo |

They should be decided separately, and they do not carry equal weight.

## 2. The one hard coupling: M3 forces M1

A Postgres on the Mini sits behind Xfinity NAT. Render cannot reach it.

Cloudflare Tunnel publishes **HTTP hostnames**, not raw TCP — the runbook's
step 3 form takes `localhost:8502` and a subdomain, and that is an HTTP hop.
Exposing Postgres would need `cloudflared access tcp` running client-side,
which Render's runtime cannot do.

So: **if the database moves, the app must move with it.** M1 is independent of
M3 (the app can run on the Mini and still read Neon over the network); M3 is
not independent of M1. M2 works with either database.

## 3. What the Mini uniquely offers

Compute and disk are cheap and managed. The Mini offers exactly one capability
that cannot be bought at this price: **a residential IP address.**

That is the whole argument, and it is a strong one:

- 14 plans are WAF-blocked because their sites reject datacentre IPs —
  **$486B of $5,689B tracked AUM, 8.5%** — holding coverage at 137 of 148
  (base spec §7).
- Genuine ongoing loss concentrates in six plans — ASRS, KPERS, NV PERS,
  NM PERA, CORP AZ, STRS Ohio — "roughly $245B and ~155 documents of history
  that stops growing."
- Portal spec §4 **promoted the residential proxy from a revisit path to
  required**, at $10–75/month against a $20–50/month total budget. The Mini
  makes that line item disappear rather than shrink.

The code already supports this. Both `pipeline.py::_resolve_plan_ids` and
`refresh_cafrs.py::_resolve_plan_ids` say so in their docstrings:

> "Explicit CLI args win and bypass the block list — that is how you run a
> blocked plan by hand from a residential IP."

Naming the blocked plans on the CLI is a supported path, not a workaround. No
fetcher change, no proxy configuration, no new dependency.

Everything else the Mini offers is a cost saving with a reliability cost
attached.

## 4. The tension with the base spec, stated plainly

The 2026-08-16 spec's success criterion is:

> "nothing routine requires James's attention, **no machine of his is in the
> path of the app working**, and adding a user is one row."

and §1: "GHA remains the only scheduler. No Render cron, no Task Scheduler in
the critical path." §10 accepts moving the single point of failure from James's
PC to "Neon plus Render" precisely because **"Both are managed with backups,
which is the point."**

A full migration reverses that decision two weeks after it landed.

**The honest counter-argument:** the Mini is materially different from "James's
PC." It is dedicated, headless, always-on, on wired Ethernet, with reboot
recovery verified by a cold-boot test with nobody at the machine. That is a
server, not a laptop that gets closed.

**The counter-counter-argument:** it is still one disk, one house, one Xfinity
line, one power circuit — and the runbook's own Outstanding section opens with
"Backups. Nothing yet."

Both are true. The resolution is the distinction the base spec itself already
draws in §4, "The one deliberate local exception": the recordings catalogue is
allowed to run on James's machine because it is **explicitly best-effort** —
"if James's machine is off for a month, nothing else degrades and no cloud job
depends on its output."

That is the test. Work that is best-effort may live on the Mini. Work the app's
correctness depends on may not.

The WAF-blocked plans pass this test cleanly. If the house loses power,
coverage stops advancing on 14 plans and **nothing user-facing breaks** — the
site, the digests, the briefings and the other 137 plans are untouched. It is
not in the path of the app working.

## 5. Move-by-move

### 5.1 M1 — Streamlit app: Render → Mini. **Recommend no.**

**Gains:** ~$7–25/mo; no cold starts; no build minutes; no deploy latency; a
real disk, so `downloads/` stops being a cache that evaporates.

**Costs:** availability regresses to residential Xfinity with no failover and
no SLA. Secrets move onto an unencrypted disk — FileVault is off with
auto-login, deliberately, because unattended reboot requires it. Today the Mini
hosts nothing sensitive; after M1 `~/apps/pensiongraph/.env` holds
`ANTHROPIC_API_KEY`, `RESEND_API_KEY` and `ADMIN_PASSWORD`. The runbook
anticipated exactly this: "Revisit if client-confidential material ever lands
on the disk."

**Steps, if done anyway:**

1. Add `Dockerfile` (`python:3.12-slim`, `pip install -r requirements.txt`,
   CMD `streamlit run app.py --server.port=8502 --server.address=0.0.0.0
   --server.headless=true`), `docker-compose.yml` with
   `restart: unless-stopped`, and `.dockerignore`. Note `packages.txt`
   (`libsqlite3-dev`) is a Render-ism and is not needed in the image.
2. Move `pensiongraph.com` nameservers from Namecheap
   (`dns1/dns2.registrar-servers.com`) to Cloudflare. The tunnel writes DNS
   automatically **only for zones hosted at Cloudflare**. This is a registrar
   change with a propagation window and a TLS re-issue.
3. Clone to `~/apps/pensiongraph`; hand-write `.env` with the six variables
   `render.yaml` declares — `DATABASE_URL`, `APP_BASE_URL`, `RESEND_API_KEY`,
   `APPROVAL_EMAIL_FROM`, `APPROVAL_EMAIL_RECIPIENT`, `ADMIN_PASSWORD` — plus
   `RECORDINGS_DIR` and `DOWNLOADS_DIR`.
4. `docker compose up -d --build`; publish the tunnel hostname
   (HTTP → `localhost:8502`). **Skip runbook step 4.** PensionGraph is a public
   site with an in-app `ADMIN_PASSWORD` gate on the Archive/Drafts/Admin tabs;
   fronting the whole site with Cloudflare Access would be wrong. Port 8501 is
   occupied by the test app pending teardown, so 8502.
5. Run in parallel with Render on a test subdomain for a week. Cut the apex and
   `www` only after that; delete the Render service only after *that*.
6. Add `deploy pensiongraph` to the PowerShell `$PROFILE` function.

**Reversibility:** high. Render can be recreated from `render.yaml` in minutes.

### 5.2 M2 — Scheduled jobs: GHA → Mini. **Recommend: only under pressure.**

**Gains:** dissolves `nextsteps.md` A3. Private repos cap Actions at 2,000
minutes/month and the daily Playwright pipeline is approaching it. Running it
on the Mini removes the ceiling without publishing the code and history.

**Costs:** GitHub Actions is not just a cron — it is an operations layer that is
currently free. Replacing it means, per workflow: a launchd plist, absolute
binary paths (the runbook's non-interactive-SSH `PATH` gotcha applies to launchd
too), a log destination with rotation, a lockfile standing in for
`concurrency: daily-pipeline`, and failure notification. Resend is already wired
so notification is cheap, but this is roughly 150 lines of glue that becomes
yours to maintain. Also lost: log retention, manual re-runs, `workflow_dispatch`,
and managed secret storage.

Two second-order consequences:

- **`insights/github_dispatch.py` becomes vestigial.** Anything relying on
  `GITHUB_DISPATCH_TOKEN` needs a local equivalent or deletion.
- **Four workflows still `git commit && git push`** (`notes/`,
  `cafr_summaries/`, `data/asset_class_mappings.json`). Moving them puts a
  write-capable deploy key on the unencrypted disk. The `!cancelled()` guards on
  those steps keep their current rationale unchanged.

**If done:** piecemeal, heaviest first. `daily-pipeline.yml` (360-minute
timeout, all the Playwright minutes) buys nearly all of the A3 benefit alone.
Keep `test.yml` on GHA regardless — CI belongs there.

**Try A3 first.** Making the repo public is free, instant, removes the ceiling
completely, and the underlying data is all public record. It is the cheaper
answer to the same problem.

### 5.3 M3 — Neon → local Postgres. **Recommend no.**

**Gains:** ~$19/mo, and it makes the 2026-08-25 failure mode impossible. That
outage — Neon's 5 GB monthly transfer quota exhausted, compute suspended — took
down Streamlit, all eight crons and local shells simultaneously. A local
Postgres has no transfer meter. It would also close `nextsteps.md` C1, which
nobody has yet looked at.

**Costs:** the largest of the four. Requires M1 (§2). Trades Neon's managed
backups and point-in-time recovery for a `pg_dump` you write and must test. Puts
**subscriber email addresses** — personal data belonging to third parties — on
an unencrypted volume in a house. That is the condition the runbook flagged as
the trigger for revisiting FileVault, and FileVault-on breaks the unattended
reboot recovery the whole host design rests on.

Note the egress discipline stays valuable regardless: the deferred
`extracted_text` column and the `undefer()` rule in CLAUDE.md are correct
engineering, not a Neon workaround. Removing the meter would remove the
*feedback* that found the N+1, not the N+1.

**Steps, if done anyway:** Postgres container on a named volume; `pg_dump` from
Neon and restore; verify with `scripts/compare_backends.py`; repoint
`DATABASE_URL`. **Hard prerequisites:** a nightly `pg_dump` to R2 or B2 with a
**restore actually tested**, and a decision on FileVault. Time Machine is not a
substitute — the runbook is right that snapshots of a running database are not
reliably restorable.

**Reversibility:** lowest of the four. Keep the Neon project alive and suspended
for a month.

### 5.4 M4 — PDF store on local disk. **Recommend no.**

The 460 GB disk comfortably holds the 7.32 GB across 4,542 documents that
`2026-08-29-pdf-retention-design.md` sizes. But cost is not the constraint — R2
is ~$0.11/month with zero egress — and the spec's entire argument is
**durability**: 2,633 of 4,542 documents already have no recoverable local file.
Content-addressed objects in R2 are strictly better than one unbacked local
disk.

Ship the R2 plan as designed. Note the design already accommodates a machine
that happens to hold the file: `local_path` "stops being the only copy but
remains useful", and `open_local_or_remote()` prefers a present local file. The
Mini gets that benefit for free without becoming the store.

### 5.5 The two that should happen

**WAF-blocked fetches (§3).** The highest-value, lowest-risk move available.

**Recordings catalogue.** Currently Windows Task Scheduler running
`scripts/run_recordings.bat --no-downloads`, writing to
`D:\PensionGraph\meetingrecordings` (`video_storage.py`). `RECORDINGS_DIR` is
already an env-var override — "so the same code can run on a non-Windows test
box" — so this is configuration, not code. Already a sanctioned local exception;
the Mini is a strictly better host than a desktop.

## 6. Cost

| Line | Today | After the recommended stages | After a full migration |
|---|---|---|---|
| Render | ~$7–25/mo | unchanged | $0 |
| Neon (Launch) | ~$19/mo | unchanged | $0 |
| R2 (PDF store, planned) | ~$0.11/mo | unchanged | $0 |
| Residential proxy (portal spec §4, required) | $10–75/mo | **$0 — dissolved** | $0 |
| GitHub Actions | free to 2,000 min/mo | unchanged | free (unused) |
| Claude API | ~$10–15/mo | unchanged | unchanged |

Against the base spec's ~$20–50/month budget, the recommended stages **remove
the largest pending line item without adding one**. A full migration saves a
further ~$26–44/month and spends it on availability, backups and
encryption-at-rest.

## 7. Risks to verify before committing to anything

1. **Playwright Chromium on linux/arm64.** `requirements-pipeline.txt` pins
   `playwright==1.49.0`; `daily-pipeline.yml` runs
   `playwright install --with-deps chromium` on `ubuntu-latest` (x86-64). arm64
   Linux is supported but has historically been the flakiest corner of
   Playwright's matrix. Build the image and run one plan before planning around
   it. Fallback is `platform: linux/amd64` under Rosetta — works, markedly
   slower. **This gates §3 and M2 both.**
2. **Native wheels on aarch64** — `PyMuPDF`, `lxml`, `psycopg[binary]`. Wheels
   exist; a miss turns into a long source compile. Verify at image-build time.
3. **Streamlit behind Cloudflare Tunnel** (M1 only). Streamlit is
   WebSocket-dependent, and this is where Streamlit reverse-proxy setups usually
   break. Test the full app, not `curl -sI`.
4. **DNS move for M1** (§5.1 step 2) — registrar change, propagation window, TLS
   re-issue. `www.pensiongraph.com` was only just repointed on 2026-08-29
   (`nextsteps.md` B2); do not stack a second DNS change on an unsettled one.
5. **The site must stay public.** Runbook step 4 is skipped for PensionGraph.
6. **Port 8501** is occupied by the test app pending teardown.

## 8. Recommended sequence

**Stage 0 — backups on the Mini.** Independently worth doing; the runbook lists
it as outstanding, and it gates every stage below. Time Machine to an external
drive, and — before any database lands there — a nightly `pg_dump` with a tested
restore.

**Stage 1 — WAF-blocked fetches from the Mini.** Nightly, naming the blocked
plans explicitly on the CLI, writing to Neon over the network. Restores 8.5% of
tracked AUM, dissolves `nextsteps.md` A4 and portal spec §4's required proxy,
and costs nothing. Best-effort by construction: a house outage stalls 14 plans
and breaks nothing user-facing. Verify risk 1 first.

**Stage 2 — recordings catalogue moves off Windows.** Cheap, tidy, already
sanctioned.

**Stage 3 — `daily-pipeline` to the Mini, only if the Actions ceiling bites.**
Revisit A3 (public repo) first; it is the cheaper answer to the same problem.

**Stage 4 — M1 and M3. Probably never.** At this stage of the product, trading
managed backups, an SLA and encrypted-at-rest storage for ~$30/month is a bad
trade. Revisit if Render or Neon costs rise materially, or if the base spec's
phase-2 trigger fires and the architecture is being reopened anyway.

Stage 1 alone captures the largest single quantified gain in `nextsteps.md`,
costs nothing, and leaves the low-maintenance architecture intact.

## 9. What this spec deliberately does not decide

- **Whether the app should ever leave Render.** Stage 4 is "probably never", not
  "no". The trigger is a cost or architecture change, not this document.
- **Tailscale, and the team-domain rename.** Runbook Outstanding items,
  unrelated to PensionGraph.
- **A3 (public repo).** Raised here as the cheaper alternative to Stage 3, but
  it is an independent decision with its own consequences.

## 10. Verification

Stage 1 is done when, on a schedule:

- The 14 plan IDs in `data/waf_blocked_plans.json` and
  `data/waf_blocked_cafr_plans.json` fetch successfully from the Mini —
  confirming the residential IP is in fact the difference, which is the
  assumption the whole stage rests on and which has never been tested against
  all 14.
- Their rows land in Neon and are visible in the Streamlit app on Render with no
  deploy step, per the shared-database property.
- A `fetch_runs` row records the invocation with `source='local'`
  (`pipeline.py` sets this from the absence of `GITHUB_ACTIONS`), so the Admin
  tab distinguishes Mini runs from GHA runs.
- Per-plan "last updated" advances for the six plans that carry the real loss.
- The host survives a cold reboot with the job still scheduled — the runbook's
  existing recovery chain, extended to cover the new unit.
