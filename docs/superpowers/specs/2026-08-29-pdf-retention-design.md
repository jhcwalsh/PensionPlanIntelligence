# PDF retention: the R2 document store

**Date:** 2026-08-29
**Status:** draft, pending review.
**Implements:** base spec §1 ("PDFs move to R2"), portal spec §2.3 (R2 as a
prerequisite for retroactive body search) and §3 (durable copy per document).
**Blocks:** `2026-08-29-performance-coverage-design.md` §5.

## Goal

Source PDFs are fetched, extracted, and then discarded. Everything that later
needs the original document — re-extraction at a higher cap, a new structured
extractor, a durable link — is either impossible or a re-download that may
no longer resolve. This spec makes the PDF a retained artefact.

## Decisions taken

| Question | Decision |
|---|---|
| Store | **Cloudflare R2**, per base spec §1. |
| Backfill | **Everything, now.** ~7.3 GB. Link rot only makes this harder later. |
| Upload site | **The GHA daily pipeline**, at fetch time. Keeps GHA the only scheduler. |
| Key scheme | **Content-addressed** by SHA-256, not by URL or path. See §3.1. |
| Versioning | **Explicit.** R2 has no S3-style object versioning; content-addressing supplies it. |
| Deletion | **Never, by default.** No lifecycle expiry on document objects. |

---

## 1. Why now

Measured 2026-08-29 against the live corpus:

| Metric | Value |
|---|---|
| Documents | 4,542 |
| Total PDF bytes (recorded `file_size_bytes`) | 7.32 GB |
| Mean / max document | 1.6 MB / 89.8 MB |
| PDFs still present on local disk | **1,909** |
| PDFs already gone | **2,633** |

The decisive figure is a link-rot sample: of 20 randomly-sampled documents
whose PDF is gone locally, **19 (95%) are still fetchable from their source
URL today**. One returned 404.

That number is the argument for doing this now rather than later. Backfill is
viable *today* and monotonically less viable every month. The 5% already lost
is a floor that only rises.

**The cost is not the obstacle.** At R2's ~$0.015/GB/month, 7.32 GB is
roughly **$0.11/month**, with zero egress charges. This is noise against the
existing ~$10-15/month API spend and the Render/Neon lines.

## 2. What retention unblocks

This is not a speculative decoupling. Four concrete, already-diagnosed
problems trace to the same missing artefact:

1. **The 450 truncated documents.** `MAX_STORED_CHARS` was raised from 150k
   to 2,000,000 once Postgres removed the file-size ceiling, but those rows
   still hold truncated text and there is nothing to re-extract *from*.
   Portal spec §2.3 already identified this as gated on R2.
2. **The 5 `missing_file` CAFRs** stuck at "pending extract" (nextsteps D3) —
   only 45 of 140 CAFR PDFs remain on disk.
3. **Performance coverage §5** — recovering board packs whose performance
   table sits past the old truncation point.
4. **Fetch/extract coupling.** CLAUDE.md documents that structured extraction
   must run in the same job as the fetch *because* the PDF exists only on the
   runner. Base spec §1 notes this constraint caused two separate defects
   found in the 2026-08-15 review.

Two independent features (D3, performance coverage) hit this wall in two
sessions. That is the signal it is a shared dependency, not a per-feature
workaround.

## 3. Architecture

One R2 bucket, `pension-documents`, separate from anything the abandoned
`pension-db` route used.

### 3.1 Content-addressed keys

```
documents/<sha256>.pdf
```

The key is the SHA-256 of the file bytes. Not the URL, not `plan_id/filename`.

**Why.** R2 does not support S3-style object versioning (established in the
2026-07-08 db-to-r2 spec and still true). Content-addressing supplies what
versioning would:

- **Idempotent.** Re-uploading an identical PDF is a no-op; no duplicate
  objects, no overwrite decision.
- **A restated document is a new object**, not a silent clobber of the old.
  A plan republishing a corrected board pack keeps both, and the `documents`
  row records which hash it was extracted from.
- **Dedupes across plans.** Multi-employer systems that publish the same PDF
  under several plan URLs store one object.
- **No path-escaping problems.** Source URLs contain `%20`, query strings,
  and unicode; a hex digest does not.

The human-meaningful naming already exists in the `documents` row (plan,
filename, URL, dates). The object store does not need to duplicate it.

### 3.2 Schema

Two columns on `documents`, no new table:

| Column | Meaning |
|---|---|
| `content_sha256` | the R2 key; null = not stored |
| `r2_uploaded_at` | when it landed; null = not stored |

`local_path` stays as-is. It stops being the only copy but remains useful
for a machine that happens to have the file (the recordings tab already
depends on this pattern for video).

Per CLAUDE.md: add the columns to the model and re-run `init_db()`. **No
ALTER TABLE migration**, no Alembic.

### 3.3 Access module

New `pdf_store.py`, thin over boto3 against R2's S3-compatible endpoint:

- `put(path_or_bytes) -> sha256` — hash, upload if absent, return the key.
- `get(sha256) -> bytes` — fetch, verifying the digest matches on the way out.
- `exists(sha256) -> bool` — a cheap HEAD, for backfill resume.
- `open_local_or_remote(document)` — the ergonomic path for callers: return a
  local file if one exists, else pull from R2 to a temp file. This is what
  the CAFR/actuarial/performance extractors call, so they stop caring where
  the PDF lives.

**Dependency note.** `boto3` (and `moto` for tests) were *removed* in the
Postgres cutover, since they existed only for the abandoned R2 database
route. They come back for this — worth flagging so the re-addition reads as
deliberate rather than as a regression to a rejected design. The rejected
thing was R2-as-database-sync-bus; R2-as-object-store was always the plan
(base spec §1).

## 4. Where it runs

**Upload happens in the GHA daily pipeline, at fetch time** — the moment the
PDF exists, before anything can lose it. This is base spec §1's design
(`daily-pipeline`: "PDFs → R2") and keeps GHA the only scheduler.

New GHA secrets, declared at **job** level for the same reason `DATABASE_URL`
is: a step added later must not silently fall back to a no-op.
`tests/test_deployment_config.py` already asserts this pattern for
`DATABASE_URL` and gets an equivalent assertion here.

- `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET`

**Failure posture: non-fatal.** An R2 outage must not fail the daily
pipeline. Upload failures log and leave `content_sha256` null; the backfill
job (§5) sweeps them up later. The pipeline's job is fetching and
extracting — retention is additive to that, and must not become a new way for
it to break.

## 5. Backfill

A one-shot, resumable script, `scripts/backfill_pdf_store.py`, run manually
(not scheduled) and safe to re-run:

```
for each document lacking content_sha256:
    if local_path exists         → hash, upload, record        # 1,909 docs
    else                         → re-fetch from url, upload   # 2,633 docs
                                   404/dead → record as unrecoverable, continue
```

Ordering: **local-file documents first** (free, no network, no link-rot
risk), then re-fetches ordered newest-first — recent documents are both more
likely to still resolve and more likely to matter.

Rate-limited and resumable via `exists()`, so an interrupted run costs
nothing. WAF-blocked plans (`data/waf_blocked_plans.json`) are skipped in the
re-fetch path as everywhere else.

**Unrecoverable documents get recorded as such** — a null `content_sha256`
plus a marker — so the corpus honestly distinguishes "not yet stored" from
"gone forever." That number is the permanent floor, and it should be visible
rather than inferred.

## 6. What this spec does *not* do

Deliberately narrow. This spec stores PDFs and makes them retrievable. It
does **not**:

- **Re-extract the 450 truncated documents.** Retention makes that possible;
  doing it is separate work with its own API cost, and the performance-
  coverage spec only wants the subset its TOC evidence points at.
- **Change `MAX_STORED_CHARS` or the extraction split.** Portal spec §2.3's
  "split the cap" decision stands on its own.
- **Build portal-canonical URLs or link-liveness** (portal spec §3). Those
  consume this store; they are not part of it.
- **Serve PDFs to the Streamlit app.** No user-facing download path here.

## 7. Testing

`moto` mocks S3/R2 for unit tests, as it did before the Postgres cutover:

- `put()` is idempotent — same bytes twice yields one object, same key.
- `get()` verifies the digest and raises on mismatch rather than returning
  corrupt bytes.
- `open_local_or_remote()` prefers a present local file, falls back to R2,
  and raises a clear error when neither exists.
- Upload failure inside the pipeline is **non-fatal** — the document row is
  still written, `content_sha256` stays null. This is the test that protects
  §4's failure posture, and it is the one most likely to matter in practice.
- Backfill resume: a document already carrying `content_sha256` is skipped
  without a network call.
- Deployment config: R2 secrets declared at job level, mirroring the existing
  `DATABASE_URL` assertion.

Live verification on a handful of real documents — upload, re-download,
byte-compare — before the backfill runs at scale.

## 8. Sequencing

1. `pdf_store.py` + schema columns + tests.
2. Wire upload into the daily pipeline (forward retention starts here — every
   day this slips is more documents at risk).
3. Backfill script; run local-file portion, then re-fetch portion.
4. Only then: performance coverage §5 assumes stored objects.
