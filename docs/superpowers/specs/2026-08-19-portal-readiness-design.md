# Portal readiness: design

**Date:** 2026-08-19
**Status:** approved.
**Extends:** `docs/superpowers/specs/2026-08-16-low-maintenance-app-design.md`
(the "base spec"). This addendum supersedes the base spec where they conflict;
everything not mentioned here stands unchanged.

## Goal

The base spec designs an app that runs itself for a small invited audience. This
addendum asks a different question: what does that app need in order to be a
*portal* for U.S. public pension materials — catalogued, analysed, searchable,
with links you can trust?

The answer is narrower than expected. No architectural decision in the base spec
has to be reversed. The migration path, the data layer and the storage model all
point the right way. What is missing is a **search strategy**, a **provenance
layer**, and an explicit acknowledgement that **coverage** stops being optional.

## Decisions taken

| Question | Decision |
|---|---|
| Portal direction | **Committed.** Plan for it now, not as a phase-2 maybe. |
| Audience | **Still invited-only.** No public signup, no billing. Base spec §5 stands. |
| Search scope | Summaries **and** document bodies. |
| Storage for bodies | **Keep `GzippedText`.** Add an indexed `tsvector` beside it; never store plaintext. |
| Extraction cap | **Split it.** Full text for storage and search; truncation only for the LLM prompt. |
| Link verification | Provenance for every document, plus liveness piggybacked on the daily fetch. |
| WAF-blocked coverage | **Proxy promoted to required.** |
| Phase 2 (static site) | **Unchanged** — stays on base spec §8's existing triggers. |
| `rfp_records` facets | **Hide** until something refreshes them. Keep the rows. |

---

## 1. The load-bearing assumption

This addendum adopts portal-grade **capability** for a deliberately closed
**audience**. That combination is coherent, but it is the assumption everything
below rests on, and it is worth stating because it cuts against the obvious
reading.

The case for promoting the static-site port and the WAF proxy to mandatory rests
on *public exposure*: anonymous readers cannot calibrate a gap, so named large
plans simply absent reads as unreliable, and Streamlit's weak deep-linking and
absent SEO become product defects. With a known audience reading honest
"last updated" labels, that argument weakens considerably — which is why only one
of the two is promoted here (§4), and the other is not (§6).

**Reopening trigger.** If the audience ever opens — public signup, unauthenticated
access, or an ambition to be discoverable — then base spec §8's phase-2 port and
the "invited now, no public signup" decision both reopen *together*, as one
decision rather than two. They are recorded here so that reopening is deliberate
rather than drifted into.

## 2. Search

Search is the gap with the sharpest edge: it is the one subsystem that does not
survive the migration at all, and the base spec does not mention it — not in §1,
not in §9's sequence, and not in §10's dialect risks, which list only datetimes,
`LENGTH()` over `BYTEA`, and id preservation.

### 2.1 The defect: it degrades silently, it does not fail

Search today is SQLite **FTS5** — a `summaries_fts` virtual table with sync
triggers and `bm25()` ranking, plus four shadow tables. FTS5 has no Postgres
equivalent.

Worse than not migrating, it migrates *quietly*:

- `_init_fts` (`database.py:1099-1108`) wraps `CREATE VIRTUAL TABLE … USING fts5` in a bare `except Exception: return False`.
- `search_summaries` (`database.py:1288-1315`) wraps the `MATCH`/`bm25()` query in a second bare `except`, falling through to a legacy ILIKE substring scan.

Both were written for "this SQLite build lacks FTS5". Both will swallow "this is
not SQLite" identically. On Neon, `init_db()` therefore **succeeds**, no error is
raised, no test fails, and ranked search silently becomes an unranked substring
scan.

This directly contradicts base spec §10's own principle that the app should
*"fail visibly rather than silently"*. Both `except` clauses become explicit
dialect detection: on a non-SQLite engine the FTS5 path is not attempted at all,
and a missing Postgres search index is an error, not a shrug.

### 2.2 The design

Postgres `tsvector` with a **GIN** index, over summaries and document bodies
alike. `websearch_to_tsquery` for user input (it accepts quoted phrases and
`-exclusions` without teaching anyone a syntax), `ts_rank_cd` for ranking,
meeting date descending as the tie-break — preserving today's ordering contract.

**`GzippedText` stays.** The base spec's instruction to keep it (§2) is correct,
but for a better reason than the one given. The temptation is to drop it, since
gzip exists only to dodge GitHub's 100 MB limit and Postgres deletes that limit.
The measured numbers say otherwise:

| | Size |
|---|---|
| `documents.extracted_text`, compressed (today) | **35 MB** |
| Same text, uncompressed | **123 MB** (3.5×) |
| `tsvector` index over it, estimated | ~50 MB |
| **Keep gzip + index** | **~85 MB** |
| **Drop gzip + index** | ~173 MB |

Against Neon's 0.5 GB free tier, and with the corpus growing ~17 documents/day,
the compressed option is the difference between comfortable and cramped. So:
store gzipped bytes, store a `tsvector` beside them, and never store the
plaintext. Snippets require decompressing only the handful of rows actually
displayed, which is a per-result read, not a scan.

This does mean `ts_headline` cannot run against the stored column. Highlighting
is generated from the decompressed text of displayed results instead.

### 2.3 The truncation problem

`extractor.py:32` sets `MAX_TEXT_CHARS = 150_000` and every extraction path
truncates to it. Measured against the corpus: **444 documents — 10.5% — sit
exactly at that cap.** Those are the large board packets, which is to say
precisely the documents where full-text search earns its keep. Indexing what is
stored today would deliver "searchable board materials" that search only the
first ~35 pages of the longest ones.

The cap is legitimate where it originated: it bounds the cost of the Claude
summarisation call. It has no business bounding what gets stored and indexed,
because search costs nothing per token.

**Split it into two limits:** full extracted text for storage and indexing, and a
separate truncation applied only when assembling the LLM prompt.

**Sequencing consequence.** PDFs today exist only on the runner and are never
kept, so the 444 already-truncated documents cannot be re-extracted from anything
currently held. Removing the cap improves documents fetched *after* the change
only — unless the R2 PDF store (base spec §1) lands first, at which point
re-extraction becomes a local operation over stored objects.

**R2 is therefore a prerequisite for retroactive body search, not a parallel
workstream.** This is the strongest argument the portal ambition supplies for
R2's priority, and it did not exist in the base spec's own reasoning.

Note also that removing the cap grows the corpus by an unknown multiple —
concentrated in that 10.5% — which further strengthens the decision to keep
compression.

### 2.4 Sizing check

`tsvector` is capped at 1 MB per value in Postgres. The current corpus is nowhere
near it: median document text is 0.01 MB, p99 is 0.15 MB, maximum 0.16 MB. After
the cap is lifted the maximum will rise, so the write path must handle an
oversized document by indexing a bounded prefix rather than failing the insert.

## 3. Provenance and verified links

Every document gets:

- a **stable canonical URL** on the portal, independent of the source's URL structure;
- a **durable copy in R2**, so a dead source does not break the link;
- its **source URL** and **fetched-at** date displayed;
- a **liveness status** for that source URL.

Liveness costs almost nothing. The daily pipeline already requests every one of
these URLs across 137 plans; today it discards the outcome. It records it
instead — a column and a write inside a code path that already runs. No new
scheduled job, no additional request volume, no new failure mode.

**Correction to the review that prompted this addendum:** `document_health` is
not a foundation for this. Despite its name and its 3,600 rows, it scores PDF
*extraction* quality — blank, scanned and garbled page counts, a structure
score — and every row carries `prompt_version='rfp_v1'`, with 2,085 verdicts of
`NO_TASK_CONTENT` meaning "no RFP-relevant content". It measures a different
thing, for a subsystem that no longer exists. See §5.

## 4. Coverage: the proxy becomes required

Base spec §7 drops 14 WAF-blocked plans and files a proxy under "revisit path".
This addendum promotes it to **required**.

The reasoning is not the public-credibility argument — that one is weakened by
staying invited-only (§1). It is simpler: coverage is a *data quality* problem
that hurts any audience equally. A user researching Florida gets nothing today,
and FRS is $210B. The genuine ongoing loss concentrates in six plans — ASRS,
KPERS, NV PERS, NM PERA, CORP AZ, STRS Ohio — roughly $245B.

At $10–75/month against a $20–50/month infrastructure budget this is the cheapest
material improvement available, and the base spec already establishes it as a
fetcher configuration change rather than an architectural one.

## 5. Two frozen relics, not one

The base spec documents `rfp_records` as deliberately frozen: the code is cut,
the 189 rows stay because `twin_builder.build_rfp_facets` and
`scripts/build_manager_roster` still read them, and the affected facets' freshness
dates stop advancing.

There is a **second** relic with the same shape: `document_health`, 3,600 rows,
every one stamped `prompt_version='rfp_v1'`. It is not read by the twins, so it
is inert rather than misleading, but it should be documented alongside
`rfp_records` rather than discovered again later.

`rfp_records` is the one that matters, because it surfaces. Consultant, actuary
and custodian relationships attributed to named plans, never refreshed, are wrong
in a way a freshness date does not adequately signal — a stale relationship reads
as a current one.

**Decision: hide the RFP-derived facets** from the twin display until something
refreshes them. The rows stay, because `build_manager_roster` still consumes
them. If consultant data is wanted later, the extraction code is recoverable
from git history and un-hiding is one change.

**CORRECTED during implementation (2026-08-19).** This section originally said
"only the user-visible `rfp_state` facet is suppressed". That was wrong in both
directions:

- `governance_people` is **also** RFP-derived, and it is the facet carrying the consultant/actuary/custodian relationships that motivated this decision. Suppressing only `rfp_state` would have left the actual credibility risk on screen.
- But `governance_people` is a **mixture**: `twin_builder` appends live `ips_relationship` and `actuary_relationship` entries to the same list, so hiding the whole facet would have discarded good, current data.

Implemented as: the `rfp_state` expander removed outright, and
`governance_people` **filtered** by relationship `basis` — `rfp_awarded` and
`rfp_incumbent` withheld, everything else shown. A relationship with no `basis`
is kept, since unknown provenance is not evidence of staleness.

## 6. What deliberately does not change

- **Phase 2 stays on base spec §8's triggers.** Streamlit is adequate for a small known audience. §2's no-queries rule — already satisfied — keeps the port a front-end-only job whenever the triggers fire.
- **§5's auth design stands unchanged.** Auth0 passwordless plus `allowed_emails`, per-person identity, one row to add a user.
- **§2's data-layer rule is untouched** and is what makes everything here affordable.
- **No billing, no public signup.**

## 7. Migration impact

The base spec's §9 sequence stands. Two items must land *inside* step 3 rather
than after it, because both are expensive to reverse once data is in Neon:

1. **The search schema** — `tsvector` columns and GIN indexes are part of the target schema, not a follow-up. Getting this wrong means a second full-corpus write.
2. **The gzip decision** — settled here as "keep", so step 3's migration moves `BYTEA` across unchanged and computes vectors on arrival.

Independent of the sequence, and safe to do at any point:

3. **The truncation split** (§2.3), whose retroactive value is gated on R2.
4. **Hiding the RFP facets** (§5), a display change.
5. **The proxy** (§4), a fetcher configuration change.

The datetime audit (`docs/superpowers/plans/2026-08-19-datetime-audit.md`) is
unaffected by anything in this addendum and proceeds independently.

## 8. Corrections this addendum makes to the base spec

- **§10's dialect risks are incomplete.** They list datetimes, `LENGTH()` over `BYTEA`, and id preservation. Search is a fourth, and unlike the others it fails silently.
- **§11's Postgres CI container is a prerequisite, not an enhancement.** SQLite ignores `DateTime(timezone=True)` entirely and has no `tsvector`, so neither the datetime work nor the search work can be verified without it.
- **§1's R2 priority was under-argued.** It is a prerequisite for retroactive body search (§2.3), not merely a decoupling convenience.
- **§7's proxy moves from "revisit path" to required** (§4).

## 9. Risks

**Search quality is a product judgement, not a migration checkbox.** `tsvector`
with `ts_rank_cd` is not `bm25`, and results will be ordered differently from
today's. This needs a human comparing real queries side by side before cutover,
not a passing test.

**Lifting the truncation cap grows the corpus by an unmeasured multiple.** It is
concentrated in the 10.5% of documents currently at the cap, and it should be
measured on a sample before it is applied to the full corpus.

**Liveness checking introduces a new way to be wrong in public.** A source that
returns 200 with a "page moved" body reads as live. The status must be presented
as "last successfully fetched", which is a fact, rather than "link is good",
which is an inference.

**The invited-only decision is doing real work here.** Three of this addendum's
conclusions — no static-site port, no public signup, an acceptable coverage
gap — depend on it. §1 records the reopening trigger for exactly that reason.
