# Relevance gating: pay to read a document only once we know it is worth reading

**Date:** 2026-08-30
**Status:** draft, pending review.
**Motivated by:** an unmetered $6.98 summarisation run on 2026-08-29, and the
42,665 unread OCR pages it exposed sitting behind them.
**Relates to:** `2026-08-29-pdf-retention-design.md` (the R2 store this will
read from), `2026-08-29-performance-coverage-design.md` (the first consumer).

## Goal

Every paid call in this project is currently made **before** anyone knows
whether the document is worth the money. Extraction OCRs whatever has an
empty text layer; summarisation summarises whatever has text. Both are
volume-driven, and volume is exactly the wrong signal — a 1,074-page scanned
appendix costs the most and is the least likely to be worth reading.

This spec inverts that: **probe cheaply, report, then spend on a human's
say-so.**

## Decisions taken

| Question | Decision |
|---|---|
| What counts as relevant | **Narrow** — asset allocation, returns/performance, manager hires and fires. Not general board business. |
| Who decides | **The gate recommends; a human approves.** Auto-proceed is opt-in per run, never the default. |
| Role of date | **Prioritiser, not filter.** See §2 — the measurement says date alone saves almost nothing. |
| Probe cost ceiling | ~2-3 pages per document, so a verdict costs about the price of one page of OCR. |
| Where verdicts live | A table, so a document is probed once and the answer outlives the run. |

---

## 1. What this is protecting against, measured

All figures taken from the live corpus on 2026-08-30.

| | |
|---|---|
| Documents total | 5,076 |
| Documents stuck at `ocr_partial` | **354** |
| Pages in those documents | 78,065 |
| Pages already OCR'd (stopped at the 100-page cap) | 35,400 |
| **Pages never read** | **42,665** |
| Mean / largest such document | 221 / 1,074 pages |

OCR is billed per page: **$0.88 over 67 recorded calls, so ~1.3c/page**,
measured rather than assumed. Finishing those 42,665 pages blind therefore
costs roughly **$560** — with no idea, beforehand, whether any of them
contain anything.

For scale, all recorded API spend across this project's four days of cost
tracking is **$25.12**. The unread backlog is twenty times the entire
metered history of the system.

**The 100-page cap is not a saving.** It is why all 354 documents are
*incomplete*: money was spent reading the first 100 pages of each, and the
allocation tables in a board pack are rarely in the first 100 pages. The
current design manages to pay for a lot of paper and still miss the answer.

## 2. Date is a prioritiser, not a filter

The instinct that "relevant and old is not the same as relevant and new" is
right, and it shapes ranking. It does **not** work as the primary gate here,
and the data is the reason:

| Year | Docs | Unread pages | Cost to finish |
|---|---|---|---|
| 2026 | 144 | 20,732 | $272 |
| 2025 | 100 | 10,985 | $144 |
| 2024 | 55 | 5,197 | $68 |
| <=2023 | 40 | 4,739 | $62 |
| undated | 15 | 1,010 | $13 |

**74% of the backlog is 2025-26.** Excluding everything before 2024 removes
$75 of $559 — 13%. A date filter alone leaves essentially the whole problem
in place, because the backlog is already recent.

So date enters as **rank, not veto**:

- Documents are ordered newest-first within a relevance tier, so a fixed
  budget buys the most current picture available.
- Age *demotes*: a 2021 allocation table is real but superseded, and competes
  for budget against 2026 material rather than being processed because it
  passed a threshold.
- **Undated documents are ranked, never dropped.** 15 of these documents
  carry no parseable date, and the fetcher deliberately keeps undated links
  for the same reason (see `fetcher.py`'s MIN_DATE comment) — absence of a
  date is missing metadata, not evidence of age.

The one place date *is* a veto: material old enough to be superseded by a
later document from the same plan covering the same fiscal year. That is a
duplicate, not an old document.

## 3. The probe

For a document whose full read would cost real money, spend about one page's
worth to decide.

```
probe(document):
    pages = cheapest available sample:
        text layer if one exists (free)
        else OCR of pages 1-3    (~4c)
    ask Haiku, one call:
        does this contain, or say it contains, any of:
          - asset allocation / target weights
          - returns or performance vs benchmark
          - manager appointments, terminations or searches
        if it names page numbers or a contents entry, return them
    -> verdict, confidence, page hints
```

Two things make this cheap enough to be worth doing:

**The first pages are unusually informative.** Board packs open with an
agenda or a table of contents that names what is inside. This is the same
observation that drives the truncated-pack handling in the performance
coverage spec: you do not need to read a 1,074-page document to learn
whether it contains a performance report — you need to read its contents
page.

**Page hints turn a full read into a partial one.** When the probe returns
"performance report, pages 340-372", the follow-up OCRs 33 pages, not 1,074.
The 100-page cap stops being a truncation that loses the answer and becomes
irrelevant, because we are no longer reading from page 1 hoping to arrive.

Haiku, one call, on ~3 pages: on the order of **1-2c per document**. Probing
all 354 backlog documents costs roughly **$5** and tells us which slice of
the $560 is worth spending.

## 4. Recommend, do not decide

The gate never spends on its own initiative. It produces a **worklist**:

```
$ python -m scripts.relevance_report --backlog ocr

  354 documents probed        $4.91 spent probing
  ------------------------------------------------
  relevant, 2026        88 docs    4,102 pages    $54   <- recommended
  relevant, 2025        41 docs    1,880 pages    $25
  relevant, older       19 docs      910 pages    $12
  not relevant         206 docs   35,773 pages   $469   <- skipped
```

The operator approves a tier, not a document. `--approve relevant-2026`
spends that line and nothing else. This is the property that was missing on
2026-08-29: a number in front of a human *before* the money moves, rather
than an estimate afterwards.

**Auto-proceed exists but is opt-in** (`--auto` with a `--budget` ceiling),
for the daily pipeline where a human is not present. Even there the budget is
a hard stop, not a guideline, and the run reports what it spent.

### Why not just decide automatically

Because the probe will be wrong sometimes, and the two error directions cost
very differently. A false negative silently loses a document nobody knows to
look for. A false positive spends money. Keeping a human on the approval step
means the false negatives are visible in the "not relevant" line — with page
counts and cost, so a suspiciously large skipped tier invites a second look
rather than passing unnoticed.

## 5. Applying the same gate to summarisation

The 2026-08-29 run is the worked example. 472 documents, $6.24:

| type | docs | cost | yielded investment actions | yielded performance data |
|---|---|---|---|---|
| board_pack | 30 | $3.76 | 60% | 73% |
| minutes | 322 | $2.34 | 30% | 33% |
| agenda | 120 | $0.15 | 7% | 0% |

Two things worth taking from this rather than the obvious one:

- **The waste was smaller than it looked.** 120 near-useless agendas cost
  15c, because small documents route to Haiku. Model routing is already an
  accidental relevance proxy: expensive documents are also the valuable ones.
  Genuine waste was roughly $1.75, mostly minutes returning nothing.
- **Which is exactly why summarisation is the lower priority.** The gate
  belongs on OCR first, where a single document can cost $14 and the routing
  proxy does not exist.

Summarisation therefore gets the same verdict table but a looser default:
`board_pack` proceeds, `agenda` is skipped unless the probe says otherwise,
`minutes` are probed. Expected saving is small; the value is consistency and
the audit trail, not the money.

## 6. Where verdicts live

A new `relevance_verdict` table, one row per document:

| Column | Meaning |
|---|---|
| `document_id` | unique |
| `verdict` | `relevant` / `not_relevant` / `uncertain` |
| `topics` | which of the three narrow categories hit |
| `page_hints` | where in the document, when the probe could tell |
| `probe_cost_usd` | what the verdict itself cost |
| `probed_at`, `model` | provenance |

Per CLAUDE.md: add the model, run `init_db()`, no ALTER TABLE system. A
one-off column script only if it lands after the table exists.

Verdicts persist so a document is probed once. `uncertain` is a real value,
not a failure — it routes to the human worklist rather than being guessed
either way.

## 7. What this spec does *not* do

- **It does not re-OCR anything by itself.** It produces a priced worklist.
  Spending remains a separate, approved act.
- **It does not raise `MAX_VISION_OCR_PAGES`.** Page hints make the cap
  mostly moot; changing it is a separate decision.
- **It does not touch the fetch path.** Downloading is free and stays
  unconditional — retention (the R2 spec) depends on it.
- **It does not re-summarise the 472 documents from 2026-08-29.** That money
  is spent and the output is real.

## 8. Testing

- The probe never runs on a document with an existing verdict.
- A budget ceiling is a hard stop: a run that would exceed it stops, having
  spent no more than the ceiling.
- `--approve` spends only the named tier.
- With no `--approve` and no `--auto`, **zero paid calls are made.** This is
  the test that encodes 2026-08-29 — a run whose stated purpose is not to
  spend must be structurally incapable of spending, not merely intended not
  to.
- Undated documents appear in the worklist rather than being filtered out.
- `uncertain` verdicts route to the human tier, never to auto-proceed.

## 9. Sequencing

1. Verdict table + probe + `relevance_report` (no spending path at all).
2. Probe the 354-document OCR backlog. ~$5, and it prices the other $560.
3. Approval path and budget ceilings.
4. Only then: summarisation gating (§5), which is worth little on its own.
