# Catalogue what a document contains; read it only when something needs it

**Date:** 2026-08-30
**Status:** draft, pending review. Supersedes the first draft of this file,
which proposed a probe-and-approve gate costing ~$54 up front.
**Relates to:** `2026-08-29-pdf-retention-design.md` (the R2 store),
`2026-08-29-performance-coverage-design.md` (the first consumer).

## Goal

An image-only document costs money to read because every page is a separate
vision call. The current design reads from page 1 until it runs out of
budget, which pays the most for the documents least likely to repay it.

**OCR's job is to find out what is in a document, not to read it.** Ten
pages is enough to learn that. What comes out is a catalogue entry — this
board pack contains a performance report, an allocation review, and four
committee items — which is stored and kept. If something later needs the
performance table, the catalogue says which document and roughly where, and
*that* is when the rest gets read.

## Decisions taken

| Question | Decision |
|---|---|
| Purpose of OCR on image-only documents | **Cataloguing only.** Never bulk transcription. |
| How many pages | **First 10.** Enough for an agenda or contents page. |
| Reading the rest | **On demand only**, when a named need points at a specific document. |
| The $560 backlog | **Rejected.** Not acceptable, and §1 shows it is also unnecessary. |
| What counts as worth cataloguing | **Narrow** — asset allocation, returns/performance, manager hires and fires. |
| Who decides | **The catalogue recommends; a human approves** any run that would spend. |
| Date | **Prioritiser, not filter.** See §4. |
| Summarisation | **Left alone.** See §6. |

---

## 1. The existing backlog costs nothing, because we already have the text

The first draft of this spec opened with a $560 exposure: 354 documents at
`ocr_partial`, 42,665 pages never read. That number was real but it framed
the wrong question, and checking the rows dissolved most of it.

**All 354 of those documents already hold text.** Mean ~150,000 characters
each — on the order of 100 pages, already OCR'd, already stored:

| | |
|---|---|
| `ocr_partial` documents | 354 |
| ...that already hold extracted text | **354** |
| ...truncated at the old 150,000-character cap | **337** |
| Mean stored text | ~150,000 chars (~100 pages) |

So two caps fired on these documents, not one: OCR stopped at 100 pages
*and* storage stopped at 150,000 characters. `ocr_partial` names the first
and hides the second.

The consequence is the useful part: **cataloguing the existing 354 requires
no OCR at all.** A catalogue needs an agenda or contents page, which lives in
the first few thousand characters of text we are already holding. Building
catalogue entries for all 354 costs one cheap model call each against ~5,000
characters — on the order of **$1 for the whole backlog**, against $560 to
read it blind.

The $560 is not deferred or budgeted. It is not spent, because it was never
the thing that produces value.

## 2. The catalogue

One row per document, built once:

| Field | Meaning |
|---|---|
| `document_id` | unique |
| `contains` | which of the three narrow categories appear |
| `sections` | the contents/agenda entries, with page numbers where given |
| `page_hints` | where the relevant material sits, when the source says |
| `source` | `existing_text` or `ocr_10pp` — how the entry was built |
| `built_at`, `model`, `cost_usd` | provenance and what it cost |

Built two ways, by circumstance:

- **From text we already have** (the 354, and anything with a text layer) —
  free of OCR, one cheap model call.
- **From the first 10 pages via OCR** (~13c) — only for a document that is
  image-only *and* has no stored text at all.

The catalogue is the durable artefact. It is small, it is cheap, and it
answers the question that actually gets asked: *which documents contain
performance data, and where?*

## 3. Reading the rest: on demand, never in bulk

There is no batch job that reads page 101 onward. That path exists only when
something concrete asks for it:

```
need: "2026 allocation targets for plan X"
  -> catalogue: document 6371 contains allocation review, pages 340-372
  -> OCR pages 340-372          33 pages, ~43c
  -> stop
```

This is the difference the first draft missed. Reading pages 340-372 of one
document because a named need points there is not the same act as reading
42,665 pages in case something is in them, even though both are "OCR the
rest". The first is answering a question; the second is buying an option
nobody has asked to exercise.

`MAX_VISION_OCR_PAGES` stops mattering: we are no longer starting at page 1
hoping to arrive somewhere.

## 4. Date is a prioritiser, not a filter

"Relevant and old is not the same as relevant and new" is right, and it
shapes ranking. It cannot be the gate, and the measurement is why:

| Year | Docs | Unread pages |
|---|---|---|
| 2026 | 144 | 20,732 |
| 2025 | 100 | 10,985 |
| 2024 | 55 | 5,197 |
| <=2023 | 40 | 4,739 |
| undated | 15 | 1,010 |

**74% of the backlog is 2025-26.** Excluding everything before 2024 removes
13% of it. So date orders the work rather than eliminating it:

- Catalogue entries are ranked newest-first, so on-demand reads reach for
  current material first.
- Age demotes: a 2021 allocation table is real but superseded, and competes
  against 2026 material rather than qualifying on its own.
- **Undated documents are ranked, never dropped** — 15 carry no parseable
  date, and the fetcher deliberately keeps undated links for the same reason
  (see `fetcher.py`'s MIN_DATE comment). A missing date is missing metadata,
  not evidence of age.

The one place date is a veto: a document superseded by a later one from the
same plan covering the same fiscal year. That is a duplicate.

## 5. Recommend, do not decide

Nothing here spends on its own. A run that would cost money prints what it
would cost and stops:

```
$ python -m scripts.catalogue --backlog

  354 documents, catalogue entries buildable from existing text
  estimated cost   $1.04     (Haiku, ~5k chars each)
  OCR required     0 documents

  nothing spent. re-run with --approve to proceed.
```

**With neither `--approve` nor `--auto`, no paid call is reachable.** Not
"intended not to spend" — structurally unable to. That is the failure of
2026-08-29 written as a constraint: a flag whose name implies safety is not
safety, and the guarantee has to live at the call site.

`--auto` exists for the daily pipeline, where no human is present, and takes
a hard `--budget` ceiling that stops the run rather than warning.

## 6. Summarisation is left alone

The first draft proposed extending this to summarisation. That was wrong,
and the daily numbers say so:

| | |
|---|---|
| Typical day, since mid-August | 3-39 summaries, median ~20 |
| Recorded summarise spend, four normal days | ~$0.90 |
| Cost of a normal day | **~17c** |

The daily pipeline only ever sees documents published since the last run, so
volume already gates it. `summarizer.should_skip()` already drops empty and
non-substantive files for free. Adding a probe call per document to decide
whether to make a summary call could plausibly cost more than it saves, on a
path that has worked since May.

The 2026-08-29 run was 12-150x a normal day. That was a bulk operation
wearing a daily operation's clothes, and §5's approval gate is the correct
fix for it — not a per-document relevance test.

## 7. What this spec does *not* do

- **It does not spend $560, or budget it, or stage it.** The number is
  recorded in §1 as the thing being declined.
- **It does not re-OCR the 354.** Their catalogue comes from stored text.
- **It does not change `MAX_STORED_CHARS`** — though §1 shows 337 documents
  were truncated by the old 150k cap, which is worth its own look, separately.
- **It does not gate summarisation** (§6).
- **It does not touch the fetch path.** Downloading is free and unconditional.

## 8. Testing

- With neither `--approve` nor `--auto`, zero paid calls are reachable. The
  test that encodes 2026-08-29.
- A budget ceiling is a hard stop: a run that would exceed it stops having
  spent no more than the ceiling.
- A document with an existing catalogue entry is never re-catalogued.
- Cataloguing prefers stored text and only reaches for OCR when there is no
  text at all — asserted on the emitted calls, not just the return value.
- OCR cataloguing never exceeds 10 pages, whatever the document's length.
- On-demand reads are bounded by the catalogue's page hints.
- Undated documents appear in the ranking rather than being filtered out.

## 9. Sequencing

1. Catalogue table + builder from existing text. No OCR path, no spending
   path.
2. Build entries for the 354 from stored text (~$1, one approval).
3. See what they say. That answers whether any on-demand reading is worth
   doing before a line of it is written.
4. Only then: the 10-page OCR path for image-only documents with no stored
   text, and the on-demand page-range read.
