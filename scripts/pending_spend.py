"""What the unspent Claude work in this corpus would cost, without spending it.

Two backlogs accumulate whenever the pipeline runs in a no-spend mode:

  * documents with text but no summary  -> `summarizer.run_summarizer`
  * documents recorded `ocr_deferred`   -> re-extract with OCR on

Both are recoverable and neither expires. This prices them so the decision to
fund one is made against a number rather than a guess.

    python -m scripts.pending_spend
    python -m scripts.pending_spend --plans nmpera,lasers_la

Read-only: it never calls Claude and never writes.

**It also never loads `extracted_text`.** Summing that column over a few
hundred documents is what exhausted Neon's monthly transfer quota on
2026-08-25 and suspended the whole project (see CLAUDE.md). Instead it reads
`octet_length()` -- the *compressed* size, which Postgres computes server-side
and returns as an integer -- and converts with a ratio measured from a small
sample. Every estimate below is therefore an estimate, and says so.
"""
from __future__ import annotations

import argparse
import gzip
import sys
from decimal import Decimal

from sqlalchemy import func
from sqlalchemy.orm import undefer

import costs
import database
from database import Document, ExtractionDetail, Summary

# Assumptions, all conservative-ish and all stated in the output so a reader
# can disagree with them rather than having to trust the total.
CHARS_PER_TOKEN = 4              # English prose against Claude's tokenizer
SUMMARY_OUTPUT_TOKENS = 900      # max_tokens is 1500; summaries run shorter
OCR_TOKENS_PER_PAGE_IN = 2_000   # a page rendered at scale 2, as an image
OCR_TOKENS_PER_PAGE_OUT = 800    # the transcribed text coming back
RATIO_SAMPLE = 40                # documents read in full, to calibrate


def _gzip_ratio(session, plan_ids) -> float:
    """Uncompressed chars per stored byte, measured not assumed.

    The one place this script reads text, deliberately bounded to
    RATIO_SAMPLE documents and biased towards the largest, because those are
    what the bill is made of.

    Two things make a naive average wrong here, both found by checking the
    numbers against real rows rather than trusting them:

    * The ratio is not constant. It tracks document size -- measured at 1.6
      on a 1 KB agenda and 3.4 on a 150 KB board pack, because gzip needs
      volume before its dictionary earns anything back.
    * So a median over all documents is dominated by the many small ones and
      understates the few large ones. Those large ones are precisely where
      the cost is. Summing first and dividing once weights each document by
      its size, which is the weighting the estimate actually wants.
    """
    q = session.query(Document).options(undefer(Document.extracted_text)) \
        .filter(Document.extracted_text.isnot(None)) \
        .order_by(func.octet_length(Document.extracted_text).desc())
    if plan_ids:
        q = q.filter(Document.plan_id.in_(plan_ids))

    total_chars = total_packed = 0
    for doc in q.limit(RATIO_SAMPLE):
        body = doc.extracted_text or ""
        if len(body) < 500:
            continue
        total_chars += len(body)
        total_packed += len(gzip.compress(body.encode()))
    return (total_chars / total_packed) if total_packed else 4.5


def _price(model: str, tokens_in: int, tokens_out: int) -> Decimal:
    p = costs.PRICES[model]
    return (Decimal(tokens_in) * p.input + Decimal(tokens_out) * p.output) / costs.MILLION


def summarization_backlog(session, plan_ids, ratio):
    """Documents holding text that nothing has summarized yet.

    Model routing mirrors `summarizer.choose_model`, but from estimated
    length alone: that function also greps the first 5,000 characters for
    investment keywords, and reading them would defeat the point of this
    script. Anything large enough to reach the keyword branch is therefore
    counted as Sonnet -- the expensive side -- so the total leans high.
    """
    q = session.query(
        Document.plan_id, Document.doc_type,
        func.octet_length(Document.extracted_text),
    ).outerjoin(Summary, Summary.document_id == Document.id) \
     .filter(Document.extraction_status == "done",
             Document.extracted_text.isnot(None),
             Summary.id.is_(None))
    if plan_ids:
        q = q.filter(Document.plan_id.in_(plan_ids))

    rows = q.all()
    total = Decimal(0)
    by_model = {"claude-haiku-4-5-20251001": [0, Decimal(0)],
                "claude-sonnet-4-6": [0, Decimal(0)]}
    for _plan, doc_type, packed in rows:
        chars = int((packed or 0) * ratio)
        tokens_in = max(1, chars // CHARS_PER_TOKEN)

        if chars < 8_000:
            model = "claude-haiku-4-5-20251001"
        elif doc_type == "agenda" and chars < 20_000:
            model = "claude-haiku-4-5-20251001"
        elif doc_type == "minutes" and chars < 15_000:
            model = "claude-haiku-4-5-20251001"
        elif chars >= 20_000:
            model = "claude-sonnet-4-6"     # assumed keyword hit; see docstring
        else:
            model = "claude-haiku-4-5-20251001"

        cost = _price(model, tokens_in, SUMMARY_OUTPUT_TOKENS)
        by_model[model][0] += 1
        by_model[model][1] += cost
        total += cost
    return len(rows), by_model, total


#: Reasons that mean "OCR is still owed on this document", as opposed to a
#: property of the document itself. `ocr_deferred` is a funding decision;
#: `ocr_unavailable` is the API having been unreachable when it was tried.
#: Both are retryable and both must stay priced — on 2026-08-31 an exhausted
#: credit balance rewrote 30 deferred rows as `ocr_empty`, which silently
#: emptied this report of work that was still outstanding.
OCR_OWED_REASONS = ("ocr_deferred", "ocr_unavailable")


def ocr_backlog(session, plan_ids):
    """Documents whose text layer is empty and whose OCR is still owed."""
    q = session.query(Document.plan_id, Document.page_count,
                      ExtractionDetail.pages_total) \
        .join(ExtractionDetail, ExtractionDetail.document_id == Document.id) \
        .filter(ExtractionDetail.reason.in_(OCR_OWED_REASONS))
    if plan_ids:
        q = q.filter(Document.plan_id.in_(plan_ids))

    rows = q.all()
    pages = sum((r[1] or r[2] or 0) for r in rows)
    cost = _price("claude-sonnet-4-6",
                  pages * OCR_TOKENS_PER_PAGE_IN,
                  pages * OCR_TOKENS_PER_PAGE_OUT)
    return len(rows), pages, cost


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--plans", help="comma-separated plan ids (default: all)")
    args = ap.parse_args()
    plan_ids = [p.strip() for p in args.plans.split(",")] if args.plans else None

    session = database.SessionLocal()
    try:
        ratio = _gzip_ratio(session, plan_ids)
        n_sum, by_model, sum_cost = summarization_backlog(session, plan_ids, ratio)
        n_ocr, ocr_pages, ocr_cost = ocr_backlog(session, plan_ids)
    finally:
        session.close()

    scope = ", ".join(plan_ids) if plan_ids else "all plans"
    print(f"\nUnspent Claude work — {scope}")
    print(f"(gzip ratio {ratio:.1f} chars/byte, size-weighted over the "
          f"{RATIO_SAMPLE} largest documents)\n")

    print(f"  Summarization      {n_sum:>6} documents   ${sum_cost:>8.2f}")
    for model, (count, cost) in by_model.items():
        if count:
            print(f"    {model:<32}{count:>6} docs      ${cost:>8.2f}")
    print(f"  OCR (deferred)     {n_ocr:>6} documents   ${ocr_cost:>8.2f}"
          f"   [{ocr_pages:,} pages]")
    print(f"  {'':<18} {'':>6}                ${sum_cost + ocr_cost:>8.2f}  total\n")

    print("Estimates, not quotes. Input tokens come from compressed column "
          f"sizes scaled by the measured ratio at {CHARS_PER_TOKEN} chars/token; "
          f"output is assumed at {SUMMARY_OUTPUT_TOKENS} tokens per summary and "
          f"{OCR_TOKENS_PER_PAGE_OUT} per OCR'd page. Documents over 20k chars "
          "are all costed as Sonnet, so the summarization figure leans high.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
