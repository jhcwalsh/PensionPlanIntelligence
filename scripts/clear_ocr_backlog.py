"""OCR the documents whose OCR is still owed, and nothing else.

    python -m scripts.clear_ocr_backlog                 # price it, spend nothing
    python -m scripts.clear_ocr_backlog --approve --budget 3.00

Two reasons in ``extraction_details`` mean OCR is owed rather than pointless:
``ocr_deferred`` (extraction ran with OCR off to keep spend down) and
``ocr_unavailable`` (OCR was attempted and every page failed at the API, so
nothing was learned about the document). Both are retryable; ``ocr_empty`` is
not, because that one is a claim about the document.

**Without ``--approve`` no paid call is reachable**, following
``scripts/catalogue.py``: OCR_ENABLED is never set on the unapproved path, so
``extract_pdf`` returns at its gate instead of reaching the API.

**Why not ``pipeline.py --retry-failed``.** That flag does the job but not
only the job. ``run_extractor(retry_failed=True)`` selects every document with
``extraction_status='failed'`` -- all 158 of them, ignoring any plan ids given
on the command line -- resets them to pending, and runs OCR over the lot. Most
have no local file and fail again for free, but the balance was never priced,
and an unpriced OCR run is how this project spends money it did not mean to.
``run_extractor`` takes ``doc_ids``; this passes exactly the priced set.

Documents whose file is missing are skipped rather than attempted: OCR needs
the bytes, and 105 documents have a row whose PDF the plan has since deleted.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from rich.console import Console

import database
from database import Document, ExtractionDetail
from scripts.pending_spend import OCR_OWED_REASONS

console = Console(legacy_windows=False)

# Measured, not assumed: the 2026-08 OCR runs recorded in api_usage came out
# near 1.3 cents a page. scripts/pending_spend.py prices the same backlog from
# token estimates and lands a little higher; both are estimates, and the
# --budget ceiling is what actually protects the run.
COST_PER_PAGE = 0.013


def backlog(session, plan_ids: list[str] | None = None):
    """Documents owed OCR that still have a file to OCR."""
    q = (session.query(Document, ExtractionDetail)
         .join(ExtractionDetail, ExtractionDetail.document_id == Document.id)
         .filter(ExtractionDetail.reason.in_(OCR_OWED_REASONS)))
    if plan_ids:
        q = q.filter(Document.plan_id.in_(plan_ids))

    have, missing = [], 0
    for doc, detail in q.order_by(Document.meeting_date.desc().nullslast(),
                                  Document.id):
        path = Path(doc.local_path) if doc.local_path else None
        if path is None or not path.exists():
            missing += 1
            continue
        have.append((doc, detail))
    return have, missing


def _pages(doc, detail) -> int:
    return detail.pages_total or doc.page_count or 0


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("plan_ids", nargs="*", help="limit to these plans")
    ap.add_argument("--approve", action="store_true",
                    help="actually spend. Without this, no paid call is reachable.")
    ap.add_argument("--budget", type=float, default=5.0,
                    help="hard ceiling in USD; the run stops rather than exceeding it")
    ap.add_argument("--limit", type=int)
    args = ap.parse_args()

    session = database.SessionLocal()
    try:
        docs, missing = backlog(session, args.plan_ids or None)
        if args.limit:
            docs = docs[:args.limit]

        if missing:
            console.print(f"[yellow]{missing}[/yellow] owed OCR but have no "
                          f"file — skipped, the plan deleted the PDF")
        if not docs:
            console.print("Nothing owed OCR that still has a file.")
            return 0

        pages = sum(_pages(d, e) for d, e in docs)
        est = pages * COST_PER_PAGE
        by_plan: dict[str, int] = {}
        for d, _ in docs:
            by_plan[d.plan_id] = by_plan.get(d.plan_id, 0) + 1

        console.print(f"\n[bold]{len(docs)}[/bold] documents, "
                      f"[bold]{pages}[/bold] pages to OCR")
        for plan, n in sorted(by_plan.items(), key=lambda x: -x[1]):
            console.print(f"   {plan:22}{n:>5}")
        console.print(f"estimated cost   [bold]${est:.2f}[/bold]")
        console.print(f"budget ceiling   ${args.budget:.2f}")

        if est > args.budget:
            console.print(f"\n[red]Estimate ${est:.2f} exceeds the ceiling "
                          f"${args.budget:.2f}. Raise --budget deliberately or "
                          f"narrow with plan ids / --limit.[/red]\n")
            return 1

        if not args.approve:
            console.print("\n[yellow]Nothing spent. Re-run with --approve "
                          "to proceed.[/yellow]\n")
            return 0

        # Imported here, and OCR_ENABLED set here, so the unapproved path above
        # cannot reach the API even by accident.
        import extractor
        from extractor import run_extractor

        doc_ids = [d.id for d, _ in docs]
        n_before = len(docs)

        # Close before handing off. OCR of 91 pages takes minutes, and Neon
        # kills a connection left idle in a transaction -- the first run of
        # this script died on IdleInTransactionSessionTimeout at the summary
        # query below, after the work was done and committed. run_extractor
        # opens and manages its own session.
        session.close()
        extractor.OCR_ENABLED = True
        run_extractor(doc_ids=doc_ids)

        session = database.SessionLocal()
        still, _ = backlog(session, args.plan_ids or None)
        console.print(f"\n[bold]{n_before - len(still)}[/bold] cleared, "
                      f"[bold]{len(still)}[/bold] still owed")
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
