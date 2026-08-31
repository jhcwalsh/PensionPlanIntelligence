"""Read the part of a long document that holds the numbers.

    python -m scripts.read_sections                       # price it, spend nothing
    python -m scripts.read_sections --approve --budget 1.00

The summariser compresses every document to ~50,000 characters before Claude
sees it, filling that budget from the front. 810 documents are longer than
that, and an allocation table is a dense numeric grid sitting deep --
measured at 31% in on a real board pack. This finds the table for free and
pays only to read the slice it chose.

**Without `--approve` no paid call is reachable.** Not "does not spend by
default" -- the call site is never entered. On 2026-08-29 a run whose entire
purpose was to avoid spending made 472 paid calls, because a flag named
`--extract-only` also summarises. A guarantee that lives in a flag's name is
not a guarantee, so this one lives at the call site and has a test asserting
no client is built.

`--budget` is a hard stop, checked before each call against what has already
been spent *plus what is in flight*. It stops the run; it does not warn. The
in-flight term is what keeps it a ceiling under `--workers`: without it, W
concurrent calls could each be mid-request when the limit is reached and the
run would overshoot by W calls with nobody having decided to.

Nothing here writes to `documents`. Reads are stored beside the text in
`document_section_read` -- no extracted material is overwritten or deleted.
"""
from __future__ import annotations

import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal

from rich.console import Console
from sqlalchemy import func
from sqlalchemy.orm import undefer

import costs
import database
import section_finder
from database import Document, DocumentSectionRead
from llm_openrouter import MODEL
from summarizer import SMART_TRUNCATE_TARGET
from targeted_extract import extract_window

console = Console(legacy_windows=False)


def _printable(s) -> str:
    """Make text safe for whatever encoding stdout actually has.

    Headings come from PDF text layers and carry private-use glyphs -- the
    Symbol-font bullet arrives as \\uf0a7. Printed to a cp1252-redirected
    stdout that raises UnicodeEncodeError, and on the first full corpus run it
    killed the process after twelve windows. A progress line must never be
    able to end a paid run.
    """
    enc = getattr(sys.stdout, "encoding", None) or "ascii"
    return str(s or "").encode(enc, "replace").decode(enc, "replace")


def _say(msg: str) -> None:
    """Print progress, but never let printing end the run.

    Belt and braces over _printable: the sanitiser covers the encoding fault
    actually seen, and this covers the next one nobody predicted. Work that
    has been paid for and committed must not be followed by a crash.
    """
    try:
        console.print(msg)
    except Exception:                                       # noqa: BLE001
        try:
            print(msg.encode("ascii", "replace").decode("ascii"))
        except Exception:                                   # noqa: BLE001
            pass


def backlog_documents(session):
    """Documents the summariser truncated, with no section read yet.

    Length is pre-filtered with octet_length on the *compressed* column rather
    than by loading the text: extracted_text is deferred precisely because
    loading it in bulk is what exhausted Neon's transfer quota on 2026-08-25.

    The threshold is deliberately loose. Measured over 120 sampled documents
    the gzip ratio runs from 1.98 to 6.63, so no single factor converts bytes
    to characters -- a tight threshold silently drops real documents, and this
    one did: //3 selected 759 where 810 exceed the character limit. A factor
    of 10 is comfortably past the observed maximum and costs almost nothing,
    because the byte volume is dominated by a handful of huge documents that
    every threshold selects anyway (33.9 MB at //10 against 27.0 MB at //3).

    Over-selection is harmless: _worklist re-checks the true character length
    for free once the text is in hand. Under-selection is invisible.
    """
    min_bytes = SMART_TRUNCATE_TARGET // 10
    return (session.query(Document)
            .outerjoin(DocumentSectionRead,
                       DocumentSectionRead.document_id == Document.id)
            .filter(Document.extracted_text.isnot(None),
                    func.octet_length(Document.extracted_text) > min_bytes,
                    DocumentSectionRead.document_id.is_(None))
            .options(undefer(Document.extracted_text))
            .order_by(Document.meeting_date.desc().nullslast(), Document.id))


def _estimate_cost(n_windows: int) -> Decimal:
    """Rough cost of n windows, for the priced worklist."""
    price = costs.PRICES[MODEL]
    tokens_in = section_finder.WINDOW // 4
    return n_windows * (Decimal(tokens_in) * price.input
                        + Decimal(800) * price.output) / costs.MILLION


def _worklist(docs, top: int, limit: int | None = None):
    """Rank documents for free. Returns (worklist, n_without_candidates).

    Documents with no candidate are counted and reported, never handed an
    arbitrary window -- that would spend money to extract nothing and look
    like a model failure rather than a document without a returns table.

    ``limit`` counts documents that *have* candidates, and stops the scan
    once it has that many. Limiting in SQL instead would cut before ranking,
    and the three newest documents in this corpus all have no candidate --
    so `--limit 3` read nothing at all, which is a verification run that
    verifies nothing.
    """
    work, blank = [], 0
    for doc in docs:
        text = doc.extracted_text or ""
        if len(text) <= SMART_TRUNCATE_TARGET:
            continue
        cands = section_finder.find_candidates(text)[:top]
        if not cands:
            blank += 1
            continue
        work.append((doc, text, cands))
        if limit and len(work) >= limit:
            break
    return work, blank


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--approve", action="store_true",
                    help="actually spend. Without this, no paid call is reachable.")
    ap.add_argument("--budget", type=float, default=2.0,
                    help="hard ceiling in USD; the run stops rather than exceeding it")
    ap.add_argument("--limit", type=int,
                    help="read at most N documents that have a candidate "
                         "section (counts readable documents, not scanned ones)")
    ap.add_argument("--top", type=int, default=1,
                    help="windows to read per document (default 1)")
    ap.add_argument("--workers", type=int, default=12,
                    help="concurrent reads (default 12). A window takes ~45s, "
                         "so serial is 12 hours for the full corpus.")
    args = ap.parse_args()

    session = database.SessionLocal()
    try:
        work, blank = _worklist(backlog_documents(session).yield_per(50),
                                args.top, args.limit)

        n_windows = sum(len(c) for _, _, c in work)
        if blank:
            scope = "scanned so far" if args.limit else "in the corpus"
            _say(f"[yellow]{blank}[/yellow] long documents {scope} have "
                          f"no candidate section — reported, not guessed at")
        if not work:
            _say("Nothing to read — no truncated document has a "
                          "candidate section awaiting a read.")
            return 0

        est = _estimate_cost(n_windows)
        _say(f"\n[bold]{len(work)}[/bold] documents, "
                      f"[bold]{n_windows}[/bold] windows to read")
        _say(f"estimated cost   [bold]${est:.2f}[/bold]   "
                      f"({MODEL}, {section_finder.WINDOW:,} chars each)")
        _say(f"budget ceiling   ${args.budget:.2f}")

        if not args.approve:
            _say("\n[yellow]Nothing spent. Re-run with --approve "
                          "to proceed.[/yellow]\n")
            return 0

        jobs = [(doc, text, cand)
                for doc, text, cands in work for cand in cands]
        spent = Decimal(0)
        done = failed = stopped = 0
        per_window = _estimate_cost(1)

        # Reading a 30,000-char window takes ~45 seconds -- the model is fast,
        # but a window holding a full returns grid produces a long tool call.
        # Serially that is 12 hours for this corpus, which is not a run anyone
        # supervises. The work is embarrassingly parallel: independent
        # documents, no shared state but the budget.
        #
        # Only the worker threads call the API. Every database write stays on
        # this thread, because a Session is not thread-safe and the failure
        # mode if it were shared is silent corruption rather than an error.
        it = iter(jobs)
        pending: dict = {}

        exhausted = False

        def submit_next(ex) -> bool:
            """Submit one job unless the budget cannot cover what is in flight.

            The ceiling counts calls already dispatched, priced at the observed
            average once there is one. Without that term, W workers could each
            be mid-call when the limit is reached and the run would overshoot
            by W calls with nobody having decided to.
            """
            nonlocal exhausted
            avg = (spent / done) if done else per_window
            for job in it:
                if float(spent + avg * len(pending)) >= args.budget:
                    return False
                pending[ex.submit(extract_window, job[1], job[2])] = job
                return True
            exhausted = True
            return False

        def top_up(ex) -> None:
            while len(pending) < args.workers and submit_next(ex):
                pass

        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            # Exactly one call before the first result, then fill to --workers.
            # Until something completes there is no observed cost, so the
            # in-flight term can only be priced at the estimate -- and an
            # estimate wrong by 100x is what let extract_performance_reports
            # run at max_tokens=4096 for thirty documents. Buying the first
            # observation with one call instead of twelve bounds that mistake
            # to a single window, at the price of 45 seconds once.
            submit_next(ex)
            while pending:
                fut = next(as_completed(list(pending)))
                doc, text, cand = pending.pop(fut)
                try:
                    data, cost = fut.result()
                except Exception as e:                      # noqa: BLE001
                    # One plan's quirk must not cost the other 809 documents.
                    _say(f"  [red]{doc.id} {_printable(doc.filename)}: "
                         f"{_printable(e)}[/red]")
                    failed += 1
                    top_up(ex)
                    continue
                spent += cost
                rows = data.get("returns", [])
                session.add(DocumentSectionRead(
                    document_id=doc.id,
                    offset=cand.offset,
                    heading=(cand.heading or "")[:200],
                    returns_json=json.dumps(rows),
                    model=MODEL,
                    cost_usd=cost,
                ))
                session.commit()
                done += 1
                pct = 100 * cand.offset // max(len(text), 1)
                _say(f"  [green]{done:>4}[/green]/{len(jobs)} "
                     f"{_printable(doc.filename)[:36]:<36} @{pct:>3}%  "
                     f"{len(rows):>3} rets  ${spent:.3f}  "
                     f"{_printable(cand.heading)[:26]}")
                top_up(ex)
        stopped = 0 if exhausted else 1

        _say(f"\nread [bold]{done}[/bold] windows, "
                      f"{failed} failed, spent [bold]${spent:.4f}[/bold]")
        if stopped:
            _say(f"[yellow]Stopped on the ${args.budget:.2f} "
                          f"budget ceiling.[/yellow]")
        return stopped
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
