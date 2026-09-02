"""Re-extract documents that were truncated at the old 150,000-character cap.

`MAX_STORED_CHARS` is 2,000,000 now. It was 150,000 while the database was a
SQLite file in git, 11 MB from GitHub's hard limit, and 449 documents were cut
off there. Against Postgres that constraint does not exist, and since
2026-08-29 the PDFs are retained in R2, so the text can simply be read again.

**No model is involved.** This is PyMuPDF's text layer, locally, for free. The
only documents that would cost anything are image-only ones needing OCR, and
those are not in this backlog -- they never got past extraction in the first
place and sit at `extraction_status='failed'`.

Three safety properties, in order of how much they matter:

1. **It never shrinks a document.** If re-extraction yields less text than is
   already stored, the stored text is kept and the document is reported as
   `shrank`. A truncated 150,000 characters is worth more than a complete
   400 -- and a PDF that re-reads short is the signature of a bad retained
   copy or a changed source, which is a thing to investigate rather than to
   overwrite. This is the property to preserve if anything here is edited.
2. **It only touches documents that are actually at the cap.** Something long
   for honest reasons is left alone.
3. **It is resumable and idempotent.** A document that has grown past the cap
   no longer matches the query, so re-running picks up only what is left.

Usage:
    python -m scripts.reextract_truncated              # dry run, reports only
    python -m scripts.reextract_truncated --apply      # actually write
    python -m scripts.reextract_truncated --apply --limit 20
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from rich.console import Console
from sqlalchemy.orm import undefer

import database
import extractor
import pdf_store
from database import Document

console = Console(legacy_windows=False)

# The cap this corpus was cut off at, kept here rather than imported: it is
# history, not configuration, and extractor.MAX_STORED_CHARS is deliberately
# no longer this number.
OLD_CAP = 150_000
# A document truncated at the cap lands within a few characters of it -- the
# slice is exact, but a later re-save could differ trivially. Anything below
# this was not truncated.
NEAR = 200


# Bounds on the COMPRESSED size of a document truncated at OLD_CAP.
#
# extracted_text is gzipped on disk, so octet_length() measures bytes, not
# characters (CLAUDE.md). Measured ratios across this corpus run 1.98 to 6.63
# characters per byte, which puts 150,000 characters somewhere between ~22.6 kB
# and ~75.8 kB. The band below is wider than that on both sides, because being
# generous here costs a few extra exact checks while being tight loses
# documents silently.
MIN_BYTES = 10_000
MAX_BYTES = 130_000


def candidates(session, limit: int | None = None) -> list[Document]:
    """Documents sitting at the old cap, newest first.

    Narrowed in SQL before any text is read. The obvious implementation --
    undefer extracted_text and measure len() on every extracted document --
    pulls the whole corpus's text across the network to answer a question
    about lengths. That is precisely the read shape that exhausted Neon's
    transfer quota on 2026-08-25, and it crashed the connection the first time
    this script ran, with the backfill already loading the same database.

    So: octet_length in the database to get a shortlist, then undefer only the
    shortlist to check exact character counts. Compressed bytes cannot answer
    the question on their own -- the ratio varies more than threefold -- but
    they exclude the bulk of the corpus for free.
    """
    from sqlalchemy import Integer, func

    # type_=Integer is load-bearing, not decoration. extracted_text is a
    # GzippedText TypeDecorator, so SQLAlchemy infers NullType for
    # octet_length() over it, and the untyped integer bounds then go to
    # psycopg's binary protocol as parameters it cannot describe. The symptom
    # is not a type error but `ProtocolViolation: server conn crashed?`, which
    # reads like a dead database rather than a bad query -- and Neon is fine
    # throughout.
    length = func.octet_length(Document.extracted_text, type_=Integer)

    shortlist = [
        row[0] for row in
        session.query(Document.id)
        .filter(Document.extraction_status == "done",
                length.between(MIN_BYTES, MAX_BYTES))
        .order_by(Document.id.desc())
        .all()
    ]

    out = []
    for start in range(0, len(shortlist), 100):
        batch = (session.query(Document)
                 .options(undefer(Document.extracted_text))
                 .filter(Document.id.in_(shortlist[start:start + 100]))
                 .all())
        for doc in sorted(batch, key=lambda d: -d.id):
            text = doc.extracted_text
            if text and OLD_CAP - NEAR <= len(text) <= OLD_CAP + NEAR:
                out.append(doc)
                if limit and len(out) >= limit:
                    return out
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="scripts.reextract_truncated",
        description="Re-extract documents truncated at the old 150k cap.")
    parser.add_argument("--apply", action="store_true",
                        help="Write the results. Without this, reports only.")
    parser.add_argument("--limit", type=int,
                        help="Stop after this many documents.")
    args = parser.parse_args(argv)

    # Sessions here are deliberately short-lived. Extracting one board packet
    # takes PyMuPDF minutes, and a session held across that is an idle
    # connection Neon terminates -- the failure surfaces at close() as
    # `ProtocolViolation: server conn crashed?`, long after the work
    # succeeded, and reads like a dead database. The same fault cost a run of
    # scripts/read_sections.py earlier. So: read, release, extract, reconnect
    # to write.
    session = database.get_session()
    try:
        docs = candidates(session, args.limit)
        # Capture the stored lengths while the objects are live, then end the
        # read transaction. Nothing below needs extracted_text again, so this
        # is the last time the corpus's text crosses the network.
        before_by_id = {d.id: len(d.extracted_text or "") for d in docs}
        session.commit()
        console.print(f"[bold]{len(docs)}[/bold] documents at the "
                      f"{OLD_CAP:,}-character cap")
        if not args.apply:
            console.print("[yellow]Dry run.[/yellow] Re-run with --apply to write.")
        console.print()

        counts = {"grew": 0, "same": 0, "shrank": 0, "no_pdf": 0, "error": 0}
        gained = 0

        for doc in docs:
            before = before_by_id[doc.id]
            try:
                # Yields a bare path and deletes it afterwards if it came from
                # R2; raises FileNotFoundError when the PDF is neither on disk
                # nor retained, which is the backfill's problem, not an error
                # here.
                with pdf_store.document_pdf(doc) as path:
                    outcome = extractor._extract_from_path(doc, path)
            except FileNotFoundError:
                counts["no_pdf"] += 1
                continue
            except Exception as e:                       # noqa: BLE001
                counts["error"] += 1
                console.print(f"  [red]{doc.id} {type(e).__name__}: "
                              f"{str(e)[:70]}[/red]")
                continue

            # An outcome that did not reach 'done' is not a re-extraction --
            # an image-only re-read returns almost nothing, and the shrink
            # guard below would catch it anyway. Refusing here says why.
            if outcome.status != "done":
                counts["error"] += 1
                console.print(f"  [yellow]{doc.id} re-read status="
                              f"{outcome.status} reason={outcome.reason}, "
                              f"keeping stored text[/yellow]")
                continue

            text, pages = outcome.text, outcome.pages
            after = len(text or "")
            if after < before:
                # Never trade a truncated document for a shorter one.
                counts["shrank"] += 1
                console.print(f"  [yellow]{doc.id} re-read SHORTER "
                              f"({before:,} -> {after:,}), keeping stored "
                              f"text[/yellow]")
                continue
            if after <= before + NEAR:
                counts["same"] += 1
                continue

            counts["grew"] += 1
            gained += after - before
            console.print(f"  {doc.id} {doc.plan_id:16s} "
                          f"{before:,} -> [green]{after:,}[/green]")
            if args.apply:
                # A fresh session per write, committed immediately. One long
                # session across 449 multi-minute extractions is an idle
                # connection Neon will drop, and losing it after 400
                # documents would discard every uncommitted one.
                w = database.get_session()
                try:
                    row = w.get(Document, doc.id)
                    row.extracted_text = text
                    if pages:
                        row.page_count = pages
                    w.commit()
                finally:
                    w.close()

        console.print()
        console.rule("Re-extraction complete" if args.apply else "Dry run complete")
        for k, v in counts.items():
            if v:
                console.print(f"  {k:10s} {v}")
        console.print(f"  [bold]characters recovered: {gained:,}[/bold]"
                      f"{'' if args.apply else ' (not written)'}")
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
