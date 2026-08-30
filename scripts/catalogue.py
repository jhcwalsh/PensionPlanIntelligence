"""Build catalogue entries: what a document contains, not what it says.

    python -m scripts.catalogue --backlog          # price it, spend nothing
    python -m scripts.catalogue --backlog --approve --budget 3.00

**Without `--approve` no paid call is reachable.** Not "does not spend by
default" -- the client is never constructed and the call site is never
entered. On 2026-08-29 a run whose entire purpose was to avoid spending made
472 paid calls, because a flag named `--extract-only` also summarises. A
guarantee that lives in a flag's name is not a guarantee, so this one lives
at the call site and has a test asserting no client is built.

`--budget` is a hard stop, checked before each call against what has already
been spent. It stops the run; it does not warn.

Nothing here writes to `documents`. Catalogue entries are built *from*
stored text and stored beside it -- no extracted material is overwritten,
truncated or deleted.
"""
from __future__ import annotations

import argparse
import json
import sys
from decimal import Decimal

from rich.console import Console
from sqlalchemy.orm import undefer

import costs
import database
from database import Document, DocumentCatalogue, ExtractionDetail

console = Console(legacy_windows=False)

MODEL = "claude-haiku-4-5-20251001"
MAX_OUTPUT_TOKENS = 2048
# The contents page or opening agenda. Board packs put it first; 6k characters
# is a few pages of text and comfortably covers it without paying to send a
# 150,000-character document to be told what its first page says.
HEAD_CHARS = 6_000

SYSTEM_PROMPT = (
    "You are cataloguing a US public pension board document. You are given "
    "only its opening pages -- typically an agenda or table of contents.\n\n"
    "Your job is to record WHAT THE DOCUMENT CONTAINS, not to summarise it. "
    "Do not infer content from the plan's general activities; report only "
    "what these pages actually indicate is inside.\n\n"
    "The three categories that matter:\n"
    "  asset_allocation  - target or actual asset allocation, policy weights\n"
    "  performance       - returns, performance vs benchmark, manager returns\n"
    "  manager_changes   - manager hires, terminations, searches, RFPs\n\n"
    "If the opening pages give page numbers, tab numbers or item numbers for "
    "that material, record them verbatim in page_hints. If they do not, leave "
    "page_hints empty rather than guessing."
)

TOOL_SCHEMA = {
    "name": "record_catalogue",
    "description": "Record what this document contains, from its opening pages.",
    "input_schema": {
        "type": "object",
        "properties": {
            "contains": {
                "type": "array",
                "items": {"type": "string",
                          "enum": ["asset_allocation", "performance",
                                   "manager_changes"]},
                "description": "Categories the opening pages show are present. "
                               "Empty list if none.",
            },
            "sections": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Contents or agenda entries, verbatim, in order. "
                               "Empty if the pages are not a contents/agenda.",
            },
            "page_hints": {
                "type": "string",
                "description": "Where the matched material sits, as the source "
                               "states it (e.g. 'Tab 7, pp. 340-372'). Empty "
                               "if not stated.",
            },
        },
        "required": ["contains", "sections", "page_hints"],
    },
}


def backlog_documents(session):
    """Documents that already hold text and have no catalogue entry yet.

    Scoped to `ocr_partial` -- the 354 image-only documents whose OCR stopped
    at a cap. They already hold ~150,000 characters each, so their catalogue
    costs no OCR at all.
    """
    return (session.query(Document)
            .join(ExtractionDetail, ExtractionDetail.document_id == Document.id)
            .outerjoin(DocumentCatalogue,
                       DocumentCatalogue.document_id == Document.id)
            .filter(ExtractionDetail.reason == "ocr_partial",
                    Document.extracted_text.isnot(None),
                    DocumentCatalogue.document_id.is_(None))
            .options(undefer(Document.extracted_text))
            .order_by(Document.meeting_date.desc().nullslast(), Document.id))


def _estimate_cost(n: int) -> Decimal:
    """Rough per-document cost, for the priced worklist."""
    price = costs.PRICES[MODEL]
    tokens_in = HEAD_CHARS // 4
    return n * (Decimal(tokens_in) * price.input
                + Decimal(400) * price.output) / costs.MILLION


def build_entry(doc) -> tuple[dict, Decimal]:
    """One paid call. Only ever reached from an approved run."""
    from summarizer import _get_client

    head = (doc.extracted_text or "")[:HEAD_CHARS]
    msg = _get_client().messages.create(
        model=MODEL,
        max_tokens=MAX_OUTPUT_TOKENS,
        system=[{"type": "text", "text": SYSTEM_PROMPT}],
        tools=[TOOL_SCHEMA],
        tool_choice={"type": "tool", "name": "record_catalogue"},
        messages=[{"role": "user", "content":
                   f"Document: {doc.filename}\n\n--- OPENING PAGES ---\n{head}"}],
    )
    cost = costs.cost_usd(MODEL, msg.usage)
    for block in msg.content:
        if block.type == "tool_use" and block.name == "record_catalogue":
            return block.input, cost
    raise RuntimeError(f"no tool call; stop_reason={msg.stop_reason}")


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--backlog", action="store_true",
                    help="the ocr_partial documents that already hold text")
    ap.add_argument("--approve", action="store_true",
                    help="actually spend. Without this, no paid call is reachable.")
    ap.add_argument("--budget", type=float, default=5.0,
                    help="hard ceiling in USD; the run stops rather than exceeding it")
    ap.add_argument("--limit", type=int)
    args = ap.parse_args()

    if not args.backlog:
        ap.error("nothing selected; pass --backlog")

    session = database.SessionLocal()
    try:
        docs = backlog_documents(session)
        if args.limit:
            docs = docs.limit(args.limit)
        docs = docs.all()

        if not docs:
            console.print("Nothing to catalogue -- every candidate has an entry.")
            return 0

        est = _estimate_cost(len(docs))
        console.print(f"\n[bold]{len(docs)}[/bold] documents to catalogue "
                      f"from stored text (no OCR)")
        console.print(f"estimated cost   [bold]${est:.2f}[/bold]   "
                      f"({MODEL}, {HEAD_CHARS:,} chars each)")
        console.print(f"budget ceiling   ${args.budget:.2f}")

        if not args.approve:
            console.print("\n[yellow]Nothing spent. Re-run with --approve "
                          "to proceed.[/yellow]\n")
            return 0

        spent = Decimal(0)
        done = stopped = 0
        for doc in docs:
            if float(spent) >= args.budget:
                console.print(f"\n[yellow]Budget ${args.budget:.2f} reached "
                              f"after {done} documents. Stopping.[/yellow]")
                stopped = 1
                break
            try:
                data, cost = build_entry(doc)
            except Exception as e:                      # noqa: BLE001
                console.print(f"  [red]{doc.id} {doc.filename}: {e}[/red]")
                continue
            spent += cost
            session.add(DocumentCatalogue(
                document_id=doc.id,
                contains=json.dumps(data.get("contains", [])),
                sections=json.dumps(data.get("sections", [])[:40]),
                page_hints=(data.get("page_hints") or "")[:2000],
                source="existing_text",
                model=MODEL,
                cost_usd=cost,
            ))
            session.commit()
            done += 1
            hits = ",".join(data.get("contains", [])) or "-"
            console.print(f"  [green]{done:>4}[/green] {str(doc.filename)[:46]:<46} {hits}")

        console.print(f"\ncatalogued [bold]{done}[/bold] documents, "
                      f"spent [bold]${spent:.2f}[/bold]")
        return stopped
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
