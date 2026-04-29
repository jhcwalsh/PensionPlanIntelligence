"""
Generate fixture LLM responses for the integration test.

Computes the cache key the orchestrator will use for each chunk of each
fixture document, then writes a canned response under
fixtures/llm_responses/{key}.json. The keys depend on (prompt text,
chunk text, plan_id, document_id) — running this script after editing
the prompt or fixtures regenerates them.

Usage:
    python -m scripts.seed_llm_fixtures
"""

from __future__ import annotations

import json
from pathlib import Path

from lib.pipeline_diagnostic import TASK_PROFILES
from rfp.ids import compute_rfp_id
from rfp.llm import _load_prompt, cache_key
from rfp.relevance import chunk_relevant_pages, split_pages

REPO = Path(__file__).resolve().parents[1]
FIXTURE_DOCS = REPO / "fixtures" / "documents"
FIXTURE_RESPONSES = REPO / "fixtures" / "llm_responses"

# Stable "document_id" values used by the integration test when it inserts
# Document rows. They must match exactly so cache keys line up.
DOC_IDS = {
    "calpers_2024_board.txt": (1, "calpers", "https://www.calpers.ca.gov/board/2024-03/packet.pdf"),
    "calstrs_2024_investment.txt": (2, "calstrs", "https://www.calstrs.com/board/2024-04/packet.pdf"),
    "calpers_2024_governance.txt": (3, "calpers", "https://www.calpers.ca.gov/governance/2024-02/packet.pdf"),
}


def _record_for(plan_id: str, doc_id: int, doc_url: str, payload: dict) -> dict:
    rec = dict(payload)
    rec["plan_id"] = plan_id
    rec["rfp_id"] = compute_rfp_id(
        plan_id=plan_id,
        rfp_type=rec["rfp_type"],
        anchor_date=rec.get("release_date") or rec.get("response_due_date") or rec.get("award_date"),
        title=rec["title"],
    )
    rec["source_document"] = {
        "url": doc_url,
        "page_number": rec.pop("_page", 1),
        "document_id": doc_id,
    }
    return rec


# Hand-written records keyed by document filename. The integration test
# treats these as the ground truth ("golden") output for each document.
RECORDS_BY_DOC: dict[str, list[dict]] = {
    "calpers_2024_board.txt": [
        {
            "_page": 12,
            "rfp_type": "Consultant",
            "title": "Investment Consulting Services",
            "status": "Planned",
            "release_date": "2024-03-15",
            "response_due_date": "2024-05-01",
            "award_date": None,
            "mandate_size_usd_millions": 1.2,
            "asset_class": None,
            "incumbent_manager": "Wilshire Associates",
            "incumbent_manager_id": None,
            "shortlisted_managers": [],
            "awarded_manager": None,
            "source_quote": (
                "Staff recommends issuing a Request for Proposal for general "
                "investment consulting services. The Board's contract with "
                "Wilshire Associates expires December 31, 2024. RFP would be "
                "released March 15, 2024 with responses due May 1, 2024."
            ),
            "extraction_confidence": 0.95,
        },
        {
            "_page": 28,
            "rfp_type": "Manager",
            "title": "Global Equity Manager Search",
            "status": "FinalistsNamed",
            "release_date": None,
            "response_due_date": None,
            "award_date": None,
            "mandate_size_usd_millions": 500.0,
            "asset_class": "Global Equity",
            "incumbent_manager": "Northern Trust",
            "incumbent_manager_id": None,
            "shortlisted_managers": ["BlackRock", "State Street Global Advisors", "Vanguard"],
            "awarded_manager": None,
            "source_quote": (
                "The search committee interviewed three finalists for the active "
                "global equity mandate: BlackRock, State Street Global Advisors, "
                "and Vanguard. Mandate size $500 million."
            ),
            "extraction_confidence": 0.85,
        },
    ],
    "calstrs_2024_investment.txt": [
        {
            "_page": 8,
            "rfp_type": "Actuary",
            "title": "Actuarial Services Contract",
            "status": "Awarded",
            "release_date": None,
            "response_due_date": None,
            "award_date": "2024-07-01",
            "mandate_size_usd_millions": 2.5,
            "asset_class": None,
            "incumbent_manager": "Segal Consulting",
            "incumbent_manager_id": None,
            "shortlisted_managers": [],
            "awarded_manager": "Cheiron",
            "source_quote": (
                "The Investment Committee voted 7-0 to award the five-year "
                "actuarial services contract to Cheiron, effective July 1, 2024. "
                "Cheiron replaces the incumbent Segal Consulting."
            ),
            "extraction_confidence": 0.95,
        },
    ],
    "calpers_2024_governance.txt": [],
}


def main() -> None:
    profile = TASK_PROFILES["rfp"]
    prompt = _load_prompt()
    FIXTURE_RESPONSES.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []

    for fname, (doc_id, plan_id, doc_url) in DOC_IDS.items():
        text = (FIXTURE_DOCS / fname).read_text()
        pages = split_pages(text)
        chunks = chunk_relevant_pages(pages, profile)

        records = [_record_for(plan_id, doc_id, doc_url, r)
                   for r in RECORDS_BY_DOC[fname]]

        if not chunks:
            # Governance doc: no chunks → nothing to cache.
            continue

        # Place each record into the chunk that contains its source page.
        for chunk in chunks:
            chunk_pages = {p.page_number for p in chunk.pages}
            chunk_records = [r for r in records
                             if r["source_document"]["page_number"] in chunk_pages]
            key = cache_key(prompt, chunk.text, plan_id, doc_id)
            path = FIXTURE_RESPONSES / f"{key}.json"
            path.write_text(json.dumps({"rfps": chunk_records}, indent=2))
            written.append(path)

    for p in written:
        print(p.name)
    print(f"\nWrote {len(written)} fixture responses to {FIXTURE_RESPONSES}")


if __name__ == "__main__":
    main()
