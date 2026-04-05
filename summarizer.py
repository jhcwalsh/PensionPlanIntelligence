"""
Claude-powered summarization and structured data extraction from pension documents.
"""

import json
import os
import re
from datetime import datetime
from pathlib import Path

import anthropic
from dotenv import load_dotenv
from rich.console import Console
from tenacity import retry, stop_after_attempt, wait_exponential

from database import Document, Summary, get_session, get_unsummarized_documents

_ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(_ENV_PATH, override=True)
console = Console()

MODEL = "claude-sonnet-4-6"
MAX_CONTEXT_CHARS = 150_000  # leave room for prompt overhead

_client: anthropic.Anthropic | None = None


def _get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            # Re-attempt load in case CWD changed since import
            load_dotenv(_ENV_PATH, override=True)
            api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError(f"ANTHROPIC_API_KEY not set. Check {_ENV_PATH}")
        _client = anthropic.Anthropic(api_key=api_key)
    return _client


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are a financial analyst specializing in public pension fund governance.
You extract structured information from pension board meeting documents with precision.
Always respond in valid JSON as instructed. Be concise but comprehensive."""


def build_extraction_prompt(doc: Document, plan_name: str) -> str:
    text = (doc.extracted_text or "")[:MAX_CONTEXT_CHARS]
    date_str = doc.meeting_date.strftime("%B %d, %Y") if doc.meeting_date else "unknown date"
    doc_type = doc.doc_type or "document"

    return f"""Below is the text of a {doc_type} from {plan_name}, meeting date: {date_str}.

Analyze it and return a JSON object with exactly these fields:

{{
  "summary": "2-4 sentence plain English summary of what this document covers and what was discussed or decided",
  "key_topics": ["list", "of", "main", "topics", "discussed"],
  "decisions": [
    {{"description": "what was decided", "vote": "e.g. 7-0 or unanimous or null if not a vote"}}
  ],
  "investment_actions": [
    {{
      "action": "hire|fire|rebalance|allocation_change|commitment|other",
      "description": "e.g. Hired BlackRock for $500M global equity mandate",
      "manager": "manager name if applicable",
      "asset_class": "e.g. Global Equity, Private Equity, Real Estate",
      "amount_millions": 500
    }}
  ],
  "performance_data": [
    {{
      "period": "e.g. Q3 2024 or FY2024",
      "asset_class": "e.g. Total Fund or Private Equity",
      "return_pct": 8.5,
      "benchmark_pct": 7.2,
      "note": "optional comment"
    }}
  ],
  "notable_items": ["Any other notable items: fee disclosures, ESG votes, policy changes, actuarial updates"]
}}

If a section has no relevant content, use an empty list [].
Return ONLY the JSON object, no markdown or explanation.

DOCUMENT TEXT:
{text}"""


# ---------------------------------------------------------------------------
# API call with retry
# ---------------------------------------------------------------------------

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=4, max=30))
def call_claude(prompt: str) -> str:
    message = _get_client().messages.create(
        model=MODEL,
        max_tokens=4096,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}]
    )
    return message.content[0].text


def parse_response(raw: str) -> dict:
    """Parse Claude's JSON response, handling common formatting issues."""
    # Strip markdown code fences if present
    raw = raw.strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\n?", "", raw)
        raw = re.sub(r"\n?```$", "", raw)
    return json.loads(raw)


# ---------------------------------------------------------------------------
# Main summarization logic
# ---------------------------------------------------------------------------

def summarize_document(doc: Document, plan_name: str) -> Summary | None:
    """Generate a Summary for a single document. Returns Summary object (not committed)."""
    if not doc.extracted_text or len(doc.extracted_text.strip()) < 100:
        console.print(f"  [yellow]Skipping {doc.filename} — insufficient text[/yellow]")
        return None

    console.print(f"  Summarizing [cyan]{doc.filename}[/cyan] ({len(doc.extracted_text):,} chars)")

    try:
        prompt = build_extraction_prompt(doc, plan_name)
        raw = call_claude(prompt)
        data = parse_response(raw)
    except json.JSONDecodeError as e:
        console.print(f"  [red]JSON parse error: {e}[/red]")
        return None
    except Exception as e:
        console.print(f"  [red]Claude API error: {e}[/red]")
        return None

    summary = Summary(
        document_id=doc.id,
        summary_text=data.get("summary", ""),
        key_topics=json.dumps(data.get("key_topics", [])),
        investment_actions=json.dumps(data.get("investment_actions", [])),
        decisions=json.dumps(data.get("decisions", [])),
        performance_data=json.dumps(data.get("performance_data", [])),
        generated_at=datetime.utcnow(),
        model_used=MODEL,
    )

    # Append notable items to summary text if present
    notable = data.get("notable_items", [])
    if notable:
        summary.summary_text += "\n\nNotable items: " + "; ".join(notable)

    return summary


def run_summarizer(doc_ids: list[int] = None):
    """
    Summarize all extracted-but-unsummarized documents (or specific doc_ids).
    """
    session = get_session()
    try:
        if doc_ids:
            docs = (
                session.query(Document)
                .filter(Document.id.in_(doc_ids), Document.extraction_status == "done")
                .all()
            )
        else:
            docs = get_unsummarized_documents(session)

        if not docs:
            console.print("[yellow]No documents pending summarization.[/yellow]")
            return

        console.print(f"[bold]Summarizing {len(docs)} documents with Claude...[/bold]")

        # Pre-load plan names
        from database import Plan
        plan_names = {p.id: p.name for p in session.query(Plan).all()}

        done = 0
        for doc in docs:
            plan_name = plan_names.get(doc.plan_id, doc.plan_id)
            summary = summarize_document(doc, plan_name)
            if summary:
                session.add(summary)
                session.commit()
                done += 1
                console.print(f"    [green]Saved summary for doc {doc.id}[/green]")

        console.print(f"\n[bold green]{done}/{len(docs)} documents summarized.[/bold green]")

    finally:
        session.close()


if __name__ == "__main__":
    run_summarizer()
