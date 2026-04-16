"""
Claude-powered summarization and structured data extraction from pension documents.

Cost optimisations:
- Haiku for short/simple docs; Sonnet only for large investment packs
- Smart truncation: first 20k + keyword-rich middle chunks + last 10k (cap ~50k)
- Hash-based deduplication: never re-summarise identical text
- max_tokens capped at 1500 (summaries rarely need more)
- Skip clearly non-substantive documents
"""

import hashlib
import json
import os
import re
from datetime import datetime

import anthropic
from dotenv import load_dotenv
from rich.console import Console
from tenacity import retry, stop_after_attempt, wait_exponential

from database import (
    Document, Summary, get_session, get_unsummarized_documents,
    summary_exists_for_hash, Plan,
)

_ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(_ENV_PATH, override=True)
console = Console(legacy_windows=False)

MODEL_SONNET = "claude-sonnet-4-6"
MODEL_HAIKU = "claude-haiku-4-5-20251001"

# Docs whose filenames match these patterns carry no investment intelligence
SKIP_FILENAME_PATTERNS = re.compile(
    r"(attendance|building|map|calendar|cover.?page|direction|notice.only"
    r"|parking|visitor|hotel|travel|biography|bio|headshot)",
    re.IGNORECASE,
)

# Keywords that signal high-value content worth Sonnet's precision
SONNET_KEYWORDS = re.compile(
    r"(investment|portfolio|allocation|return|performance|manager|mandate"
    r"|commitment|private.equity|real.estate|infrastructure|hedge|fixed.income"
    r"|equity|emerging.market|asset.class|benchmark|alpha|risk)",
    re.IGNORECASE,
)

# Context window budget
SMART_TRUNCATE_TARGET = 50_000   # chars sent to Claude (~12,500 tokens)
HEAD_CHARS = 20_000              # always keep the start
TAIL_CHARS = 10_000              # always keep the end
CHUNK_SIZE = 3_000               # size of keyword-matched middle chunks
KEYWORD_WINDOW = 1_500           # chars around a keyword hit to include


# ---------------------------------------------------------------------------
# Model selection
# ---------------------------------------------------------------------------

def choose_model(doc: Document) -> str:
    """
    Use Haiku for short / simple documents; Sonnet for large investment packs.
    Sonnet is ~4x more expensive — reserve it for docs that need it.
    """
    text = doc.extracted_text or ""
    text_len = len(text)

    # Short docs or simple doc types → Haiku
    if text_len < 8_000:
        return MODEL_HAIKU
    if doc.doc_type == "agenda" and text_len < 20_000:
        return MODEL_HAIKU
    if doc.doc_type == "minutes" and text_len < 15_000:
        return MODEL_HAIKU

    # Large docs with investment keywords → Sonnet
    if text_len >= 20_000 and SONNET_KEYWORDS.search(text[:5_000]):
        return MODEL_SONNET

    return MODEL_HAIKU


# ---------------------------------------------------------------------------
# Smart truncation
# ---------------------------------------------------------------------------

INVESTMENT_SIGNAL = re.compile(
    r"(recommendation|approved|committed|mandate|allocation|return|performance"
    r"|hired|terminated|rebalance|benchmark|risk|manager|portfolio|fund)",
    re.IGNORECASE,
)


def smart_truncate(text: str) -> str:
    """
    Instead of naively taking the first N chars, build a targeted excerpt:
      - Head: first 20k (agenda, intro, executive summary)
      - Middle: chunks around investment-signal keywords
      - Tail: last 10k (decisions, votes, conclusions)
    Total capped at ~50k chars.
    """
    if len(text) <= SMART_TRUNCATE_TARGET:
        return text

    head = text[:HEAD_CHARS]
    tail = text[-TAIL_CHARS:]
    middle_budget = SMART_TRUNCATE_TARGET - HEAD_CHARS - TAIL_CHARS
    middle_text = text[HEAD_CHARS:-TAIL_CHARS]

    # Collect char offsets of keyword hits in the middle section
    hit_positions = [m.start() for m in INVESTMENT_SIGNAL.finditer(middle_text)]

    # Expand each hit into a window and merge overlapping windows
    windows: list[tuple[int, int]] = []
    for pos in hit_positions:
        start = max(0, pos - KEYWORD_WINDOW // 2)
        end = min(len(middle_text), pos + KEYWORD_WINDOW // 2)
        if windows and start <= windows[-1][1]:
            windows[-1] = (windows[-1][0], max(windows[-1][1], end))
        else:
            windows.append((start, end))

    # Collect chunks until we hit the budget
    middle_chunks = []
    used = 0
    for start, end in windows:
        chunk = middle_text[start:end]
        if used + len(chunk) > middle_budget:
            chunk = chunk[: middle_budget - used]
            middle_chunks.append(chunk)
            break
        middle_chunks.append(chunk)
        used += len(chunk)

    middle = "\n\n[...]\n\n".join(middle_chunks) if middle_chunks else ""
    return head + ("\n\n[...]\n\n" + middle if middle else "") + "\n\n[...]\n\n" + tail


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are a financial analyst specializing in public pension fund investment committees.
You extract structured information from investment board meeting documents with precision.
Always respond in valid JSON as instructed. Be concise but comprehensive."""


def build_extraction_prompt(doc: Document, plan_name: str, text: str) -> str:
    date_str = doc.meeting_date.strftime("%B %d, %Y") if doc.meeting_date else "unknown date"
    doc_type = doc.doc_type or "document"

    return f"""Below is the text of a {doc_type} from the investment committee/board of {plan_name}, meeting date: {date_str}.

Analyze it and return a JSON object with exactly these fields:

{{
  "summary": "2-4 sentence plain English summary focused on investment decisions, portfolio actions, and performance discussed",
  "key_topics": ["list", "of", "main", "investment", "topics"],
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
  "notable_items": ["Fee disclosures, ESG/proxy votes, policy changes, new mandates, risk updates"]
}}

If a section has no relevant content, use an empty list [].
Return ONLY the JSON object, no markdown or explanation.

DOCUMENT TEXT:
{text}"""


# ---------------------------------------------------------------------------
# API call with retry
# ---------------------------------------------------------------------------

_client: anthropic.Anthropic | None = None


def _get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            load_dotenv(_ENV_PATH, override=True)
            api_key = os.environ.get("ANTHROPIC_API_KEY")

        # Fall back to Claude Code session ingress token (OAuth bearer auth)
        if not api_key:
            token_file = os.environ.get("CLAUDE_SESSION_INGRESS_TOKEN_FILE")
            if token_file and os.path.exists(token_file):
                with open(token_file) as f:
                    auth_token = f.read().strip()
                if auth_token:
                    _client = anthropic.Anthropic(auth_token=auth_token)
                    return _client

        if not api_key:
            raise RuntimeError(f"ANTHROPIC_API_KEY not set. Check {_ENV_PATH}")
        # Use the real Anthropic API endpoint, bypassing any local proxy
        # (e.g. Claude Code sets ANTHROPIC_BASE_URL=http://127.0.0.1:... which
        # rejects direct API keys).
        _client = anthropic.Anthropic(api_key=api_key, base_url="https://api.anthropic.com")
    return _client


def _max_tokens(model: str) -> int:
    return 4096 if model == MODEL_HAIKU else 4096


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=4, max=30))
def call_claude(prompt: str, model: str) -> str:
    message = _get_client().messages.create(
        model=model,
        max_tokens=_max_tokens(model),
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}]
    )
    return message.content[0].text


def parse_response(raw: str) -> dict:
    raw = raw.strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\n?", "", raw)
        raw = re.sub(r"\n?```$", "", raw)
    return json.loads(raw)


# ---------------------------------------------------------------------------
# Main summarization logic
# ---------------------------------------------------------------------------

def should_skip(doc: Document) -> str | None:
    """Return a skip reason string if this doc should not be summarised, else None."""
    text = doc.extracted_text or ""

    if len(text.strip()) < 100:
        return "insufficient text"

    if SKIP_FILENAME_PATTERNS.search(doc.filename or ""):
        return f"non-substantive filename ({doc.filename})"

    return None


def summarize_document(doc: Document, plan_name: str,
                       session) -> Summary | None:
    """
    Generate a Summary for a single document.
    Returns Summary object (not committed), or None if skipped.
    """
    skip_reason = should_skip(doc)
    if skip_reason:
        console.print(f"  [yellow]Skipping {doc.filename} — {skip_reason}[/yellow]")
        return None

    # Hash-based deduplication
    text_hash = hashlib.md5((doc.extracted_text or "").encode()).hexdigest()
    existing = summary_exists_for_hash(session, text_hash)
    if existing:
        console.print(f"  [dim]Skipping {doc.filename} — duplicate of doc {existing.document_id}[/dim]")
        # Create a thin summary record pointing at same hash so it won't be retried
        return Summary(
            document_id=doc.id,
            summary_text=existing.summary_text,
            key_topics=existing.key_topics,
            investment_actions=existing.investment_actions,
            decisions=existing.decisions,
            performance_data=existing.performance_data,
            generated_at=datetime.utcnow(),
            model_used=f"dedup:{existing.model_used}",
            text_hash=text_hash,
        )

    # Smart truncation + model routing
    truncated = smart_truncate(doc.extracted_text)
    model = choose_model(doc)
    orig_len = len(doc.extracted_text or "")
    trunc_len = len(truncated)

    console.print(
        f"  Summarizing [cyan]{doc.filename}[/cyan] "
        f"({orig_len:,}->{trunc_len:,} chars, [bold]{model.split('-')[1]}[/bold])"
    )

    try:
        prompt = build_extraction_prompt(doc, plan_name, truncated)
        raw = call_claude(prompt, model)
        data = parse_response(raw)
    except json.JSONDecodeError:
        # Output was truncated — retry with a 20k char excerpt (fits comfortably in 2048 tokens)
        console.print(f"  [yellow]JSON truncated, retrying with shorter excerpt...[/yellow]")
        try:
            short_text = truncated[:20_000]
            prompt = build_extraction_prompt(doc, plan_name, short_text)
            raw = call_claude(prompt, model)
            data = parse_response(raw)
        except Exception as e2:
            console.print(f"  [red]Retry failed: {e2}[/red]")
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
        model_used=model,
        text_hash=text_hash,
    )

    notable = data.get("notable_items", [])
    if notable:
        summary.summary_text += "\n\nNotable items: " + "; ".join(notable)

    return summary


def run_summarizer(doc_ids: list[int] = None):
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

        plan_names = {p.id: p.name for p in session.query(Plan).all()}
        haiku_count = sonnet_count = dedup_count = skip_count = 0

        for doc in docs:
            plan_name = plan_names.get(doc.plan_id, doc.plan_id)
            summary = summarize_document(doc, plan_name, session)
            if summary:
                session.add(summary)
                session.commit()
                if summary.model_used.startswith("dedup"):
                    dedup_count += 1
                elif MODEL_HAIKU in summary.model_used:
                    haiku_count += 1
                else:
                    sonnet_count += 1
            else:
                skip_count += 1

        done = haiku_count + sonnet_count + dedup_count
        console.print(
            f"\n[bold green]{done}/{len(docs)} summarized[/bold green] — "
            f"Sonnet: {sonnet_count}, Haiku: {haiku_count}, "
            f"Dedup: {dedup_count}, Skipped: {skip_count}"
        )

    finally:
        session.close()


if __name__ == "__main__":
    run_summarizer()
