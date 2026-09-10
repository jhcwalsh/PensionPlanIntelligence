"""
Claude-powered summarization and structured data extraction from pension documents.

Cost optimisations:
- Haiku for short/simple docs; Sonnet only for large investment packs
- Smart truncation: first 20k + keyword-rich middle chunks + last 10k (cap ~50k)
- Hash-based deduplication: never re-summarise identical text
- max_tokens capped at 4096 on Haiku, 6000 on Sonnet 5 (summaries rarely
  need more; the Sonnet figure is the old 4096 plus 30% for its tokenizer)
- Skip clearly non-substantive documents
- One Message Batch per run, at half the standard rate: nothing waits on a
  summary, so there is no reason to pay the interactive price. The
  synchronous path is kept behind SUMMARIZE_MODE=sync for local one-offs.
"""

import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime

import anthropic
from sqlalchemy.orm import undefer

import batching
import costs
from dotenv import load_dotenv
from rich.console import Console
from tenacity import (
    retry,
    retry_if_not_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from database import (
    utcnow,
    Document, DocumentSkip, Summary, get_session, get_unsummarized_documents,
    summary_exists_for_hash, Plan,
)

_ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(_ENV_PATH, override=True)
console = Console(legacy_windows=False)

MODEL_SONNET = "claude-sonnet-5"
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


def _build_client() -> anthropic.Anthropic:
    """A raw client. Credential logic only — see _get_client for the wrapper."""
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
                return anthropic.Anthropic(auth_token=auth_token)

    if not api_key:
        raise RuntimeError(f"ANTHROPIC_API_KEY not set. Check {_ENV_PATH}")
    # Use the real Anthropic API endpoint, bypassing any local proxy
    # (e.g. Claude Code sets ANTHROPIC_BASE_URL=http://127.0.0.1:... which
    # rejects direct API keys).
    return anthropic.Anthropic(api_key=api_key,
                               base_url="https://api.anthropic.com")


def _get_client():
    """The shared client, instrumented so every call records what it cost.

    Most modules import this one. The three CAFR/IPS extractors build their own
    and wrap it the same way — see costs.instrument.
    """
    global _client
    if _client is None:
        _client = costs.instrument(_build_client())
    return _client


def _max_tokens(model: str) -> int:
    # Sonnet 5 tokenises ~30% heavier than 4.6; 4096 tuned for 4.6 would
    # truncate equivalent output.
    return 4096 if model == MODEL_HAIKU else 6000


class ClaudeRefusedError(RuntimeError):
    """Claude returned stop_reason='refusal'. Permanent — don't retry."""
    pass


def _record_refusal(session, doc: Document, error_message: str) -> None:
    """Insert a DocumentSkip row so future runs don't re-attempt this doc."""
    session.merge(DocumentSkip(
        document_id=doc.id,
        reason="refusal",
        error_message=error_message,
    ))
    session.commit()
    console.print(
        f"  [yellow]Claude refused {doc.filename}; recorded permanent skip "
        f"(saves ~$0.10/run on retries)[/yellow]"
    )


def _unwrap(exc: Exception) -> Exception:
    """Surface the real exception inside a tenacity RetryError wrapper."""
    if hasattr(exc, "last_attempt"):
        try:
            return exc.last_attempt.exception() or exc
        except Exception:
            pass
    return exc


def request_params(prompt: str, model: str) -> dict:
    """The Messages API request for one summary.

    One function for both paths: ``messages.create(**params)`` in sync mode,
    and the ``params`` of one batch request otherwise. Any divergence between
    the two would show up as summaries that differ by which path ran them.
    """
    params = dict(
        model=model,
        max_tokens=_max_tokens(model),
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )
    if model != MODEL_HAIKU:
        # Sonnet 5 thinks by default when the parameter is omitted, billed
        # as output. A JSON summary of a document it has in front of it
        # gains nothing from that, so it is off. Haiku 4.5 still takes the
        # older budget_tokens form and is left alone.
        params["thinking"] = {"type": "disabled"}
    return params


def message_text(message) -> str:
    """The text of a response, or the reason there is none.

    The first *text* block, not content[0]: with adaptive thinking on, a
    thinking block comes first and has no .text.
    """
    if not message.content:
        diagnostic = (
            f"stop_reason={message.stop_reason}, "
            f"input_tokens={message.usage.input_tokens}, "
            f"output_tokens={message.usage.output_tokens}"
        )
        if message.stop_reason == "refusal":
            raise ClaudeRefusedError(f"Claude refused ({diagnostic})")
        raise RuntimeError(f"Claude returned empty content ({diagnostic})")
    for block in message.content:
        # A block with no type at all is treated as text: every SDK block
        # carries one, so this only matters to hand-rolled stand-ins.
        if getattr(block, "type", "text") == "text":
            return block.text
    raise RuntimeError(
        f"Claude returned no text block (stop_reason={message.stop_reason})")


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=4, max=30),
    # Refusals are deterministic — retrying just costs money. Other empty
    # responses (max_tokens, transient API hiccups) still get the 3-attempt
    # treatment.
    retry=retry_if_not_exception_type(ClaudeRefusedError),
)
def call_claude(prompt: str, model: str) -> str:
    message = _get_client().messages.create(**request_params(prompt, model))
    return message_text(message)


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


@dataclass
class PendingCall:
    """A document that needs a model call: everything but the response."""
    doc: Document
    plan_name: str
    model: str
    text_hash: str
    truncated: str
    prompt: str


def prepare_document(doc: Document, plan_name: str,
                     session) -> Summary | PendingCall | None:
    """Decide what a document needs, without calling the model.

    Returns a ready Summary (a duplicate of one already held), a PendingCall
    (the request the model should see), or None (nothing worth summarising).
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
        return _dedup_summary(doc, existing, text_hash)

    # Smart truncation + model routing
    truncated = smart_truncate(doc.extracted_text)
    model = choose_model(doc)
    orig_len = len(doc.extracted_text or "")
    trunc_len = len(truncated)

    console.print(
        f"  Summarizing [cyan]{doc.filename}[/cyan] "
        f"({orig_len:,}->{trunc_len:,} chars, [bold]{model.split('-')[1]}[/bold])"
    )
    return PendingCall(
        doc=doc, plan_name=plan_name, model=model, text_hash=text_hash,
        truncated=truncated,
        prompt=build_extraction_prompt(doc, plan_name, truncated),
    )


def _dedup_summary(doc: Document, existing: Summary, text_hash: str) -> Summary:
    """A thin summary record pointing at the same hash so it won't be retried."""
    return Summary(
        document_id=doc.id,
        summary_text=existing.summary_text,
        key_topics=existing.key_topics,
        investment_actions=existing.investment_actions,
        decisions=existing.decisions,
        performance_data=existing.performance_data,
        generated_at=utcnow(),
        model_used=f"dedup:{existing.model_used}",
        text_hash=text_hash,
    )


def _summary_from(pending: PendingCall, data: dict) -> Summary:
    summary = Summary(
        document_id=pending.doc.id,
        summary_text=data.get("summary", ""),
        key_topics=json.dumps(data.get("key_topics", [])),
        investment_actions=json.dumps(data.get("investment_actions", [])),
        decisions=json.dumps(data.get("decisions", [])),
        performance_data=json.dumps(data.get("performance_data", [])),
        generated_at=utcnow(),
        model_used=pending.model,
        text_hash=pending.text_hash,
    )
    notable = data.get("notable_items", [])
    if notable:
        summary.summary_text += "\n\nNotable items: " + "; ".join(notable)
    return summary


def finish_document(pending: PendingCall, message, session) -> Summary | None:
    """Turn the model's response into a Summary, or record why there is none.

    Shared by both paths. The one follow-up call — a truncated JSON response
    retried on a shorter excerpt — is made synchronously even in batch mode:
    it is rare, and a second batch round-trip for a handful of documents
    would hold the whole run open for nothing.
    """
    doc = pending.doc
    try:
        data = parse_response(message_text(message))
    except json.JSONDecodeError:
        # Output was truncated — retry with a 20k char excerpt (fits comfortably in 2048 tokens)
        console.print(f"  [yellow]JSON truncated, retrying with shorter excerpt...[/yellow]")
        try:
            short_text = pending.truncated[:20_000]
            prompt = build_extraction_prompt(doc, pending.plan_name, short_text)
            data = parse_response(call_claude(prompt, pending.model))
        except ClaudeRefusedError as e2:
            _record_refusal(session, doc, str(e2))
            return None
        except Exception as e2:
            console.print(f"  [red]Retry failed: {_unwrap(e2)}[/red]")
            return None
    except ClaudeRefusedError as e:
        _record_refusal(session, doc, str(e))
        return None
    except Exception as e:
        console.print(f"  [red]Claude API error: {_unwrap(e)}[/red]")
        return None
    return _summary_from(pending, data)


def summarize_document(doc: Document, plan_name: str,
                       session) -> Summary | None:
    """
    Generate a Summary for a single document, synchronously.
    Returns Summary object (not committed), or None if skipped.
    """
    prepared = prepare_document(doc, plan_name, session)
    if not isinstance(prepared, PendingCall):
        return prepared
    try:
        message = _create_with_retry(request_params(prepared.prompt, prepared.model))
    except Exception as e:
        console.print(f"  [red]Claude API error: {_unwrap(e)}[/red]")
        return None
    return finish_document(prepared, message, session)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=4, max=30))
def _create_with_retry(params: dict):
    return _get_client().messages.create(**params)


# ---------------------------------------------------------------------------
# Batch path
# ---------------------------------------------------------------------------

#: How long a run will wait for its batch before cancelling it. The
#: daily-pipeline job has 360 minutes in total and has usually spent 30–40
#: of them fetching and extracting before it gets here. The mechanics live
#: in batching.run_message_batch, shared with the CAFR actuarial extractor.
BATCH_TIMEOUT_MINUTES = int(os.environ.get("SUMMARIZE_BATCH_TIMEOUT_MIN", "180"))
BATCH_POLL_SECONDS = 30


def _batch_requests(pendings: list[PendingCall]) -> list[dict]:
    return [
        {"custom_id": str(p.doc.id),
         "params": request_params(p.prompt, p.model)}
        for p in pendings
    ]


def _run_batch(client, requests: list[dict],
               poll_seconds: float, timeout_minutes: float) -> list:
    """One batch for the prebuilt requests; results in the API's order.

    Anything not summarised — an ``errored`` result, or one caught by a
    timeout cancellation — is picked up by the next run. Takes requests
    rather than pendings so nothing touches an ORM object between the
    caller's commit and the wait: reading an expired attribute would
    open a transaction again.
    """
    return batching.run_message_batch(
        client, requests, poll_seconds=poll_seconds,
        timeout_minutes=timeout_minutes, log=console.print)


_result_detail = batching.result_detail


def _collect_batch(pendings: list[PendingCall], results, session) -> dict:
    """Apply each batch result to its document. Returns counts by outcome."""
    by_id = {str(p.doc.id): p for p in pendings}
    counts = {"haiku": 0, "sonnet": 0, "failed": 0}
    for item in results:
        pending = by_id.get(item.custom_id)
        if pending is None:
            continue
        result = item.result
        if result.type != "succeeded":
            detail = _result_detail(result)
            console.print(
                f"  [red]{pending.doc.filename}: batch result {result.type} "
                f"({detail}); left for the next run[/red]"
            )
            counts["failed"] += 1
            continue
        message = result.message
        usage = getattr(message, "usage", None)
        if usage is not None and not costs.mock_mode():
            # The batch path bypasses the instrumented client's create(), so
            # this is where its spend gets recorded — at the batch rate.
            import database
            database.record_api_usage(pending.model, usage, batch=True)
        try:
            summary = finish_document(pending, message, session)
            if summary is None:
                counts["failed"] += 1
                continue
            session.add(summary)
            session.commit()
        except Exception as e:  # noqa: BLE001
            # One document's failure must not poison the session for the rest.
            session.rollback()
            console.print(f"  [red]{pending.doc.filename}: {_unwrap(e)}[/red]")
            counts["failed"] += 1
            continue
        counts["haiku" if pending.model == MODEL_HAIKU else "sonnet"] += 1
    return counts


def run_summarizer(doc_ids: list[int] = None, mode: str | None = None,
                   poll_seconds: float | None = None,
                   timeout_minutes: float | None = None):
    """Summarise every document that needs it.

    ``mode`` is "batch" (default) or "sync"; the environment variable
    SUMMARIZE_MODE sets it when the argument is omitted.
    """
    mode = mode or os.environ.get("SUMMARIZE_MODE", "batch")
    if mode not in ("batch", "sync"):
        raise ValueError(f"SUMMARIZE_MODE must be 'batch' or 'sync', not {mode!r}")
    poll_seconds = BATCH_POLL_SECONDS if poll_seconds is None else poll_seconds
    timeout_minutes = BATCH_TIMEOUT_MINUTES if timeout_minutes is None else timeout_minutes

    session = get_session()
    try:
        if doc_ids:
            docs = (
                session.query(Document)
                .options(undefer(Document.extracted_text))
                .filter(Document.id.in_(doc_ids), Document.extraction_status == "done")
                .all()
            )
        else:
            docs = get_unsummarized_documents(session)

        if not docs:
            console.print("[yellow]No documents pending summarization.[/yellow]")
            return

        console.print(f"[bold]Summarizing {len(docs)} documents with Claude ({mode})...[/bold]")

        plan_names = {p.id: p.name for p in session.query(Plan).all()}
        haiku_count = sonnet_count = dedup_count = skip_count = 0
        pendings: list[PendingCall] = []
        # In sync mode a duplicate finds the original's summary already
        # committed. In batch mode nothing is committed until the batch
        # returns, so a second copy of the same text in one run would be
        # summarised — and paid for — twice. Hold the copies back instead.
        pending_hashes: set[str] = set()
        held_duplicates: list[tuple[Document, str]] = []

        for doc in docs:
            plan_name = plan_names.get(doc.plan_id, doc.plan_id)
            if mode == "sync":
                summary = summarize_document(doc, plan_name, session)
            else:
                summary = prepare_document(doc, plan_name, session)
                if isinstance(summary, PendingCall):
                    if summary.text_hash in pending_hashes:
                        held_duplicates.append((doc, summary.text_hash))
                    else:
                        pending_hashes.add(summary.text_hash)
                        pendings.append(summary)
                    continue
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

        if pendings:
            # Build the requests, then close the transaction the prepare pass
            # opened, then wait: Neon terminates a transaction idle for five
            # minutes, and a batch can take longer.
            requests = _batch_requests(pendings)
            session.commit()
            results = _run_batch(_get_client(), requests, poll_seconds, timeout_minutes)
            counts = _collect_batch(pendings, results, session)
            haiku_count += counts["haiku"]
            sonnet_count += counts["sonnet"]
            skip_count += counts["failed"]

        for doc, text_hash in held_duplicates:
            existing = summary_exists_for_hash(session, text_hash)
            if existing is None:
                # The original's call failed; the copy waits with it.
                skip_count += 1
                continue
            session.add(_dedup_summary(doc, existing, text_hash))
            session.commit()
            dedup_count += 1

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
