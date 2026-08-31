"""DeepSeek V4 Flash through OpenRouter, metered the same way as Anthropic.

Its own module because OpenRouter is OpenAI-shaped and everything else here
is Anthropic-shaped, and the differences are not cosmetic:

  * the tool call arrives as a JSON *string* in
    ``choices[0].message.tool_calls[0].function.arguments``, not as a parsed
    ``tool_use`` block;
  * truncation shows as ``finish_reason == "length"``, not
    ``stop_reason == "max_tokens"``;
  * usage is ``prompt_tokens``/``completion_tokens``, which ``costs.cost_usd``
    does not recognise -- see ``adapt_usage``.

Reads OPENROUTER_API_KEY. Used only for schema-constrained extraction;
summarising stays on Anthropic.
"""
from __future__ import annotations

import json
import os
from decimal import Decimal
from types import SimpleNamespace

import costs
import database

MODEL = "deepseek/deepseek-v4-flash"
BASE_URL = "https://openrouter.ai/api/v1"
MAX_OUTPUT_TOKENS = 16_384


class ResponseTruncated(RuntimeError):
    """finish_reason was 'length'. The arguments JSON is incomplete and must
    not be saved as if it were a result."""


def adapt_usage(usage):
    """OpenAI token names -> the names costs.cost_usd reads.

    Without this every call costs 0.00 and the spend silently disappears.
    """
    return SimpleNamespace(
        input_tokens=getattr(usage, "prompt_tokens", 0) or 0,
        output_tokens=getattr(usage, "completion_tokens", 0) or 0,
        cache_creation_input_tokens=0,
        cache_read_input_tokens=0,
    )


def _raw_call(**kwargs):
    from openai import OpenAI
    key = os.environ.get("OPENROUTER_API_KEY")
    if not key:
        raise RuntimeError("OPENROUTER_API_KEY not set")
    client = OpenAI(api_key=key, base_url=BASE_URL)
    return client.chat.completions.create(**kwargs)


def call_tool(system: str, user: str, schema: dict,
              tool_name: str) -> tuple[dict, Decimal]:
    resp = _raw_call(
        model=MODEL,
        max_tokens=MAX_OUTPUT_TOKENS,
        messages=[{"role": "system", "content": system},
                  {"role": "user", "content": user}],
        tools=[{"type": "function",
                "function": {"name": tool_name, "parameters": schema}}],
        tool_choice={"type": "function", "function": {"name": tool_name}},
        # Route for tool-calling accuracy rather than price: the whole design
        # rests on the tool call being well-formed.
        extra_body={"provider": {"sort": "throughput"}, "route": "exacto"},
    )
    choice = resp.choices[0]
    usage = adapt_usage(resp.usage)
    cost = costs.cost_usd(MODEL, usage)

    # Record before raising. A truncated call is still a billed call, and the
    # failures are exactly the ones worth seeing in api_usage afterwards.
    if not costs.mock_mode():
        database.record_api_usage(MODEL, usage)

    if choice.finish_reason == "length":
        raise ResponseTruncated(
            f"in={usage.input_tokens} out={usage.output_tokens}")
    calls = getattr(choice.message, "tool_calls", None)
    if not calls:
        raise RuntimeError(
            f"no tool call; finish_reason={choice.finish_reason}")
    return json.loads(calls[0].function.arguments), cost
