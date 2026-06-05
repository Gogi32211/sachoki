"""
ai_journal/llm.py — thin Anthropic client for the journal.

- Structured output via forced tool_use (model MUST return validated JSON; no
  fragile text parsing).
- Prompt caching (cache_control: ephemeral) on the big static system block, so
  repeated sessions / batched candidates reuse it cheaply.
- Key from backend/.env (ANTHROPIC_API_KEY), never from the browser.
"""
from __future__ import annotations

import os
import logging

from dotenv import load_dotenv

# Load backend/.env regardless of CWD.
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

import anthropic  # noqa: E402

log = logging.getLogger(__name__)

# Models (resolved against the account's model list).
MODEL_DECISION = os.environ.get("AIJ_MODEL_DECISION", "claude-sonnet-4-6")
MODEL_LESSON   = os.environ.get("AIJ_MODEL_LESSON",   "claude-haiku-4-5-20251001")
MODEL_REFLECT  = os.environ.get("AIJ_MODEL_REFLECT",  "claude-opus-4-8")

_client: anthropic.Anthropic | None = None


def client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        _client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY
    return _client


def call_structured(system: str, user_text: str, schema: dict, *,
                    model: str, tool_name: str = "emit",
                    max_tokens: int = 4096, cache_system: bool = True) -> tuple[dict, dict]:
    """Force the model to return JSON matching `schema` via tool_use.
    Returns (parsed_input_dict, usage_dict). Raises on no tool_use block."""
    sys_block = [{
        "type": "text",
        "text": system,
        **({"cache_control": {"type": "ephemeral"}} if cache_system else {}),
    }]
    tool = {
        "name": tool_name,
        "description": "Return the structured result for this task.",
        "input_schema": schema,
    }
    resp = client().messages.create(
        model=model,
        max_tokens=max_tokens,
        system=sys_block,
        tools=[tool],
        tool_choice={"type": "tool", "name": tool_name},
        messages=[{"role": "user", "content": user_text}],
    )
    usage = {
        "input_tokens": resp.usage.input_tokens,
        "output_tokens": resp.usage.output_tokens,
        "cache_read": getattr(resp.usage, "cache_read_input_tokens", 0) or 0,
        "cache_write": getattr(resp.usage, "cache_creation_input_tokens", 0) or 0,
        "model": model,
    }
    for block in resp.content:
        if block.type == "tool_use" and block.name == tool_name:
            return block.input, usage
    raise RuntimeError("LLM returned no tool_use block")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    schema = {
        "type": "object",
        "properties": {
            "verdict": {"type": "string", "enum": ["BUY", "WATCH", "SKIP"]},
            "conviction": {"type": "integer"},
            "reason": {"type": "string"},
        },
        "required": ["verdict", "conviction", "reason"],
    }
    out, usage = call_structured(
        system="You are a disciplined trading analyst. Be terse.",
        user_text="Setup: V3=42, T2G, RTB phase D, RSI 58, V×20 volume spike. "
                  "Note: history shows strong HH-continuation but ~flat 5d directional edge. "
                  "Decide BUY/WATCH/SKIP with a one-line reason.",
        schema=schema, model=MODEL_LESSON, tool_name="decide",
    )
    print("structured output:", out)
    print("usage:", usage)
