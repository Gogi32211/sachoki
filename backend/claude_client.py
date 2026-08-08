"""
claude_client.py — Backend-only Claude API client for the AI analyst layer.

SECURITY RULES:
- ANTHROPIC_API_KEY read server-side only, never returned to frontend
- Falls back gracefully when key is missing or API is unavailable
- Results are passed to callers who may cache them
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any

log = logging.getLogger(__name__)

_MODEL = os.environ.get("CLAUDE_MODEL", "claude-haiku-4-5-20251001")


def _client():
    """Lazily construct Anthropic client; returns None if unavailable."""
    try:
        import anthropic
        key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not key:
            return None
        return anthropic.Anthropic(api_key=key)
    except Exception:
        return None


def ask(prompt: str, system: str = "", max_tokens: int = 1024,
        model: str | None = None) -> str | None:
    """
    Send a prompt to Claude. Returns the text response or None on failure.
    Never raises — callers must handle None as 'use fallback'.
    `model` overrides the default (e.g. 'claude-opus-5' for the brain's final decider).
    """
    client = _client()
    if client is None:
        return None
    try:
        msgs = [{"role": "user", "content": prompt}]
        kwargs: dict[str, Any] = {"model": model or _MODEL, "max_tokens": max_tokens,
                                  "messages": msgs}
        if system:
            kwargs["system"] = system
        resp = client.messages.create(**kwargs)
        # Opus 5 may emit a ThinkingBlock before the text — take the first TEXT block,
        # not blindly content[0] ('ThinkingBlock' object has no attribute 'text').
        for block in (resp.content or []):
            t = getattr(block, "text", None)
            if t is not None:
                return t
        return None
    except Exception as exc:
        log.warning("Claude API call failed: %s", exc)
        return None


def ask_json(prompt: str, system: str = "", max_tokens: int = 1024,
             model: str | None = None) -> dict | list | None:
    """Like ask() but parses JSON. Returns None on failure."""
    raw = ask(prompt, system=system, max_tokens=max_tokens, model=model)
    if raw is None:
        return None
    text = raw.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
    try:
        return json.loads(text)
    except Exception:
        import re
        m = re.search(r"(\{[\s\S]*\}|\[[\s\S]*\])", text)
        if m:
            try:
                return json.loads(m.group(1))
            except Exception:
                pass
        log.warning("Could not parse JSON from Claude response")
        return None
