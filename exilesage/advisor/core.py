"""
ExileSage advisor core — the agentic loop.

Flow:
    ask(question)
      → classify_query()  [Haiku, cheap]
      → select model from MODEL_MAP
      → agentic loop: messages.create → handle tool_use → repeat until end_turn
      → return final text

All model names and limits come from `exilesage.config`. The system prompt
comes from `exilesage.advisor.system_prompt`. Tool schemas come from
`exilesage.advisor.tool_defs`. Tool implementations come from
`exilesage.tools.*`.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

# Load .env if present (dev convenience)
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent.parent / ".env")
except ImportError:
    pass

# Make the project root importable when this module is run directly or from
# contexts where the package isn't installed (dev / tests / CLI shim).
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import anthropic  # noqa: E402

from exilesage.config import (  # noqa: E402
    MODEL_MAP,
    ROUTER_MODEL,
    MAX_TOOL_ITER,
    QueryType,
)
from exilesage.advisor.system_prompt import SYSTEM_PROMPT  # noqa: E402
from exilesage.advisor.tool_defs import TOOL_DEFINITIONS  # noqa: E402
from exilesage.tools.mods import search_mods  # noqa: E402
from exilesage.tools.items import search_base_items  # noqa: E402
from exilesage.tools.currencies import search_currencies  # noqa: E402
from exilesage.tools.augments import search_augments  # noqa: E402

log = logging.getLogger(__name__)

# ── Query classification ──────────────────────────────────────────────────────

_CLASSIFIER_SYSTEM = (
    "You are a query classifier for ExileSage, a Path of Exile 2 advisor. "
    "Classify the user's question into exactly one of these categories:\n"
    "  factual    — simple lookup: 'what does X do', 'what are the stats of Y'\n"
    "  crafting   — how to craft / improve a specific item\n"
    "  analysis   — comparative or evaluative: 'what's best for X', 'is Y good'\n"
    "  guide      — how to play a build / mechanic walkthrough\n"
    "  innovation — open-ended design: 'design a novel build', 'invent a strategy'\n"
    "Respond with ONE lowercase word from that list and nothing else."
)


def classify_query(question: str) -> QueryType:
    """
    Classify a user question into a QueryType so the right model can be chosen.

    Uses the cheap ROUTER_MODEL (Haiku). On any error, defaults to ANALYSIS,
    which routes to Sonnet — a safe middle ground.
    """
    try:
        client = anthropic.Anthropic()
        resp = client.messages.create(
            model=ROUTER_MODEL,
            max_tokens=16,
            system=_CLASSIFIER_SYSTEM,
            messages=[{"role": "user", "content": question}],
        )
        # Extract the first text block
        text = ""
        for block in resp.content:
            if getattr(block, "type", None) == "text":
                text = block.text.strip().lower()
                break
        # Strip punctuation/whitespace and match
        token = text.split()[0].strip(".,:;!?") if text else ""
        for qt in QueryType:
            if qt.value == token:
                log.info("classify_query: %r → %s", question[:60], qt.value)
                return qt
        log.warning("classify_query: unrecognized token %r, defaulting to ANALYSIS", text)
        return QueryType.ANALYSIS
    except Exception as e:
        log.warning("classify_query failed (%s); defaulting to ANALYSIS", e)
        return QueryType.ANALYSIS


# ── Tool dispatch ─────────────────────────────────────────────────────────────

_TOOL_DISPATCH = {
    "search_mods":       search_mods,
    "search_base_items": search_base_items,
    "search_currencies": search_currencies,
    "search_augments":   search_augments,
}


def execute_tool(tool_name: str, tool_input: dict) -> str:
    """
    Dispatch a tool_use block to the matching Python function and return its
    result as a JSON string (which is what the Anthropic API expects inside a
    tool_result content block).
    """
    fn = _TOOL_DISPATCH.get(tool_name)
    if fn is None:
        err = {"error": f"unknown tool: {tool_name}"}
        log.error("execute_tool: %s", err)
        return json.dumps(err)
    try:
        log.info("execute_tool: %s(%s)", tool_name, tool_input)
        result = fn(**tool_input)
        return json.dumps(result, default=str)
    except Exception as e:
        log.exception("execute_tool: %s raised", tool_name)
        return json.dumps({"error": str(e), "tool": tool_name})


# ── Agentic loop ──────────────────────────────────────────────────────────────

def _extract_text(response: Any) -> str:
    """Concatenate all text blocks from an Anthropic response."""
    parts = []
    for block in response.content:
        if getattr(block, "type", None) == "text":
            parts.append(block.text)
    return "\n".join(parts).strip()


def ask(question: str, query_type: QueryType | None = None) -> str:
    """
    Answer a PoE2 question using the ExileSage advisor.

    Parameters
    ----------
    question : str
        The user's natural-language question.
    query_type : QueryType | None
        Optional override. If None, the question is classified automatically.

    Returns
    -------
    str
        The advisor's final text response.
    """
    if query_type is None:
        query_type = classify_query(question)

    model = MODEL_MAP[query_type]
    log.info("ask: query_type=%s model=%s", query_type.value, model)

    client = anthropic.Anthropic()
    messages: list[dict] = [{"role": "user", "content": question}]

    response = None
    for iteration in range(MAX_TOOL_ITER):
        log.debug("ask: iteration %d/%d", iteration + 1, MAX_TOOL_ITER)
        response = client.messages.create(
            model=model,
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            tools=TOOL_DEFINITIONS,
            tool_choice={"type": "auto"},
            messages=messages,
        )

        if response.stop_reason == "end_turn":
            return _extract_text(response)

        if response.stop_reason == "tool_use":
            # Append the assistant message verbatim (content is a list of
            # blocks including any text and all tool_use blocks).
            messages.append({"role": "assistant", "content": response.content})

            # Execute every tool_use block in this turn and collect results.
            tool_results = []
            for block in response.content:
                if getattr(block, "type", None) == "tool_use":
                    result_str = execute_tool(block.name, dict(block.input))
                    tool_results.append(
                        {
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": result_str,
                        }
                    )

            if not tool_results:
                # Defensive: stop_reason said tool_use but no blocks present.
                log.warning("ask: tool_use stop_reason but no tool_use blocks")
                return _extract_text(response)

            messages.append({"role": "user", "content": tool_results})
            continue

        # Any other stop_reason (max_tokens, stop_sequence, etc.) — return what we have.
        log.info("ask: stop_reason=%s, returning partial", response.stop_reason)
        return _extract_text(response)

    # Iteration limit reached — extract any text from the final response and
    # append a note so the caller knows the loop was truncated.
    log.warning("ask: hit MAX_TOOL_ITER=%d", MAX_TOOL_ITER)
    tail = _extract_text(response) if response is not None else ""
    note = (
        f"\n\n_(Note: reached the tool-use iteration limit of {MAX_TOOL_ITER}. "
        "The answer above may be incomplete — try asking a more focused question.)_"
    )
    return (tail + note).strip()
