"""
Unit tests for exilesage.advisor.core — no API key required.
All Anthropic API calls are mocked.
"""

import json
from unittest.mock import patch, MagicMock

import pytest

from exilesage.advisor.core import (
    execute_tool,
    _extract_text,
    classify_query,
)
from exilesage.config import QueryType, MAX_TOOL_ITER


# ── execute_tool ─────────────────────────────────────────────────────────────

def test_execute_tool_known_tool():
    """execute_tool dispatches to search_mods and returns JSON string."""
    result = execute_tool("search_mods", {"query": "fire", "limit": 2})
    parsed = json.loads(result)
    assert isinstance(parsed, list)


def test_execute_tool_unknown_tool():
    """execute_tool returns error JSON for unregistered tool names."""
    result = execute_tool("nonexistent_tool", {})
    parsed = json.loads(result)
    assert "error" in parsed
    assert "unknown tool" in parsed["error"]


def test_execute_tool_bad_kwargs():
    """execute_tool returns error JSON when kwargs are invalid."""
    result = execute_tool("search_mods", {"nonexistent_param": 42})
    parsed = json.loads(result)
    assert "error" in parsed


def test_execute_tool_result_is_json_string():
    """execute_tool always returns a JSON string, never raw objects."""
    result = execute_tool("search_currencies", {"query": "orb", "limit": 1})
    assert isinstance(result, str)
    json.loads(result)  # must not raise


# ── _extract_text ────────────────────────────────────────────────────────────

def _make_response(blocks):
    """Create a mock response with given content blocks."""
    resp = MagicMock()
    resp.content = blocks
    return resp


def _text_block(text):
    block = MagicMock()
    block.type = "text"
    block.text = text
    return block


def _tool_use_block(name="search_mods", input_data=None, block_id="tu_1"):
    block = MagicMock()
    block.type = "tool_use"
    block.name = name
    block.input = input_data or {}
    block.id = block_id
    return block


def test_extract_text_single_block():
    resp = _make_response([_text_block("hello")])
    assert _extract_text(resp) == "hello"


def test_extract_text_multiple_blocks():
    resp = _make_response([_text_block("a"), _text_block("b")])
    assert _extract_text(resp) == "a\nb"


def test_extract_text_mixed_blocks():
    """Only text blocks are extracted, tool_use blocks are ignored."""
    resp = _make_response([
        _text_block("answer"),
        _tool_use_block(),
        _text_block("more"),
    ])
    assert _extract_text(resp) == "answer\nmore"


def test_extract_text_empty_content():
    resp = _make_response([])
    assert _extract_text(resp) == ""


# ── classify_query (mocked) ─────────────────────────────────────────────────

def _mock_classify_response(token: str):
    """Build a mock Anthropic response that returns a classification token."""
    resp = MagicMock()
    resp.content = [_text_block(token)]
    return resp


@patch("exilesage.advisor.core._get_client")
def test_classify_query_factual(mock_client):
    mock_client.return_value.messages.create.return_value = _mock_classify_response("factual")
    assert classify_query("what does Chaos Orb do?") == QueryType.FACTUAL


@patch("exilesage.advisor.core._get_client")
def test_classify_query_crafting(mock_client):
    mock_client.return_value.messages.create.return_value = _mock_classify_response("crafting")
    assert classify_query("how do I craft a wand?") == QueryType.CRAFTING


@patch("exilesage.advisor.core._get_client")
def test_classify_query_unrecognized_defaults_analysis(mock_client):
    mock_client.return_value.messages.create.return_value = _mock_classify_response("maybe")
    assert classify_query("something weird") == QueryType.ANALYSIS


@patch("exilesage.advisor.core._get_client")
def test_classify_query_api_error_defaults_analysis(mock_client):
    mock_client.return_value.messages.create.side_effect = Exception("API down")
    assert classify_query("test") == QueryType.ANALYSIS


@patch("exilesage.advisor.core._get_client")
def test_classify_query_empty_response_defaults_analysis(mock_client):
    resp = MagicMock()
    resp.content = []
    mock_client.return_value.messages.create.return_value = resp
    assert classify_query("test") == QueryType.ANALYSIS


# ── TOOL_DISPATCH sync ───────────────────────────────────────────────────────

def test_tool_dispatch_matches_tool_defs():
    """M2: every tool in TOOL_DEFINITIONS must have a matching dispatch entry."""
    from exilesage.advisor.tool_defs import TOOL_DEFINITIONS
    from exilesage.tools import TOOL_DISPATCH
    for tool_def in TOOL_DEFINITIONS:
        name = tool_def["name"]
        assert name in TOOL_DISPATCH, (
            f"Tool '{name}' in TOOL_DEFINITIONS but missing from TOOL_DISPATCH"
        )


def test_tool_dispatch_no_extras():
    """Every entry in TOOL_DISPATCH must have a matching TOOL_DEFINITIONS schema."""
    from exilesage.advisor.tool_defs import TOOL_DEFINITIONS
    from exilesage.tools import TOOL_DISPATCH
    def_names = {td["name"] for td in TOOL_DEFINITIONS}
    for name in TOOL_DISPATCH:
        assert name in def_names, (
            f"Tool '{name}' in TOOL_DISPATCH but missing from TOOL_DEFINITIONS"
        )
