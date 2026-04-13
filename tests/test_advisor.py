"""
Integration tests for the ExileSage advisor core.
Requires ANTHROPIC_API_KEY — skipped automatically when not set.
Run with API tests: pytest -m integration
Run without: pytest (default, skips these)
"""

import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

import pytest
from exilesage.advisor.core import ask, classify_query
from exilesage.config import QueryType

NEEDS_KEY = pytest.mark.skipif(
    not os.environ.get("ANTHROPIC_API_KEY", "").startswith("sk-ant"),
    reason="ANTHROPIC_API_KEY not set — run with API key to enable integration tests",
)


# ── Classification tests ──────────────────────────────────────────────────────

@NEEDS_KEY
@pytest.mark.integration
def test_classify_factual():
    result = classify_query("What does a Chaos Orb do?")
    assert result == QueryType.FACTUAL, f"Expected FACTUAL, got {result}"

@NEEDS_KEY
@pytest.mark.integration
def test_classify_crafting():
    result = classify_query("How do I craft a +1 gems wand?")
    assert result in (QueryType.CRAFTING, QueryType.GUIDE), f"Expected crafting-like, got {result}"

@NEEDS_KEY
@pytest.mark.integration
def test_classify_innovation():
    result = classify_query("Design a completely novel cold damage build no one has tried")
    assert result == QueryType.INNOVATION, f"Expected INNOVATION, got {result}"


# ── Answer grounding tests ────────────────────────────────────────────────────

@NEEDS_KEY
@pytest.mark.integration
def test_ask_returns_nonempty_string():
    result = ask("What does a Chaos Orb do?")
    assert isinstance(result, str) and len(result) > 20, "ask() returned empty or non-string"

@NEEDS_KEY
@pytest.mark.integration
def test_ask_chaos_orb_grounded():
    result = ask("What does a Chaos Orb do?").lower()
    assert "chaos" in result, "Answer doesn't mention chaos"
    assert any(w in result for w in ("modifier", "mod", "affix", "explicit")), \
        "Answer doesn't mention modifiers/mods"

@NEEDS_KEY
@pytest.mark.integration
def test_ask_essence_grounded():
    result = ask("How do essences work in crafting?").lower()
    assert "essence" in result, "Answer doesn't mention essence"
    assert any(w in result for w in ("guarantee", "guaranteed", "specific", "guaranteed mod")), \
        "Answer doesn't explain essence guarantee mechanic"

@NEEDS_KEY
@pytest.mark.integration
def test_ask_wand_craft_mentions_wand():
    result = ask("How do I craft a +1 to all spell skills wand?").lower()
    assert "wand" in result, "Answer doesn't mention wand"
    assert "spell" in result, "Answer doesn't mention spell"

@NEEDS_KEY
@pytest.mark.integration
def test_ask_nonsense_no_crash():
    result = ask("asdfghjkl poe2 xyzzy nonexistent item build zzz")
    assert isinstance(result, str), "ask() crashed on nonsense input"

@NEEDS_KEY
@pytest.mark.integration
def test_ask_wand_items_grounded():
    result = ask("What are the base wand types in PoE2?").lower()
    assert "wand" in result, "Answer doesn't mention wand bases"
