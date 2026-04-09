"""
Unit tests for all 4 tool functions. No API key required.
Tests run against the live exilesage.db.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from exilesage.tools.mods import search_mods
from exilesage.tools.items import search_base_items
from exilesage.tools.currencies import search_currencies
from exilesage.tools.augments import search_augments
from exilesage.config import MAX_RESULTS


# ── search_mods ───────────────────────────────────────────────────────────────

def test_search_mods_returns_list():
    assert isinstance(search_mods(query="cold"), list)

def test_search_mods_required_keys():
    results = search_mods(query="strength", limit=3)
    for r in results:
        assert "id" in r, "mod result missing 'id'"
        assert "name" in r, "mod result missing 'name'"
        assert "domain" in r, "mod result missing 'domain'"

def test_search_mods_domain_filter():
    results = search_mods(domain="item", limit=10)
    assert len(results) > 0
    assert all(r["domain"] == "item" for r in results), "domain filter not applied"

def test_search_mods_generation_type_filter():
    results = search_mods(generation_type="prefix", limit=10)
    assert len(results) > 0
    assert all(r.get("generation_type") == "prefix" for r in results), "generation_type filter failed"

def test_search_mods_stat_id():
    results = search_mods(stat_id="cold", limit=5)
    assert len(results) > 0, "stat_id='cold' returned no mods"

def test_search_mods_limit_respected():
    assert len(search_mods(query="life", limit=3)) <= 3

def test_search_mods_max_cap():
    results = search_mods(query="a", limit=999)
    assert len(results) <= MAX_RESULTS, f"exceeded MAX_RESULTS cap: {len(results)}"

def test_search_mods_garbage_returns_empty():
    assert search_mods(query="xyzzy_nonexistent_99999") == []

def test_search_mods_returns_plain_dicts():
    results = search_mods(query="strength", limit=1)
    if results:
        assert type(results[0]) is dict, "result is not a plain dict"

def test_search_mods_no_args_returns_results():
    results = search_mods()
    assert len(results) > 0, "search_mods() with no args returned nothing"


# ── search_base_items ─────────────────────────────────────────────────────────

def test_search_items_wand_returns_results():
    results = search_base_items(query="wand")
    assert len(results) > 0, "wand query returned no items"

def test_search_items_by_class():
    results = search_base_items(item_class="Wand", limit=10)
    assert len(results) > 0
    assert all(r["item_class"] == "Wand" for r in results), "item_class filter not applied"

def test_search_items_level_range():
    results = search_base_items(min_level=60, max_level=80, limit=20)
    assert len(results) > 0
    for r in results:
        lvl = r.get("drop_level") or 0
        assert 60 <= lvl <= 80, f"item drop_level {lvl} outside [60, 80]"

def test_search_items_no_args_returns_results():
    assert len(search_base_items()) > 0

def test_search_items_limit_respected():
    assert len(search_base_items(limit=5)) <= 5

def test_search_items_max_cap():
    assert len(search_base_items(limit=999)) <= MAX_RESULTS

def test_search_items_garbage_returns_empty():
    assert search_base_items(query="xyzzy_nonexistent_99999") == []

def test_search_items_returns_plain_dicts():
    results = search_base_items(limit=1)
    if results:
        assert type(results[0]) is dict


# ── search_currencies ─────────────────────────────────────────────────────────

def test_search_currencies_chaos_orb():
    results = search_currencies(query="chaos")
    names = [r["name"] for r in results]
    assert "Chaos Orb" in names, f"Chaos Orb not found, got: {names}"

def test_search_currencies_has_description_key():
    results = search_currencies(query="orb", limit=1)
    if results:
        assert "description" in results[0], "currency result missing 'description' key"

def test_search_currencies_limit_respected():
    assert len(search_currencies(limit=3)) <= 3

def test_search_currencies_max_cap():
    assert len(search_currencies(limit=999)) <= MAX_RESULTS

def test_search_currencies_garbage_returns_empty():
    assert search_currencies(query="xyzzy_nonexistent_99999") == []

def test_search_currencies_no_args_returns_results():
    assert len(search_currencies()) > 0


# ── search_augments ───────────────────────────────────────────────────────────

def test_search_augments_by_slot():
    results = search_augments(slot="Body Armour", limit=5)
    assert len(results) > 0, "slot='Body Armour' returned no augments"

def test_search_augments_by_query():
    results = search_augments(query="Eye", limit=5)
    assert len(results) > 0, "query='Eye' returned no augments"

def test_search_augments_garbage_returns_empty():
    assert search_augments(query="xyzzy_nonexistent_99999") == []

def test_search_augments_returns_plain_dicts():
    results = search_augments(limit=1)
    if results:
        assert type(results[0]) is dict

def test_search_augments_no_args_returns_results():
    assert len(search_augments()) > 0
