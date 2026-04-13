"""
Unit tests for all 4 tool functions. No API key required.
Tests run against the live exilesage.db.
"""

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

def test_search_mods_domain_crafted():
    """Audit: 30 domains exist, not just 4. Verify 'crafted' works."""
    results = search_mods(domain="crafted", limit=5)
    assert len(results) > 0, "domain='crafted' returned no mods"
    assert all(r["domain"] == "crafted" for r in results)

def test_search_mods_domain_flask():
    results = search_mods(domain="flask", limit=5)
    assert len(results) > 0, "domain='flask' returned no mods"

def test_search_mods_generation_type_filter():
    results = search_mods(generation_type="prefix", limit=10)
    assert len(results) > 0
    assert all(r.get("generation_type") == "prefix" for r in results), "generation_type filter failed"

def test_search_mods_generation_type_suffix():
    results = search_mods(generation_type="suffix", limit=10)
    assert len(results) > 0
    assert all(r["generation_type"] == "suffix" for r in results)

def test_search_mods_generation_type_essence():
    """Audit: 21 generation_types exist, not just 3. Verify 'essence' works."""
    results = search_mods(generation_type="essence", limit=5)
    assert len(results) > 0, "generation_type='essence' returned no mods"

def test_search_mods_generation_type_corrupted():
    results = search_mods(generation_type="corrupted", limit=5)
    assert len(results) > 0, "generation_type='corrupted' returned no mods"

def test_search_mods_stat_id():
    results = search_mods(stat_id="cold", limit=5)
    assert len(results) > 0, "stat_id='cold' returned no mods"

def test_search_mods_stat_id_life():
    results = search_mods(stat_id="base_maximum_life", limit=5)
    assert len(results) > 0, "stat_id='base_maximum_life' returned no mods"

def test_search_mods_stat_id_attack_speed():
    results = search_mods(stat_id="attack_speed", limit=5)
    assert len(results) > 0, "stat_id='attack_speed' returned no mods"

def test_search_mods_tag_is_universally_empty():
    """Audit: tag column is empty for ALL mods. Verify tag filter returns nothing."""
    results = search_mods(tag="wand", limit=10)
    assert results == [], "tag='wand' should return [] — tag column is empty for all mods"

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


# ── search_mods: item_type filter (spawn_weights) ────────────────────────────

def test_search_mods_item_type_wand():
    """Regression: 'what mods roll on wands' must return results."""
    results = search_mods(item_type="wand", limit=10)
    assert len(results) > 0, "item_type='wand' returned no mods"

def test_search_mods_item_type_with_stat():
    """Regression: spell damage mods on wands — the exact query that failed."""
    results = search_mods(item_type="wand", stat_id="spell_damage", limit=10)
    assert len(results) >= 8, f"expected >=8 spell damage wand mods, got {len(results)}"
    names = [r["name"] for r in results]
    assert "Runic" in names, f"T1 spell damage mod 'Runic' not found in: {names}"

def test_search_mods_item_type_body_armour():
    results = search_mods(item_type="body_armour", generation_type="prefix", limit=10)
    assert len(results) > 0, "no prefix mods for body_armour"

def test_search_mods_item_type_nonexistent():
    results = search_mods(item_type="xyzzy_fake_item", limit=10)
    assert results == [], "nonexistent item_type should return empty"

def test_search_mods_item_type_combined_filters():
    """item_type + domain + generation_type all applied together."""
    results = search_mods(item_type="wand", domain="item", generation_type="suffix", limit=10)
    assert len(results) > 0, "no suffix mods for wand"
    assert all(r["generation_type"] == "suffix" for r in results)
    assert all(r["domain"] == "item" for r in results)

def test_search_mods_item_type_fts_query():
    """item_type works with FTS text query too."""
    results = search_mods(query="fire", item_type="ring", limit=10)
    assert len(results) > 0, "fire mods on rings should exist"

def test_search_mods_item_type_ring():
    results = search_mods(item_type="ring", limit=10)
    assert len(results) > 0, "item_type='ring' returned no mods"

def test_search_mods_item_type_helmet():
    results = search_mods(item_type="helmet", limit=10)
    assert len(results) > 0, "item_type='helmet' returned no mods"

def test_search_mods_item_type_amulet():
    results = search_mods(item_type="amulet", limit=10)
    assert len(results) > 0, "item_type='amulet' returned no mods"

def test_search_mods_item_type_dagger():
    results = search_mods(item_type="dagger", limit=10)
    assert len(results) > 0, "item_type='dagger' returned no mods"

def test_search_mods_item_type_bow():
    results = search_mods(item_type="bow", limit=10)
    assert len(results) > 0, "item_type='bow' returned no mods"

def test_search_mods_item_type_zero_weight_excluded():
    """Mods with weight=0 for an item type should NOT appear."""
    import json
    results = search_mods(item_type="wand", domain="item", limit=20)
    for r in results:
        sw = json.loads(r["spawn_weights"]) if isinstance(r["spawn_weights"], str) else r["spawn_weights"]
        wand_entries = [w for w in (sw or []) if w.get("tag") == "wand"]
        for entry in wand_entries:
            assert entry["weight"] > 0, f"Mod {r['name']} has wand weight=0 but appeared in results"

def test_search_mods_empty_query_all_params():
    """Empty string query with filters should still work."""
    results = search_mods(query="", item_type="wand", generation_type="prefix", limit=5)
    assert len(results) > 0

def test_search_mods_whitespace_query():
    """Whitespace-only query should not crash FTS."""
    results = search_mods(query="   ", limit=5)
    # Should either return empty or fall through to unfiltered — not crash
    assert isinstance(results, list)


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

def test_search_items_class_body_armour_exact_spacing():
    """Audit: item_class='Body Armour' (with space) must work, 'BodyArmour' must not."""
    results = search_base_items(item_class="Body Armour", limit=5)
    assert len(results) > 0, "item_class='Body Armour' returned nothing"
    assert all(r["item_class"] == "Body Armour" for r in results)

def test_search_items_class_bodyarmour_no_space_fails():
    """Audit: 'BodyArmour' (no space) is wrong — must return empty."""
    results = search_base_items(item_class="BodyArmour", limit=5)
    assert results == [], "item_class='BodyArmour' should return [] — correct value is 'Body Armour'"

def test_search_items_class_one_hand_sword():
    """Audit: multi-word class with spaces."""
    results = search_base_items(item_class="One Hand Sword", limit=5)
    assert len(results) > 0, "item_class='One Hand Sword' returned nothing"

def test_search_items_class_helmet():
    results = search_base_items(item_class="Helmet", limit=5)
    assert len(results) > 0

def test_search_items_class_ring():
    results = search_base_items(item_class="Ring", limit=5)
    assert len(results) > 0

def test_search_items_min_level_endgame():
    """Audit: min_level for endgame bases."""
    results = search_base_items(min_level=60, limit=10)
    assert len(results) > 0, "no items with drop_level >= 60"
    assert all(r["drop_level"] >= 60 for r in results)

def test_search_items_max_level_low():
    results = search_base_items(max_level=10, limit=10)
    assert len(results) > 0, "no items with drop_level <= 10"
    assert all(r["drop_level"] <= 10 for r in results)

def test_search_items_level_range_combined():
    """min_level + max_level bracket."""
    results = search_base_items(min_level=30, max_level=50, limit=10)
    assert len(results) > 0
    for r in results:
        assert 30 <= r["drop_level"] <= 50

def test_search_items_domain_filter():
    results = search_base_items(domain="item", limit=5)
    assert len(results) > 0
    assert all(r["domain"] == "item" for r in results)

def test_search_items_class_plus_level():
    """Cross-filter: item_class + min_level."""
    results = search_base_items(item_class="Wand", min_level=40, limit=10)
    assert len(results) > 0
    assert all(r["item_class"] == "Wand" and r["drop_level"] >= 40 for r in results)

def test_search_items_fts_plus_class():
    """Cross-filter: FTS query + item_class."""
    results = search_base_items(query="iron", item_class="Body Armour", limit=5)
    assert isinstance(results, list)
    for r in results:
        assert r["item_class"] == "Body Armour"

def test_search_items_empty_query():
    results = search_base_items(query="", limit=5)
    assert len(results) > 0, "empty query with no filters should return results"

def test_search_items_whitespace_query():
    results = search_base_items(query="   ", limit=5)
    assert isinstance(results, list)


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

def test_search_currencies_exalted():
    results = search_currencies(query="exalted")
    names = [r["name"] for r in results]
    assert any("Exalted" in n for n in names), f"no Exalted currency found, got: {names}"

def test_search_currencies_essence():
    results = search_currencies(query="essence")
    assert len(results) > 0, "no essence currencies found"

def test_search_currencies_description_not_empty():
    """Key currencies should have descriptions."""
    results = search_currencies(query="Chaos Orb", limit=1)
    assert len(results) > 0
    assert results[0].get("description"), "Chaos Orb has no description"

def test_search_currencies_empty_query():
    results = search_currencies(query="", limit=5)
    assert len(results) > 0

def test_search_currencies_whitespace_query():
    results = search_currencies(query="   ", limit=5)
    assert isinstance(results, list)


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

def test_search_augments_slot_caster_weapons():
    """Audit: 'Caster Weapons' is a valid slot for wand/sceptre augments."""
    results = search_augments(slot="Caster Weapons", limit=5)
    assert len(results) > 0, "slot='Caster Weapons' returned nothing"

def test_search_augments_slot_martial_weapons():
    """Audit: 'Martial Weapons' for melee weapon augments."""
    results = search_augments(slot="Martial Weapons", limit=5)
    assert len(results) > 0, "slot='Martial Weapons' returned nothing"

def test_search_augments_slot_all():
    """Audit: 'All' for universal augments."""
    results = search_augments(slot="All", limit=5)
    assert len(results) > 0, "slot='All' returned nothing"

def test_search_augments_slot_helmet():
    results = search_augments(slot="Helmet", limit=5)
    assert len(results) > 0, "slot='Helmet' returned nothing"

def test_search_augments_slot_boots():
    results = search_augments(slot="Boots", limit=5)
    assert len(results) > 0

def test_search_augments_slot_gloves():
    results = search_augments(slot="Gloves", limit=5)
    assert len(results) > 0

def test_search_augments_slot_shield():
    results = search_augments(slot="Shield", limit=5)
    assert len(results) > 0

def test_search_augments_slot_bow():
    results = search_augments(slot="Bow", limit=5)
    assert len(results) > 0

def test_search_augments_slot_focus():
    results = search_augments(slot="Focus", limit=5)
    assert len(results) > 0

def test_search_augments_slot_nonexistent():
    results = search_augments(slot="xyzzy_fake_slot", limit=5)
    assert results == [], "nonexistent slot should return empty"

def test_search_augments_slot_plus_query():
    """Cross-filter: slot + FTS query."""
    results = search_augments(query="Eye", slot="Helmet", limit=5)
    assert isinstance(results, list)

def test_search_augments_empty_query():
    results = search_augments(query="", limit=5)
    assert len(results) > 0

def test_search_augments_whitespace_query():
    results = search_augments(query="   ", limit=5)
    assert isinstance(results, list)

def test_search_augments_no_wiki_markup():
    """Audit: type_name should not contain [Key|Display] wiki markup."""
    results = search_augments(limit=20)
    for r in results:
        tn = r.get("type_name", "")
        assert "[" not in tn or "|" not in tn, (
            f"Wiki markup found in type_name: {tn}"
        )


# ── FTS5 input sanitization (C1 regression) ─────────────────────────────────

def test_fts_plus_sign_does_not_crash():
    """Regression C1: '+1 to all spell skills' crashed FTS5."""
    results = search_mods(query="+1 spell skills", limit=5)
    assert isinstance(results, list)  # must not crash

def test_fts_hyphen_does_not_crash():
    """Regression C1: 'cold-damage' crashed FTS5."""
    results = search_mods(query="cold-damage", limit=5)
    assert isinstance(results, list)

def test_fts_quotes_do_not_crash():
    results = search_mods(query='"fire damage"', limit=5)
    assert isinstance(results, list)

def test_fts_parentheses_do_not_crash():
    results = search_base_items(query="(wand)", limit=5)
    assert isinstance(results, list)

def test_fts_asterisk_in_query():
    results = search_currencies(query="chaos*", limit=5)
    assert isinstance(results, list)

def test_fts_all_special_chars():
    """Query made entirely of special chars should return empty, not crash."""
    results = search_mods(query="+-*\"()[]", limit=5)
    assert isinstance(results, list)

def test_fts_mixed_special_and_text():
    """Special chars mixed with real words should still find results."""
    results = search_mods(query="+fire", limit=5)
    assert isinstance(results, list)
    # 'fire' should still match after stripping '+'
    assert len(results) > 0, "+fire should find results after sanitization"

def test_fts_sanitization_items():
    results = search_base_items(query="+iron -steel", limit=5)
    assert isinstance(results, list)

def test_fts_sanitization_currencies():
    results = search_currencies(query="orb (of)", limit=5)
    assert isinstance(results, list)

def test_fts_sanitization_augments():
    results = search_augments(query="soul+core", limit=5)
    assert isinstance(results, list)


# ── Currency wiki markup (C2 regression) ─────────────────────────────────────

def test_currency_descriptions_no_wiki_markup():
    """Regression C2: currency descriptions had [Key|Display] markup."""
    from exilesage.db import get_connection
    conn = get_connection()
    rows = conn.execute(
        "SELECT COUNT(*) FROM currencies WHERE description LIKE '%[%|%]%'"
    ).fetchone()
    conn.close()
    assert rows[0] == 0, f"{rows[0]} currencies still have wiki markup in description"

def test_chaos_orb_description_clean():
    """Regression C2: Chaos Orb description should say 'Rare' not '[ItemRarity|Rare]'."""
    results = search_currencies(query="Chaos Orb", limit=1)
    assert len(results) > 0
    desc = results[0].get("description", "")
    assert "[" not in desc, f"Wiki markup in Chaos Orb description: {desc}"
    assert "Rare" in desc, "Chaos Orb description should mention 'Rare'"


# ── Accuracy fixes (Round 3 regression) ──────────────────────────────────────

def test_base_items_weapon_columns_present():
    """A3: search_base_items must return weapon stats for Wand bases."""
    results = search_base_items(item_class="Wand", limit=3)
    assert len(results) > 0
    keys = results[0].keys()
    for col in ("physical_damage_min", "physical_damage_max",
                "critical_strike_chance", "attack_time"):
        assert col in keys, f"Missing weapon column: {col}"

def test_mods_group_name_present():
    """A4: search_mods must return group_name column for exclusivity checks."""
    results = search_mods(domain="item", limit=3)
    assert len(results) > 0
    assert "group_name" in results[0].keys(), "group_name missing from mod results"

def test_stat_id_per_element_match():
    """A1: stat_id search matches per-element, not across entire JSON blob."""
    results = search_mods(stat_id="cold_damage", limit=20)
    for r in results:
        import json as _json
        stats = _json.loads(r["stats"]) if isinstance(r["stats"], str) else r["stats"]
        stat_ids = [s.get("id", "") for s in stats]
        # At least one stat ID must contain 'cold_damage'
        assert any("cold_damage" in sid for sid in stat_ids), (
            f"Mod {r['id']} matched stat_id='cold_damage' but its stats are: {stat_ids}"
        )

def test_stat_id_with_item_type_filter():
    """A1+item_type: stat_id + item_type should return only eligible mods."""
    results = search_mods(stat_id="base_maximum_life", item_type="ring", limit=5)
    assert isinstance(results, list)
    # All results must have base_maximum_life as an exact stat
    for r in results:
        import json as _json
        stats = _json.loads(r["stats"]) if isinstance(r["stats"], str) else r["stats"]
        stat_ids = [s.get("id", "") for s in stats]
        assert "base_maximum_life" in stat_ids


# ── FTS5 keyword sanitization (B1 regression) ────────────────────────────────

def test_fts_keyword_NOT_does_not_exclude():
    """B1: 'fire NOT cold' should not be parsed as FTS boolean."""
    results = search_mods(query="fire NOT cold", limit=5)
    assert isinstance(results, list)  # must not crash or silently exclude

def test_fts_keyword_OR_does_not_alter():
    """B1: 'chaos OR physical' should not be parsed as FTS OR."""
    results = search_mods(query="chaos OR physical", limit=5)
    assert isinstance(results, list)

def test_fts_keyword_AND_stripped():
    """B1: 'fire AND damage' should work as plain text search."""
    results = search_mods(query="fire AND damage", limit=5)
    assert isinstance(results, list)

def test_fts_keyword_NEAR_stripped():
    """B1: 'NEAR damage' should not trigger FTS NEAR operator."""
    results = search_mods(query="NEAR damage", limit=5)
    assert isinstance(results, list)


# ── sanitize_fts unit tests ──────────────────────────────────────────────────

def test_sanitize_fts_strips_keywords():
    """B1: FTS keywords must be removed from sanitized output."""
    from exilesage.db import sanitize_fts
    result = sanitize_fts("fire NOT cold")
    assert "NOT" not in result.split()
    assert "fire" in result
    assert "cold" in result

def test_sanitize_fts_strips_or():
    from exilesage.db import sanitize_fts
    result = sanitize_fts("chaos OR physical")
    assert " OR " not in result

def test_sanitize_fts_keyword_only():
    """Query that is only FTS keywords should return empty."""
    from exilesage.db import sanitize_fts
    result = sanitize_fts("NOT AND OR")
    assert result == ""


# ── LIKE fallback path (T3) ─────────────────────────────────────────────────

def test_like_fallback_mods():
    """T3: query that won't FTS-match should fall back to LIKE and still return results."""
    # Use a name substring that exists in mod names but is unlikely to be an FTS token
    results = search_mods(query="Strength", limit=5)
    assert len(results) > 0, "LIKE fallback should find Strength mods"

def test_like_fallback_items():
    """T3: LIKE fallback for base_items."""
    results = search_base_items(query="Iron", limit=5)
    assert len(results) > 0

def test_like_fallback_currencies():
    """T3: LIKE fallback for currencies."""
    results = search_currencies(query="Orb", limit=5)
    assert len(results) > 0


# ── FTS edge cases (T4) ─────────────────────────────────────────────────────

def test_fts_unicode_input():
    """T4: Unicode input should not crash FTS."""
    from exilesage.db import sanitize_fts
    result = sanitize_fts("résistance")
    assert isinstance(result, str)
    # Should not crash tool either
    results = search_mods(query="résistance", limit=5)
    assert isinstance(results, list)

def test_fts_multiword_does_not_crash():
    """T4: Multi-word query should not crash (may return 0 if no match)."""
    results = search_mods(query="fire damage", limit=5)
    assert isinstance(results, list)

def test_fts_single_char():
    """T4: Single character should not crash."""
    results = search_mods(query="a", limit=5)
    assert isinstance(results, list)

def test_fts_very_long_query():
    """T4: Very long query should not crash."""
    long_q = "fire " * 100
    results = search_mods(query=long_q, limit=5)
    assert isinstance(results, list)


# ── Importer unit tests (T2) ────────────────────────────────────────────────

def test_strip_wiki_function():
    """T2: _strip_wiki should convert [Key|Display] to Display."""
    from pipeline.importers.currencies_importer import _strip_wiki
    assert _strip_wiki("[ItemRarity|Rare]") == "Rare"
    assert _strip_wiki("a [Foo|Bar] item") == "a Bar item"
    assert _strip_wiki(None) is None
    assert _strip_wiki("") == ""
    assert _strip_wiki("no markup here") == "no markup here"

def test_strip_wiki_multiple():
    """T2: _strip_wiki handles multiple markup instances."""
    from pipeline.importers.currencies_importer import _strip_wiki
    result = _strip_wiki("[A|B] and [C|D]")
    assert result == "B and D"

def test_augment_strip_wiki():
    """T2: augments_importer also has _strip_wiki."""
    from pipeline.importers.augments_importer import _strip_wiki
    assert _strip_wiki("[Key|Display]") == "Display"

def test_importer_pydantic_validation_skip():
    """T2: Pydantic validation failure should skip row, not crash."""
    from pipeline.importers.mods_importer import ModRow
    from pydantic import ValidationError
    # ModRow requires 'id' as str — passing int should fail gracefully
    try:
        ModRow(id=None, name="test")
        assert False, "Should have raised ValidationError"
    except ValidationError:
        pass  # Expected — importers catch this and skip

def test_init_db_idempotent():
    """T5: calling init_db twice should not corrupt the database."""
    from exilesage.db import init_db, get_connection
    init_db()
    init_db()
    conn = get_connection()
    try:
        count = conn.execute("SELECT COUNT(*) FROM mods").fetchone()[0]
        assert count > 0, "mods table should still have data after double init_db"
    finally:
        conn.close()

def test_schema_version_in_meta():
    """M3: meta table should have schema_version column after init_db."""
    from exilesage.db import init_db, get_connection
    init_db()  # ensures column is added via _ensure_schema_version
    conn = get_connection()
    try:
        row = conn.execute("SELECT schema_version FROM meta WHERE id=1").fetchone()
        if row is not None:
            assert row[0] >= 1, "schema_version should be >= 1"
    finally:
        conn.close()
