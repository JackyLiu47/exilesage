"""
Transforms raw repoe JSON into clean, typed, domain-focused data structures.
Outputs go to data/processed/ as separate focused JSON files.
"""

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Base Items
# ---------------------------------------------------------------------------

def process_base_items(raw: dict, item_classes: dict) -> dict:
    """
    Extract crafting-relevant base items (weapons, armour, jewellery).
    Filters out internal/unreleased items.
    """
    CRAFTING_DOMAINS = {"item", "flask", "jewel", "abyss_jewel", "sanctum_relic", "undefined"}
    SKIP_CLASSES = {"StackableCurrency", "HideoutDoodad", "Microtransaction",
                    "QuestItem", "DivinationCard", "LabyrinthTrinket"}

    out = {}
    for item_id, item in raw.items():
        if item.get("release_state") != "released":
            continue
        item_class = item.get("item_class", "")
        if item_class in SKIP_CLASSES:
            continue

        props = item.get("properties") or {}
        out[item_id] = {
            "id": item_id,
            "name": item["name"],
            "item_class": item_class,
            "domain": item.get("domain"),
            "drop_level": item.get("drop_level"),
            "tags": item.get("tags", []),
            "implicits": item.get("implicits", []),
            "requirements": item.get("requirements"),
            # Defences
            "armour": props.get("armour"),
            "evasion": props.get("evasion"),
            "energy_shield": props.get("energy_shield"),
            # Weapon
            "physical_damage_min": props.get("physical_damage_min"),
            "physical_damage_max": props.get("physical_damage_max"),
            "critical_strike_chance": props.get("critical_strike_chance"),
            "attack_time": props.get("attack_time"),
            "range": props.get("range"),
            # Flask
            "charges_max": props.get("charges_max"),
            "charges_per_use": props.get("charges_per_use"),
            "duration": props.get("duration"),
            "life_per_use": props.get("life_per_use"),
            "mana_per_use": props.get("mana_per_use"),
            # Stack (currency)
            "stack_size": props.get("stack_size"),
        }
    return out


# ---------------------------------------------------------------------------
# Currencies (extracted from base_items)
# ---------------------------------------------------------------------------

def process_currencies(raw_base_items: dict) -> dict:
    """Pull all StackableCurrency items — orbs, essences, fragments, etc."""
    out = {}
    for item_id, item in raw_base_items.items():
        if item.get("release_state") != "released":
            continue
        if item.get("item_class") != "StackableCurrency":
            continue

        props = item.get("properties") or {}
        out[item_id] = {
            "id": item_id,
            "name": item["name"],
            "tags": item.get("tags", []),
            "drop_level": item.get("drop_level"),
            "stack_size": props.get("stack_size"),
            "stack_size_currency_tab": props.get("stack_size_currency_tab"),
            "full_stack_turns_into": props.get("full_stack_turns_into"),
            "description": props.get("description"),
            "inherits_from": item.get("inherits_from"),
        }
    return out


# ---------------------------------------------------------------------------
# Mods / Affixes
# ---------------------------------------------------------------------------

def process_mods(raw: dict) -> dict:
    """
    Clean mod data. Keeps all fields relevant to crafting:
    generation_type, domain, spawn_weights, stats, groups, is_essence_only, etc.
    """
    out = {}
    for mod_id, mod in raw.items():
        out[mod_id] = {
            "id": mod_id,
            "name": mod.get("name", ""),
            "generation_type": mod.get("generation_type"),   # prefix / suffix / unique etc.
            "domain": mod.get("domain"),                     # item / flask / jewel etc.
            "group": mod.get("group"),
            "type": mod.get("type"),
            "required_level": mod.get("required_level", 0),
            "is_essence_only": mod.get("is_essence_only", False),
            "tags": mod.get("tags", []),
            "spawn_weights": mod.get("spawn_weights", []),   # [{tag, weight}]
            "generation_weights": mod.get("generation_weights", []),
            "grants_effects": mod.get("grants_effects", []),
            "stats": [
                {
                    "id": s.get("id"),
                    "min": s.get("min"),
                    "max": s.get("max"),
                }
                for s in mod.get("stats", [])
            ],
            "adds_tags": mod.get("adds_tags", []),
            "implicit_tags": mod.get("implicit_tags", []),
        }
    return out


# ---------------------------------------------------------------------------
# Augments (Soul Cores, Runes, socketables)
# ---------------------------------------------------------------------------

def process_augments(raw: dict) -> dict:
    """Clean augment/socketable data (Soul Cores, Runes, Talismans)."""
    out = {}
    for aug_id, aug in raw.items():
        out[aug_id] = {
            "id": aug_id,
            "type_id": aug.get("type_id"),
            "type_name": aug.get("type_name"),
            "required_level": aug.get("required_level"),
            "limit": aug.get("limit"),
            "categories": aug.get("categories", {}),  # slot → {stat_text, stats, target}
        }
    return out


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def process_all(raw_data: dict[str, dict | list], processed_dir: Path) -> None:
    """
    Run all processors on raw data and write results to processed_dir.

    Args:
        raw_data:      Dict of {file_key: raw_json} from the fetcher.
        processed_dir: Output directory for processed JSON files.
    """
    processed_dir.mkdir(parents=True, exist_ok=True)

    processors = {
        "currencies":  lambda d: process_currencies(d.get("base_items", {})),
        "base_items":  lambda d: process_base_items(d.get("base_items", {}), d.get("item_classes", {})),
        "mods":        lambda d: process_mods(d.get("mods", {})),
        "augments":    lambda d: process_augments(d.get("augments", {})),
    }

    for name, fn in processors.items():
        if not any(k in raw_data for k in ["base_items", "mods", "augments", "item_classes"]):
            continue
        try:
            result = fn(raw_data)
            if result is None:
                continue
            out_path = processed_dir / f"{name}.json"
            out_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
            logger.info(f"[processed] {name} → {len(result)} entries → {out_path}")
        except Exception as e:
            logger.error(f"Failed to process {name}: {e}", exc_info=True)
