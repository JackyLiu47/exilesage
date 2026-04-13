"""
Anthropic API tool schemas for the ExileSage advisor.

These schemas describe the 4 search tools the advisor can call. The actual
Python implementations live in `exilesage.tools.*` and are dispatched from
`exilesage.advisor.core.execute_tool`.
"""

TOOL_DEFINITIONS: list[dict] = [
    {
        "name": "search_mods",
        "description": (
            "Search PoE2 modifiers/affixes by keyword, stat ID, or item type. "
            "Use this to find what mods can appear on items, their value ranges, "
            "and spawn weights. Use item_type to find mods eligible for a "
            "specific item type (e.g. wand, ring)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Keyword to search mod names and types (FTS)",
                },
                "domain": {
                    "type": "string",
                    "description": (
                        "Filter by mod domain. Common values: item (regular gear), "
                        "flask, crafted, atlas, chest (strongbox), monster, "
                        "sanctum_relic, expedition_relic. Use 'item' for normal "
                        "gear mods."
                    ),
                },
                "generation_type": {
                    "type": "string",
                    "description": (
                        "Filter by mod type. Common values: prefix, suffix "
                        "(the two main craftable types), corrupted, essence, "
                        "unique (unique item mods), talisman."
                    ),
                },
                "stat_id": {
                    "type": "string",
                    "description": (
                        "Search by stat ID substring in the stats JSON. "
                        "Examples: 'spell_damage', 'cold_damage', "
                        "'base_maximum_life', 'attack_speed', "
                        "'critical_strike_chance', 'fire_damage_resistance'"
                    ),
                },
                "item_type": {
                    "type": "string",
                    "description": (
                        "Filter mods that can spawn on this item type. "
                        "Uses spawn_weights (weight > 0). THIS is how you find "
                        "'what mods roll on wands' — do NOT use tag for this. "
                        "Values: wand, dagger, claw, sword, axe, mace, spear, "
                        "flail, staff, warstaff, bow, crossbow, sceptre, "
                        "ring, amulet, belt, quiver, gloves, boots, helmet, "
                        "body_armour, shield, focus, "
                        "str_armour, dex_armour, int_armour, "
                        "str_dex_armour, str_int_armour, dex_int_armour, "
                        "str_dex_int_armour, talisman, jewel"
                    ),
                },
                "limit": {"type": "integer", "default": 10},
            },
            "required": [],
        },
    },
    {
        "name": "search_base_items",
        "description": (
            "Search PoE2 base item types by name or class. Use to find item "
            "bases, their defence values, requirements, and implicit mods. "
            "Use item_class with EXACT casing and spacing as listed."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Item name keyword (FTS search)",
                },
                "item_class": {
                    "type": "string",
                    "description": (
                        "Exact item class name. MUST match exactly including "
                        "spaces and casing. Common values: Wand, Dagger, Claw, "
                        "'One Hand Sword', 'Two Hand Sword', 'One Hand Axe', "
                        "'Two Hand Axe', 'One Hand Mace', 'Two Hand Mace', "
                        "Spear, Flail, Staff, Warstaff, Bow, Crossbow, Sceptre, "
                        "'Body Armour', Helmet, Gloves, Boots, Shield, Buckler, "
                        "Focus, Ring, Amulet, Belt, Quiver, Jewel, "
                        "LifeFlask, ManaFlask, UtilityFlask, "
                        "'Active Skill Gem', 'Support Skill Gem', SoulCore, "
                        "Talisman, Map"
                    ),
                },
                "domain": {
                    "type": "string",
                    "description": (
                        "Filter by domain. Usually 'item' for gear. "
                        "Other values: flask, misc, tablet"
                    ),
                },
                "min_level": {
                    "type": "integer",
                    "description": (
                        "Minimum drop level (inclusive). Range is 1-84. "
                        "Use to find endgame bases (e.g. min_level=60)."
                    ),
                },
                "max_level": {
                    "type": "integer",
                    "description": (
                        "Maximum drop level (inclusive). Range is 1-84. "
                        "Combine with min_level for a level bracket."
                    ),
                },
                "limit": {"type": "integer", "default": 10},
            },
            "required": [],
        },
    },
    {
        "name": "search_currencies",
        "description": (
            "Search PoE2 currency items and their effects. Use to explain what "
            "a currency does or find currencies relevant to a crafting method."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Currency name or effect keyword (FTS search)",
                },
                "limit": {"type": "integer", "default": 10},
            },
            "required": [],
        },
    },
    {
        "name": "search_augments",
        "description": (
            "Search PoE2 augments (Soul Cores, Runes, Talismans) and their "
            "slot bonuses. Use slot to filter by equipment category."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Augment name keyword (FTS search on type_id and type_name)",
                },
                "slot": {
                    "type": "string",
                    "description": (
                        "Equipment slot/category key from the augment's categories "
                        "JSON. Valid values: 'Body Armour', 'Helmet', 'Gloves', "
                        "'Boots', 'Armour' (any armour piece), 'Shield', "
                        "'Caster Weapons' (wand, sceptre), "
                        "'Martial Weapons' (sword, axe, mace, etc.), "
                        "'Bow', 'Sceptre', 'Focus', 'All' (universal augments)"
                    ),
                },
                "limit": {"type": "integer", "default": 10},
            },
            "required": [],
        },
    },
]
