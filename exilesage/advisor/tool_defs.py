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
            "Search PoE2 modifiers/affixes by keyword, domain, generation type, "
            "or stat ID. Use this to find what mods can appear on items, their "
            "value ranges, and spawn weights."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Keyword to search mod names and types",
                },
                "domain": {
                    "type": "string",
                    "description": "Filter by domain: item, flask, jewel, atlas",
                },
                "generation_type": {
                    "type": "string",
                    "description": "Filter: prefix, suffix, unique_component",
                },
                "tag": {
                    "type": "string",
                    "description": "Filter by item tag e.g. wand, ring, str_armour",
                },
                "stat_id": {
                    "type": "string",
                    "description": "Search by stat ID substring e.g. 'cold_damage'",
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
            "bases, their defence values, requirements, and implicit mods."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Item name keyword",
                },
                "item_class": {
                    "type": "string",
                    "description": "e.g. Wand, BodyArmour, Helmet, Ring",
                },
                "domain": {"type": "string"},
                "min_level": {"type": "integer"},
                "max_level": {"type": "integer"},
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
                    "description": "Currency name or effect keyword",
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
            "slot bonuses."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Augment name keyword",
                },
                "slot": {
                    "type": "string",
                    "description": "Equipment slot e.g. Body Armour, Boots, Helmet",
                },
                "limit": {"type": "integer", "default": 10},
            },
            "required": [],
        },
    },
]
