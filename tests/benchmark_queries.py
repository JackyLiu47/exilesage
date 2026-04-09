"""
Stage 1 acceptance queries — 8/10 must return grounded, tool-backed answers.
Run: python -m pytest tests/test_advisor.py -v
"""

BENCHMARK_QUERIES = [
    # Factual — Haiku
    {
        "id": "BQ01",
        "query": "What does a Chaos Orb do?",
        "type": "factual",
        "must_use_tool": True,
        "must_mention": ["chaos", "explicit"],
    },
    {
        "id": "BQ02",
        "query": "What is the maximum life roll on a body armour?",
        "type": "factual",
        "must_use_tool": True,
        "must_mention": ["life", "maximum"],
    },
    {
        "id": "BQ03",
        "query": "What is the difference between a prefix and a suffix?",
        "type": "factual",
        "must_use_tool": False,  # domain knowledge in system prompt
        "must_mention": ["prefix", "suffix"],
    },
    # Crafting — Sonnet
    {
        "id": "BQ04",
        "query": "What mods can spawn on a wand?",
        "type": "crafting",
        "must_use_tool": True,
        "must_mention": ["wand"],
    },
    {
        "id": "BQ05",
        "query": "Which currencies reroll explicit modifiers?",
        "type": "crafting",
        "must_use_tool": True,
        "must_mention": ["chaos", "explicit"],
    },
    {
        "id": "BQ06",
        "query": "How do essences work in crafting?",
        "type": "crafting",
        "must_use_tool": True,
        "must_mention": ["essence"],
    },
    {
        "id": "BQ07",
        "query": "What base items can roll movement speed as an implicit?",
        "type": "crafting",
        "must_use_tool": True,
        "must_mention": ["movement"],
    },
    # Analysis — Sonnet
    {
        "id": "BQ08",
        "query": "What augments work with cold damage builds?",
        "type": "analysis",
        "must_use_tool": True,
        "must_mention": ["cold"],
    },
    {
        "id": "BQ09",
        "query": "What is the best base for an armour and evasion hybrid chest?",
        "type": "analysis",
        "must_use_tool": True,
        "must_mention": ["armour", "evasion"],
    },
    # Guide — Sonnet
    {
        "id": "BQ10",
        "query": "How do I craft a +1 to all spell skills wand?",
        "type": "guide",
        "must_use_tool": True,
        "must_mention": ["wand", "spell"],
    },
]
