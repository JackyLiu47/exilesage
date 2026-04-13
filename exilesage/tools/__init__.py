"""ExileSage tool registry — single source of truth for tool dispatch."""

from exilesage.tools.mods import search_mods
from exilesage.tools.items import search_base_items
from exilesage.tools.currencies import search_currencies
from exilesage.tools.augments import search_augments

# Auto-built dispatch table. core.py imports this instead of maintaining its own.
# To add a new tool: (1) create the module, (2) import + add here, (3) add schema to tool_defs.py.
TOOL_DISPATCH: dict[str, callable] = {
    "search_mods": search_mods,
    "search_base_items": search_base_items,
    "search_currencies": search_currencies,
    "search_augments": search_augments,
}
