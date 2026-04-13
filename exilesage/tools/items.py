"""Tool: search_base_items — query the base_items table for ExileSage."""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from exilesage.db import get_connection, sanitize_fts
from exilesage.config import MAX_RESULTS

log = logging.getLogger(__name__)

_SELECT = (
    "SELECT id, name, item_class, domain, drop_level, "
    "tags, implicits, armour, evasion, energy_shield, "
    "physical_damage_min, physical_damage_max, "
    "critical_strike_chance, attack_time FROM base_items"
)


def search_base_items(
    query: str = "",
    item_class: str | None = None,
    domain: str | None = None,
    min_level: int | None = None,
    max_level: int | None = None,
    limit: int = 10,
) -> list[dict]:
    """Search the base_items table and return a list of matching item dicts.

    Parameters
    ----------
    query:      Free-text search against base_items_fts (name, item_class, domain).
    item_class: Exact match filter on the item_class column (e.g. "Body Armour").
    domain:     Exact match filter on the domain column (e.g. "item").
    min_level:  Minimum drop_level (inclusive).
    max_level:  Maximum drop_level (inclusive).
    limit:      Maximum rows to return (capped at MAX_RESULTS).
    """
    conn = None
    try:
        limit = min(limit, MAX_RESULTS)
        conn = get_connection()

        if query:
            rows = _search_fts(conn, query, item_class, domain, min_level, max_level, limit)
            if not rows:
                log.debug("FTS returned 0 results for %r — falling back to LIKE", query)
                rows = _search_like(conn, query, item_class, domain, min_level, max_level, limit)
        else:
            rows = _search_filtered(conn, item_class, domain, min_level, max_level, limit)

        return [dict(r) for r in rows]

    except Exception:
        log.warning("search_base_items failed", exc_info=True)
        return []
    finally:
        if conn:
            conn.close()


# ── Internal helpers ──────────────────────────────────────────────────────────

def _where_clauses(
    item_class: str | None,
    domain: str | None,
    min_level: int | None,
    max_level: int | None,
) -> tuple[list[str], list]:
    clauses: list[str] = []
    params: list = []
    if item_class:
        clauses.append("item_class = ?")
        params.append(item_class)
    if domain:
        clauses.append("domain = ?")
        params.append(domain)
    if min_level is not None:
        clauses.append("drop_level >= ?")
        params.append(min_level)
    if max_level is not None:
        clauses.append("drop_level <= ?")
        params.append(max_level)
    return clauses, params


def _search_fts(conn, query, item_class, domain, min_level, max_level, limit):
    if not query or not query.strip():
        return []
    fts_term = sanitize_fts(query)
    if not fts_term:
        return []
    extra_clauses, extra_params = _where_clauses(item_class, domain, min_level, max_level)

    base_sql = (
        f"{_SELECT} WHERE rowid IN "
        "(SELECT rowid FROM base_items_fts WHERE base_items_fts MATCH ?) "
    )
    params: list = [fts_term]

    if extra_clauses:
        base_sql += "AND " + " AND ".join(extra_clauses) + " "
        params.extend(extra_params)

    base_sql += "ORDER BY rowid LIMIT ?"
    params.append(limit)

    return conn.execute(base_sql, params).fetchall()


def _search_like(conn, query, item_class, domain, min_level, max_level, limit):
    like_val = f"%{query}%"
    extra_clauses, extra_params = _where_clauses(item_class, domain, min_level, max_level)

    base_sql = f"{_SELECT} WHERE (name LIKE ? OR item_class LIKE ?) "
    params: list = [like_val, like_val]

    if extra_clauses:
        base_sql += "AND " + " AND ".join(extra_clauses) + " "
        params.extend(extra_params)

    base_sql += "LIMIT ?"
    params.append(limit)

    return conn.execute(base_sql, params).fetchall()


def _search_filtered(conn, item_class, domain, min_level, max_level, limit):
    extra_clauses, extra_params = _where_clauses(item_class, domain, min_level, max_level)

    if extra_clauses:
        sql = f"{_SELECT} WHERE " + " AND ".join(extra_clauses) + " LIMIT ?"
        extra_params.append(limit)
        return conn.execute(sql, extra_params).fetchall()

    return conn.execute(f"{_SELECT} LIMIT ?", [limit]).fetchall()


# ── Standalone test ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import pprint
    results = search_base_items(query="iron", item_class="Body Armour", limit=5)
    pprint.pprint(results)
    print(f"\n{len(results)} result(s)")
