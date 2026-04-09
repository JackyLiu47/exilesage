"""Tool: search_augments — query the augments table for ExileSage."""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from exilesage.db import get_connection
from exilesage.config import MAX_RESULTS

log = logging.getLogger(__name__)

_SELECT = "SELECT id, type_id, type_name, required_level, categories FROM augments"


def search_augments(
    query: str = "",
    slot: str | None = None,
    limit: int = 10,
) -> list[dict]:
    """Search the augments table and return a list of matching augment dicts.

    Parameters
    ----------
    query: Free-text search against augments_fts (type_id, type_name).
    slot:  Filter augments whose categories JSON object contains this slot key
           (e.g. "helmet", "body_armour", "weapon").
    limit: Maximum rows to return (capped at MAX_RESULTS).
    """
    conn = None
    try:
        limit = min(limit, MAX_RESULTS)
        conn = get_connection()

        if query:
            rows = _search_fts(conn, query, slot, limit)
            if not rows:
                log.debug("FTS returned 0 results for %r — falling back to LIKE", query)
                rows = _search_like(conn, query, slot, limit)
        else:
            rows = _search_filtered(conn, slot, limit)

        return [dict(r) for r in rows]

    except Exception:
        log.warning("search_augments failed", exc_info=True)
        return []
    finally:
        if conn:
            conn.close()


# ── Internal helpers ──────────────────────────────────────────────────────────

def _slot_clause(slot: str | None) -> tuple[str, list]:
    """Return a SQL fragment and params for the slot filter, if any."""
    if slot:
        # categories is a JSON object; check that the key exists
        sql = "json_extract(categories, '$.' || ?) IS NOT NULL"
        return sql, [slot]
    return "", []


def _search_fts(conn, query: str, slot: str | None, limit: int):
    if not query or not query.strip():
        return []
    fts_term = query.strip() + "*"
    slot_sql, slot_params = _slot_clause(slot)

    base_sql = (
        f"{_SELECT} WHERE rowid IN "
        "(SELECT rowid FROM augments_fts WHERE augments_fts MATCH ?) "
    )
    params: list = [fts_term]

    if slot_sql:
        base_sql += f"AND {slot_sql} "
        params.extend(slot_params)

    base_sql += "ORDER BY rowid LIMIT ?"
    params.append(limit)

    return conn.execute(base_sql, params).fetchall()


def _search_like(conn, query: str, slot: str | None, limit: int):
    like_val = f"%{query}%"
    slot_sql, slot_params = _slot_clause(slot)

    base_sql = f"{_SELECT} WHERE (type_id LIKE ? OR type_name LIKE ?) "
    params: list = [like_val, like_val]

    if slot_sql:
        base_sql += f"AND {slot_sql} "
        params.extend(slot_params)

    base_sql += "LIMIT ?"
    params.append(limit)

    return conn.execute(base_sql, params).fetchall()


def _search_filtered(conn, slot: str | None, limit: int):
    slot_sql, slot_params = _slot_clause(slot)

    if slot_sql:
        sql = f"{_SELECT} WHERE {slot_sql} LIMIT ?"
        slot_params.append(limit)
        return conn.execute(sql, slot_params).fetchall()

    return conn.execute(f"{_SELECT} LIMIT ?", [limit]).fetchall()


# ── Standalone test ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import pprint
    results = search_augments(query="essence", limit=5)
    pprint.pprint(results)
    print(f"\n{len(results)} result(s)")
