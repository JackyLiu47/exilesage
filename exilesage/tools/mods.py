"""Tool: search_mods — query the mods table for ExileSage."""

import sys
import json
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from exilesage.db import get_connection
from exilesage.config import MAX_RESULTS

log = logging.getLogger(__name__)

_SELECT = (
    "SELECT rowid, id, name, type, domain, generation_type, "
    "required_level, stats, spawn_weights, tags FROM mods"
)


def search_mods(
    query: str = "",
    domain: str | None = None,
    generation_type: str | None = None,
    tag: str | None = None,
    stat_id: str | None = None,
    limit: int = 10,
) -> list[dict]:
    """Search the mods table and return a list of matching mod dicts.

    Parameters
    ----------
    query:           Free-text search against mods_fts (name, group_name, type, domain, generation_type).
    domain:          Exact match filter on the domain column (e.g. "item", "flask").
    generation_type: Exact match filter on generation_type (e.g. "prefix", "suffix").
    tag:             Filter mods whose tags JSON array contains this value.
    stat_id:         Filter mods that reference this stat id anywhere in the stats JSON array.
    limit:           Maximum rows to return (capped at MAX_RESULTS).
    """
    conn = None
    try:
        limit = min(limit, MAX_RESULTS)
        conn = get_connection()
        rows: list = []

        if stat_id:
            rows = _search_by_stat_id(conn, stat_id, domain, generation_type, tag, limit)
        elif query:
            rows = _search_fts(conn, query, domain, generation_type, tag, limit)
            if not rows:
                log.debug("FTS returned 0 results for %r — falling back to LIKE", query)
                rows = _search_like(conn, query, domain, generation_type, tag, limit)
        else:
            rows = _search_filtered(conn, domain, generation_type, tag, limit)

        return [dict(r) for r in rows]

    except Exception:
        log.warning("search_mods failed", exc_info=True)
        return []
    finally:
        if conn:
            conn.close()


# ── Internal helpers ──────────────────────────────────────────────────────────

def _where_clauses(
    domain: str | None,
    generation_type: str | None,
    tag: str | None,
    prefix: str = "",
) -> tuple[list[str], list]:
    """Build extra WHERE clauses and param list for optional filters."""
    clauses: list[str] = []
    params: list = []
    tbl = f"{prefix}." if prefix else ""
    if domain:
        clauses.append(f"{tbl}domain = ?")
        params.append(domain)
    if generation_type:
        clauses.append(f"{tbl}generation_type = ?")
        params.append(generation_type)
    if tag:
        clauses.append(f"EXISTS (SELECT 1 FROM json_each({tbl}tags) WHERE value = ?)")
        params.append(tag)
    return clauses, params


def _search_fts(conn, query, domain, generation_type, tag, limit):
    if not query or not query.strip():
        return []
    fts_term = query.strip() + "*"
    extra_clauses, extra_params = _where_clauses(domain, generation_type, tag)

    base_sql = (
        f"{_SELECT} WHERE rowid IN "
        "(SELECT rowid FROM mods_fts WHERE mods_fts MATCH ?) "
    )
    params: list = [fts_term]

    if extra_clauses:
        base_sql += "AND " + " AND ".join(extra_clauses) + " "
        params.extend(extra_params)

    base_sql += "ORDER BY rowid LIMIT ?"
    params.append(limit)

    return conn.execute(base_sql, params).fetchall()


def _search_like(conn, query, domain, generation_type, tag, limit):
    like_val = f"%{query}%"
    extra_clauses, extra_params = _where_clauses(domain, generation_type, tag)

    base_sql = f"{_SELECT} WHERE (name LIKE ? OR type LIKE ?) "
    params: list = [like_val, like_val]

    if extra_clauses:
        base_sql += "AND " + " AND ".join(extra_clauses) + " "
        params.extend(extra_params)

    base_sql += "LIMIT ?"
    params.append(limit)

    return conn.execute(base_sql, params).fetchall()


def _search_filtered(conn, domain, generation_type, tag, limit):
    extra_clauses, extra_params = _where_clauses(domain, generation_type, tag)

    if extra_clauses:
        sql = f"{_SELECT} WHERE " + " AND ".join(extra_clauses) + " LIMIT ?"
        extra_params.append(limit)
        return conn.execute(sql, extra_params).fetchall()

    return conn.execute(f"{_SELECT} LIMIT ?", [limit]).fetchall()


def _search_by_stat_id(conn, stat_id, domain, generation_type, tag, limit):
    like_val = f"%{stat_id}%"
    extra_clauses, extra_params = _where_clauses(domain, generation_type, tag)

    base_sql = (
        f"{_SELECT} WHERE ("
        "json_extract(stats, '$[0].id') LIKE ? OR stats LIKE ?"
        ") "
    )
    params: list = [like_val, like_val]

    if extra_clauses:
        base_sql += "AND " + " AND ".join(extra_clauses) + " "
        params.extend(extra_params)

    base_sql += "LIMIT ?"
    params.append(limit)

    return conn.execute(base_sql, params).fetchall()


# ── Standalone test ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import pprint
    results = search_mods(query="fire damage", domain="item", limit=5)
    pprint.pprint(results)
    print(f"\n{len(results)} result(s)")
