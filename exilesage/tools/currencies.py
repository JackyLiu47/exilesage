"""Tool: search_currencies — query the currencies table for ExileSage."""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from exilesage.db import get_connection
from exilesage.config import MAX_RESULTS

log = logging.getLogger(__name__)

_SELECT = "SELECT id, name, description, tags, stack_size FROM currencies"


def search_currencies(
    query: str = "",
    limit: int = 10,
) -> list[dict]:
    """Search the currencies table and return a list of matching currency dicts.

    Parameters
    ----------
    query: Free-text search against currencies_fts (name, description).
    limit: Maximum rows to return (capped at MAX_RESULTS).
    """
    conn = None
    try:
        limit = min(limit, MAX_RESULTS)
        conn = get_connection()

        if query:
            rows = _search_fts(conn, query, limit)
            if not rows:
                log.debug("FTS returned 0 results for %r — falling back to LIKE", query)
                rows = _search_like(conn, query, limit)
        else:
            rows = conn.execute(f"{_SELECT} LIMIT ?", [limit]).fetchall()

        return [dict(r) for r in rows]

    except Exception:
        log.warning("search_currencies failed", exc_info=True)
        return []
    finally:
        if conn:
            conn.close()


# ── Internal helpers ──────────────────────────────────────────────────────────

def _search_fts(conn, query: str, limit: int):
    if not query or not query.strip():
        return []
    fts_term = query.strip() + "*"
    sql = (
        f"{_SELECT} WHERE rowid IN "
        "(SELECT rowid FROM currencies_fts WHERE currencies_fts MATCH ?) "
        "ORDER BY rowid LIMIT ?"
    )
    return conn.execute(sql, [fts_term, limit]).fetchall()


def _search_like(conn, query: str, limit: int):
    like_val = f"%{query}%"
    sql = f"{_SELECT} WHERE (name LIKE ? OR description LIKE ?) LIMIT ?"
    return conn.execute(sql, [like_val, like_val, limit]).fetchall()


# ── Standalone test ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import pprint
    results = search_currencies(query="orb", limit=5)
    pprint.pprint(results)
    print(f"\n{len(results)} result(s)")
