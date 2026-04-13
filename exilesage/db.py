"""SQLite connection and schema management."""

import re
import sqlite3
import logging
from pathlib import Path

from exilesage.config import DB_PATH, SCHEMA_PATH

log = logging.getLogger(__name__)


def get_connection(db_path=None) -> sqlite3.Connection:
    """Return a WAL-mode connection with row_factory set."""
    conn = sqlite3.connect(db_path or DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


_FTS_SPECIAL = re.compile(r'[+\-*"(){}[\]^~:@#]')
_FTS_KEYWORDS = re.compile(r'\b(AND|OR|NOT|NEAR)\b', re.IGNORECASE)


def sanitize_fts(query: str) -> str:
    """Sanitize a query string for FTS5 MATCH.

    Strips FTS5 operators (+, -, *, ", etc.) and boolean keywords (AND, OR,
    NOT, NEAR) that would alter query semantics, then appends * for prefix
    matching. Returns empty string if nothing remains.
    """
    cleaned = _FTS_SPECIAL.sub(" ", query)
    cleaned = _FTS_KEYWORDS.sub(" ", cleaned).strip()
    if not cleaned:
        return ""
    return cleaned + "*"


def init_db() -> None:
    """Create all tables from schema.sql if they don't exist."""
    schema = Path(SCHEMA_PATH).read_text(encoding="utf-8")
    with get_connection() as conn:
        conn.executescript(schema)
    log.info("DB initialised at %s", DB_PATH)
