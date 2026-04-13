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


CURRENT_SCHEMA_VERSION = 1

# Numbered migrations: each is (version, sql). Run in order when schema_version < target.
_MIGRATIONS: list[tuple[int, str]] = [
    # (2, "ALTER TABLE mods ADD COLUMN patch_version TEXT;"),
]


def init_db() -> None:
    """Create all tables from schema.sql if they don't exist, then run migrations."""
    schema = Path(SCHEMA_PATH).read_text(encoding="utf-8")
    with get_connection() as conn:
        conn.executescript(schema)
        _ensure_schema_version(conn)
        _apply_migrations(conn)
    log.info("DB initialised at %s (schema v%d)", DB_PATH, CURRENT_SCHEMA_VERSION)


def _ensure_schema_version(conn: sqlite3.Connection) -> None:
    """Add schema_version column to meta if missing (upgrade from pre-v1 DBs)."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(meta)").fetchall()}
    if "schema_version" not in cols:
        conn.execute("ALTER TABLE meta ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1")
        conn.commit()


def _apply_migrations(conn: sqlite3.Connection) -> None:
    """Run any pending schema migrations based on schema_version in meta."""
    row = conn.execute(
        "SELECT schema_version FROM meta WHERE id = 1"
    ).fetchone()
    if row is None:
        return  # meta row not yet created (ingest.py creates it)
    current = row[0] if row[0] is not None else 1
    for version, sql in _MIGRATIONS:
        if current < version:
            log.info("Applying migration to schema v%d", version)
            conn.execute(sql)
            conn.execute(
                "UPDATE meta SET schema_version = ? WHERE id = 1", (version,)
            )
            current = version
    conn.commit()
