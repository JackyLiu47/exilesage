"""SQLite connection and schema management."""

import re
import sqlite3
import logging
from pathlib import Path

from exilesage import config  # module-level import; attribute access = call-time resolution
from exilesage.config import SCHEMA_PATH

log = logging.getLogger(__name__)


def get_connection(db_path=None) -> sqlite3.Connection:
    """Return a WAL-mode connection with row_factory set."""
    conn = sqlite3.connect(db_path or config.DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


# Whitelist approach: keep word chars (letters, digits, underscore), whitespace,
# and extended unicode (Latin-1 Supplement onward — covers é, ü, CJK, etc.).
# Everything else (., , = / < > ; ! ? % & | ` ' U+2019, etc.) becomes a space
# so FTS5 tokens split cleanly and no syntax errors are raised.
_FTS_SPECIAL = re.compile(r"[^\w\s\u00C0-\uFFFF]")
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
    log.info("DB initialised at %s (schema v%d)", config.DB_PATH, CURRENT_SCHEMA_VERSION)


def _add_column_if_missing(conn: sqlite3.Connection, table: str, column_def: str) -> None:
    """Add a column to *table* only if it does not already exist.

    *column_def* is the full column specification, e.g. ``"extra TEXT DEFAULT 'x'"``.
    The column name is the first whitespace-separated token of *column_def*.
    This is a no-op (not an error) when the column is already present, working
    around SQLite's lack of ``ADD COLUMN IF NOT EXISTS``.

    Does **not** commit — the caller owns the transaction boundary.
    """
    # column_def must start with a plain (unquoted) identifier
    col_name = column_def.split()[0]
    assert col_name.isidentifier(), f"column_def must start with a plain identifier, got {col_name!r}"
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if col_name not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column_def}")


def _ensure_schema_version(conn: sqlite3.Connection) -> None:
    """Add schema_version column to meta if missing (upgrade from pre-v1 DBs).

    Does **not** commit — the caller owns the transaction boundary (init_db
    via ``with conn:``).  A premature commit here would destroy any active
    savepoint that a caller might have opened, making rollback impossible.
    """
    _add_column_if_missing(conn, "meta", "schema_version INTEGER NOT NULL DEFAULT 1")


def _apply_migrations(conn: sqlite3.Connection) -> None:
    """Run any pending schema migrations based on schema_version in meta.

    Does **not** commit — caller owns transaction boundary (init_db via ``with conn:``).
    """
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
