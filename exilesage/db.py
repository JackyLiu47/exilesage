"""SQLite connection and schema management."""

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


def init_db() -> None:
    """Create all tables from schema.sql if they don't exist."""
    schema = Path(SCHEMA_PATH).read_text(encoding="utf-8")
    with get_connection() as conn:
        conn.executescript(schema)
    log.info("DB initialised at %s", DB_PATH)
