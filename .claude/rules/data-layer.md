# Data Layer Rules

## Query rules

CRITICAL: Never load data/processed/*.json directly after S1.3 is complete.
YOU MUST: Query SQLite only via exilesage/tools/ functions.
IMPORTANT: Use FTS5 virtual tables first; fall back to LIKE only if FTS returns 0 results.

## Tool function contract

All tool functions MUST:
- Accept typed args: query: str = "", plus domain-specific filters
- Return List[dict] — never raw sqlite3.Row objects
- Hard cap results at MAX_RESULTS (default 20) from exilesage/config.py
- Never raise exceptions to caller — catch and return [] with logged warning

## Importer rules

- Pydantic models in pipeline/importers/*.py are the schema contract — each importer defines its own row model
- Validation failure = log warning + skip row, never crash the import
- Always update meta table with import timestamp and row counts after each importer runs
- FTS5 content tables must be rebuilt after bulk insert (INSERT INTO {table}_fts SELECT ...)

## SQLite conventions

- DB path: data/exilesage.db (from config.py DB_PATH)
- JSON arrays/objects stored as TEXT — use json_extract() in queries where needed
- Connection via exilesage/db.py get_connection() — never open sqlite3.connect() directly
- WAL mode enabled on first connect for concurrent read safety
