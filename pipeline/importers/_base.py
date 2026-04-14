"""Base helpers for ExileSage importers.

Public API:
  _safe_replace_table — atomic full-replace: DELETE + INSERT + FTS rebuild + meta update.
"""

import logging
import re
import sqlite3
from collections.abc import Iterable

log = logging.getLogger(__name__)

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(name: str, label: str) -> None:
    """Raise ValueError if `name` is not a safe SQL identifier.

    Only allows names matching ^[A-Za-z_][A-Za-z0-9_]*$ to prevent
    SQL injection via f-string interpolation of table/column names.
    """
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(
            f"Invalid SQL identifier for {label}: {name!r}. "
            "Must match ^[A-Za-z_][A-Za-z0-9_]*$"
        )


def _safe_replace_table(
    conn: sqlite3.Connection,
    table: str,
    insert_sql: str,
    rows: Iterable[tuple],
    fts_table: str | None = None,
    meta_col: str | None = None,
    min_rows: int | None = None,
) -> int:
    """Atomic full-replace: DELETE + INSERT + FTS rebuild + meta update.

    Single transaction — rollback on any failure preserves old data.
    FK check runs INSIDE the transaction so violations trigger rollback.
    Returns row count inserted.

    Args:
        conn:       Open sqlite3 connection with no open transaction (caller owns lifecycle).
        table:      Name of the table to replace (validated against identifier regex).
        insert_sql: Parameterised INSERT SQL — values come from `rows`.
        rows:       Iterable of tuples to insert. Empty list is valid (wipes table).
                    Generators are eagerly materialised.
        fts_table:  If provided, issues FTS5 external-content rebuild after insert.
        meta_col:   If provided, updates meta.{meta_col} = len(rows) WHERE id = 1.
        min_rows:   If provided, raise ValueError before any DB writes when
                    len(rows) < min_rows.

    Raises:
        RuntimeError: If called with an open transaction on the connection.
        ValueError:   If table, fts_table, or meta_col fail the identifier check,
                      or if len(rows) < min_rows.
        RuntimeError: If PRAGMA foreign_key_check finds orphan rows after import
                      (transaction is rolled back — data is NOT committed).
        Exception:    Any sqlite3 error from DELETE / INSERT / FTS rebuild propagates
                      after ROLLBACK (caller decides how to handle pipeline failure).
    """
    # --- Reentrancy guard ---------------------------------------------------
    # PRAGMA foreign_keys=OFF is silently ignored inside an open transaction,
    # and BEGIN IMMEDIATE would raise OperationalError stomping caller's work.
    if conn.in_transaction:
        raise RuntimeError(
            "_safe_replace_table requires a connection with no open transaction. "
            "Commit or rollback the caller's transaction before calling this function."
        )

    # --- Identifier validation (SQL-injection guard) -----------------------
    _validate_identifier(table, "table")
    if fts_table is not None:
        _validate_identifier(fts_table, "fts_table")
    if meta_col is not None:
        _validate_identifier(meta_col, "meta_col")

    # --- Eager-materialise rows (supports generators) ----------------------
    rows = list(rows)

    # --- min_rows pre-flight check -----------------------------------------
    if min_rows is not None and len(rows) < min_rows:
        raise ValueError(
            f"_safe_replace_table: only {len(rows)} rows supplied for {table!r} "
            f"but min_rows={min_rows}. Aborting before any DB writes."
        )

    # --- Disable FK checks for bulk import --------------------------------
    # PRAGMA foreign_keys is connection-scoped, not transactional.
    # Must be re-enabled in finally to guarantee cleanup even on crash.
    conn.execute("PRAGMA foreign_keys=OFF")

    try:
        # Acquire write lock immediately; prevents race with concurrent readers.
        conn.execute("BEGIN IMMEDIATE")

        try:
            # Full wipe — including the valid "league removed everything" case.
            conn.execute(f'DELETE FROM "{table}"')

            # executemany on an empty iterable is skipped — some sqlite builds
            # raise an error for an empty list; avoid it unconditionally.
            if rows:
                conn.executemany(insert_sql, rows)

            # FTS5 external-content rebuild syncs virtual table with source table.
            if fts_table is not None:
                conn.execute(
                    f'INSERT INTO "{fts_table}"("{fts_table}") VALUES(\'rebuild\')'
                )

            # Meta bookkeeping.
            if meta_col is not None:
                conn.execute(
                    f'UPDATE meta SET "{meta_col}" = ? WHERE id = 1',
                    (len(rows),),
                )

            # --- FK integrity check INSIDE transaction ---------------------
            # Run before COMMIT so violations trigger rollback — data is never
            # durably committed when there are orphan rows.
            violations = conn.execute("PRAGMA foreign_key_check").fetchall()
            if violations:
                raise RuntimeError(
                    f"FK violation after importing into {table!r}: {list(violations)}"
                )

            conn.commit()
            log.info("replaced %d rows in %s", len(rows), table)

        except RuntimeError:
            conn.rollback()
            log.warning("FK violation in %s — transaction rolled back", table)
            raise
        except Exception:
            conn.rollback()
            raise

    finally:
        # Always restore FK enforcement — even if we rolled back.
        conn.execute("PRAGMA foreign_keys=ON")

    return len(rows)
