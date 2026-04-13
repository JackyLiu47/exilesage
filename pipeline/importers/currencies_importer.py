"""Import currencies from processed JSON into SQLite."""

import re
import sys
import json
import logging
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, field_validator

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from exilesage.db import get_connection
from exilesage.config import PROCESSED_DIR

log = logging.getLogger(__name__)

_WIKI_RE = re.compile(r'\[([^|]+)\|([^\]]+)\]')


def _strip_wiki(s: str | None) -> str | None:
    """Strip PoE wiki markup like [Key|Display] → Display."""
    if not s:
        return s
    return _WIKI_RE.sub(r'\2', s)


class CurrencyRow(BaseModel):
    """Pydantic model for currency entries."""

    id: str
    name: Optional[str] = None
    tags: list = Field(default_factory=list)
    drop_level: Optional[int] = None
    stack_size: Optional[int] = None
    stack_size_currency_tab: Optional[int] = None
    full_stack_turns_into: Optional[str] = None
    description: Optional[str] = None
    inherits_from: Optional[str] = None

    @field_validator("name", mode="before")
    @classmethod
    def normalize_name(cls, v: Optional[str]) -> str:
        """Ensure name is a string (default empty string)."""
        return v if v else ""

    @field_validator("drop_level", mode="before")
    @classmethod
    def normalize_drop_level(cls, v: Optional[int]) -> int:
        """Ensure drop_level is an integer (default 0)."""
        return v if v is not None else 0


def run(db_path: Optional[str] = None) -> tuple[int, int]:
    """
    Import currencies from data/processed/currencies.json into SQLite.

    Args:
        db_path: Optional path to database (defaults to config.DB_PATH)

    Returns:
        Tuple of (imported_count, skipped_count)
    """
    currencies_file = PROCESSED_DIR / "currencies.json"

    if not currencies_file.exists():
        log.error("Currencies file not found: %s", currencies_file)
        return 0, 0

    # Load currencies JSON
    try:
        with open(currencies_file, "r", encoding="utf-8") as f:
            currencies_data = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        log.error("Failed to load currencies.json: %s", e)
        return 0, 0

    imported = 0
    skipped = 0
    rows_to_insert = []

    # Validate and prepare rows
    for currency_id, currency_dict in currencies_data.items():
        try:
            currency_row = CurrencyRow(**currency_dict)

            # Prepare tuple for insertion (matches table columns in order)
            row = (
                currency_row.id,
                currency_row.name,
                json.dumps(currency_row.tags) if currency_row.tags else None,
                currency_row.drop_level,
                currency_row.stack_size,
                currency_row.stack_size_currency_tab,
                currency_row.full_stack_turns_into,
                _strip_wiki(currency_row.description),
                currency_row.inherits_from,
            )
            rows_to_insert.append(row)
            imported += 1

        except Exception as e:
            log.warning("Validation failed for currency %s: %s", currency_id, e)
            skipped += 1
            continue

    # Insert all rows in one batch
    if rows_to_insert:
        with get_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.executemany(
                """
                INSERT OR REPLACE INTO currencies (
                    id, name, tags, drop_level, stack_size,
                    stack_size_currency_tab, full_stack_turns_into,
                    description, inherits_from
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows_to_insert,
            )

            # Rebuild FTS5 index
            cursor.execute("INSERT INTO currencies_fts(currencies_fts) VALUES('rebuild')")

            # Update meta table
            cursor.execute(
                "UPDATE meta SET currencies_count=? WHERE id=1",
                (imported,),
            )

            conn.commit()

    # Print summary
    print(f"Imported {imported} currencies ({skipped} skipped)")

    return imported, skipped


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    run()
