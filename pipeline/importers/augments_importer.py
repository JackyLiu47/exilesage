import json
import logging
import re
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field

from exilesage.config import PROCESSED_DIR
from exilesage.db import get_connection
from pipeline.importers._base import _safe_replace_table

logger = logging.getLogger(__name__)

_WIKI_RE = re.compile(r'\[([^|]+)\|([^\]]+)\]')


def _strip_wiki(s: str | None) -> str | None:
    """Strip PoE wiki markup like [Key|Display] → Display."""
    if not s:
        return s
    return _WIKI_RE.sub(r'\2', s)


class AugmentRow(BaseModel):
    """Pydantic model for augment entries."""

    id: str
    type_id: str
    type_name: str
    required_level: Optional[int] = 0
    limit: Optional[str] = Field(None, alias="limit")
    categories: dict

    class Config:
        populate_by_name = True

    def model_post_init(self, __context):
        """Post-init validation: ensure required_level defaults to 0."""
        if self.required_level is None:
            self.required_level = 0


def run(db_path: Optional[str] = None) -> tuple[int, int]:
    """
    Import augments from processed JSON into SQLite database.

    Args:
        db_path: Optional path to exilesage.db. If None, uses default from get_connection.

    Returns:
        tuple: (imported_count, skipped_count)
    """
    # Load augments.json
    augments_file = Path(PROCESSED_DIR) / "augments.json"
    if not augments_file.exists():
        logger.error(f"Augments file not found: {augments_file}")
        return 0, 0

    try:
        with open(augments_file, "r", encoding="utf-8") as f:
            augments_data = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"Failed to load augments.json: {e}")
        return 0, 0

    # Validate and prepare rows
    rows = []
    skipped = 0

    for item_id, item_data in augments_data.items():
        try:
            # Ensure the 'id' field is set from the dict key if not present
            if "id" not in item_data:
                item_data["id"] = item_id

            augment = AugmentRow(**item_data)

            # Prepare row for insertion
            row = (
                augment.id,
                augment.type_id,
                _strip_wiki(augment.type_name),
                augment.required_level or 0,
                _strip_wiki(augment.limit),
                json.dumps(augment.categories),
            )
            rows.append(row)

        except Exception as e:
            logger.warning(
                f"Validation error for augment {item_id}: {e}, skipping"
            )
            skipped += 1

    with get_connection(db_path) as conn:
        _safe_replace_table(
            conn,
            table="augments",
            insert_sql="""
                INSERT INTO augments
                (id, type_id, type_name, required_level, limit_info, categories)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows=rows,
            fts_table="augments_fts",
            meta_col="augments_count",
        )

    imported = len(rows)
    print(f"Imported {imported} augments ({skipped} skipped)")

    return imported, skipped


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
