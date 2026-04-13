"""Import mods from processed JSON into SQLite."""

import json
import logging
from typing import Optional

from pydantic import BaseModel, Field, field_validator

from exilesage.db import get_connection
from exilesage.config import PROCESSED_DIR, MAX_RESULTS

log = logging.getLogger(__name__)


class ModRow(BaseModel):
    """Pydantic model for mod entries."""

    id: str
    name: Optional[str] = None
    generation_type: Optional[str] = None
    domain: Optional[str] = None
    group: Optional[str] = Field(None, alias="group")
    type: Optional[str] = None
    required_level: Optional[int] = None
    is_essence_only: Optional[bool] = None
    tags: list = Field(default_factory=list)
    spawn_weights: list = Field(default_factory=list)
    generation_weights: list = Field(default_factory=list)
    grants_effects: list = Field(default_factory=list)
    stats: list = Field(default_factory=list)
    adds_tags: list = Field(default_factory=list)
    implicit_tags: list = Field(default_factory=list)

    @field_validator("name", mode="before")
    @classmethod
    def normalize_name(cls, v: Optional[str]) -> str:
        """Ensure name is a string (default empty string)."""
        return v if v else ""

    @field_validator("required_level", mode="before")
    @classmethod
    def normalize_required_level(cls, v: Optional[int]) -> int:
        """Ensure required_level is an integer (default 0)."""
        return v if v is not None else 0

    @field_validator("is_essence_only", mode="before")
    @classmethod
    def normalize_is_essence_only(cls, v: Optional[bool]) -> int:
        """Convert bool to int (0/1) for SQLite."""
        if v is None:
            return 0
        return 1 if v else 0


def run(db_path: Optional[str] = None) -> tuple[int, int]:
    """
    Import mods from data/processed/mods.json into SQLite.

    Args:
        db_path: Optional path to database (defaults to config.DB_PATH)

    Returns:
        Tuple of (imported_count, skipped_count)
    """
    mods_file = PROCESSED_DIR / "mods.json"

    if not mods_file.exists():
        log.error("Mods file not found: %s", mods_file)
        return 0, 0

    # Load mods JSON
    try:
        with open(mods_file, "r", encoding="utf-8") as f:
            mods_data = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        log.error("Failed to load mods.json: %s", e)
        return 0, 0

    imported = 0
    skipped = 0
    rows_to_insert = []

    # Validate and prepare rows
    for mod_id, mod_dict in mods_data.items():
        try:
            mod_row = ModRow(**mod_dict)

            # Prepare tuple for insertion (matches table columns in order)
            row = (
                mod_row.id,
                mod_row.name,
                mod_row.generation_type,
                mod_row.domain,
                mod_row.group,  # maps to group_name in DB
                mod_row.type,
                mod_row.required_level,
                mod_row.is_essence_only,
                json.dumps(mod_row.tags) if mod_row.tags else None,
                json.dumps(mod_row.spawn_weights) if mod_row.spawn_weights else None,
                json.dumps(mod_row.generation_weights) if mod_row.generation_weights else None,
                json.dumps(mod_row.grants_effects) if mod_row.grants_effects else None,
                json.dumps(mod_row.stats) if mod_row.stats else None,
                json.dumps(mod_row.adds_tags) if mod_row.adds_tags else None,
                json.dumps(mod_row.implicit_tags) if mod_row.implicit_tags else None,
            )
            rows_to_insert.append(row)
            imported += 1

        except Exception as e:
            log.warning("Validation failed for mod %s: %s", mod_id, e)
            skipped += 1
            continue

    # Insert all rows in one batch
    if rows_to_insert:
        with get_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.executemany(
                """
                INSERT OR REPLACE INTO mods (
                    id, name, generation_type, domain, group_name, type,
                    required_level, is_essence_only, tags, spawn_weights,
                    generation_weights, grants_effects, stats, adds_tags,
                    implicit_tags
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows_to_insert,
            )

            # Rebuild FTS5 index
            cursor.execute("INSERT INTO mods_fts(mods_fts) VALUES('rebuild')")

            # Update meta table
            cursor.execute(
                "UPDATE meta SET mods_count=? WHERE id=1",
                (imported,),
            )

            conn.commit()

    # Print summary
    print(f"Imported {imported} mods ({skipped} skipped)")

    return imported, skipped


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    run()
