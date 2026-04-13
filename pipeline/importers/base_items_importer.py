"""
PoE2 base_items importer for ExileSage.
Loads base_items.json and populates the base_items table in SQLite.
"""

import json
import logging
import sys
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from exilesage.db import get_connection
from exilesage.config import PROCESSED_DIR

log = logging.getLogger(__name__)


class BaseItemRow(BaseModel):
    """Pydantic v2 model for base_items.json entries."""

    id: str
    name: Optional[str] = Field(default=None)
    item_class: Optional[str] = None
    domain: Optional[str] = None
    drop_level: Optional[int] = None
    tags: Optional[list] = None
    implicits: Optional[list] = None
    requirements: Optional[dict] = None
    armour: Optional[int] = None
    evasion: Optional[int] = None
    energy_shield: Optional[int] = None
    physical_damage_min: Optional[float] = None
    physical_damage_max: Optional[float] = None
    critical_strike_chance: Optional[float] = None
    attack_time: Optional[float] = None
    range: Optional[int] = None
    charges_max: Optional[int] = None
    charges_per_use: Optional[int] = None
    duration: Optional[int] = None
    life_per_use: Optional[int] = None
    mana_per_use: Optional[int] = None
    stack_size: Optional[int] = None

    @field_validator("name", mode="before")
    @classmethod
    def default_name(cls, v):
        return v if v is not None else ""

    @field_validator("drop_level", mode="before")
    @classmethod
    def default_drop_level(cls, v):
        return v if v is not None else 0

    @field_validator(
        "armour", "evasion", "energy_shield",
        "charges_max", "charges_per_use", "duration",
        "life_per_use", "mana_per_use", "stack_size", "range",
        mode="before",
    )
    @classmethod
    def unwrap_int_range(cls, v):
        """JSON stores these as {"min": X, "max": X} — extract max."""
        if isinstance(v, dict):
            return v.get("max") if v.get("max") is not None else v.get("min")
        return v

    @field_validator(
        "physical_damage_min", "physical_damage_max",
        "critical_strike_chance", "attack_time",
        mode="before",
    )
    @classmethod
    def unwrap_float_range(cls, v):
        """Same dict pattern for float fields."""
        if isinstance(v, dict):
            return v.get("max") if v.get("max") is not None else v.get("min")
        return v

    class Config:
        extra = "ignore"


def run(db_path: Optional[str] = None) -> tuple[int, int]:
    """
    Load base_items.json and import to SQLite database.

    Args:
        db_path: Optional override for database path (defaults to exilesage.db)

    Returns:
        Tuple of (imported_count, skipped_count)
    """
    # Load JSON data
    json_path = PROCESSED_DIR / "base_items.json"

    if not json_path.exists():
        log.error(f"Source file not found: {json_path}")
        return 0, 0

    try:
        raw_data = json.loads(json_path.read_text(encoding="utf-8"))
    except Exception as e:
        log.error(f"Failed to load JSON: {e}")
        return 0, 0

    # Validate and transform entries
    valid_rows = []
    skipped = 0

    for item_id, item_data in raw_data.items():
        try:
            # Ensure the 'id' field is set from the dict key if not present
            if "id" not in item_data:
                item_data["id"] = item_id
            # Validate with Pydantic
            row = BaseItemRow(**item_data)
            valid_rows.append(row)
        except Exception as e:
            log.warning(f"Validation error for {item_id}: {e}")
            skipped += 1

    if not valid_rows:
        log.warning("No valid base_items to import")
        return 0, skipped

    # Prepare data for database insert
    conn = get_connection(db_path)
    cursor = conn.cursor()

    try:
        # Build INSERT data
        insert_data = []
        for row in valid_rows:
            # Build properties JSON from specified fields
            properties = {
                "charges_max": row.charges_max,
                "charges_per_use": row.charges_per_use,
                "duration": row.duration,
                "life_per_use": row.life_per_use,
                "mana_per_use": row.mana_per_use,
            }

            insert_data.append((
                row.id,
                row.name,
                row.item_class,
                row.domain,
                row.drop_level,
                json.dumps(row.tags) if row.tags else None,
                json.dumps(row.implicits) if row.implicits else None,
                json.dumps(row.requirements) if row.requirements else None,
                json.dumps(properties),
                row.armour,
                row.evasion,
                row.energy_shield,
                row.physical_damage_min,
                row.physical_damage_max,
                row.critical_strike_chance,
                row.attack_time,
                row.stack_size,
            ))

        # INSERT OR REPLACE in single executemany
        cursor.executemany(
            """INSERT OR REPLACE INTO base_items
               (id, name, item_class, domain, drop_level, tags, implicits,
                requirements, properties, armour, evasion, energy_shield,
                physical_damage_min, physical_damage_max, critical_strike_chance,
                attack_time, stack_size)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            insert_data
        )

        # Rebuild FTS5 index
        cursor.execute("INSERT INTO base_items_fts(base_items_fts) VALUES('rebuild')")

        # Update meta table
        cursor.execute(
            "UPDATE meta SET base_items_count=?, last_import_at=CURRENT_TIMESTAMP WHERE id=1",
            (len(valid_rows),)
        )

        conn.commit()

        log.info(f"Imported {len(valid_rows)} base_items ({skipped} skipped)")
        print(f"Imported {len(valid_rows)} base_items ({skipped} skipped)")

        return len(valid_rows), skipped

    except Exception as e:
        log.error(f"Database error: {e}")
        conn.rollback()
        return 0, skipped
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    imported, skipped = run()
    if imported == 0 and skipped == 0:
        sys.exit(1)
