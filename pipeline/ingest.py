"""
Ingest all processed JSON data into exilesage.db.
Run: python -m pipeline.ingest
"""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from exilesage.db import init_db, get_connection
from exilesage.config import DB_PATH
from pipeline.importers import (
    mods_importer,
    base_items_importer,
    currencies_importer,
    augments_importer,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)


def run() -> None:
    log.info("Initialising DB at %s", DB_PATH)
    init_db()

    # Ensure meta row exists
    with get_connection() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO meta (id, patch_version, last_import_at) VALUES (1, 'unknown', datetime('now'))"
        )

    steps = [
        ("mods",       mods_importer),
        ("base_items", base_items_importer),
        ("currencies", currencies_importer),
        ("augments",   augments_importer),
    ]

    total_imported = 0
    total_skipped = 0

    for name, module in steps:
        log.info("Importing %s ...", name)
        imported, skipped = module.run()
        total_imported += imported
        total_skipped += skipped

    # Stamp import time
    with get_connection() as conn:
        conn.execute("UPDATE meta SET last_import_at = datetime('now') WHERE id = 1")

    print(f"\nIngest complete — {total_imported:,} rows imported, {total_skipped} skipped.")


if __name__ == "__main__":
    run()
