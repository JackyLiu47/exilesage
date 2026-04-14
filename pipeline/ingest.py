"""
Ingest all processed JSON data into exilesage.db.
Run: python -m pipeline.ingest

Phase structure
---------------
IMPORT_PHASES declares the import order as a list of lists.  The outer list is
phases (executed in order); the inner list is importers within a phase.

Phase-failure semantics (strict FK safety, option a):
  If any importer in phase N fails, subsequent phases (N+1, N+2, …) are NOT
  started.  FK ordering exists because downstream tables depend on upstream data.
  Importing phase-N+1 tables when phase N has gaps produces orphan FK references.
  The pipeline exits with SystemExit(1) and reports which phase failed.

Intra-phase failures use the existing partial-failure behaviour: log error,
collect the failing importer name, continue to the next importer in the same
phase.  Only after the full phase completes do we check whether to abort.

IMPORT_PHASES convention:
- Each phase is a list of importers that have NO FK dependencies between them.
- If importer B has an FK to a table produced by importer A, put B in a LATER phase than A.
- Within-phase partial failures are collected but execution continues through the phase.
- Inter-phase failures abort subsequent phases (FK safety).
"""

import logging
import types
from typing import List, Tuple

from exilesage.db import init_db, get_connection
from exilesage import config  # attribute access = call-time resolution; patchable via exilesage.config.DB_PATH
from pipeline.importers import (
    mods_importer,
    base_items_importer,
    currencies_importer,
    augments_importer,
)
from scraper.repoe import fetch_patch_version

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)


def _get_manifest_path():
    """Return path to the repoe manifest JSON (injectable for tests)."""
    from pathlib import Path
    return Path(config.DB_PATH).parent / "raw" / "_manifest.json"

# ---------------------------------------------------------------------------
# Phase registry
# ---------------------------------------------------------------------------

# Each entry is (name: str, module: ModuleType).  The module must expose run().
# Phases are executed in order; within a phase importers run sequentially.
#
# Future phases (commented out — plug in during Stage 2A):
# Phase 2A will add subsequent phases (FK-linked importers in separate phases):
# Phase 2: [ ("skill_gems", skill_gems_importer) ]
# Phase 3: [ ("skills", skills_importer) ]
# Phase 4: [ ("skill_gem_grants", junction_importer) ]            # FK → skill_gems, skills
# Phase 5: [ ("skill_levels", skill_levels_importer) ]             # FK → skills
# Phase 6: [ ("gem_tags", gem_tags_importer), ("ascendancies", ascendancies_importer) ]  # no FKs

IMPORT_PHASES: List[List[Tuple[str, types.ModuleType]]] = [
    # Phase 1 — base tables, no FK dependencies
    [
        ("mods",       mods_importer),
        ("base_items", base_items_importer),
        ("currencies", currencies_importer),
        ("augments",   augments_importer),
    ],
    # Phase 2A will add more phases here once the importers exist.
]


# ---------------------------------------------------------------------------
# Core phase-runner (extracted for testability)
# ---------------------------------------------------------------------------

def run_phases(
    phases: List[List[Tuple[str, types.ModuleType]]],
) -> Tuple[int, int]:
    """Execute importers phase-by-phase with FK-safe abort semantics.

    Args:
        phases: list of phases, each a list of (name, module) tuples.

    Returns:
        (total_imported, total_skipped) accumulated across all phases.

    Raises:
        SystemExit(1): if any phase contains at least one importer failure.
                       Subsequent phases are skipped entirely before raising.
    """
    total_imported = 0
    total_skipped = 0

    for phase_idx, phase in enumerate(phases, start=1):
        log.info("=== Phase %d (%d importer(s)) ===", phase_idx, len(phase))
        phase_failures: list[str] = []

        for name, module in phase:
            log.info("  Importing %s ...", name)
            try:
                imported, skipped = module.run()
                total_imported += imported
                total_skipped += skipped
                log.info("  %s: %d imported, %d skipped", name, imported, skipped)
            except Exception as exc:
                log.error("  Importer %s failed: %s", name, exc, exc_info=True)
                phase_failures.append(name)

        if phase_failures:
            log.error(
                "Phase %d had %d failure(s): %s — aborting subsequent phases",
                phase_idx,
                len(phase_failures),
                phase_failures,
            )
            log.error(
                "%d rows imported, %d skipped (partial — subsequent phases aborted).",
                total_imported,
                total_skipped,
            )
            raise SystemExit(1)

    return total_imported, total_skipped


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run() -> None:
    """Initialise the DB, then run all import phases in order."""
    log.info("Initialising DB at %s", config.DB_PATH)
    init_db()

    # Ensure meta row exists
    with get_connection() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO meta (id, patch_version, last_import_at) "
            "VALUES (1, 'unknown', datetime('now'))"
        )

    total_imported, total_skipped = run_phases(IMPORT_PHASES)

    # Stamp import time + patch_version only on clean run (all phases succeeded)
    patch_version = fetch_patch_version("mods")

    # S6: If GitHub API is unreachable and patch_version is unknown, use http_last_modified
    # from the manifest as a pseudo-version string ("lm:YYYY-MM-DD").
    if patch_version is None:
        try:
            import json
            from pathlib import Path
            manifest_path = _get_manifest_path()
            if manifest_path.exists():
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                lm = manifest.get("mods", {}).get("http_last_modified")
                if lm:
                    # Extract date portion: "Fri, 28 Feb 2026 13:55:43 GMT" → "2026-02-28"
                    from email.utils import parsedate_to_datetime
                    dt = parsedate_to_datetime(lm)
                    patch_version = f"lm:{dt.strftime('%Y-%m-%d')}"
        except Exception as exc:
            log.warning("ingest: LM fallback failed: %s", exc)

    with get_connection() as conn:
        if patch_version is not None:
            conn.execute(
                "UPDATE meta SET patch_version = ?, last_import_at = datetime('now') WHERE id = 1",
                (patch_version,),
            )
        else:
            conn.execute("UPDATE meta SET last_import_at = datetime('now') WHERE id = 1")

    print(f"\nIngest complete — {total_imported:,} rows imported, {total_skipped} skipped.")


if __name__ == "__main__":
    run()
