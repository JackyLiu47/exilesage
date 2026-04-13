"""
Main update pipeline. Run this to refresh all PoE2 game data.

Usage:
    python -m pipeline.update                  # fetch crafting files only
    python -m pipeline.update --all            # fetch every repoe file
    python -m pipeline.update --force          # re-download even if cached
    python -m pipeline.update --diff           # show what changed vs last run
"""

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

# Project root = parent of this file's directory
ROOT = Path(__file__).parent.parent
RAW_DIR = ROOT / "data" / "raw"
PROCESSED_DIR = ROOT / "data" / "processed"
DIFF_DIR = ROOT / "data" / "diffs"

from scraper.repoe import fetch_all, REPOE_FILES, CRAFTING_FILES
from scraper.processor import process_all

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def compute_diff(old_path: Path, new_data: dict) -> dict:
    """Compare new processed data against previous snapshot. Returns diff summary."""
    if not old_path.exists():
        return {"status": "new", "added": len(new_data), "removed": 0, "changed": 0}

    old_data = json.loads(old_path.read_text(encoding="utf-8"))
    old_keys = set(old_data.keys())
    new_keys = set(new_data.keys())

    added = new_keys - old_keys
    removed = old_keys - new_keys
    changed = {
        k for k in old_keys & new_keys
        if json.dumps(old_data[k], sort_keys=True) != json.dumps(new_data[k], sort_keys=True)
    }

    return {
        "status": "updated" if (added or removed or changed) else "unchanged",
        "added": sorted(added),
        "removed": sorted(removed),
        "changed": sorted(changed),
        "added_count": len(added),
        "removed_count": len(removed),
        "changed_count": len(changed),
    }


def run(fetch_all_files: bool = False, force: bool = False, show_diff: bool = False) -> None:
    subset = None if fetch_all_files else CRAFTING_FILES
    label = "all files" if fetch_all_files else f"{len(subset)} crafting files"

    logger.info(f"=== PoE2 data update — fetching {label} ===")
    raw_data = fetch_all(RAW_DIR, subset=subset, force=force)

    if not raw_data:
        logger.error("No data fetched. Aborting.")
        return

    logger.info("=== Processing raw data ===")

    # Snapshot processed files before overwrite for diffing
    snapshots = {}
    if show_diff:
        for name in ["currencies", "base_items", "mods", "augments"]:
            p = PROCESSED_DIR / f"{name}.json"
            if p.exists():
                snapshots[name] = json.loads(p.read_text(encoding="utf-8"))

    process_all(raw_data, PROCESSED_DIR)

    if show_diff:
        DIFF_DIR.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        all_diffs = {}

        for name in ["currencies", "base_items", "mods", "augments"]:
            new_path = PROCESSED_DIR / f"{name}.json"
            if not new_path.exists():
                continue
            new_data = json.loads(new_path.read_text(encoding="utf-8"))
            old_data = snapshots.get(name, {})
            old_path = PROCESSED_DIR / f"{name}.json"  # same file, we already snapshotted

            # Build diff from snapshot
            old_keys = set(old_data.keys())
            new_keys = set(new_data.keys())
            added = sorted(new_keys - old_keys)
            removed = sorted(old_keys - new_keys)
            changed = sorted(
                k for k in old_keys & new_keys
                if json.dumps(old_data[k], sort_keys=True) != json.dumps(new_data[k], sort_keys=True)
            )
            diff = {
                "status": "updated" if (added or removed or changed) else "unchanged",
                "added_count": len(added),
                "removed_count": len(removed),
                "changed_count": len(changed),
                "added": added[:50],    # cap list length for readability
                "removed": removed[:50],
                "changed": changed[:50],
            }
            all_diffs[name] = diff

            status = diff["status"].upper()
            logger.info(
                f"[diff] {name}: {status} | "
                f"+{diff['added_count']} added, "
                f"-{diff['removed_count']} removed, "
                f"~{diff['changed_count']} changed"
            )

        diff_path = DIFF_DIR / f"diff_{timestamp}.json"
        diff_path.write_text(json.dumps(all_diffs, indent=2), encoding="utf-8")
        logger.info(f"[diff saved] {diff_path}")

    logger.info("=== Update complete ===")
    _print_summary(PROCESSED_DIR)


def _print_summary(processed_dir: Path) -> None:
    """Print a quick summary of what's in processed data."""
    for name in ["currencies", "base_items", "mods", "augments"]:
        p = processed_dir / f"{name}.json"
        if p.exists():
            data = json.loads(p.read_text(encoding="utf-8"))
            logger.info(f"  {name:20s}: {len(data):>6,} entries")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update PoE2 game data")
    parser.add_argument("--all",   action="store_true", help="Fetch all repoe files (not just crafting subset)")
    parser.add_argument("--force", action="store_true", help="Re-download even if cached")
    parser.add_argument("--diff",  action="store_true", help="Compute and save a diff vs previous data")
    args = parser.parse_args()

    run(fetch_all_files=args.all, force=args.force, show_diff=args.diff)
