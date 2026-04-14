"""
Fetches raw PoE2 game data from repoe-fork.github.io/poe2.
All files are pre-extracted from game client and kept up to date by the community.
"""

import hashlib
import httpx
import json
import logging
import re
from pathlib import Path
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# GitHub API for patch version detection
GITHUB_API_URL = "https://api.github.com/repos/repoe-fork/poe2/commits"
_EXPORT_RE = re.compile(r"\bExport\s+([\d.]+)\b")

# Module-level cache: file_key → version string (or None)
_patch_version_cache: dict[str, str | None] = {}

BASE_URL = "https://repoe-fork.github.io/poe2"

# All available data files on repoe-fork for PoE2
REPOE_FILES = {
    "base_items":           "base_items.json",
    "mods":                 "mods.json",
    "mods_by_base":         "mods_by_base.json",
    "item_classes":         "item_classes.json",
    "tags":                 "tags.json",
    "tag_details":          "tag_details.json",
    "augments":             "augments.json",       # Soul Cores / socketables
    "uniques":              "uniques.json",
    "skill_gems":           "skill_gems.json",
    "skills":               "skills.json",
    "gem_tags":             "gem_tags.json",
    "ascendancies":         "ascendancies.json",
    "characters":           "characters.json",
    "buffs":                "buffs.json",
    "keywords":             "keywords.json",
    "flavour":              "flavour.json",
    "world_areas":          "world_areas.json",
    "default_monster_stats":"default_monster_stats.json",
    "cost_types":           "cost_types.json",
    "active_skill_types":   "active_skill_types.json",
}

# Subset to fetch on every run (crafting-critical data)
CRAFTING_FILES = {
    "base_items",
    "mods",
    "mods_by_base",
    "item_classes",
    "tags",
    "augments",
    "uniques",
}


def fetch_file(
    name: str,
    raw_dir: Path,
    client: httpx.Client,
    force: bool = False,
) -> tuple[dict | list | None, str | None, str | None, str | None]:
    """Download a single repoe JSON file into raw_dir.

    Returns:
        (parsed_json, sha256_hex, http_last_modified, etag)
        sha256/http_last_modified/etag are None when taken from cache.
    """
    filename = REPOE_FILES[name]
    dest = raw_dir / filename

    if dest.exists() and not force:
        logger.info(f"[cache] {filename} already exists, skipping")
        with open(dest, encoding="utf-8") as f:
            return json.load(f), None, None, None

    url = f"{BASE_URL}/{filename}"
    logger.info(f"[fetch] {url}")
    resp = client.get(url, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    content = resp.content

    dest.write_bytes(content)
    logger.info(f"[saved] {dest} ({len(content) / 1024:.1f} KB)")

    sha256 = hashlib.sha256(content).hexdigest()
    http_last_modified = resp.headers.get("Last-Modified")
    etag = resp.headers.get("ETag")

    return data, sha256, http_last_modified, etag


def fetch_patch_version(file_key: str, client: httpx.Client | None = None) -> str | None:
    """Fetch the patch version for a repoe file from GitHub commit messages.

    Hits the GitHub API to find the latest commit message matching 'Export X.X.X...',
    returns the version string, or None on any failure. Results cached in-process.

    Args:
        file_key: Key from REPOE_FILES (e.g. "mods").
        client:   Optional httpx.Client for injection (tests). Creates own if None.

    Returns:
        Version string like "4.4.0.6.6", or None if not found/error.
    """
    if file_key in _patch_version_cache:
        return _patch_version_cache[file_key]

    if file_key not in REPOE_FILES:
        logger.warning("fetch_patch_version: unknown file key %r", file_key)
        _patch_version_cache[file_key] = None
        return None

    filename = REPOE_FILES[file_key]
    url = f"{GITHUB_API_URL}?path=data/{filename}&per_page=1"

    try:
        headers = {"User-Agent": "poe2craf/0.1 (github data pipeline)"}
        if client is None:
            with httpx.Client(headers=headers) as _client:
                resp = _client.get(url, timeout=15)
        else:
            resp = client.get(url, timeout=15)

        resp.raise_for_status()
        commits = resp.json()

        if not commits:
            logger.warning("fetch_patch_version: no commits returned for %r", file_key)
            _patch_version_cache[file_key] = None
            return None

        message = commits[0].get("commit", {}).get("message", "")
        match = _EXPORT_RE.search(message)
        if match:
            version = match.group(1)
            _patch_version_cache[file_key] = version
            return version

        logger.warning("fetch_patch_version: no Export pattern in message %r", message[:100])
        _patch_version_cache[file_key] = None
        return None

    except Exception as exc:
        logger.warning("fetch_patch_version: failed for %r: %s", file_key, exc)
        _patch_version_cache[file_key] = None
        return None


def fetch_all(
    raw_dir: Path,
    subset: set[str] | None = None,
    force: bool = False,
    client: httpx.Client | None = None,
) -> dict[str, dict | list]:
    """
    Download repoe PoE2 data files into raw_dir.

    Args:
        raw_dir:  Directory to write raw JSON files into.
        subset:   Which file keys to fetch. Defaults to CRAFTING_FILES.
        force:    Re-download even if file already exists.
        client:   Optional httpx.Client for injection (tests). Creates own if None.

    Returns:
        Dict mapping file key → parsed JSON data.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)
    targets = subset if subset is not None else CRAFTING_FILES

    results: dict[str, dict | list] = {}
    # Track per-file metadata for the manifest update
    file_meta: dict[str, dict] = {}

    default_headers = {"User-Agent": "poe2craf/0.1 (github data pipeline; not a browser)"}

    def _do_fetch(http_client: httpx.Client) -> None:
        for name in targets:
            if name not in REPOE_FILES:
                logger.warning(f"Unknown file key: {name}")
                continue
            try:
                data, sha256, last_modified, etag = fetch_file(name, raw_dir, http_client, force=force)
                results[name] = data
                file_meta[name] = {
                    "sha256": sha256,
                    "http_last_modified": last_modified,
                    "etag": etag,
                }
            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error fetching {name}: {e}")
            except Exception as e:
                logger.error(f"Failed to fetch {name}: {e}")

    if client is not None:
        _do_fetch(client)
    else:
        with httpx.Client(headers=default_headers, follow_redirects=True) as _client:
            _do_fetch(_client)

    # Write a manifest so we know when each file was last fetched
    manifest_path = raw_dir / "_manifest.json"
    manifest: dict = {}
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            manifest = {}

    now = datetime.now(timezone.utc).isoformat()
    for name in results:
        meta = file_meta.get(name, {})
        manifest[name] = {
            "file": REPOE_FILES[name],
            "fetched_at": now,
            "sha256": meta.get("sha256"),
            "http_last_modified": meta.get("http_last_modified"),
            "etag": meta.get("etag"),
        }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    return results
