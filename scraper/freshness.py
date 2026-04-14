"""
Freshness detection for ExileSage data pipeline.

Provides:
  detect_staleness          — check if manifest data is stale
  fetch_latest_poe2_patch_date  — poll GGG RSS for latest PoE2 patch date
"""

from __future__ import annotations

import json
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Callable

import httpx

log = logging.getLogger(__name__)

DEFAULT_RSS_URL = "https://www.pathofexile.com/news/rss"

# Filter PoE2 patch items: title must start with "Path of Exile 2" (colon or space variant).
# The OR version-regex branch was removed: it matched PoE1 titles like "3.25.0 Settlers Patch Notes".
_POE2_TITLE_PREFIXES = ("Path of Exile 2:", "Path of Exile 2 ")


# ---------------------------------------------------------------------------
# Staleness detection (2D.3)
# ---------------------------------------------------------------------------

def detect_staleness(
    manifest_path: Path,
    *,
    max_age_days: int = 7,
    rss_fetcher: Callable[[], datetime | None] | None = None,
    remote_checker: Callable[[str], str | None] | None = None,
) -> dict:
    """Determine whether the local data is stale.

    Args:
        manifest_path:   Path to _manifest.json written by fetch_all.
        max_age_days:    Maximum age before data is considered stale.
        rss_fetcher:     Callable() → datetime | None. If provided, checked
                         against manifest's newest fetched_at. Pass None to skip.
        remote_checker:  Callable(file_key) → hash str | None. If provided,
                         compared against each entry's stored sha256. Pass None
                         to skip. Stops on first mismatch.

    Returns:
        {
            "stale": bool,
            "reasons": list[str],   # each reason code for stale condition
            "max_fetched_at": str,  # ISO-8601 of the newest entry, or "" if none
        }
    """
    reasons: list[str] = []

    # -- Load manifest ---------------------------------------------------------
    try:
        manifest: dict = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        log.warning("detect_staleness: could not read manifest at %s: %s", manifest_path, exc)
        return {"stale": True, "reasons": ["no_manifest_data"], "max_fetched_at": ""}

    if not manifest:
        return {"stale": True, "reasons": ["no_manifest_data"], "max_fetched_at": ""}

    # -- Parse fetched_at timestamps -------------------------------------------
    fetched_datetimes: list[datetime] = []
    for key, entry in manifest.items():
        raw = entry.get("fetched_at")
        if raw:
            try:
                dt = datetime.fromisoformat(raw)
                # Ensure tz-aware
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                fetched_datetimes.append(dt)
            except Exception:
                pass

    if not fetched_datetimes:
        return {"stale": True, "reasons": ["no_manifest_data"], "max_fetched_at": ""}

    newest_fetched = max(fetched_datetimes)
    max_fetched_at = newest_fetched.isoformat()
    now = datetime.now(timezone.utc)

    # -- Condition 1: manifest age ---------------------------------------------
    age_days = (now - newest_fetched).total_seconds() / 86400
    if age_days > max_age_days:
        reasons.append("manifest_age_exceeded")

    # -- Condition 2: RSS patch date newer than manifest -----------------------
    if rss_fetcher is not None:
        try:
            rss_date = rss_fetcher()
            if rss_date is not None:
                if rss_date.tzinfo is None:
                    rss_date = rss_date.replace(tzinfo=timezone.utc)
                if rss_date > newest_fetched:
                    reasons.append("rss_patch_newer")
        except Exception as exc:
            log.warning("detect_staleness: rss_fetcher raised: %s", exc)

    # -- Condition 3: remote ETag differs from stored --------------------------
    # Compares stored etag against remote opaque freshness token.
    # If either side is None, the comparison is skipped for that file.
    if remote_checker is not None:
        for key, entry in manifest.items():
            stored_token = entry.get("etag")
            if stored_token is None:
                continue
            try:
                remote_token = remote_checker(key)
                if remote_token is not None and remote_token != stored_token:
                    reasons.append(f"etag_changed:{key}")
            except Exception as exc:
                log.warning("detect_staleness: remote_checker raised for %r: %s", key, exc)

    return {
        "stale": len(reasons) > 0,
        "reasons": reasons,
        "max_fetched_at": max_fetched_at,
    }


# ---------------------------------------------------------------------------
# GGG RSS polling (2D.4)
# ---------------------------------------------------------------------------

def fetch_latest_poe2_patch_date(
    rss_url: str = DEFAULT_RSS_URL,
    client: httpx.Client | None = None,
) -> datetime | None:
    """Fetch the GGG RSS feed and return the most recent PoE2 patch pubDate.

    Filters <item> elements where the <title> starts with "Path of Exile 2:"
    OR matches a version-pattern + "patch" keyword.

    Args:
        rss_url: RSS feed URL (injectable for tests).
        client:  httpx.Client (injectable for tests). Creates own if None.

    Returns:
        Timezone-aware datetime of the newest matching item, or None if none found.
    """
    try:
        if client is None:
            with httpx.Client() as _client:
                resp = _client.get(rss_url, timeout=15)
                resp.raise_for_status()
                xml_bytes = resp.content
        else:
            resp = client.get(rss_url, timeout=15)
            resp.raise_for_status()
            xml_bytes = resp.content
    except Exception as exc:
        log.warning("fetch_latest_poe2_patch_date: HTTP error: %s", exc)
        return None

    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as exc:
        log.warning("fetch_latest_poe2_patch_date: XML parse error: %s", exc)
        return None

    patch_dates: list[datetime] = []

    for item in root.iter("item"):
        title_el = item.find("title")
        pubdate_el = item.find("pubDate")

        if title_el is None or pubdate_el is None:
            continue

        title = (title_el.text or "").strip()
        pubdate_str = (pubdate_el.text or "").strip()

        is_poe2 = any(title.startswith(p) for p in _POE2_TITLE_PREFIXES)
        if not is_poe2:
            continue

        try:
            dt = parsedate_to_datetime(pubdate_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            patch_dates.append(dt)
        except Exception as exc:
            log.warning("fetch_latest_poe2_patch_date: could not parse pubDate %r: %s", pubdate_str, exc)

    if not patch_dates:
        return None

    return max(patch_dates)
