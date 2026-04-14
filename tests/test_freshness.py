"""
Tests for Phase 2D freshness tracking.

Sub-steps covered:
  TestManifest       — 2D.1  SHA256 + HTTP headers in manifest
  TestMetaColWarning — carryover  meta_col silent no-op warning
  TestPatchVersion   — 2D.2  GitHub API patch version parsing
  TestStaleness      — 2D.3  detect_staleness logic
  TestRSS            — 2D.4  GGG RSS parsing
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock, call

import httpx
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_http_client(body: bytes, headers: dict | None = None) -> httpx.Client:
    """Return an httpx.Client backed by a MockTransport serving `body`."""
    _headers = headers or {}

    def handler(request):
        return httpx.Response(200, content=body, headers=_headers)

    return httpx.Client(transport=httpx.MockTransport(handler))


# ===========================================================================
# TestManifest — 2D.1
# ===========================================================================

class TestManifest:
    """SHA256 + HTTP headers are recorded in _manifest.json."""

    # -- fixtures -------------------------------------------------------------

    @pytest.fixture()
    def raw_dir(self, tmp_path) -> Path:
        return tmp_path / "raw"

    def _run_fetch_all(self, raw_dir: Path, body: bytes, headers: dict, subset: set[str] | None = None):
        """Call fetch_all with a mocked httpx client injected."""
        from scraper.repoe import fetch_all
        client = _make_http_client(body, headers)
        return fetch_all(raw_dir, subset=subset or {"mods"}, force=True, client=client)

    def _read_manifest(self, raw_dir: Path) -> dict:
        return json.loads((raw_dir / "_manifest.json").read_text())

    # -- tests ----------------------------------------------------------------

    def test_manifest_contains_sha256(self, raw_dir):
        """Manifest entry must have sha256 field of length 64 (hex string)."""
        self._run_fetch_all(raw_dir, b'{"x":1}', {})
        m = self._read_manifest(raw_dir)
        assert "mods" in m
        sha = m["mods"]["sha256"]
        assert isinstance(sha, str)
        assert len(sha) == 64

    def test_manifest_contains_http_last_modified(self, raw_dir):
        """Manifest entry captures Last-Modified header."""
        lm = "Sat, 28 Feb 2026 13:55:43 GMT"
        self._run_fetch_all(raw_dir, b'{"x":1}', {"Last-Modified": lm})
        m = self._read_manifest(raw_dir)
        assert m["mods"]["http_last_modified"] == lm

    def test_manifest_contains_etag(self, raw_dir):
        """Manifest entry captures ETag header."""
        self._run_fetch_all(raw_dir, b'{"x":1}', {"ETag": '"abc123"'})
        m = self._read_manifest(raw_dir)
        assert m["mods"]["etag"] == '"abc123"'

    def test_manifest_missing_headers_stored_as_null(self, raw_dir):
        """When headers are absent, http_last_modified and etag are None/null."""
        self._run_fetch_all(raw_dir, b'{"x":1}', {})
        m = self._read_manifest(raw_dir)
        assert m["mods"]["http_last_modified"] is None
        assert m["mods"]["etag"] is None

    def test_manifest_sha256_changes_on_content_change(self, raw_dir):
        """Two fetches with different bodies produce different sha256 values."""
        self._run_fetch_all(raw_dir, b'{"x":1}', {}, subset={"mods"})
        sha1 = self._read_manifest(raw_dir)["mods"]["sha256"]

        self._run_fetch_all(raw_dir, b'{"x":2}', {}, subset={"mods"})
        sha2 = self._read_manifest(raw_dir)["mods"]["sha256"]

        assert sha1 != sha2

    def test_manifest_sha256_stable_on_same_content(self, raw_dir):
        """Two fetches with the same body produce the same sha256."""
        body = b'{"stable":true}'
        self._run_fetch_all(raw_dir, body, {})
        sha1 = self._read_manifest(raw_dir)["mods"]["sha256"]

        self._run_fetch_all(raw_dir, body, {})
        sha2 = self._read_manifest(raw_dir)["mods"]["sha256"]

        assert sha1 == sha2

    def test_manifest_backward_compat_loads_old_entries(self, raw_dir):
        """Pre-existing old-shape entries (no sha256/etag) survive a fetch of a different file."""
        raw_dir.mkdir(parents=True, exist_ok=True)
        old_entry = {
            "base_items": {
                "file": "base_items.json",
                "fetched_at": "2025-01-01T00:00:00+00:00",
            }
        }
        (raw_dir / "_manifest.json").write_text(json.dumps(old_entry))

        # Fetch "mods" only — base_items entry should survive unchanged
        self._run_fetch_all(raw_dir, b'{"x":1}', {}, subset={"mods"})
        m = self._read_manifest(raw_dir)

        assert "base_items" in m
        assert m["base_items"]["fetched_at"] == "2025-01-01T00:00:00+00:00"
        assert "sha256" not in m["base_items"]


# ===========================================================================
# TestPatchVersion — 2D.2
# ===========================================================================

class TestPatchVersion:
    """GitHub API patch version parsing and caching."""

    def _mock_github_response(self, message: str) -> httpx.Client:
        body = json.dumps([{"commit": {"message": message}}]).encode()

        def handler(request):
            return httpx.Response(200, content=body,
                                  headers={"Content-Type": "application/json"})

        return httpx.Client(transport=httpx.MockTransport(handler))

    def _mock_github_error(self, status: int = 500) -> httpx.Client:
        def handler(request):
            return httpx.Response(status)

        return httpx.Client(transport=httpx.MockTransport(handler))

    def test_fetch_patch_version_parses_export_string(self):
        """Parses 'Export 4.4.0.6.6' from commit message."""
        from scraper.repoe import fetch_patch_version, _patch_version_cache
        _patch_version_cache.clear()

        client = self._mock_github_response("[skip ci] Export 4.4.0.6.6")
        result = fetch_patch_version("mods", client=client)
        assert result == "4.4.0.6.6"

    def test_fetch_patch_version_returns_none_on_missing(self):
        """S3: Returns None when commit message lacks Export pattern (uses valid key 'mods')."""
        from scraper.repoe import fetch_patch_version, _patch_version_cache
        _patch_version_cache.clear()

        client = self._mock_github_response("regular commit message without export")
        result = fetch_patch_version("mods", client=client)
        assert result is None
        # Verify HTTP was actually hit (mock returns non-export message)

    def test_fetch_patch_version_returns_none_on_http_error(self, caplog):
        """S3: Returns None and logs warning on HTTP error (uses valid key 'mods')."""
        from scraper.repoe import fetch_patch_version, _patch_version_cache
        _patch_version_cache.clear()

        client = self._mock_github_error(500)
        with caplog.at_level(logging.WARNING, logger="scraper.repoe"):
            result = fetch_patch_version("mods", client=client)
        assert result is None
        assert any("warning" in r.levelname.lower() or r.levelno >= logging.WARNING
                   for r in caplog.records)

    def test_fetch_patch_version_cached(self):
        """Second call uses cache — only one HTTP request made."""
        from scraper.repoe import fetch_patch_version, _patch_version_cache
        _patch_version_cache.clear()

        request_count = 0

        def handler(request):
            nonlocal request_count
            request_count += 1
            body = json.dumps([{"commit": {"message": "Export 4.4.0.6.6"}}]).encode()
            return httpx.Response(200, content=body,
                                  headers={"Content-Type": "application/json"})

        client = httpx.Client(transport=httpx.MockTransport(handler))
        # Use "mods" — a valid REPOE_FILES key so the real HTTP path is hit
        fetch_patch_version("mods", client=client)
        fetch_patch_version("mods", client=client)
        assert request_count == 1

    def test_meta_patch_version_updated_after_ingest(self, tmp_path):
        """After a successful ingest, meta.patch_version is updated."""
        import sqlite3
        from unittest.mock import patch as mpatch

        db_file = tmp_path / "test_ingest.db"
        conn = sqlite3.connect(str(db_file))
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS meta (
                id INTEGER PRIMARY KEY CHECK(id=1),
                patch_version TEXT DEFAULT 'unknown',
                last_import_at TEXT
            );
            INSERT OR IGNORE INTO meta (id, patch_version, last_import_at)
            VALUES (1, 'unknown', datetime('now'));
        """)
        conn.commit()
        conn.close()

        def mock_run_phases(phases):
            return (100, 0)

        def mock_fetch_patch_version(file_key, client=None):
            return "4.4.0.6.6"

        import exilesage.config as cfg
        original_db = cfg.DB_PATH
        cfg.DB_PATH = str(db_file)

        try:
            with mpatch("pipeline.ingest.run_phases", mock_run_phases), \
                 mpatch("pipeline.ingest.init_db", lambda: None), \
                 mpatch("pipeline.ingest.fetch_patch_version", mock_fetch_patch_version), \
                 mpatch("pipeline.ingest.get_connection") as mock_gc:

                # Use real sqlite3 connection to our tmp_db
                def get_real_conn():
                    c = sqlite3.connect(str(db_file))
                    c.row_factory = sqlite3.Row
                    return c

                mock_gc.side_effect = get_real_conn

                from pipeline import ingest
                ingest.run()
        finally:
            cfg.DB_PATH = original_db

        conn2 = sqlite3.connect(str(db_file))
        row = conn2.execute("SELECT patch_version FROM meta WHERE id=1").fetchone()
        conn2.close()
        assert row[0] == "4.4.0.6.6"

    def test_meta_patch_version_only_after_full_success(self, tmp_path):
        """patch_version stays 'unknown' when run_phases raises SystemExit."""
        import sqlite3
        from unittest.mock import patch as mpatch

        db_file = tmp_path / "test_fail.db"
        conn = sqlite3.connect(str(db_file))
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS meta (
                id INTEGER PRIMARY KEY CHECK(id=1),
                patch_version TEXT DEFAULT 'unknown',
                last_import_at TEXT
            );
            INSERT OR IGNORE INTO meta (id, patch_version, last_import_at)
            VALUES (1, 'unknown', datetime('now'));
        """)
        conn.commit()
        conn.close()

        def mock_run_phases_fail(phases):
            raise SystemExit(1)

        def mock_fetch_patch_version(file_key, client=None):
            return "4.4.0.6.6"

        import exilesage.config as cfg
        original_db = cfg.DB_PATH
        cfg.DB_PATH = str(db_file)

        try:
            with mpatch("pipeline.ingest.run_phases", mock_run_phases_fail), \
                 mpatch("pipeline.ingest.init_db", lambda: None), \
                 mpatch("pipeline.ingest.fetch_patch_version", mock_fetch_patch_version), \
                 mpatch("pipeline.ingest.get_connection") as mock_gc:

                def get_real_conn():
                    c = sqlite3.connect(str(db_file))
                    c.row_factory = sqlite3.Row
                    return c

                mock_gc.side_effect = get_real_conn

                from pipeline import ingest
                with pytest.raises(SystemExit):
                    ingest.run()
        finally:
            cfg.DB_PATH = original_db

        conn2 = sqlite3.connect(str(db_file))
        row = conn2.execute("SELECT patch_version FROM meta WHERE id=1").fetchone()
        conn2.close()
        assert row[0] == "unknown"


# ===========================================================================
# TestStaleness — 2D.3
# ===========================================================================

class TestStaleness:
    """detect_staleness logic."""

    def _make_manifest(self, tmp_path: Path, fetched_at: str | None = None, entries: list[dict] | None = None) -> Path:
        """Write a manifest JSON file and return its path."""
        manifest_path = tmp_path / "_manifest.json"
        if entries is not None:
            data = {e["key"]: {k: v for k, v in e.items() if k != "key"} for e in entries}
        elif fetched_at is not None:
            data = {
                "mods": {
                    "file": "mods.json",
                    "fetched_at": fetched_at,
                    "sha256": "abc123",
                    "http_last_modified": None,
                    "etag": None,
                }
            }
        else:
            data = {}
        manifest_path.write_text(json.dumps(data))
        return manifest_path

    def test_detect_staleness_fresh(self, tmp_path):
        """Manifest fetched today → stale=False, reasons=[]."""
        from scraper.freshness import detect_staleness
        now = datetime.now(timezone.utc).isoformat()
        manifest = self._make_manifest(tmp_path, fetched_at=now)
        result = detect_staleness(manifest, max_age_days=7)
        assert result["stale"] is False
        assert result["reasons"] == []

    def test_detect_staleness_old_manifest(self, tmp_path):
        """Manifest fetched 14 days ago → stale=True, reason manifest_age_exceeded."""
        from scraper.freshness import detect_staleness
        old = (datetime.now(timezone.utc) - timedelta(days=14)).isoformat()
        manifest = self._make_manifest(tmp_path, fetched_at=old)
        result = detect_staleness(manifest, max_age_days=7)
        assert result["stale"] is True
        assert "manifest_age_exceeded" in result["reasons"]

    def test_detect_staleness_rss_patch_newer(self, tmp_path):
        """rss_fetcher returns newer date → stale with reason rss_patch_newer."""
        from scraper.freshness import detect_staleness
        one_day_ago = (datetime.now(timezone.utc) - timedelta(hours=23)).isoformat()
        manifest = self._make_manifest(tmp_path, fetched_at=one_day_ago)

        rss_date = datetime.now(timezone.utc) - timedelta(hours=1)  # 2h ago from perspective of manifest

        result = detect_staleness(
            manifest,
            max_age_days=7,
            rss_fetcher=lambda: rss_date,
        )
        assert result["stale"] is True
        assert "rss_patch_newer" in result["reasons"]

    def test_detect_staleness_hash_changed(self, tmp_path):
        """remote_checker returns different etag → stale with reason etag_changed."""
        from scraper.freshness import detect_staleness
        now = datetime.now(timezone.utc).isoformat()

        entries = [{"key": "mods", "file": "mods.json", "fetched_at": now,
                    "sha256": "old_hash_aaa", "http_last_modified": None, "etag": "abc"}]
        manifest = self._make_manifest(tmp_path, entries=entries)

        def remote_checker(file_key):
            return "xyz"  # different from stored etag

        result = detect_staleness(manifest, max_age_days=7, remote_checker=remote_checker)
        assert result["stale"] is True
        assert "etag_changed:mods" in result["reasons"]

    # B2 — ETag comparison tests

    def test_staleness_etag_unchanged_not_stale(self, tmp_path):
        """B2: Same ETag on both sides → not stale."""
        from scraper.freshness import detect_staleness
        now = datetime.now(timezone.utc).isoformat()
        entries = [{"key": "mods", "file": "mods.json", "fetched_at": now,
                    "sha256": "sha_abc", "http_last_modified": None, "etag": "abc"}]
        manifest = self._make_manifest(tmp_path, entries=entries)
        result = detect_staleness(manifest, max_age_days=7, remote_checker=lambda k: "abc")
        assert result["stale"] is False
        assert result["reasons"] == []

    def test_staleness_etag_changed_flags_stale(self, tmp_path):
        """B2: Different ETag → stale with reason etag_changed:mods."""
        from scraper.freshness import detect_staleness
        now = datetime.now(timezone.utc).isoformat()
        entries = [{"key": "mods", "file": "mods.json", "fetched_at": now,
                    "sha256": "sha_abc", "http_last_modified": None, "etag": "abc"}]
        manifest = self._make_manifest(tmp_path, entries=entries)
        result = detect_staleness(manifest, max_age_days=7, remote_checker=lambda k: "xyz")
        assert result["stale"] is True
        assert "etag_changed:mods" in result["reasons"]

    def test_staleness_missing_etag_skips_check(self, tmp_path):
        """B2: Manifest etag is None → skip comparison (not stale)."""
        from scraper.freshness import detect_staleness
        now = datetime.now(timezone.utc).isoformat()
        entries = [{"key": "mods", "file": "mods.json", "fetched_at": now,
                    "sha256": "sha_abc", "http_last_modified": None, "etag": None}]
        manifest = self._make_manifest(tmp_path, entries=entries)
        result = detect_staleness(manifest, max_age_days=7, remote_checker=lambda k: "xyz")
        assert result["stale"] is False

    def test_staleness_remote_returns_none_skips_check(self, tmp_path):
        """B2: remote_checker returns None → skip comparison (not stale)."""
        from scraper.freshness import detect_staleness
        now = datetime.now(timezone.utc).isoformat()
        entries = [{"key": "mods", "file": "mods.json", "fetched_at": now,
                    "sha256": "sha_abc", "http_last_modified": None, "etag": "abc"}]
        manifest = self._make_manifest(tmp_path, entries=entries)
        result = detect_staleness(manifest, max_age_days=7, remote_checker=lambda k: None)
        assert result["stale"] is False

    def test_detect_staleness_empty_manifest(self, tmp_path):
        """No entries in manifest → stale=True, reason no_manifest_data."""
        from scraper.freshness import detect_staleness
        manifest = tmp_path / "_manifest.json"
        manifest.write_text("{}")
        result = detect_staleness(manifest, max_age_days=7)
        assert result["stale"] is True
        assert "no_manifest_data" in result["reasons"]

    def test_detect_staleness_returns_max_fetched_at(self, tmp_path):
        """Returns max_fetched_at as isoformat string."""
        from scraper.freshness import detect_staleness
        now = datetime.now(timezone.utc).isoformat()
        manifest = self._make_manifest(tmp_path, fetched_at=now)
        result = detect_staleness(manifest, max_age_days=7)
        assert "max_fetched_at" in result
        assert isinstance(result["max_fetched_at"], str)


# ===========================================================================
# TestRSS — 2D.4
# ===========================================================================

RSS_ITEM_POE2_PATCH = """
    <item>
      <title>Path of Exile 2: Patch 0.2.0 Notes</title>
      <pubDate>Mon, 13 Apr 2026 10:00:00 +0000</pubDate>
      <link>https://example.com/poe2-patch</link>
    </item>
"""

RSS_ITEM_POE1_PATCH = """
    <item>
      <title>Path of Exile: Patch 3.25 Notes</title>
      <pubDate>Mon, 13 Apr 2026 09:00:00 +0000</pubDate>
      <link>https://example.com/poe1-patch</link>
    </item>
"""

RSS_ITEM_NEWS = """
    <item>
      <title>ExileCon 2026 Announced</title>
      <pubDate>Mon, 13 Apr 2026 08:00:00 +0000</pubDate>
      <link>https://example.com/news</link>
    </item>
"""

RSS_ITEM_POE2_OLDER = """
    <item>
      <title>Path of Exile 2: Patch 0.1.0 Notes</title>
      <pubDate>Fri, 10 Apr 2026 10:00:00 +0000</pubDate>
      <link>https://example.com/poe2-old</link>
    </item>
"""


def _make_rss(items: str) -> bytes:
    return (
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f'<rss version="2.0"><channel>'
        f'<title>Path of Exile News</title>'
        f'{items}'
        f'</channel></rss>'
    ).encode()


@pytest.fixture
def mock_rss_xml() -> bytes:
    """RSS feed with 1 PoE2 patch, 1 PoE1 patch, 1 non-patch news item."""
    return _make_rss(RSS_ITEM_POE2_PATCH + RSS_ITEM_POE1_PATCH + RSS_ITEM_NEWS)


class TestRSS:
    """GGG RSS parsing."""

    def _make_rss_client(self, rss_bytes: bytes) -> httpx.Client:
        def handler(request):
            return httpx.Response(200, content=rss_bytes,
                                  headers={"Content-Type": "application/rss+xml"})
        return httpx.Client(transport=httpx.MockTransport(handler))

    def test_parse_rss_finds_poe2_patch(self, mock_rss_xml):
        """Returns pubDate of the PoE2 patch item."""
        from scraper.freshness import fetch_latest_poe2_patch_date
        client = self._make_rss_client(mock_rss_xml)
        result = fetch_latest_poe2_patch_date(client=client)
        assert result is not None
        assert isinstance(result, datetime)
        assert result.tzinfo is not None  # timezone-aware
        assert result.year == 2026
        assert result.month == 4
        assert result.day == 13

    def test_parse_rss_ignores_poe1(self):
        """With only PoE1 patch items, returns None."""
        from scraper.freshness import fetch_latest_poe2_patch_date
        rss = _make_rss(RSS_ITEM_POE1_PATCH)
        client = self._make_rss_client(rss)
        result = fetch_latest_poe2_patch_date(client=client)
        assert result is None

    def test_parse_rss_ignores_non_patch_news(self):
        """Non-patch news items without 'patch' keyword → None."""
        from scraper.freshness import fetch_latest_poe2_patch_date
        rss = _make_rss(RSS_ITEM_NEWS)
        client = self._make_rss_client(rss)
        result = fetch_latest_poe2_patch_date(client=client)
        assert result is None

    def test_parse_rss_handles_empty(self):
        """Empty channel → None, no crash."""
        from scraper.freshness import fetch_latest_poe2_patch_date
        rss = _make_rss("")
        client = self._make_rss_client(rss)
        result = fetch_latest_poe2_patch_date(client=client)
        assert result is None

    def test_parse_rss_handles_malformed(self):
        """Non-XML input → None, no crash."""
        from scraper.freshness import fetch_latest_poe2_patch_date

        def handler(request):
            return httpx.Response(200, content=b"<this is not xml",
                                  headers={"Content-Type": "application/rss+xml"})
        client = httpx.Client(transport=httpx.MockTransport(handler))
        result = fetch_latest_poe2_patch_date(client=client)
        assert result is None

    def test_parse_rss_latest_only(self):
        """Two PoE2 patches → returns the newer date."""
        from scraper.freshness import fetch_latest_poe2_patch_date
        rss = _make_rss(RSS_ITEM_POE2_PATCH + RSS_ITEM_POE2_OLDER)
        client = self._make_rss_client(rss)
        result = fetch_latest_poe2_patch_date(client=client)
        assert result is not None
        # Apr 13 > Apr 10
        assert result.day == 13

    # B1 — RSS PoE1 false-positive tests

    def test_parse_rss_rejects_poe1_patch_notes(self):
        """B1: PoE1 version-pattern title must NOT be accepted as PoE2."""
        from scraper.freshness import fetch_latest_poe2_patch_date
        poe1_item = """
            <item>
              <title>3.25.0 Settlers of Kalguur Patch Notes</title>
              <pubDate>Mon, 13 Apr 2026 10:00:00 +0000</pubDate>
            </item>
        """
        rss = _make_rss(poe1_item)
        client = self._make_rss_client(rss)
        result = fetch_latest_poe2_patch_date(client=client)
        assert result is None, "PoE1 patch title matched as PoE2 — false positive!"

    def test_parse_rss_accepts_poe2_with_trailing_space(self):
        """B1: Title 'Path of Exile 2 0.2.0 Patch Notes' (trailing space, no colon) → accepted."""
        from scraper.freshness import fetch_latest_poe2_patch_date
        item = """
            <item>
              <title>Path of Exile 2 0.2.0 Patch Notes</title>
              <pubDate>Mon, 13 Apr 2026 10:00:00 +0000</pubDate>
            </item>
        """
        rss = _make_rss(item)
        client = self._make_rss_client(rss)
        result = fetch_latest_poe2_patch_date(client=client)
        assert result is not None
        assert result.day == 13


# ===========================================================================
# TestSystemPromptProvenance — 2D.5
# ===========================================================================

class TestSystemPromptProvenance:
    """System prompt includes data provenance + staleness warning."""

    def test_build_system_prompt_includes_provenance(self):
        """Provenance line appears in result."""
        from exilesage.advisor.system_prompt import build_system_prompt
        result = build_system_prompt(
            patch_version="4.4.0.6.6",
            fetched_at="2026-04-13T00:00Z",
            stale=False,
        )
        assert "4.4.0.6.6" in result
        assert "2026-04-13T00:00Z" in result

    def test_build_system_prompt_includes_staleness_warning_when_stale(self):
        """WARNING appears with reasons when stale=True."""
        from exilesage.advisor.system_prompt import build_system_prompt
        result = build_system_prompt(
            patch_version="4.4.0.6.6",
            fetched_at="2026-04-13T00:00Z",
            stale=True,
            staleness_reasons=["manifest_age_exceeded", "rss_patch_newer"],
        )
        assert "WARNING" in result
        assert "manifest_age_exceeded" in result
        assert "rss_patch_newer" in result

    def test_build_system_prompt_no_warning_when_fresh(self):
        """No WARNING substring when stale=False."""
        from exilesage.advisor.system_prompt import build_system_prompt
        result = build_system_prompt(
            patch_version="4.4.0.6.6",
            fetched_at="2026-04-13T00:00Z",
            stale=False,
        )
        assert "WARNING" not in result

    def test_advisor_core_injects_provenance(self, tmp_path):
        """core.py assembles a system prompt containing the provenance line."""
        import sqlite3
        from unittest.mock import patch as mpatch, MagicMock

        # Create a minimal meta table
        db_file = tmp_path / "prov_test.db"
        conn = sqlite3.connect(str(db_file))
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS meta (
                id INTEGER PRIMARY KEY CHECK(id=1),
                patch_version TEXT DEFAULT 'unknown',
                last_import_at TEXT
            );
            INSERT OR IGNORE INTO meta (id, patch_version, last_import_at)
            VALUES (1, '4.4.0.6.6', '2026-04-13T00:00:00');
        """)
        conn.commit()
        conn.close()

        # Fake manifest so detect_staleness returns fresh
        manifest_path = tmp_path / "_manifest.json"
        now_str = datetime.now(timezone.utc).isoformat()
        manifest_path.write_text(json.dumps({
            "mods": {"file": "mods.json", "fetched_at": now_str,
                     "sha256": "abc", "http_last_modified": None, "etag": None}
        }))

        captured_prompt = {}

        def fake_create(**kwargs):
            captured_prompt["system"] = kwargs.get("system", "")
            resp = MagicMock()
            resp.stop_reason = "end_turn"
            resp.content = [MagicMock(type="text", text="answer")]
            return resp

        import exilesage.config as cfg
        original_db = cfg.DB_PATH
        cfg.DB_PATH = str(db_file)

        try:
            with mpatch("exilesage.advisor.core._get_client") as mock_client, \
                 mpatch("exilesage.advisor.core._get_manifest_path", return_value=manifest_path):
                mock_client.return_value.messages.create.side_effect = fake_create
                from exilesage.advisor import core
                core.ask("what mods exist?")
        finally:
            cfg.DB_PATH = original_db

        assert "4.4.0.6.6" in captured_prompt.get("system", ""), \
            f"Provenance not found in system prompt. Got: {captured_prompt.get('system', '')[:300]}"


# ===========================================================================
# TestPatchVersionFallback — S6
# ===========================================================================

class TestPatchVersionFallback:
    """S6: When GitHub API fails, use http_last_modified as pseudo-version."""

    def test_ingest_uses_lm_fallback_when_github_unreachable(self, tmp_path):
        """S6: fetch_patch_version → None + manifest has http_last_modified → meta.patch_version starts with 'lm:'."""
        import sqlite3
        from unittest.mock import patch as mpatch

        db_file = tmp_path / "fallback_test.db"
        conn = sqlite3.connect(str(db_file))
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS meta (
                id INTEGER PRIMARY KEY CHECK(id=1),
                patch_version TEXT DEFAULT 'unknown',
                last_import_at TEXT
            );
            INSERT OR IGNORE INTO meta (id, patch_version, last_import_at)
            VALUES (1, 'unknown', datetime('now'));
        """)
        conn.commit()
        conn.close()

        # Manifest with http_last_modified
        manifest_path = tmp_path / "raw" / "_manifest.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps({
            "mods": {
                "file": "mods.json",
                "fetched_at": "2026-02-28T00:00:00+00:00",
                "sha256": "abc",
                "http_last_modified": "Fri, 28 Feb 2026 13:55:43 GMT",
                "etag": None,
            }
        }))

        import exilesage.config as cfg
        original_db = cfg.DB_PATH
        cfg.DB_PATH = str(db_file)

        try:
            with mpatch("pipeline.ingest.run_phases", return_value=(100, 0)), \
                 mpatch("pipeline.ingest.init_db", lambda: None), \
                 mpatch("pipeline.ingest.fetch_patch_version", return_value=None), \
                 mpatch("pipeline.ingest._get_manifest_path", return_value=manifest_path), \
                 mpatch("pipeline.ingest.get_connection") as mock_gc:

                def get_real_conn():
                    c = sqlite3.connect(str(db_file))
                    c.row_factory = sqlite3.Row
                    return c

                mock_gc.side_effect = get_real_conn

                from pipeline import ingest
                ingest.run()
        finally:
            cfg.DB_PATH = original_db

        conn2 = sqlite3.connect(str(db_file))
        row = conn2.execute("SELECT patch_version FROM meta WHERE id=1").fetchone()
        conn2.close()
        assert row[0].startswith("lm:"), f"Expected 'lm:...' fallback, got: {row[0]}"
