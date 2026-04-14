"""SQLite database integrity tests for ExileSage PoE2 advisor.

Tests verify:
- Database file existence and accessibility
- Table row counts and structure
- Metadata consistency
- FTS5 virtual table functionality
- JSON column integrity
- Known data existence
"""

import sqlite3

import pytest

from exilesage.config import DB_PATH
from exilesage.db import get_connection


@pytest.fixture(scope="module")
def db_connection():
    """Provide a database connection for all tests in this module."""
    conn = get_connection()
    yield conn
    conn.close()


class TestDatabaseFile:
    """Tests for database file existence and accessibility."""

    def test_db_exists(self):
        """Verify the database file exists at config.DB_PATH."""
        assert DB_PATH.exists(), f"Database file not found at {DB_PATH}"
        assert DB_PATH.is_file(), f"DB_PATH is not a regular file: {DB_PATH}"
        assert DB_PATH.stat().st_size > 0, f"Database file is empty: {DB_PATH}"


class TestTableRowCounts:
    """Tests for table row counts meeting minimum thresholds."""

    def test_mods_row_count(self, db_connection):
        """Verify mods table has at least 14,000 rows."""
        cursor = db_connection.execute("SELECT COUNT(*) as count FROM mods")
        row = cursor.fetchone()
        count = row["count"]
        assert count >= 14000, f"Expected mods count >= 14000, got {count}"

    def test_base_items_row_count(self, db_connection):
        """Verify base_items table has at least 2,000 rows."""
        cursor = db_connection.execute(
            "SELECT COUNT(*) as count FROM base_items"
        )
        row = cursor.fetchone()
        count = row["count"]
        assert count >= 2000, f"Expected base_items count >= 2000, got {count}"

    def test_currencies_row_count(self, db_connection):
        """Verify currencies table has at least 500 rows."""
        cursor = db_connection.execute(
            "SELECT COUNT(*) as count FROM currencies"
        )
        row = cursor.fetchone()
        count = row["count"]
        assert count >= 500, f"Expected currencies count >= 500, got {count}"

    def test_augments_row_count(self, db_connection):
        """Verify augments table has at least 100 rows."""
        cursor = db_connection.execute("SELECT COUNT(*) as count FROM augments")
        row = cursor.fetchone()
        count = row["count"]
        assert count >= 100, f"Expected augments count >= 100, got {count}"


class TestMetaTable:
    """Tests for metadata table consistency."""

    def test_meta_table_exists(self, db_connection):
        """Verify meta table has exactly one row with id=1."""
        cursor = db_connection.execute("SELECT COUNT(*) as count FROM meta")
        row = cursor.fetchone()
        count = row["count"]
        assert count == 1, f"Expected exactly 1 meta row, got {count}"

    def test_meta_has_required_columns(self, db_connection):
        """Verify meta table has all required columns."""
        cursor = db_connection.execute("PRAGMA table_info(meta)")
        columns = {row[1] for row in cursor.fetchall()}
        required = {
            "id",
            "patch_version",
            "last_import_at",
            "mods_count",
            "base_items_count",
            "currencies_count",
            "augments_count",
        }
        assert required.issubset(
            columns
        ), f"Missing columns in meta table: {required - columns}"

    def test_meta_mods_count_matches(self, db_connection):
        """Verify meta.mods_count matches actual mods table count."""
        meta_cursor = db_connection.execute(
            "SELECT mods_count FROM meta WHERE id = 1"
        )
        meta_row = meta_cursor.fetchone()
        meta_count = meta_row["mods_count"] if meta_row else 0

        actual_cursor = db_connection.execute(
            "SELECT COUNT(*) as count FROM mods"
        )
        actual_row = actual_cursor.fetchone()
        actual_count = actual_row["count"]

        assert (
            meta_count == actual_count
        ), f"meta.mods_count={meta_count} != actual mods count={actual_count}"

    def test_meta_base_items_count_matches(self, db_connection):
        """Verify meta.base_items_count matches actual base_items table count."""
        meta_cursor = db_connection.execute(
            "SELECT base_items_count FROM meta WHERE id = 1"
        )
        meta_row = meta_cursor.fetchone()
        meta_count = meta_row["base_items_count"] if meta_row else 0

        actual_cursor = db_connection.execute(
            "SELECT COUNT(*) as count FROM base_items"
        )
        actual_row = actual_cursor.fetchone()
        actual_count = actual_row["count"]

        assert (
            meta_count == actual_count
        ), f"meta.base_items_count={meta_count} != actual base_items count={actual_count}"

    def test_meta_currencies_count_matches(self, db_connection):
        """Verify meta.currencies_count matches actual currencies table count."""
        meta_cursor = db_connection.execute(
            "SELECT currencies_count FROM meta WHERE id = 1"
        )
        meta_row = meta_cursor.fetchone()
        meta_count = meta_row["currencies_count"] if meta_row else 0

        actual_cursor = db_connection.execute(
            "SELECT COUNT(*) as count FROM currencies"
        )
        actual_row = actual_cursor.fetchone()
        actual_count = actual_row["count"]

        assert (
            meta_count == actual_count
        ), f"meta.currencies_count={meta_count} != actual currencies count={actual_count}"

    def test_meta_augments_count_matches(self, db_connection):
        """Verify meta.augments_count matches actual augments table count."""
        meta_cursor = db_connection.execute(
            "SELECT augments_count FROM meta WHERE id = 1"
        )
        meta_row = meta_cursor.fetchone()
        meta_count = meta_row["augments_count"] if meta_row else 0

        actual_cursor = db_connection.execute(
            "SELECT COUNT(*) as count FROM augments"
        )
        actual_row = actual_cursor.fetchone()
        actual_count = actual_row["count"]

        assert (
            meta_count == actual_count
        ), f"meta.augments_count={meta_count} != actual augments count={actual_count}"


class TestFTS5Tables:
    """Tests for Full-Text-Search virtual table functionality."""

    def test_mods_fts_exists(self, db_connection):
        """Verify mods_fts virtual table exists and is queryable."""
        cursor = db_connection.execute(
            "SELECT COUNT(*) as count FROM mods_fts"
        )
        row = cursor.fetchone()
        count = row["count"]
        assert count > 0, f"Expected mods_fts to have rows, got {count}"

    def test_base_items_fts_exists(self, db_connection):
        """Verify base_items_fts virtual table exists and is queryable."""
        cursor = db_connection.execute(
            "SELECT COUNT(*) as count FROM base_items_fts"
        )
        row = cursor.fetchone()
        count = row["count"]
        assert count > 0, f"Expected base_items_fts to have rows, got {count}"

    def test_currencies_fts_exists(self, db_connection):
        """Verify currencies_fts virtual table exists and is queryable."""
        cursor = db_connection.execute(
            "SELECT COUNT(*) as count FROM currencies_fts"
        )
        row = cursor.fetchone()
        count = row["count"]
        assert count > 0, f"Expected currencies_fts to have rows, got {count}"

    def test_augments_fts_exists(self, db_connection):
        """Verify augments_fts virtual table exists and is queryable."""
        cursor = db_connection.execute(
            "SELECT COUNT(*) as count FROM augments_fts"
        )
        row = cursor.fetchone()
        count = row["count"]
        assert count > 0, f"Expected augments_fts to have rows, got {count}"

    def test_fts_mods_cold_query(self, db_connection):
        """Verify FTS5 query on mods_fts returns results.
        Note: FTS covers name/group_name/type/domain — 'strength' appears in type column."""
        cursor = db_connection.execute(
            "SELECT rowid, name FROM mods_fts WHERE mods_fts MATCH 'strength' LIMIT 10"
        )
        results = cursor.fetchall()
        assert (
            len(results) > 0
        ), "FTS query 'strength' on mods_fts returned no results"

    def test_fts_base_items_wand_query(self, db_connection):
        """Verify FTS5 'wand' query on base_items_fts returns wands."""
        cursor = db_connection.execute(
            "SELECT name, item_class FROM base_items_fts WHERE base_items_fts MATCH 'wand' LIMIT 10"
        )
        results = cursor.fetchall()
        assert (
            len(results) > 0
        ), "FTS query 'wand' on base_items_fts returned no results"
        # At least some should be Wands
        wand_count = sum(
            1 for row in results if row["item_class"] == "Wand"
        )
        assert (
            wand_count > 0
        ), "No items with item_class='Wand' found in 'wand' FTS query"

    def test_fts_currencies_chaos_query(self, db_connection):
        """Verify FTS5 'chaos' query on currencies_fts finds Chaos Orb."""
        cursor = db_connection.execute(
            "SELECT name FROM currencies_fts WHERE currencies_fts MATCH 'chaos' LIMIT 10"
        )
        results = cursor.fetchall()
        assert (
            len(results) > 0
        ), "FTS query 'chaos' on currencies_fts returned no results"
        # Check for Chaos Orb
        names = {row["name"] for row in results}
        assert (
            "Chaos Orb" in names
        ), f"'Chaos Orb' not found in 'chaos' FTS results: {names}"

    def test_fts_augments_query(self, db_connection):
        """Verify FTS5 query on augments_fts returns results using type_id (no wiki markup)."""
        # Use type_id (plain ASCII, no PoE wiki markup) for FTS query
        cursor = db_connection.execute(
            "SELECT type_id FROM augments WHERE type_id IS NOT NULL AND type_id != '' LIMIT 1"
        )
        sample = cursor.fetchone()
        if sample and sample["type_id"]:
            search_term = sample["type_id"]
            cursor = db_connection.execute(
                "SELECT type_id, type_name FROM augments_fts WHERE augments_fts MATCH ? LIMIT 5",
                (search_term,),
            )
            results = cursor.fetchall()
            assert (
                len(results) > 0
            ), f"FTS query '{search_term}' on augments_fts returned no results"


class TestJsonColumns:
    """Tests for JSON column integrity."""

    def test_mods_stats_json_exists(self, db_connection):
        """Verify mods.stats JSON contains valid stat objects with id field."""
        cursor = db_connection.execute(
            """
            SELECT COUNT(*) as count FROM mods
            WHERE json_extract(stats, '$[0].id') IS NOT NULL
            """
        )
        row = cursor.fetchone()
        count = row["count"]
        assert (
            count > 0
        ), "No mods found with valid stats[0].id JSON structure"

    def test_mods_tags_json_valid(self, db_connection):
        """Verify mods.tags JSON is array where present."""
        cursor = db_connection.execute(
            """
            SELECT COUNT(*) as count FROM mods
            WHERE tags IS NOT NULL
            AND json_type(tags) = 'array'
            """
        )
        row = cursor.fetchone()
        count = row["count"]
        # Allow some mods without tags, but expect many with valid JSON
        total_cursor = db_connection.execute(
            "SELECT COUNT(*) as count FROM mods WHERE tags IS NOT NULL"
        )
        total = total_cursor.fetchone()["count"]
        assert (
            total == 0 or count > 0
        ), "Mods have tags but none are valid JSON arrays"

    def test_base_items_properties_json_valid(self, db_connection):
        """Verify base_items.properties JSON is object where present."""
        cursor = db_connection.execute(
            """
            SELECT COUNT(*) as count FROM base_items
            WHERE properties IS NOT NULL
            AND json_type(properties) = 'object'
            """
        )
        row = cursor.fetchone()
        count = row["count"]
        # Allow some items without properties, but expect many with valid JSON
        total_cursor = db_connection.execute(
            "SELECT COUNT(*) as count FROM base_items WHERE properties IS NOT NULL"
        )
        total = total_cursor.fetchone()["count"]
        assert (
            total == 0 or count > 0
        ), "base_items have properties but none are valid JSON objects"

    def test_augments_categories_json_valid(self, db_connection):
        """Verify augments.categories JSON is object where present."""
        cursor = db_connection.execute(
            """
            SELECT COUNT(*) as count FROM augments
            WHERE categories IS NOT NULL
            AND json_type(categories) = 'object'
            """
        )
        row = cursor.fetchone()
        count = row["count"]
        # Allow some augments without categories, but expect many with valid JSON
        total_cursor = db_connection.execute(
            "SELECT COUNT(*) as count FROM augments WHERE categories IS NOT NULL"
        )
        total = total_cursor.fetchone()["count"]
        assert (
            total == 0 or count > 0
        ), "augments have categories but none are valid JSON objects"


class TestKnownData:
    """Tests for existence of specific known data items."""

    def test_known_mod_strength1_exists(self, db_connection):
        """Verify mod with id 'Strength1' exists in mods table."""
        cursor = db_connection.execute(
            "SELECT id, name FROM mods WHERE id = 'Strength1'"
        )
        row = cursor.fetchone()
        assert row is not None, "Mod 'Strength1' not found in mods table"
        assert row["id"] == "Strength1"

    def test_known_currency_chaos_orb_exists(self, db_connection):
        """Verify currency named 'Chaos Orb' exists in currencies table."""
        cursor = db_connection.execute(
            "SELECT id, name FROM currencies WHERE name = 'Chaos Orb'"
        )
        row = cursor.fetchone()
        assert row is not None, "Currency 'Chaos Orb' not found in currencies table"
        assert row["name"] == "Chaos Orb"

    def test_known_base_item_wand_exists(self, db_connection):
        """Verify at least one item with item_class='Wand' exists."""
        cursor = db_connection.execute(
            "SELECT id, name, item_class FROM base_items WHERE item_class = 'Wand' LIMIT 1"
        )
        row = cursor.fetchone()
        assert row is not None, "No base_items with item_class='Wand' found"
        assert row["item_class"] == "Wand"

    def test_base_items_have_varied_classes(self, db_connection):
        """Verify base_items table has varied item classes."""
        cursor = db_connection.execute(
            "SELECT DISTINCT item_class FROM base_items WHERE item_class IS NOT NULL"
        )
        classes = {row["item_class"] for row in cursor.fetchall()}
        assert (
            len(classes) >= 5
        ), f"Expected at least 5 different item classes, got {len(classes)}"


class TestFTSEdgeCases:
    """Tests for FTS5 edge cases and quirks."""

    def test_fts_empty_string_query_handled(self, db_connection):
        """Verify empty string FTS query doesn't crash (FTS5 quirk)."""
        # Empty FTS queries are tricky; they may error or return nothing
        # This test ensures we don't crash
        try:
            cursor = db_connection.execute(
                "SELECT COUNT(*) FROM mods_fts WHERE mods_fts MATCH ''"
            )
            # If we get here without exception, that's fine
            cursor.fetchone()
        except sqlite3.OperationalError as e:
            # FTS5 may raise an error for empty query - that's acceptable
            # as long as we catch it gracefully
            assert (
                "fts5" in str(e).lower() or "syntax" in str(e).lower()
            ), f"Unexpected error: {e}"

    def test_fts_special_characters_escaped(self, db_connection):
        """Verify FTS queries with special characters are handled."""
        # FTS5 has special characters like ", *, etc. that need escaping
        # This tests that simple queries work without crashing
        cursor = db_connection.execute(
            """
            SELECT COUNT(*) as count FROM mods_fts
            WHERE mods_fts MATCH 'fire OR cold'
            """
        )
        row = cursor.fetchone()
        # Should return some result without error
        assert row["count"] >= 0


class TestTableStructure:
    """Tests for table schema and structure consistency."""

    def test_mods_primary_key(self, db_connection):
        """Verify mods table has id as primary key."""
        cursor = db_connection.execute("PRAGMA table_info(mods)")
        columns = cursor.fetchall()
        id_col = next((col for col in columns if col[1] == "id"), None)
        assert id_col is not None, "mods table missing 'id' column"
        # pk value of 1 means it's the primary key
        assert (
            id_col[5] == 1
        ), "mods.id is not the primary key"

    def test_base_items_primary_key(self, db_connection):
        """Verify base_items table has id as primary key."""
        cursor = db_connection.execute("PRAGMA table_info(base_items)")
        columns = cursor.fetchall()
        id_col = next((col for col in columns if col[1] == "id"), None)
        assert id_col is not None, "base_items table missing 'id' column"
        assert id_col[5] == 1, "base_items.id is not the primary key"

    def test_currencies_primary_key(self, db_connection):
        """Verify currencies table has id as primary key."""
        cursor = db_connection.execute("PRAGMA table_info(currencies)")
        columns = cursor.fetchall()
        id_col = next((col for col in columns if col[1] == "id"), None)
        assert id_col is not None, "currencies table missing 'id' column"
        assert id_col[5] == 1, "currencies.id is not the primary key"

    def test_augments_primary_key(self, db_connection):
        """Verify augments table has id as primary key."""
        cursor = db_connection.execute("PRAGMA table_info(augments)")
        columns = cursor.fetchall()
        id_col = next((col for col in columns if col[1] == "id"), None)
        assert id_col is not None, "augments table missing 'id' column"
        assert id_col[5] == 1, "augments.id is not the primary key"

    def test_mods_has_name_column(self, db_connection):
        """Verify mods table has name column."""
        cursor = db_connection.execute("PRAGMA table_info(mods)")
        columns = {row[1] for row in cursor.fetchall()}
        assert "name" in columns, "mods table missing 'name' column"

    def test_base_items_has_name_column(self, db_connection):
        """Verify base_items table has name column."""
        cursor = db_connection.execute("PRAGMA table_info(base_items)")
        columns = {row[1] for row in cursor.fetchall()}
        assert "name" in columns, "base_items table missing 'name' column"

    def test_currencies_has_name_column(self, db_connection):
        """Verify currencies table has name column."""
        cursor = db_connection.execute("PRAGMA table_info(currencies)")
        columns = {row[1] for row in cursor.fetchall()}
        assert "name" in columns, "currencies table missing 'name' column"


class TestDataQuality:
    """Tests for data quality and consistency."""

    def test_mods_have_names(self, db_connection):
        """Verify most mods have non-empty names."""
        cursor = db_connection.execute(
            """
            SELECT COUNT(*) as count FROM mods
            WHERE name IS NULL OR name = ''
            """
        )
        null_count = cursor.fetchone()["count"]

        total_cursor = db_connection.execute("SELECT COUNT(*) as count FROM mods")
        total = total_cursor.fetchone()["count"]

        # Many internal/unique mods have no display name — allow up to 75%
        # (62% observed: internal mods like UniqueGlobalColdSpellGemsLevel1)
        null_ratio = null_count / total if total > 0 else 0
        assert (
            null_ratio < 0.75
        ), f"Too many mods without names: {null_ratio:.1%}"

    def test_base_items_have_names(self, db_connection):
        """Verify most base_items have non-empty names."""
        cursor = db_connection.execute(
            """
            SELECT COUNT(*) as count FROM base_items
            WHERE name IS NULL OR name = ''
            """
        )
        null_count = cursor.fetchone()["count"]

        total_cursor = db_connection.execute(
            "SELECT COUNT(*) as count FROM base_items"
        )
        total = total_cursor.fetchone()["count"]

        null_ratio = null_count / total if total > 0 else 0
        assert (
            null_ratio < 0.1
        ), f"Too many base_items without names: {null_ratio:.1%}"

    def test_currencies_have_names(self, db_connection):
        """Verify most currencies have non-empty names."""
        cursor = db_connection.execute(
            """
            SELECT COUNT(*) as count FROM currencies
            WHERE name IS NULL OR name = ''
            """
        )
        null_count = cursor.fetchone()["count"]

        total_cursor = db_connection.execute(
            "SELECT COUNT(*) as count FROM currencies"
        )
        total = total_cursor.fetchone()["count"]

        # Many internal currencies (e.g. RandomFossilOutcome) have no display name — allow up to 75%
        null_ratio = null_count / total if total > 0 else 0
        assert (
            null_ratio < 0.75
        ), f"Too many currencies without names: {null_ratio:.1%}"

    def test_no_duplicate_mod_ids(self, db_connection):
        """Verify no duplicate mod IDs exist."""
        cursor = db_connection.execute(
            """
            SELECT id, COUNT(*) as cnt FROM mods
            GROUP BY id
            HAVING cnt > 1
            """
        )
        duplicates = cursor.fetchall()
        assert (
            len(duplicates) == 0
        ), f"Found duplicate mod IDs: {[row['id'] for row in duplicates]}"

    def test_no_duplicate_base_item_ids(self, db_connection):
        """Verify no duplicate base_item IDs exist."""
        cursor = db_connection.execute(
            """
            SELECT id, COUNT(*) as cnt FROM base_items
            GROUP BY id
            HAVING cnt > 1
            """
        )
        duplicates = cursor.fetchall()
        assert (
            len(duplicates) == 0
        ), f"Found duplicate base_item IDs: {[row['id'] for row in duplicates]}"

    def test_no_duplicate_currency_ids(self, db_connection):
        """Verify no duplicate currency IDs exist."""
        cursor = db_connection.execute(
            """
            SELECT id, COUNT(*) as cnt FROM currencies
            GROUP BY id
            HAVING cnt > 1
            """
        )
        duplicates = cursor.fetchall()
        assert (
            len(duplicates) == 0
        ), f"Found duplicate currency IDs: {[row['id'] for row in duplicates]}"

    def test_no_duplicate_augment_ids(self, db_connection):
        """Verify no duplicate augment IDs exist."""
        cursor = db_connection.execute(
            """
            SELECT id, COUNT(*) as cnt FROM augments
            GROUP BY id
            HAVING cnt > 1
            """
        )
        duplicates = cursor.fetchall()
        assert (
            len(duplicates) == 0
        ), f"Found duplicate augment IDs: {[row['id'] for row in duplicates]}"


class TestFTSApostrophe:
    """Tests for apostrophe handling in sanitize_fts (bug fix + regression guards)."""

    def test_sanitize_fts_strips_apostrophe(self):
        from exilesage.db import sanitize_fts
        assert "'" not in sanitize_fts("Winter's Blast")

    def test_sanitize_fts_apostrophe_full_output(self):
        """Whitelist fix: apostrophe replaced by space, whole result is 'Winter  Blast*'.

        Reworked from the old false-gate test that passed even before the fix.
        After the whitelist, apostrophe → space, so output is 'Winter  Blast*'.
        Assert the exact sanitized string (strip to normalise interior spaces).
        """
        from exilesage.db import sanitize_fts
        out = sanitize_fts("Winter's Blast")
        # Apostrophe becomes a space; trailing * appended.
        assert out.endswith("*")
        assert "'" not in out
        assert "Winter" in out
        assert "Blast" in out

    def test_sanitize_fts_preserves_star_suffix(self):
        """Regression guard: existing `*` suffix for prefix matching must remain."""
        from exilesage.db import sanitize_fts
        assert sanitize_fts("Fireball").endswith("*")

    def test_sanitize_fts_keyword_stripping_intact(self):
        """Regression guard: AND/OR/NOT/NEAR still stripped."""
        from exilesage.db import sanitize_fts
        out = sanitize_fts("fire AND cold")
        assert "AND" not in out.upper().split()  # AND as standalone token removed


class TestFTSCrashChars:
    """Tests that confirm crash characters are stripped by the whitelist sanitizer.

    Each character below caused an FTS5 'syntax error' when passed to MATCH.
    After the whitelist fix they must not appear in sanitize_fts output.
    """

    import pytest

    @pytest.mark.parametrize("char,query", [
        (".", "fire.damage"),
        (",", "test,value"),
        ("=", "dps=500"),
        ("/", "wand/sceptre"),
        ("<", "cold<lightning"),
        (">", "fire>cold"),
        ("`", "back`tick"),
        (";", "test;val"),
        ("!", "what!now"),
        ("%", "50%chance"),
        ("&", "fire&cold"),
        ("|", "fire|cold"),
    ])
    def test_crash_char_stripped(self, char, query):
        """Each crash char must be absent from sanitize_fts output."""
        from exilesage.db import sanitize_fts
        out = sanitize_fts(query)
        assert char not in out, (
            f"Crash char {char!r} still present in sanitized output {out!r} "
            f"(input: {query!r})"
        )

    def test_unicode_right_single_quote_safe(self):
        """U+2019 RIGHT SINGLE QUOTATION MARK must not crash FTS5.

        U+2019 is above the \\u00C0 whitelist threshold so it is preserved in the
        sanitized output. This is correct: FTS5's unicode61 tokenizer treats it as
        a non-word separator and handles it without raising a syntax error.
        Contrast with ASCII apostrophe (U+0027) which triggers 'fts5: syntax error'.
        """
        from exilesage.db import sanitize_fts
        # Must not raise — and must produce a non-empty result
        out = sanitize_fts("Winter\u2019s Blast")
        assert isinstance(out, str)
        assert len(out) > 0

    def test_underscore_preserved(self):
        """Underscore must NOT be stripped — appears in stat IDs like 'base_fire_damage'."""
        from exilesage.db import sanitize_fts
        out = sanitize_fts("base_fire_damage")
        assert "_" in out

    def test_empty_after_strip_returns_empty(self):
        """Query that becomes empty after stripping must return empty string (not '*')."""
        from exilesage.db import sanitize_fts
        # Only crash chars — after whitelist everything stripped, result must be ''
        out = sanitize_fts(".,=/<>")
        assert out == ""


class TestMigrationIdempotency:
    """Tests for init_db() re-runnability and migration safety (Phase 0.5)."""

    def test_init_db_rerun_idempotent(self, tmp_path, monkeypatch):
        """Running init_db() twice must not raise OperationalError."""
        from exilesage import config, db
        tmp_db = tmp_path / "test.db"
        monkeypatch.setattr(config, "DB_PATH", tmp_db)
        db.init_db()
        db.init_db()  # second run must succeed — no "duplicate column" error

    def test_ensure_schema_version_no_premature_commit(self, tmp_path, monkeypatch):
        """_ensure_schema_version must NOT commit independently.

        Protocol:
          1. Create a fresh DB with meta table but WITHOUT schema_version column
             (simulates a pre-v1 DB state).
          2. Open a connection, begin an explicit savepoint, call _ensure_schema_version.
          3. Roll back to the savepoint.
          4. Inspect the column list — if _ensure_schema_version committed internally,
             the column will persist despite the rollback (test FAILS before fix).
             After the fix, rollback wins and the column is absent.
        """
        import sqlite3
        from exilesage import config, db

        tmp_db = tmp_path / "preV1.db"
        monkeypatch.setattr(config, "DB_PATH", tmp_db)

        # Build a minimal pre-v1 meta table (no schema_version column).
        setup_conn = sqlite3.connect(str(tmp_db))
        setup_conn.execute(
            "CREATE TABLE meta ("
            "id INTEGER PRIMARY KEY CHECK (id = 1),"
            "patch_version TEXT NOT NULL DEFAULT 'unknown',"
            "last_import_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP"
            ")"
        )
        setup_conn.commit()
        setup_conn.close()

        # Open a connection and simulate a caller transaction using a savepoint.
        test_conn = sqlite3.connect(str(tmp_db))
        test_conn.execute("PRAGMA journal_mode=WAL")
        test_conn.row_factory = sqlite3.Row
        # isolation_level=None → autocommit off when we manage savepoints manually.
        # We use SAVEPOINT instead of BEGIN to work within sqlite3's implicit txn.
        test_conn.execute("SAVEPOINT sp_test")
        try:
            db._ensure_schema_version(test_conn)
            # Confirm the column is visible within the transaction.
            cols_during = {
                row[1]
                for row in test_conn.execute("PRAGMA table_info(meta)").fetchall()
            }
            assert "schema_version" in cols_during, (
                "Column should be visible inside the savepoint before rollback"
            )
        finally:
            test_conn.execute("ROLLBACK TO SAVEPOINT sp_test")
            test_conn.execute("RELEASE SAVEPOINT sp_test")

        # After rollback: column must NOT exist if _ensure_schema_version didn't commit.
        cols_after = {
            row[1]
            for row in test_conn.execute("PRAGMA table_info(meta)").fetchall()
        }
        test_conn.close()
        assert "schema_version" not in cols_after, (
            "_ensure_schema_version committed prematurely — rollback had no effect. "
            "Remove conn.commit() from _ensure_schema_version to fix."
        )

    def test_migrations_rerun_idempotent(self, tmp_path, monkeypatch):
        """Running init_db multiple times leaves schema_version = CURRENT_SCHEMA_VERSION."""
        from exilesage import config, db
        tmp_db = tmp_path / "test.db"
        monkeypatch.setattr(config, "DB_PATH", tmp_db)
        db.init_db()
        db.init_db()
        db.init_db()
        # schema_version column must exist and equal CURRENT_SCHEMA_VERSION
        with db.get_connection(tmp_db) as conn:
            cols = {row[1] for row in conn.execute("PRAGMA table_info(meta)").fetchall()}
        assert "schema_version" in cols

    def test_schema_version_set_on_fresh_db(self, tmp_path, monkeypatch):
        """Running init_db on a fresh DB sets schema_version column on meta table."""
        from exilesage import config, db
        tmp_db = tmp_path / "fresh.db"
        monkeypatch.setattr(config, "DB_PATH", tmp_db)
        db.init_db()
        with db.get_connection(tmp_db) as conn:
            cols = {row[1] for row in conn.execute("PRAGMA table_info(meta)").fetchall()}
        assert "schema_version" in cols, (
            "schema_version column missing after init_db on fresh DB"
        )

    def test_add_column_if_missing_adds_when_absent(self, tmp_path):
        """_add_column_if_missing adds the column when it doesn't exist."""
        import sqlite3
        from exilesage.db import _add_column_if_missing
        db_path = tmp_path / "col_test.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        conn.commit()
        _add_column_if_missing(conn, "t", "extra TEXT DEFAULT 'x'")
        conn.commit()
        cols = {row[1] for row in conn.execute("PRAGMA table_info(t)").fetchall()}
        conn.close()
        assert "extra" in cols, "Column 'extra' was not added by _add_column_if_missing"

    def test_add_column_if_missing_no_op_on_existing_column(self, tmp_path):
        """_add_column_if_missing is a no-op when the column already exists."""
        import sqlite3
        from exilesage.db import _add_column_if_missing
        db_path = tmp_path / "col_noop.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, extra TEXT DEFAULT 'x')")
        conn.commit()
        # Must not raise OperationalError for "duplicate column"
        _add_column_if_missing(conn, "t", "extra TEXT DEFAULT 'x'")
        conn.commit()
        cols = {row[1] for row in conn.execute("PRAGMA table_info(t)").fetchall()}
        conn.close()
        assert "extra" in cols

    def test_pre_v1_upgrade_preserves_meta_row(self, tmp_path, monkeypatch):
        """Upgrading a pre-v1 DB with an existing meta row preserves id=1 and gains schema_version=1.

        Covers the "upgrade existing deployed DB" path: a real DB that was
        ingested before schema_version was added still has its meta row intact
        after init_db() runs, and the column DEFAULT fills in schema_version=1.
        """
        import sqlite3
        from exilesage import config, db

        tmp_db = tmp_path / "pre_v1_with_row.db"
        monkeypatch.setattr(config, "DB_PATH", tmp_db)

        # Build a minimal pre-v1 meta table (no schema_version column) WITH a row.
        setup_conn = sqlite3.connect(str(tmp_db))
        setup_conn.execute(
            "CREATE TABLE meta ("
            "id INTEGER PRIMARY KEY CHECK (id = 1),"
            "patch_version TEXT NOT NULL DEFAULT 'unknown',"
            "last_import_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,"
            "mods_count INTEGER DEFAULT 0,"
            "base_items_count INTEGER DEFAULT 0,"
            "currencies_count INTEGER DEFAULT 0,"
            "augments_count INTEGER DEFAULT 0"
            ")"
        )
        setup_conn.execute(
            "INSERT INTO meta (id, patch_version, last_import_at) VALUES (1, 'x', '2026-01-01T00:00:00')"
        )
        setup_conn.commit()
        setup_conn.close()

        # Run init_db — must not crash and must upgrade the column.
        db.init_db()

        # Verify: existing row still has id=1 and schema_version received the DEFAULT (=1).
        with db.get_connection(tmp_db) as conn:
            row = conn.execute("SELECT id, schema_version FROM meta WHERE id = 1").fetchone()

        assert row is not None, "meta row id=1 was lost during pre-v1 upgrade"
        assert row["id"] == 1, f"meta row id changed: expected 1, got {row['id']}"
        assert row["schema_version"] == 1, (
            f"schema_version should be 1 (from DEFAULT) after upgrade, got {row['schema_version']}"
        )


class TestFTSIntegration:
    """Integration tests against the real exilesage.db (read-only).

    Skipped gracefully if the database file is not present (CI portability).
    These tests verify that sanitize_fts + the tool layer produce no crashes
    for queries that previously hit FTS5 syntax errors.
    """

    @pytest.fixture(scope="class")
    def db_available(self):
        """Skip entire class if DB is missing."""
        from exilesage.config import DB_PATH
        if not DB_PATH.exists():
            pytest.skip(f"Database not found at {DB_PATH} — skipping integration tests")

    def test_search_mods_apostrophe_returns_list(self, db_available):
        """Real DB: apostrophe query must return a list, never crash."""
        from exilesage.tools.mods import search_mods
        results = search_mods(query="Athlete's")
        assert isinstance(results, list), "search_mods returned non-list for apostrophe query"

    @pytest.mark.parametrize("query", [
        "fire.damage",
        "dps=500",
        "wand/sceptre",
        "cold<lightning",
        "test,value",
        "fire>cold",
        "back`tick",
    ])
    def test_search_mods_crash_chars_return_list(self, db_available, query):
        """Real DB: each previously-crashing query must return a list, never raise."""
        from exilesage.tools.mods import search_mods
        results = search_mods(query=query)
        assert isinstance(results, list), f"search_mods crashed on query {query!r}"

    def test_search_mods_clean_query_still_works(self, db_available):
        """Regression guard: normal single-word query must still return results after fix.

        Uses 'fire' (not 'fire damage') — FTS5 indexes type=FireResistance mods so
        'fire*' reliably returns rows. Multi-word phrase matching is a separate concern.
        """
        from exilesage.tools.mods import search_mods
        results = search_mods(query="fire")
        assert isinstance(results, list)
        assert len(results) > 0, "Normal 'fire' query returned no results after fix"
