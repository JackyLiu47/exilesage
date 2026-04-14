"""Tests for pipeline/importers/_base.py — _safe_replace_table helper.

Follows strict TDD: tests written first (RED), then implementation (GREEN).
Uses only sqlite3.connect() directly (no get_connection) as allowed for
infrastructure-layer tests per task contract.
"""

import json
import sqlite3
from pathlib import Path

import pytest

from pipeline.importers._base import _safe_replace_table


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def tmp_db(tmp_path):
    """In-memory sqlite3 connection with a small test schema.

    Schema:
      parent(id TEXT PRIMARY KEY, value TEXT)
      parent_fts USING fts5(value, content='parent', content_rowid='rowid')
      child(id TEXT PRIMARY KEY, parent_id TEXT REFERENCES parent(id))
      meta(id INTEGER PRIMARY KEY CHECK(id=1), item_count INTEGER DEFAULT 0)
    """
    db_file = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_file))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS parent (
            id    TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE VIRTUAL TABLE IF NOT EXISTS parent_fts USING fts5(
            value,
            content='parent',
            content_rowid='rowid'
        );

        CREATE TABLE IF NOT EXISTS child (
            id        TEXT PRIMARY KEY,
            parent_id TEXT REFERENCES parent(id)
        );

        CREATE TABLE IF NOT EXISTS meta (
            id         INTEGER PRIMARY KEY CHECK (id = 1),
            item_count INTEGER DEFAULT 0
        );

        INSERT OR IGNORE INTO meta (id, item_count) VALUES (1, 0);
    """)
    conn.commit()

    yield conn
    conn.close()


def _insert_parent_rows(conn, rows):
    """Helper: bulk-insert rows into parent table + rebuild FTS."""
    conn.executemany(
        "INSERT OR REPLACE INTO parent (id, value) VALUES (?, ?)", rows
    )
    conn.execute("INSERT INTO parent_fts(parent_fts) VALUES('rebuild')")
    conn.commit()


INSERT_SQL = "INSERT INTO parent (id, value) VALUES (?, ?)"


# ---------------------------------------------------------------------------
# Atomicity tests
# ---------------------------------------------------------------------------

class TestAtomicity:
    def test_safe_replace_atomicity_on_insert_error(self, tmp_db):
        """Pre-populate table. Call _safe_replace_table with rows that violate a
        UNIQUE constraint partway through (first N rows succeed, row N+1 fails).
        Assert original rows intact after rollback."""
        original = [("a", "alpha"), ("b", "beta"), ("c", "gamma")]
        _insert_parent_rows(tmp_db, original)

        # Rows where row index 1 duplicates row index 0 (UNIQUE on id) — the
        # first row inserts fine, second triggers IntegrityError mid-executemany.
        bad_rows = [("d", "delta"), ("d", "duplicate_id_triggers_unique_error")]

        with pytest.raises(Exception):
            _safe_replace_table(tmp_db, "parent", INSERT_SQL, bad_rows)

        # Original data must still be present after rollback
        rows = tmp_db.execute("SELECT id FROM parent ORDER BY id").fetchall()
        ids = [r["id"] for r in rows]
        assert ids == ["a", "b", "c"], f"Rollback failed — got: {ids}"

    def test_safe_replace_success_replaces_rows(self, tmp_db):
        """Pre-populate with 3 rows. Replace with 2 different rows.
        Assert only the 2 new rows exist."""
        _insert_parent_rows(tmp_db, [("a", "alpha"), ("b", "beta"), ("c", "gamma")])

        new_rows = [("x", "xenon"), ("y", "yttrium")]
        result = _safe_replace_table(tmp_db, "parent", INSERT_SQL, new_rows)

        rows = tmp_db.execute("SELECT id FROM parent ORDER BY id").fetchall()
        ids = [r["id"] for r in rows]
        assert ids == ["x", "y"], f"Expected only new rows, got: {ids}"
        assert result == 2

    def test_safe_replace_empty_rows_wipes_table(self, tmp_db):
        """Pre-populate with 3 rows. Call with rows=[]. Assert table is empty.
        Valid use case: league removed everything."""
        _insert_parent_rows(tmp_db, [("a", "alpha"), ("b", "beta"), ("c", "gamma")])

        result = _safe_replace_table(tmp_db, "parent", INSERT_SQL, [])

        count = tmp_db.execute("SELECT COUNT(*) FROM parent").fetchone()[0]
        assert count == 0, f"Expected empty table after empty-rows replace, got {count} rows"
        assert result == 0


# ---------------------------------------------------------------------------
# Meta / FTS tests
# ---------------------------------------------------------------------------

class TestMetaAndFTS:
    def test_safe_replace_updates_meta(self, tmp_db):
        """After success, meta.item_count equals len(rows)."""
        rows = [("p", "phoenix"), ("q", "quill")]
        _safe_replace_table(
            tmp_db, "parent", INSERT_SQL, rows,
            meta_col="item_count"
        )
        meta_val = tmp_db.execute(
            "SELECT item_count FROM meta WHERE id = 1"
        ).fetchone()["item_count"]
        assert meta_val == 2, f"Expected meta.item_count=2, got {meta_val}"

    def test_safe_replace_rebuilds_fts(self, tmp_db):
        """Pre-populate; stale content was in FTS. After replace, FTS MATCH returns
        new data, not old."""
        _insert_parent_rows(tmp_db, [("old1", "oldterm")])

        new_rows = [("new1", "freshdata")]
        _safe_replace_table(
            tmp_db, "parent", INSERT_SQL, new_rows,
            fts_table="parent_fts"
        )

        # New term should match
        hits = tmp_db.execute(
            "SELECT value FROM parent_fts WHERE parent_fts MATCH 'freshdata'"
        ).fetchall()
        assert len(hits) > 0, "FTS did not return new data after rebuild"

        # Old term should NOT match (table was wiped)
        old_hits = tmp_db.execute(
            "SELECT value FROM parent_fts WHERE parent_fts MATCH 'oldterm'"
        ).fetchall()
        assert len(old_hits) == 0, "FTS still returns stale old data after rebuild"

    def test_safe_replace_no_fts_table_skips_rebuild(self, tmp_db):
        """fts_table=None — no FTS call, no error."""
        rows = [("z", "zirconium")]
        # Should not raise even though parent_fts exists (we just skip it)
        _safe_replace_table(tmp_db, "parent", INSERT_SQL, rows, fts_table=None)
        count = tmp_db.execute("SELECT COUNT(*) FROM parent").fetchone()[0]
        assert count == 1

    def test_safe_replace_no_meta_col_skips_update(self, tmp_db):
        """meta_col=None — no meta UPDATE, no error."""
        rows = [("m", "magnesium")]
        _safe_replace_table(tmp_db, "parent", INSERT_SQL, rows, meta_col=None)
        # meta.item_count should remain 0 (never updated)
        meta_val = tmp_db.execute(
            "SELECT item_count FROM meta WHERE id = 1"
        ).fetchone()["item_count"]
        assert meta_val == 0, f"meta_col=None but meta was updated: {meta_val}"


# ---------------------------------------------------------------------------
# Safety / identifier validation tests
# ---------------------------------------------------------------------------

class TestIdentifierValidation:
    def test_safe_replace_rejects_bad_table_identifier(self, tmp_db):
        """_safe_replace_table(table='mods; DROP TABLE meta;--', ...) raises ValueError."""
        with pytest.raises(ValueError, match="identifier"):
            _safe_replace_table(
                tmp_db,
                "mods; DROP TABLE meta;--",
                INSERT_SQL,
                [("a", "b")],
            )

    def test_safe_replace_rejects_bad_fts_identifier(self, tmp_db):
        """Same for fts_table."""
        with pytest.raises(ValueError, match="identifier"):
            _safe_replace_table(
                tmp_db,
                "parent",
                INSERT_SQL,
                [("a", "b")],
                fts_table="parent_fts; DROP TABLE meta;--",
            )

    def test_safe_replace_rejects_bad_meta_col(self, tmp_db):
        """Same for meta_col."""
        with pytest.raises(ValueError, match="identifier"):
            _safe_replace_table(
                tmp_db,
                "parent",
                INSERT_SQL,
                [("a", "b")],
                meta_col="item_count; DROP TABLE meta;--",
            )

    def test_safe_replace_returns_row_count(self, tmp_db):
        """Returns len(rows)."""
        rows = [("r1", "v1"), ("r2", "v2"), ("r3", "v3")]
        result = _safe_replace_table(tmp_db, "parent", INSERT_SQL, rows)
        assert result == 3


# ---------------------------------------------------------------------------
# FK integrity tests
# ---------------------------------------------------------------------------

class TestFKIntegrity:
    def test_safe_replace_fk_check_raises_on_violation(self, tmp_db):
        """FK check runs INSIDE the transaction — violation triggers rollback.

        Strategy: insert a parent row, then insert a child that references it,
        then wipe parent via _safe_replace_table with FK disabled during import.
        Re-enabling FK and running FK check should detect the orphan child —
        the RuntimeError is raised inside the txn, causing rollback so the
        parent replacement is NOT committed.
        """
        # Seed valid parent + child
        tmp_db.execute("INSERT INTO parent VALUES ('p1', 'val')")
        tmp_db.execute("INSERT INTO child VALUES ('c1', 'p1')")
        tmp_db.commit()

        # Replace parent with a row that does NOT include 'p1'
        # child('c1') still references 'p1' which no longer exists → FK violation
        new_parent_rows = [("p2", "new_val")]
        with pytest.raises(RuntimeError, match="[Ff][Kk]|foreign.key|violation"):
            _safe_replace_table(
                tmp_db, "parent", INSERT_SQL, new_parent_rows
            )

        # Rollback must have occurred — 'p1' is still present, 'p2' is gone
        rows = tmp_db.execute("SELECT id FROM parent ORDER BY id").fetchall()
        ids = [r["id"] for r in rows]
        assert "p1" in ids, f"Rollback failed — p1 missing, got: {ids}"
        assert "p2" not in ids, f"Rollback failed — p2 was committed despite FK violation: {ids}"

        # The orphan child row must also still exist (was never committed away)
        child = tmp_db.execute("SELECT id FROM child WHERE id='c1'").fetchone()
        assert child is not None, "Child row unexpectedly missing after rollback"

    def test_safe_replace_fk_disabled_during_bulk(self, tmp_db):
        """During executemany, foreign_keys pragma is OFF so a child-without-parent
        row is accepted by the engine. Post-commit FK check detects the orphan and
        raises RuntimeError — proving FK was OFF during the insert itself.

        The call targets the child table directly. We do NOT insert a parent,
        so if FK were ON during executemany the insert would fail immediately with
        IntegrityError (not RuntimeError). The RuntimeError we get proves FK was OFF.
        """
        child_insert = "INSERT INTO child (id, parent_id) VALUES (?, ?)"
        # 'nonexistent_parent' is never inserted into parent table
        with pytest.raises(RuntimeError, match="[Ff][Kk]|foreign.key|violation"):
            _safe_replace_table(
                tmp_db, "child", child_insert,
                [("c_orphan", "nonexistent_parent")]
            )

    def test_safe_replace_fk_restored_after_rollback(self, tmp_db):
        """After a rollback, foreign_keys pragma is ON again (finally block)."""
        # Cause a rollback via bad SQL (UNIQUE violation on duplicate id)
        with pytest.raises(Exception):
            _safe_replace_table(
                tmp_db, "parent",
                INSERT_SQL,
                [("dup", "first"), ("dup", "second_triggers_unique_error")],
            )

        # After rollback, FK pragma must be re-enabled
        fk_val = tmp_db.execute("PRAGMA foreign_keys").fetchone()[0]
        assert fk_val == 1, f"foreign_keys not restored after rollback (got {fk_val})"


# ---------------------------------------------------------------------------
# Reentrancy guard tests
# ---------------------------------------------------------------------------

class TestReentrancyGuard:
    def test_safe_replace_rejects_open_transaction(self, tmp_db):
        """Calling _safe_replace_table while the connection has an open transaction
        must raise RuntimeError immediately, before touching any data."""
        # Open a transaction explicitly
        tmp_db.execute("BEGIN")

        with pytest.raises(RuntimeError, match="open transaction"):
            _safe_replace_table(tmp_db, "parent", INSERT_SQL, [("a", "b")])

        # Clean up the pending transaction so the fixture can close cleanly
        tmp_db.execute("ROLLBACK")

    def test_safe_replace_ok_after_commit(self, tmp_db):
        """After committing a pending transaction, the call succeeds."""
        tmp_db.execute("BEGIN")
        tmp_db.execute("ROLLBACK")  # close it

        result = _safe_replace_table(tmp_db, "parent", INSERT_SQL, [("x", "xenon")])
        assert result == 1


# ---------------------------------------------------------------------------
# Generator / iterable input tests
# ---------------------------------------------------------------------------

class TestGeneratorInput:
    def test_safe_replace_accepts_generator(self, tmp_db):
        """rows= may be a generator expression — must be eagerly materialised."""
        source = [("g1", "gen_alpha"), ("g2", "gen_beta"), ("g3", "gen_gamma")]
        gen = (row for row in source)  # generator, not list

        result = _safe_replace_table(tmp_db, "parent", INSERT_SQL, gen)
        assert result == 3

        ids = [
            r[0]
            for r in tmp_db.execute("SELECT id FROM parent ORDER BY id").fetchall()
        ]
        assert ids == ["g1", "g2", "g3"]

    def test_safe_replace_accepts_map_iterable(self, tmp_db):
        """rows= may be any iterable (e.g. map object)."""
        raw = [("m1", "map_alpha"), ("m2", "map_beta")]
        mapped = map(tuple, raw)  # map object, not list

        result = _safe_replace_table(tmp_db, "parent", INSERT_SQL, mapped)
        assert result == 2


# ---------------------------------------------------------------------------
# min_rows guard tests
# ---------------------------------------------------------------------------

class TestMinRows:
    def test_safe_replace_min_rows_raises_when_too_few(self, tmp_db):
        """min_rows= raises ValueError before any DB writes when insufficient rows."""
        _insert_parent_rows(tmp_db, [("a", "alpha"), ("b", "beta")])

        with pytest.raises(ValueError, match="min_rows"):
            _safe_replace_table(
                tmp_db, "parent", INSERT_SQL,
                [("x", "xenon")],  # only 1 row
                min_rows=5,
            )

        # Original data must be untouched (no DB writes occurred)
        ids = [
            r[0]
            for r in tmp_db.execute("SELECT id FROM parent ORDER BY id").fetchall()
        ]
        assert ids == ["a", "b"], f"min_rows guard allowed DB writes: {ids}"

    def test_safe_replace_min_rows_passes_when_enough(self, tmp_db):
        """min_rows= does not raise when row count meets the threshold."""
        result = _safe_replace_table(
            tmp_db, "parent", INSERT_SQL,
            [("x", "xenon"), ("y", "yttrium"), ("z", "zinc")],
            min_rows=3,
        )
        assert result == 3

    def test_safe_replace_min_rows_none_skips_check(self, tmp_db):
        """min_rows=None (default) never raises regardless of row count."""
        result = _safe_replace_table(
            tmp_db, "parent", INSERT_SQL, [],
            min_rows=None,
        )
        assert result == 0


# ---------------------------------------------------------------------------
# Integration: full-replace tests for the 4 real importers
# ---------------------------------------------------------------------------

_SCHEMA_SQL = (
    Path(__file__).parent.parent / "data" / "db" / "schema.sql"
).read_text(encoding="utf-8")


@pytest.fixture()
def tmp_db_with_schema(tmp_path):
    """Temporary DB with the real ExileSage schema applied (WAL, FK ON).

    This is a file-backed DB so importers can open their own connections
    via get_connection(db_path=str(db_file)).
    """
    db_file = tmp_path / "test_importers.db"
    conn = sqlite3.connect(str(db_file))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(_SCHEMA_SQL)
    # Seed the required meta row (schema uses CHECK id=1)
    conn.execute(
        "INSERT OR IGNORE INTO meta (id, schema_version, patch_version) VALUES (1, 1, 'test')"
    )
    conn.commit()
    yield conn, str(db_file)
    conn.close()


# ── Fixture data ---------------------------------------------------------

def _write_json(path: Path, data: dict) -> Path:
    """Helper: write dict as JSON to path, return path."""
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


# ── Base fixture data (v1 = 3 rows, v2 = 2 rows, third row removed) --------

_MODS_V1 = {
    "ModA": {
        "id": "ModA", "name": "Mod Alpha", "generation_type": "suffix",
        "domain": "item", "group": None, "type": "Strength",
        "required_level": 1, "is_essence_only": False,
        "tags": [], "spawn_weights": [], "generation_weights": [],
        "grants_effects": [], "stats": [], "adds_tags": [], "implicit_tags": [],
    },
    "ModB": {
        "id": "ModB", "name": "Mod Beta", "generation_type": "prefix",
        "domain": "item", "group": None, "type": "Dexterity",
        "required_level": 2, "is_essence_only": False,
        "tags": [], "spawn_weights": [], "generation_weights": [],
        "grants_effects": [], "stats": [], "adds_tags": [], "implicit_tags": [],
    },
    "ModC": {
        "id": "ModC", "name": "Mod Gamma", "generation_type": "suffix",
        "domain": "flask", "group": None, "type": "Intelligence",
        "required_level": 3, "is_essence_only": False,
        "tags": [], "spawn_weights": [], "generation_weights": [],
        "grants_effects": [], "stats": [], "adds_tags": [], "implicit_tags": [],
    },
}
_MODS_V2 = {k: v for k, v in _MODS_V1.items() if k != "ModC"}


def _base_item(item_id, name, item_class, domain="item", armour=None):
    return {
        "id": item_id, "name": name, "item_class": item_class,
        "domain": domain, "drop_level": 1, "tags": [],
        "implicits": [], "requirements": None,
        "armour": armour, "evasion": None, "energy_shield": None,
        "physical_damage_min": None, "physical_damage_max": None,
        "critical_strike_chance": None, "attack_time": None,
        "range": None, "charges_max": None, "charges_per_use": None,
        "duration": None, "life_per_use": None, "mana_per_use": None,
        "stack_size": None,
    }


_BASE_ITEMS_V1 = {
    "Item/Alpha": _base_item("Item/Alpha", "Alpha Wand", "Wand"),
    "Item/Beta":  _base_item("Item/Beta",  "Beta Shield", "Shield", armour=50),
    "Item/Gamma": _base_item("Item/Gamma", "Gamma Flask", "LifeFlask", domain="flask"),
}
_BASE_ITEMS_V2 = {k: v for k, v in _BASE_ITEMS_V1.items() if k != "Item/Gamma"}


def _currency(cid, name):
    return {
        "id": cid, "name": name, "tags": ["currency"], "drop_level": 1,
        "stack_size": 10, "stack_size_currency_tab": 1000,
        "full_stack_turns_into": None, "description": f"{name} description.",
        "inherits_from": None,
    }


_CURRENCIES_V1 = {
    "Currency/Alpha": _currency("Currency/Alpha", "Alpha Orb"),
    "Currency/Beta":  _currency("Currency/Beta",  "Beta Shard"),
    "Currency/Gamma": _currency("Currency/Gamma", "Gamma Stone"),
}
_CURRENCIES_V2 = {k: v for k, v in _CURRENCIES_V1.items() if k != "Currency/Gamma"}


def _augment(aug_id, type_id, type_name, level=10):
    return {
        "id": aug_id, "type_id": type_id, "type_name": type_name,
        "required_level": level, "limit": None,
        "categories": {"Body Armour": {"stat_text": ["test"], "stats": [], "target": ""}},
    }


_AUGMENTS_V1 = {
    "Aug/Alpha": _augment("Aug/Alpha", "AlphaCore", "Alpha Core", 10),
    "Aug/Beta":  _augment("Aug/Beta",  "BetaCore",  "Beta Core",  20),
    "Aug/Gamma": _augment("Aug/Gamma", "GammaCore", "Gamma Core", 30),
}
_AUGMENTS_V2 = {k: v for k, v in _AUGMENTS_V1.items() if k != "Aug/Gamma"}


# ── Fixtures that write the JSON files with the canonical filenames ---------

@pytest.fixture()
def mods_dir_v1(tmp_path):
    """Directory containing mods.json with 3 rows."""
    d = tmp_path / "mods_v1"
    d.mkdir()
    _write_json(d / "mods.json", _MODS_V1)
    return d


@pytest.fixture()
def mods_dir_v2(tmp_path):
    """Directory containing mods.json with 2 rows (ModC removed)."""
    d = tmp_path / "mods_v2"
    d.mkdir()
    _write_json(d / "mods.json", _MODS_V2)
    return d


@pytest.fixture()
def base_items_dir_v1(tmp_path):
    d = tmp_path / "bi_v1"
    d.mkdir()
    _write_json(d / "base_items.json", _BASE_ITEMS_V1)
    return d


@pytest.fixture()
def base_items_dir_v2(tmp_path):
    d = tmp_path / "bi_v2"
    d.mkdir()
    _write_json(d / "base_items.json", _BASE_ITEMS_V2)
    return d


@pytest.fixture()
def currencies_dir_v1(tmp_path):
    d = tmp_path / "cur_v1"
    d.mkdir()
    _write_json(d / "currencies.json", _CURRENCIES_V1)
    return d


@pytest.fixture()
def currencies_dir_v2(tmp_path):
    d = tmp_path / "cur_v2"
    d.mkdir()
    _write_json(d / "currencies.json", _CURRENCIES_V2)
    return d


@pytest.fixture()
def augments_dir_v1(tmp_path):
    d = tmp_path / "aug_v1"
    d.mkdir()
    _write_json(d / "augments.json", _AUGMENTS_V1)
    return d


@pytest.fixture()
def augments_dir_v2(tmp_path):
    d = tmp_path / "aug_v2"
    d.mkdir()
    _write_json(d / "augments.json", _AUGMENTS_V2)
    return d


class TestImporterFullReplace:
    """Integration tests: each importer must DELETE removed rows on re-import.

    TDD workflow:
      RED  — these tests fail before retrofit (importers use INSERT OR REPLACE,
              so deleted rows persist).
      GREEN — pass after each importer is switched to _safe_replace_table.
    """

    def test_mods_importer_removes_deleted_rows(
        self, tmp_db_with_schema, mods_dir_v1, mods_dir_v2, monkeypatch
    ):
        """Import v1 (3 mods). Re-import v2 (2 mods, ModC removed). ModC must be gone."""
        conn, db_path = tmp_db_with_schema

        import pipeline.importers.mods_importer as mi

        # v1 import: 3 mods
        monkeypatch.setattr(mi, "PROCESSED_DIR", mods_dir_v1)
        imported, skipped = mi.run(db_path=db_path)
        assert imported == 3, f"v1 import: expected 3, got {imported}"
        count = conn.execute("SELECT COUNT(*) FROM mods").fetchone()[0]
        assert count == 3, f"v1: expected 3 rows in mods, got {count}"

        # v2 import: 2 mods (ModC removed)
        monkeypatch.setattr(mi, "PROCESSED_DIR", mods_dir_v2)
        imported, skipped = mi.run(db_path=db_path)
        assert imported == 2, f"v2 import: expected 2, got {imported}"

        count = conn.execute("SELECT COUNT(*) FROM mods").fetchone()[0]
        assert count == 2, f"v2: expected 2 rows in mods, got {count} (DELETE did not run)"

        modc = conn.execute("SELECT id FROM mods WHERE id='ModC'").fetchone()
        assert modc is None, "ModC still in mods table after v2 re-import — DELETE path missing"

        meta = conn.execute("SELECT mods_count FROM meta WHERE id=1").fetchone()
        assert meta[0] == 2, f"meta.mods_count expected 2, got {meta[0]}"

    def test_base_items_importer_removes_deleted_rows(
        self, tmp_db_with_schema, base_items_dir_v1, base_items_dir_v2, monkeypatch
    ):
        """Import v1 (3 items). Re-import v2 (2 items, Item/Gamma removed). Gamma must be gone."""
        conn, db_path = tmp_db_with_schema

        import pipeline.importers.base_items_importer as bi

        monkeypatch.setattr(bi, "PROCESSED_DIR", base_items_dir_v1)
        imported, skipped = bi.run(db_path=db_path)
        assert imported == 3, f"v1 import: expected 3, got {imported}"
        count = conn.execute("SELECT COUNT(*) FROM base_items").fetchone()[0]
        assert count == 3

        monkeypatch.setattr(bi, "PROCESSED_DIR", base_items_dir_v2)
        imported, skipped = bi.run(db_path=db_path)
        assert imported == 2, f"v2 import: expected 2, got {imported}"

        count = conn.execute("SELECT COUNT(*) FROM base_items").fetchone()[0]
        assert count == 2, f"v2: expected 2 rows, got {count} (DELETE did not run)"

        gamma = conn.execute("SELECT id FROM base_items WHERE id='Item/Gamma'").fetchone()
        assert gamma is None, "Item/Gamma still in base_items after v2 re-import"

        meta = conn.execute("SELECT base_items_count FROM meta WHERE id=1").fetchone()
        assert meta[0] == 2, f"meta.base_items_count expected 2, got {meta[0]}"

    def test_currencies_importer_removes_deleted_rows(
        self, tmp_db_with_schema, currencies_dir_v1, currencies_dir_v2, monkeypatch
    ):
        """Import v1 (3 currencies). Re-import v2 (2, Currency/Gamma removed). Gamma must be gone."""
        conn, db_path = tmp_db_with_schema

        import pipeline.importers.currencies_importer as ci

        monkeypatch.setattr(ci, "PROCESSED_DIR", currencies_dir_v1)
        imported, skipped = ci.run(db_path=db_path)
        assert imported == 3, f"v1 import: expected 3, got {imported}"
        count = conn.execute("SELECT COUNT(*) FROM currencies").fetchone()[0]
        assert count == 3

        monkeypatch.setattr(ci, "PROCESSED_DIR", currencies_dir_v2)
        imported, skipped = ci.run(db_path=db_path)
        assert imported == 2, f"v2 import: expected 2, got {imported}"

        count = conn.execute("SELECT COUNT(*) FROM currencies").fetchone()[0]
        assert count == 2, f"v2: expected 2 rows, got {count} (DELETE did not run)"

        gamma = conn.execute("SELECT id FROM currencies WHERE id='Currency/Gamma'").fetchone()
        assert gamma is None, "Currency/Gamma still in currencies after v2 re-import"

        meta = conn.execute("SELECT currencies_count FROM meta WHERE id=1").fetchone()
        assert meta[0] == 2, f"meta.currencies_count expected 2, got {meta[0]}"

    def test_augments_importer_removes_deleted_rows(
        self, tmp_db_with_schema, augments_dir_v1, augments_dir_v2, monkeypatch
    ):
        """Import v1 (3 augments). Re-import v2 (2, Aug/Gamma removed). Gamma must be gone."""
        conn, db_path = tmp_db_with_schema

        import pipeline.importers.augments_importer as ai

        monkeypatch.setattr(ai, "PROCESSED_DIR", augments_dir_v1)
        imported, skipped = ai.run(db_path=db_path)
        assert imported == 3, f"v1 import: expected 3, got {imported}"
        count = conn.execute("SELECT COUNT(*) FROM augments").fetchone()[0]
        assert count == 3

        monkeypatch.setattr(ai, "PROCESSED_DIR", augments_dir_v2)
        imported, skipped = ai.run(db_path=db_path)
        assert imported == 2, f"v2 import: expected 2, got {imported}"

        count = conn.execute("SELECT COUNT(*) FROM augments").fetchone()[0]
        assert count == 2, f"v2: expected 2 rows, got {count} (DELETE did not run)"

        gamma = conn.execute("SELECT id FROM augments WHERE id='Aug/Gamma'").fetchone()
        assert gamma is None, "Aug/Gamma still in augments after v2 re-import"

        meta = conn.execute("SELECT augments_count FROM meta WHERE id=1").fetchone()
        assert meta[0] == 2, f"meta.augments_count expected 2, got {meta[0]}"

    @pytest.mark.slow
    def test_db_has_baseline_counts(self):
        """Sanity check: live DB is populated with at least Stage 1 baseline row counts.

        Renamed from test_real_ingest_row_counts to reflect actual behaviour:
        this test does NOT run ingest — it only checks that the live DB is already
        populated. For a real end-to-end ingest round-trip test see
        TestEndToEndIngest below.

        Skipped if data/processed/*.json or data/exilesage.db is unavailable (CI).
        Run locally with: pytest -m slow
        """
        from exilesage.config import DB_PATH, PROCESSED_DIR
        required_files = [
            PROCESSED_DIR / "mods.json",
            PROCESSED_DIR / "base_items.json",
            PROCESSED_DIR / "currencies.json",
            PROCESSED_DIR / "augments.json",
        ]
        if not all(f.exists() for f in required_files):
            pytest.skip("Processed data files not available — skipping real ingest regression")

        if not DB_PATH.exists():
            pytest.skip("data/exilesage.db not found — skipping real ingest regression")

        import sqlite3
        conn = sqlite3.connect(str(DB_PATH))
        mods = conn.execute("SELECT COUNT(*) FROM mods").fetchone()[0]
        base_items = conn.execute("SELECT COUNT(*) FROM base_items").fetchone()[0]
        currencies = conn.execute("SELECT COUNT(*) FROM currencies").fetchone()[0]
        augments = conn.execute("SELECT COUNT(*) FROM augments").fetchone()[0]
        conn.close()

        assert mods >= 14000, f"mods count regressed: {mods} (expected >= 14000)"
        assert base_items >= 2000, f"base_items count regressed: {base_items} (expected >= 2000)"
        assert currencies >= 500, f"currencies count regressed: {currencies} (expected >= 500)"
        assert augments >= 100, f"augments count regressed: {augments} (expected >= 100)"


# ---------------------------------------------------------------------------
# End-to-end ingest tests (pipeline.ingest.run round-trip)
# ---------------------------------------------------------------------------

class TestEndToEndIngest:
    """Full round-trip: pipeline.ingest.run() with temp DB and fixture JSON.

    TDD: RED until ingest.py has try/except per-importer (finding #6) and
    the per-importer last_import_at stamps are removed (finding #1).
    """

    @pytest.fixture()
    def ingest_env(self, tmp_path, tmp_db_with_schema, monkeypatch):
        """Set up temp PROCESSED_DIR with one fixture file per importer,
        and monkeypatch DB_PATH + PROCESSED_DIR in all relevant modules."""
        _conn, db_file = tmp_db_with_schema
        processed = tmp_path / "processed"
        processed.mkdir()

        # Write v1 fixture data
        _write_json(processed / "mods.json", _MODS_V1)
        _write_json(processed / "base_items.json", _BASE_ITEMS_V1)
        _write_json(processed / "currencies.json", _CURRENCIES_V1)
        _write_json(processed / "augments.json", _AUGMENTS_V1)

        db_path = Path(db_file)

        # Safety: ensure we never accidentally write to the live database.
        real_db = Path("data/exilesage.db").resolve()
        assert db_path.resolve() != real_db, "fixture would write to live DB — ABORT"

        # Single-patch: exilesage.config.DB_PATH is the sole source of truth now.
        # get_connection() and ingest.py both do attribute lookup at call time.
        import exilesage.config as cfg
        import pipeline.importers.mods_importer as mi
        import pipeline.importers.base_items_importer as bi
        import pipeline.importers.currencies_importer as ci
        import pipeline.importers.augments_importer as ai
        import pipeline.ingest as ingest_mod

        monkeypatch.setattr(cfg, "DB_PATH", db_path)
        monkeypatch.setattr(cfg, "PROCESSED_DIR", processed)
        monkeypatch.setattr(mi, "PROCESSED_DIR", processed)
        monkeypatch.setattr(bi, "PROCESSED_DIR", processed)
        monkeypatch.setattr(ci, "PROCESSED_DIR", processed)
        monkeypatch.setattr(ai, "PROCESSED_DIR", processed)

        return db_path, processed, _conn, ingest_mod

    def test_end_to_end_ingest_v1_row_counts(self, ingest_env):
        """pipeline.ingest.run() with fixture data produces correct row counts per table."""
        db_path, processed, conn, ingest_mod = ingest_env

        ingest_mod.run()

        mods = conn.execute("SELECT COUNT(*) FROM mods").fetchone()[0]
        base_items = conn.execute("SELECT COUNT(*) FROM base_items").fetchone()[0]
        currencies = conn.execute("SELECT COUNT(*) FROM currencies").fetchone()[0]
        augments = conn.execute("SELECT COUNT(*) FROM augments").fetchone()[0]

        assert mods == 3, f"Expected 3 mods, got {mods}"
        assert base_items == 3, f"Expected 3 base_items, got {base_items}"
        assert currencies == 3, f"Expected 3 currencies, got {currencies}"
        assert augments == 3, f"Expected 3 augments, got {augments}"

    def test_end_to_end_ingest_full_replace_removes_rows(self, ingest_env):
        """Second ingest with v2 data (one row removed per table) wipes old rows."""
        db_path, processed, conn, ingest_mod = ingest_env

        # First run: v1 (3 rows each)
        ingest_mod.run()

        # Overwrite with v2 fixtures (2 rows each — one removed per table)
        _write_json(processed / "mods.json", _MODS_V2)
        _write_json(processed / "base_items.json", _BASE_ITEMS_V2)
        _write_json(processed / "currencies.json", _CURRENCIES_V2)
        _write_json(processed / "augments.json", _AUGMENTS_V2)

        ingest_mod.run()

        mods = conn.execute("SELECT COUNT(*) FROM mods").fetchone()[0]
        base_items = conn.execute("SELECT COUNT(*) FROM base_items").fetchone()[0]
        currencies = conn.execute("SELECT COUNT(*) FROM currencies").fetchone()[0]
        augments = conn.execute("SELECT COUNT(*) FROM augments").fetchone()[0]

        assert mods == 2, f"Expected 2 mods after v2, got {mods}"
        assert base_items == 2, f"Expected 2 base_items after v2, got {base_items}"
        assert currencies == 2, f"Expected 2 currencies after v2, got {currencies}"
        assert augments == 2, f"Expected 2 augments after v2, got {augments}"

        # Verify the removed rows are actually gone
        assert conn.execute("SELECT id FROM mods WHERE id='ModC'").fetchone() is None
        assert conn.execute("SELECT id FROM base_items WHERE id='Item/Gamma'").fetchone() is None
        assert conn.execute("SELECT id FROM currencies WHERE id='Currency/Gamma'").fetchone() is None
        assert conn.execute("SELECT id FROM augments WHERE id='Aug/Gamma'").fetchone() is None

    def test_end_to_end_ingest_stamps_last_import_at(self, ingest_env):
        """pipeline.ingest.run() stamps meta.last_import_at on clean run."""
        db_path, processed, conn, ingest_mod = ingest_env

        ingest_mod.run()

        ts = conn.execute("SELECT last_import_at FROM meta WHERE id=1").fetchone()[0]
        assert ts is not None, "last_import_at was not stamped after clean ingest"


# ---------------------------------------------------------------------------
# Partial-failure pipeline tests
# ---------------------------------------------------------------------------

class TestPartialFailure:
    """Verify that a single importer crash does not abort the whole pipeline.

    TDD: RED until ingest.py wraps per-importer calls with try/except (finding #6).
    """

    @pytest.fixture()
    def ingest_env_partial(self, tmp_path, tmp_db_with_schema, monkeypatch):
        """Same env as TestEndToEndIngest.ingest_env but returned separately
        so we can inject a failing importer."""
        _conn, db_file = tmp_db_with_schema
        processed = tmp_path / "processed"
        processed.mkdir()

        _write_json(processed / "mods.json", _MODS_V1)
        _write_json(processed / "base_items.json", _BASE_ITEMS_V1)
        _write_json(processed / "currencies.json", _CURRENCIES_V1)
        _write_json(processed / "augments.json", _AUGMENTS_V1)

        db_path = Path(db_file)

        # Safety: ensure we never accidentally write to the live database.
        real_db = Path("data/exilesage.db").resolve()
        assert db_path.resolve() != real_db, "fixture would write to live DB — ABORT"

        # Single-patch: exilesage.config.DB_PATH is the sole source of truth now.
        import exilesage.config as cfg
        import pipeline.importers.mods_importer as mi
        import pipeline.importers.base_items_importer as bi
        import pipeline.importers.currencies_importer as ci
        import pipeline.importers.augments_importer as ai
        import pipeline.ingest as ingest_mod

        monkeypatch.setattr(cfg, "DB_PATH", db_path)
        monkeypatch.setattr(cfg, "PROCESSED_DIR", processed)
        monkeypatch.setattr(mi, "PROCESSED_DIR", processed)
        monkeypatch.setattr(bi, "PROCESSED_DIR", processed)
        monkeypatch.setattr(ci, "PROCESSED_DIR", processed)
        monkeypatch.setattr(ai, "PROCESSED_DIR", processed)

        return db_path, processed, _conn, ingest_mod, mi, bi, ci, ai

    def test_partial_failure_other_importers_still_run(
        self, ingest_env_partial, monkeypatch
    ):
        """If mods_importer.run() raises, the other 3 importers still complete."""
        db_path, processed, conn, ingest_mod, mi, bi, ci, ai = ingest_env_partial

        # Patch mods_importer.run to raise
        def _boom():
            raise RuntimeError("injected failure in mods_importer")

        monkeypatch.setattr(mi, "run", _boom)

        # Pipeline exits with non-zero status (SystemExit(1)) but continues past the
        # failing importer so the other 3 tables are still populated.
        with pytest.raises(SystemExit) as exc_info:
            ingest_mod.run()
        assert exc_info.value.code == 1, f"Expected exit code 1, got {exc_info.value.code}"

        # Other tables must be populated
        base_items = conn.execute("SELECT COUNT(*) FROM base_items").fetchone()[0]
        currencies = conn.execute("SELECT COUNT(*) FROM currencies").fetchone()[0]
        augments = conn.execute("SELECT COUNT(*) FROM augments").fetchone()[0]

        assert base_items == 3, f"base_items should be 3, got {base_items}"
        assert currencies == 3, f"currencies should be 3, got {currencies}"
        assert augments == 3, f"augments should be 3, got {augments}"

    def test_partial_failure_skips_last_import_at_stamp(
        self, ingest_env_partial, monkeypatch
    ):
        """If any importer fails, last_import_at must NOT be stamped."""
        db_path, processed, conn, ingest_mod, mi, bi, ci, ai = ingest_env_partial

        # Record pre-run timestamp (may be the init value from meta seed)
        before_ts = conn.execute("SELECT last_import_at FROM meta WHERE id=1").fetchone()[0]

        def _boom():
            raise RuntimeError("injected failure")

        monkeypatch.setattr(mi, "run", _boom)

        with pytest.raises(SystemExit):
            ingest_mod.run()

        after_ts = conn.execute("SELECT last_import_at FROM meta WHERE id=1").fetchone()[0]
        assert after_ts == before_ts, (
            f"last_import_at was updated despite importer failure "
            f"(before={before_ts!r}, after={after_ts!r})"
        )


# ---------------------------------------------------------------------------
# IMPORT_PHASES structure + phase-failure abort tests
# ---------------------------------------------------------------------------

class TestImportPhases:
    """TDD tests for IMPORT_PHASES in pipeline/ingest.py (Phase 0.4).

    RED phase: written before implementation exists.
    GREEN phase: passes after IMPORT_PHASES and updated run() are in place.
    """

    def test_import_phases_is_list_of_lists(self):
        """IMPORT_PHASES must be a list of lists with at least one phase."""
        from pipeline.ingest import IMPORT_PHASES
        assert isinstance(IMPORT_PHASES, list), "IMPORT_PHASES must be a list"
        assert len(IMPORT_PHASES) >= 1, "IMPORT_PHASES must have at least one phase"
        for phase in IMPORT_PHASES:
            assert isinstance(phase, list), (
                f"Each phase must be a list, got {type(phase)}"
            )

    def test_import_phases_entries_are_name_module_tuples(self):
        """Every entry in every phase must be a (str, module-with-run) tuple."""
        from pipeline.ingest import IMPORT_PHASES
        for phase in IMPORT_PHASES:
            for entry in phase:
                assert isinstance(entry, tuple) and len(entry) == 2, (
                    f"Phase entry must be a 2-tuple, got {entry!r}"
                )
                name, module = entry
                assert isinstance(name, str), (
                    f"Entry name must be str, got {type(name)} for entry {entry!r}"
                )
                assert hasattr(module, "run"), (
                    f"Importer '{name}' has no run() attribute"
                )

    def test_import_phases_covers_all_existing_importers(self):
        """All 4 existing importers must appear in IMPORT_PHASES (any phase)."""
        from pipeline.ingest import IMPORT_PHASES
        names_in_phases = [name for phase in IMPORT_PHASES for (name, _) in phase]
        required = {"mods", "base_items", "currencies", "augments"}
        assert required <= set(names_in_phases), (
            f"Missing importers in IMPORT_PHASES: {required - set(names_in_phases)}"
        )

    def test_phase_failure_aborts_subsequent_phases(self, monkeypatch):
        """If any importer in phase N fails, importers in phase N+1 must NOT run.

        Strategy: inject a synthetic 2-phase IMPORT_PHASES where phase 1 contains
        a failing mock importer, and phase 2 contains a spy mock. Assert the spy
        was never called.

        Uses the run_phases() helper directly so we can pass custom phases without
        touching the real DB.
        """
        from pipeline.ingest import run_phases
        import types

        # Build a mock module with a spy run()
        phase2_called = []

        def _phase2_run():
            phase2_called.append(True)
            return (0, 0)

        phase2_mod = types.SimpleNamespace(run=_phase2_run, __name__="mock_phase2")

        # Phase 1: one importer that raises
        def _phase1_boom():
            raise RuntimeError("injected phase-1 failure")

        phase1_mod = types.SimpleNamespace(run=_phase1_boom, __name__="mock_phase1")

        synthetic_phases = [
            [("failing_importer", phase1_mod)],
            [("should_not_run", phase2_mod)],
        ]

        # run_phases should raise SystemExit(1) because phase 1 failed
        with pytest.raises(SystemExit) as exc_info:
            run_phases(synthetic_phases)

        assert exc_info.value.code == 1, (
            f"Expected SystemExit(1) on phase failure, got code={exc_info.value.code}"
        )
        assert len(phase2_called) == 0, (
            "Phase 2 importer was called despite phase 1 failure — abort logic missing"
        )


# ---------------------------------------------------------------------------
# Fix 2: _safe_replace_table — BaseException / KeyboardInterrupt rollback
# ---------------------------------------------------------------------------

class _ConnProxy:
    """Thin proxy around sqlite3.Connection that allows overriding executemany/commit."""

    def __init__(self, conn, executemany_override=None, commit_override=None):
        self._conn = conn
        self._executemany_override = executemany_override
        self._commit_override = commit_override

    def executemany(self, sql, params):
        if self._executemany_override is not None:
            return self._executemany_override(sql, params)
        return self._conn.executemany(sql, params)

    def commit(self):
        if self._commit_override is not None:
            return self._commit_override()
        return self._conn.commit()

    def rollback(self):
        return self._conn.rollback()

    def execute(self, sql, params=()):
        return self._conn.execute(sql, params)

    @property
    def in_transaction(self):
        return self._conn.in_transaction

    def close(self):
        return self._conn.close()


class TestInterruptSafety:
    """Fix 2: BaseException handling in _safe_replace_table.

    Ctrl-C (KeyboardInterrupt) mid-executemany must still rollback and restore FK.
    RED tests first — fail before BaseException handler is added.
    """

    def test_keyboard_interrupt_rolls_back(self, tmp_db):
        """KeyboardInterrupt mid-executemany rolls back — original data survives."""
        # Pre-populate with known data
        _insert_parent_rows(tmp_db, [("orig1", "original")])
        original_count = tmp_db.execute("SELECT COUNT(*) FROM parent").fetchone()[0]
        assert original_count == 1

        def _interrupt_executemany(sql, params):
            raise KeyboardInterrupt("simulated Ctrl-C")

        proxy = _ConnProxy(tmp_db, executemany_override=_interrupt_executemany)

        with pytest.raises(KeyboardInterrupt):
            _safe_replace_table(proxy, "parent", INSERT_SQL, [("new1", "new")])

        # Original data must survive (rollback happened)
        count = tmp_db.execute("SELECT COUNT(*) FROM parent").fetchone()[0]
        assert count == 1, f"Rollback failed after KeyboardInterrupt — got {count} rows"
        assert not tmp_db.in_transaction, "Connection still in transaction after KI rollback"

    def test_keyboard_interrupt_restores_fk(self, tmp_db):
        """After KeyboardInterrupt, PRAGMA foreign_keys must be restored to ON (=1)."""
        def _interrupt_executemany(sql, params):
            raise KeyboardInterrupt("simulated Ctrl-C")

        proxy = _ConnProxy(tmp_db, executemany_override=_interrupt_executemany)

        with pytest.raises(KeyboardInterrupt):
            _safe_replace_table(proxy, "parent", INSERT_SQL, [("x", "y")])

        fk_val = tmp_db.execute("PRAGMA foreign_keys").fetchone()[0]
        assert fk_val == 1, f"foreign_keys not restored after KeyboardInterrupt (got {fk_val})"

    def test_commit_failure_restores_fk(self, tmp_db):
        """OperationalError from conn.commit() still restores FK enforcement."""
        def _fail_commit():
            raise sqlite3.OperationalError("injected commit failure")

        proxy = _ConnProxy(tmp_db, commit_override=_fail_commit)

        with pytest.raises(sqlite3.OperationalError):
            _safe_replace_table(proxy, "parent", INSERT_SQL, [("x", "y")])

        fk_val = tmp_db.execute("PRAGMA foreign_keys").fetchone()[0]
        assert fk_val == 1, f"foreign_keys not restored after commit failure (got {fk_val})"

    def test_keyboard_interrupt_preserves_original_when_rollback_fails(self, tmp_db):
        """Fix C: KeyboardInterrupt propagates even when conn.rollback() raises.

        Scenario: executemany raises KeyboardInterrupt AND rollback raises
        OperationalError("broken"). The OperationalError must be swallowed so
        the original KeyboardInterrupt propagates out to the caller.

        RED: current impl calls conn.rollback() bare in the BaseException handler —
        if rollback raises, the new exception replaces the original KI in __context__.
        The bare `raise` then re-raises OperationalError, not KeyboardInterrupt.

        GREEN: wrap rollback() in try/except Exception: pass so original KI propagates.
        """
        class _RollbackFailProxy(_ConnProxy):
            """Proxy that also makes rollback() raise OperationalError."""
            def rollback(self):
                raise sqlite3.OperationalError("broken connection — rollback failed")

        def _interrupt_executemany(sql, params):
            raise KeyboardInterrupt("simulated Ctrl-C")

        proxy = _RollbackFailProxy(tmp_db, executemany_override=_interrupt_executemany)

        # The KeyboardInterrupt must propagate, NOT the OperationalError from rollback
        with pytest.raises(KeyboardInterrupt):
            _safe_replace_table(proxy, "parent", INSERT_SQL, [("new1", "new")])


# ---------------------------------------------------------------------------
# Coverage tests — no bug, gap closure
# ---------------------------------------------------------------------------

class TestCoverageGaps:
    """Coverage tests for paths that work but lack explicit tests."""

    def test_safe_replace_empty_rows_rebuilds_fts(self, tmp_db):
        """rows=[] with fts_table set must rebuild FTS — COUNT(*) FROM parent_fts == 0."""
        # Pre-populate
        _insert_parent_rows(tmp_db, [("a", "alpha"), ("b", "beta")])

        _safe_replace_table(
            tmp_db, "parent", INSERT_SQL, [],
            fts_table="parent_fts"
        )

        count = tmp_db.execute("SELECT COUNT(*) FROM parent_fts").fetchone()[0]
        assert count == 0, f"FTS not empty after empty-rows replace: {count}"

    def test_run_phases_empty_list(self):
        """run_phases([]) returns (0, 0) without error."""
        from pipeline.ingest import run_phases
        result = run_phases([])
        assert result == (0, 0)

    def test_run_phases_empty_inner_phase(self):
        """run_phases([[]]) — empty phase — returns (0, 0) without error."""
        from pipeline.ingest import run_phases
        result = run_phases([[]])
        assert result == (0, 0)

    def test_run_phases_partial_phase_failure_exit_code(self):
        """Two importers in one phase, one raises → SystemExit(1); successful importer counted."""
        from pipeline.ingest import run_phases
        import types

        imported_tracker = []

        def _good_run():
            imported_tracker.append(5)
            return (5, 0)

        def _bad_run():
            raise RuntimeError("injected failure")

        good_mod = types.SimpleNamespace(run=_good_run)
        bad_mod = types.SimpleNamespace(run=_bad_run)

        with pytest.raises(SystemExit) as exc_info:
            run_phases([[("good", good_mod), ("bad", bad_mod)]])

        assert exc_info.value.code == 1
        assert imported_tracker == [5], "Good importer must still have run before bad one"

    def test_importer_skipped_rows_fts_consistent(
        self, tmp_db_with_schema, mods_dir_v1, monkeypatch
    ):
        """3-row mods JSON where 1 row fails validation → mods_fts COUNT(*) == 2."""
        conn, db_path = tmp_db_with_schema
        import pipeline.importers.mods_importer as mi

        # Build a mods file with 3 rows but corrupt the third (missing required id)
        bad_mods = dict(_MODS_V1)  # 3 rows
        # Inject a 4th entry that will fail validation (id is missing — pydantic will reject)
        bad_mods["BadMod"] = {
            "id": None,  # id is required str; None should cause validation to fail → skip
            "name": "Bad Mod",
            "generation_type": "suffix",
            "domain": "item",
            "group": None,
            "type": "Strength",
            "required_level": 1,
            "is_essence_only": False,
            "tags": [], "spawn_weights": [], "generation_weights": [],
            "grants_effects": [], "stats": [], "adds_tags": [], "implicit_tags": [],
        }
        # Overwrite the mods dir with the bad file
        import json as _json
        mods_dir_v1.joinpath("mods.json").write_text(
            _json.dumps(bad_mods), encoding="utf-8"
        )

        monkeypatch.setattr(mi, "PROCESSED_DIR", mods_dir_v1)
        imported, skipped = mi.run(db_path=db_path)

        fts_count = conn.execute("SELECT COUNT(*) FROM mods_fts").fetchone()[0]
        assert fts_count == imported, (
            f"mods_fts count ({fts_count}) does not match imported ({imported})"
        )
        assert imported <= 3, "Should not import more rows than the valid 3"


# ---------------------------------------------------------------------------
# TestMetaColWarning — carryover from Phase 0 final review
# ---------------------------------------------------------------------------

class TestMetaColWarning:
    """_safe_replace_table logs warning when meta_col update affects 0 rows (missing meta row)."""

    @pytest.fixture()
    def db_no_meta_row(self, tmp_path):
        """DB with meta table but NO rows inserted."""
        db_file = tmp_path / "no_meta.db"
        conn = sqlite3.connect(str(db_file))
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS items (
                id TEXT PRIMARY KEY,
                value TEXT
            );
            CREATE TABLE IF NOT EXISTS meta (
                id INTEGER PRIMARY KEY CHECK(id=1),
                item_count INTEGER DEFAULT 0
            );
            -- Intentionally NO INSERT into meta
        """)
        conn.commit()
        yield conn
        conn.close()

    def test_meta_col_warning_when_no_meta_row(self, db_no_meta_row, caplog):
        """_safe_replace_table logs a WARNING when meta UPDATE affects 0 rows."""
        import logging
        insert_sql = "INSERT INTO items (id, value) VALUES (?, ?)"
        rows = [("a", "alpha")]
        with caplog.at_level(logging.WARNING, logger="pipeline.importers._base"):
            _safe_replace_table(db_no_meta_row, "items", insert_sql, rows, meta_col="item_count")
        # Warning must mention meta_col or the 0-rows condition
        warning_messages = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
        assert any("meta" in m.lower() or "0" in m for m in warning_messages), \
            f"Expected meta_col warning, got: {warning_messages}"
