-- ExileSage SQLite Schema
-- FTS5 content tables: content_rowid links virtual table to source table

CREATE TABLE IF NOT EXISTS meta (
    id                   INTEGER PRIMARY KEY CHECK (id = 1),
    patch_version        TEXT NOT NULL DEFAULT 'unknown',
    last_import_at       TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    mods_count           INTEGER DEFAULT 0,
    base_items_count     INTEGER DEFAULT 0,
    currencies_count     INTEGER DEFAULT 0,
    augments_count       INTEGER DEFAULT 0
);

-- ── Mods ─────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS mods (
    id                  TEXT PRIMARY KEY,
    name                TEXT NOT NULL DEFAULT '',
    generation_type     TEXT,           -- prefix | suffix | unique | ...
    domain              TEXT,           -- item | flask | jewel | ...
    group_name          TEXT,           -- mod group for exclusivity
    type                TEXT,           -- internal type id
    required_level      INTEGER DEFAULT 0,
    is_essence_only     INTEGER DEFAULT 0,
    tags                TEXT,           -- JSON array
    spawn_weights       TEXT,           -- JSON array [{tag, weight}]
    generation_weights  TEXT,           -- JSON array [{tag, weight}]
    grants_effects      TEXT,           -- JSON array
    stats               TEXT,           -- JSON array [{id, min, max}]
    adds_tags           TEXT,           -- JSON array
    implicit_tags       TEXT            -- JSON array
);

CREATE VIRTUAL TABLE IF NOT EXISTS mods_fts USING fts5(
    name, group_name, type, domain, generation_type,
    content='mods', content_rowid='rowid'
);

CREATE INDEX IF NOT EXISTS idx_mods_domain           ON mods(domain);
CREATE INDEX IF NOT EXISTS idx_mods_generation_type  ON mods(generation_type);
CREATE INDEX IF NOT EXISTS idx_mods_required_level   ON mods(required_level);
CREATE INDEX IF NOT EXISTS idx_mods_group_name       ON mods(group_name);
CREATE INDEX IF NOT EXISTS idx_mods_is_essence_only  ON mods(is_essence_only);

-- ── Base Items ────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS base_items (
    id                      TEXT PRIMARY KEY,
    name                    TEXT NOT NULL DEFAULT '',
    item_class              TEXT,
    domain                  TEXT,
    drop_level              INTEGER DEFAULT 0,
    tags                    TEXT,           -- JSON array
    implicits               TEXT,           -- JSON array of mod ids
    requirements            TEXT,           -- JSON {str, dex, int, level}
    properties              TEXT,           -- JSON {armour, evasion, energy_shield, ...}
    -- flattened properties for indexed queries
    armour                  INTEGER,
    evasion                 INTEGER,
    energy_shield           INTEGER,
    physical_damage_min     REAL,
    physical_damage_max     REAL,
    critical_strike_chance  REAL,
    attack_time             REAL,
    stack_size              INTEGER
);

CREATE VIRTUAL TABLE IF NOT EXISTS base_items_fts USING fts5(
    name, item_class, domain,
    content='base_items', content_rowid='rowid'
);

CREATE INDEX IF NOT EXISTS idx_base_items_item_class  ON base_items(item_class);
CREATE INDEX IF NOT EXISTS idx_base_items_domain      ON base_items(domain);
CREATE INDEX IF NOT EXISTS idx_base_items_drop_level  ON base_items(drop_level);

-- ── Currencies ────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS currencies (
    id                      TEXT PRIMARY KEY,
    name                    TEXT NOT NULL DEFAULT '',
    tags                    TEXT,           -- JSON array
    drop_level              INTEGER DEFAULT 0,
    stack_size              INTEGER,
    stack_size_currency_tab INTEGER,
    full_stack_turns_into   TEXT,
    description             TEXT,
    inherits_from           TEXT
);

CREATE VIRTUAL TABLE IF NOT EXISTS currencies_fts USING fts5(
    name, description,
    content='currencies', content_rowid='rowid'
);

CREATE INDEX IF NOT EXISTS idx_currencies_drop_level ON currencies(drop_level);

-- ── Augments ─────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS augments (
    id              TEXT PRIMARY KEY,
    type_id         TEXT,
    type_name       TEXT,
    required_level  INTEGER DEFAULT 0,
    limit_info      TEXT,           -- renamed from 'limit' (reserved word)
    categories      TEXT            -- JSON {slot: {stat_text, stats, target}}
);

CREATE VIRTUAL TABLE IF NOT EXISTS augments_fts USING fts5(
    type_id, type_name,
    content='augments', content_rowid='rowid'
);

CREATE INDEX IF NOT EXISTS idx_augments_type_id       ON augments(type_id);
CREATE INDEX IF NOT EXISTS idx_augments_required_level ON augments(required_level);
