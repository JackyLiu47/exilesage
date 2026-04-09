# Code Review — ExileSage Stage 1

**Status: All issues resolved (2026-04-09). Kept as a closed retrospective artifact.**

---

## Overall Health at Review Time: Yellow → Green after fixes

The core pipeline and tool layer were solid. SQL injection safe, error handling consistent, 72 unit tests passing. Three warnings were found and fixed before Stage 2.

---

## Resolved Issues

### 1. DB connection never closed in tool functions ✅ Fixed
**Files:** `exilesage/tools/mods.py`, `items.py`, `currencies.py`, `augments.py`

Every tool function called `get_connection()` but never closed it. Fixed by wrapping in `conn = None` / `try` / `finally: conn.close()` in all 4 files.

### 2. FTS5 MATCH crashes on empty string ✅ Fixed
**Files:** All tool files using FTS5

Added `if not query or not query.strip(): return []` guard at the top of every `_search_fts` helper.

### 3. PoE wiki markup in `augments.type_name` not stripped ✅ Fixed
**File:** `pipeline/importers/augments_importer.py`

Added `_strip_wiki()` using `re.sub(r'\[([^|]+)\|([^\]]+)\]', r'\2', s)`, applied to `type_name` and `limit` at import time. DB re-ingested.

---

## Minor Items

- `MAX_TOOL_ITER` raised from 5 → 8 in `exilesage/config.py` ✅
- System prompt gaps (ascendancies, Waystones, gem socketing) — deferred to Stage 2 system prompt revision

---

## Lessons

- Data quality scan before writing importers would have caught wiki markup earlier
- FTS5 `_score` key was specified in data-layer.md but never implemented — removed from rules
- `exilesage/models.py` was referenced in docs but never created — importers use inline Pydantic models instead
