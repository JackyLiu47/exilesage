# Code Review — ExileSage Stage 1

## Overall Health: Yellow

The core pipeline and tool layer are solid. SQL injection is safe, error handling is consistent, and all 72 unit tests pass. Two warnings need attention before Stage 2: connection leaks in tool functions and an FTS5 empty-query crash risk. No critical issues.

---

## Critical Issues

**None found.**

---

## Warnings (fix before Stage 2 adds load)

### 1. DB connection never closed in tool functions
**Files:** `exilesage/tools/mods.py`, `items.py`, `currencies.py`, `augments.py`

Every tool function calls `get_connection()` but never calls `conn.close()` or uses a context manager. Python's GC will eventually close it, but under load (many advisor calls) this will exhaust SQLite's connection pool.

**Fix:** Wrap in `with` or close in `finally`:
```python
conn = get_connection()
try:
    rows = _search_fts(conn, ...)
    return [dict(r) for r in rows]
finally:
    conn.close()
```

### 2. FTS5 MATCH crashes on empty string
**Files:** All tool files using FTS5

SQLite FTS5 raises `OperationalError` on `MATCH ''`. The tools guard against this by only entering the FTS path when `query` is truthy, but the `_search_fts` helpers don't independently validate. If called directly, they'll crash.

**Fix:** Add guard at top of each `_search_fts`:
```python
if not query or not query.strip():
    return []
```

### 3. PoE wiki markup in `augments.type_name` not stripped
**File:** `pipeline/importers/augments_importer.py`, `exilesage/tools/augments.py`

`type_name` values contain raw wiki syntax: `[AbyssalEye|Abyssal Eye]`. The advisor will present this markup to users and Claude will need to parse it. The tool layer should strip it to display text.

**Fix:** In augments_importer, add:
```python
import re
def strip_wiki(s: str) -> str:
    return re.sub(r'\[([^|]+)\|([^\]]+)\]', r'\2', s or '')
```
Apply to `type_name` and `limit_info` at import time.

---

## Minor Issues

- **`exilesage/advisor/core.py`**: `MAX_TOOL_ITER=5` is hit by ~60% of complex queries in S1.7 testing. Raise to 8 in `config.py` for better coverage.
- **`pipeline/importers/*.py`**: Each importer reopens its own connection rather than sharing one. Fine for sequential ingest but worth unifying when ingest becomes incremental.
- **`exilesage/config.py`**: `ROOT_DIR` assumes the package is always run from repo root. Add a fallback for installed package usage.
- **`exilesage/advisor/system_prompt.py`**: No mention of PoE2-specific mechanics like Waystones, Pinnacle bosses, or Precursor Tablets — relevant for guide queries. Low priority for Stage 1.

---

## System Prompt Quality: 3.5 / 5

**Strengths:**
- Prefix/suffix/implicit/explicit distinction is clear
- Currency hierarchy is accurate
- Tool usage rules are well-enforced ("always call tools before answering")
- "Never hallucinate stat ranges" instruction is present

**Gaps for Stage 2:**
- No mention of ascendancies or passive tree (needed for build guides)
- No mention of Waystones / Atlas mechanics (endgame queries will miss context)
- No mention of PoE2 gem socketing changes vs PoE1 (common confusion point)
- Missing: skill support gem interactions, aura reservation

---

## Verdict

Ready for Stage 2 with one caveat: fix the connection leak (#1 above) before adding concurrent wiki/RAG queries — it's harmless now with single-threaded CLI but will cause issues under load.
