"""
Central runtime configuration for ExileSage.
CRITICAL: All model names and tunable constants live here — nowhere else.
"""

from enum import Enum
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────

ROOT_DIR     = Path(__file__).parent.parent
DATA_DIR     = ROOT_DIR / "data"
DB_PATH      = DATA_DIR / "exilesage.db"
SCHEMA_PATH  = DATA_DIR / "db" / "schema.sql"
RAW_DIR      = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

# ── Query classification ───────────────────────────────────────────────────────

class QueryType(str, Enum):
    FACTUAL    = "factual"     # "what does X do" → Haiku
    CRAFTING   = "crafting"    # "how do I craft X" → Sonnet
    ANALYSIS   = "analysis"    # "what's best for X" → Sonnet
    GUIDE      = "guide"       # "how do I play X build" → Sonnet
    INNOVATION = "innovation"  # "design a novel build" → Opus

# ── Model routing ─────────────────────────────────────────────────────────────

ROUTER_MODEL = "claude-haiku-4-5-20251001"

MODEL_MAP: dict[QueryType, str] = {
    QueryType.FACTUAL:    "claude-haiku-4-5-20251001",
    QueryType.CRAFTING:   "claude-sonnet-4-6",
    QueryType.ANALYSIS:   "claude-sonnet-4-6",
    QueryType.GUIDE:      "claude-sonnet-4-6",
    QueryType.INNOVATION: "claude-opus-4-6",
}

# ── Advisor limits ────────────────────────────────────────────────────────────

MAX_TOOL_ITER = 8    # max agentic loop iterations before forcing a final answer
MAX_RESULTS   = 20   # max rows returned by any tool function
