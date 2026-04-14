"""
Shared pytest fixtures for ExileSage tests.
"""

import os
from pathlib import Path

# Load .env for integration tests
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

import pytest
import sqlite3

from exilesage.config import DB_PATH
from exilesage.db import get_connection


def pytest_configure(config):
    config.addinivalue_line("markers", "integration: requires ANTHROPIC_API_KEY and uses API credits")
    config.addinivalue_line("markers", "slow: takes more than 10 seconds")


@pytest.fixture(scope="session")
def db_conn():
    """Session-scoped read-only DB connection."""
    conn = get_connection()
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def has_api_key():
    return bool(os.environ.get("ANTHROPIC_API_KEY", "").startswith("sk-ant"))


# S5: Clear module-level caches between tests to prevent contamination.
@pytest.fixture(autouse=True)
def _clear_module_caches():
    """S5: Clear module-level caches before each test."""
    from scraper.repoe import _patch_version_cache
    _patch_version_cache.clear()
    from exilesage.advisor import core as _core
    if hasattr(_core, "_PROMPT_CACHE"):
        _core._PROMPT_CACHE.clear()
    yield
    # Clear again after test for hygiene
    _patch_version_cache.clear()
    if hasattr(_core, "_PROMPT_CACHE"):
        _core._PROMPT_CACHE.clear()
