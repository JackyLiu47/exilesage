"""
Shared pytest fixtures for ExileSage tests.
"""

import sys
import os
from pathlib import Path

# Make project root importable
sys.path.insert(0, str(Path(__file__).parent.parent))

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
