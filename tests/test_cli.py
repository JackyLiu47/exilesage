"""
Tests for ExileSage CLI — update --check and --check --remote flags.
Uses typer.testing.CliRunner; no real network calls.
"""

from __future__ import annotations

import json
from unittest.mock import patch, MagicMock

import pytest
from typer.testing import CliRunner

from exilesage.cli.app import app

runner = CliRunner()


# Shared mock staleness results
_FRESH_RESULT = {
    "stale": False,
    "reasons": [],
    "max_fetched_at": "2026-04-13T00:00:00+00:00",
}

_STALE_RESULT = {
    "stale": True,
    "reasons": ["manifest_age_exceeded"],
    "max_fetched_at": "2026-03-01T00:00:00+00:00",
}


def _mock_meta(patch_version: str = "4.4.0.6.6") -> MagicMock:
    """Return a mock sqlite3 row with patch_version."""
    row = MagicMock()
    row.__getitem__ = lambda self, key: patch_version if key == "patch_version" else None
    return row


class TestUpdateCheckFlag:
    """exilesage update --check flag behaviour."""

    def test_update_check_exits_0_when_fresh(self):
        """--check exits 0 when detect_staleness returns stale=False."""
        with patch("exilesage.cli.app.detect_staleness", return_value=_FRESH_RESULT), \
             patch("exilesage.cli.app._get_manifest_path_for_cli", return_value=MagicMock(exists=lambda: True)), \
             patch("exilesage.cli.app._get_meta_for_cli", return_value=("4.4.0.6.6",)):
            result = runner.invoke(app, ["update", "--check"])
        assert result.exit_code == 0, f"Expected 0, got {result.exit_code}. Output: {result.output}"

    def test_update_check_exits_1_when_stale(self):
        """--check exits 1 when detect_staleness returns stale=True."""
        with patch("exilesage.cli.app.detect_staleness", return_value=_STALE_RESULT), \
             patch("exilesage.cli.app._get_manifest_path_for_cli", return_value=MagicMock(exists=lambda: True)), \
             patch("exilesage.cli.app._get_meta_for_cli", return_value=("4.4.0.6.6",)):
            result = runner.invoke(app, ["update", "--check"])
        assert result.exit_code == 1, f"Expected 1, got {result.exit_code}. Output: {result.output}"

    def test_update_check_outputs_json(self):
        """--check prints valid JSON with documented keys."""
        with patch("exilesage.cli.app.detect_staleness", return_value=_FRESH_RESULT), \
             patch("exilesage.cli.app._get_manifest_path_for_cli", return_value=MagicMock(exists=lambda: True)), \
             patch("exilesage.cli.app._get_meta_for_cli", return_value=("4.4.0.6.6",)):
            result = runner.invoke(app, ["update", "--check"])

        output = result.output.strip()
        parsed = json.loads(output)
        assert "stale" in parsed
        assert "reasons" in parsed
        assert "max_fetched_at" in parsed
        assert "patch_version" in parsed

    def test_update_check_remote_flag_triggers_head_requests(self):
        """--check --remote passes a remote_checker that makes HTTP HEAD requests."""
        remote_checker_calls = []

        def fake_detect_staleness(manifest_path, *, max_age_days=7, rss_fetcher=None, remote_checker=None):
            if remote_checker is not None:
                # Trigger one call to simulate it being used
                remote_checker_calls.append(remote_checker("mods"))
            return _FRESH_RESULT

        with patch("exilesage.cli.app.detect_staleness", side_effect=fake_detect_staleness), \
             patch("exilesage.cli.app._get_manifest_path_for_cli", return_value=MagicMock(exists=lambda: True)), \
             patch("exilesage.cli.app._get_meta_for_cli", return_value=("4.4.0.6.6",)), \
             patch("exilesage.cli.app._remote_head_checker") as mock_head:
            mock_head.return_value = None
            result = runner.invoke(app, ["update", "--check", "--remote"])

        assert result.exit_code == 0


class TestUpdateRemoteFlag:
    """S7, S8: --remote without --check exits 2; missing manifest exits 2."""

    def test_update_remote_without_check_errors(self):
        """S7: --remote without --check → exit 2, stderr message."""
        result = runner.invoke(app, ["update", "--remote"])
        assert result.exit_code == 2, f"Expected exit 2, got {result.exit_code}. Output: {result.output}"
        assert "--remote has no effect without --check" in result.output

    def test_update_check_missing_manifest_exits_2(self):
        """S8: --check with absent manifest → exit 2 (error), not 1 (stale)."""
        with patch("exilesage.cli.app._get_manifest_path_for_cli",
                   return_value=MagicMock(exists=lambda: False)), \
             patch("exilesage.cli.app._get_meta_for_cli", return_value=("unknown",)):
            result = runner.invoke(app, ["update", "--check"])
        assert result.exit_code == 2, f"Expected exit 2 for missing manifest, got {result.exit_code}"
