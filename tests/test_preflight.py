"""Tests for preflight size-check module — written TDD-first before implementation."""

from __future__ import annotations

import io
import json
import os
from contextlib import redirect_stdout
from unittest.mock import MagicMock, patch

import pytest

# ── Explain output fixtures ─────────────────────────────────────────────────

EXPLAIN_HIGH_CONFIDENCE = """\
== Optimized Logical Plan ==
Join Inner
:  Statistics(sizeInBytes=2.0 GiB, rowCount=5.0E+6)
+- Filter
   Statistics(sizeInBytes=47.3 GiB, rowCount=1.5E+8)
"""

EXPLAIN_MEDIUM_CONFIDENCE = """\
== Optimized Logical Plan ==
Join Inner
:  Statistics(sizeInBytes=2.0 GiB)
+- Filter
   Statistics(sizeInBytes=47.3 GiB, rowCount=1.5E+8)
"""

EXPLAIN_LOW_CONFIDENCE = """\
== Optimized Logical Plan ==
Statistics(sizeInBytes=47.3 GiB)
"""

EXPLAIN_CARTESIAN = """\
== Optimized Logical Plan ==
CartesianProduct
Statistics(sizeInBytes=999.9 TiB, rowCount=9.9E+15)
"""

EXPLAIN_SMALL = """\
== Optimized Logical Plan ==
Statistics(sizeInBytes=32.0 B, rowCount=5)
"""

EXPLAIN_EMPTY = ""


# ── Helpers ──────────────────────────────────────────────────────────────────


def _mock_df_with_explain(explain_text: str) -> MagicMock:
    """Create a mock DataFrame whose explain() prints explain_text to stdout."""
    mock_df = MagicMock()

    def fake_explain(mode=None):
        print(explain_text, end="")

    mock_df.explain.side_effect = fake_explain
    return mock_df


# ══════════════════════════════════════════════════════════════════════════════
# Unit tests for estimate_size()
# ══════════════════════════════════════════════════════════════════════════════


class TestEstimateSizeHighConfidence:
    """Root has sizeInBytes + rowCount, all join nodes have rowCount."""

    def test_returns_preflight_result(self):
        from spark_connect_mcp.preflight import estimate_size

        df = _mock_df_with_explain(EXPLAIN_HIGH_CONFIDENCE)
        result = estimate_size(df, session_id="s1")

        assert result is not None
        assert result.confidence == "high"

    def test_high_confidence_has_size_and_rows(self):
        from spark_connect_mcp.preflight import estimate_size

        df = _mock_df_with_explain(EXPLAIN_HIGH_CONFIDENCE)
        result = estimate_size(df, session_id="s1")

        assert result is not None
        assert result.estimated_bytes > 0
        assert result.estimated_rows > 0


class TestEstimateSizeMediumConfidence:
    """Root has rowCount, some join nodes missing rowCount."""

    def test_returns_medium_confidence(self):
        from spark_connect_mcp.preflight import estimate_size

        df = _mock_df_with_explain(EXPLAIN_MEDIUM_CONFIDENCE)
        result = estimate_size(df, session_id="s1")

        assert result is not None
        assert result.confidence == "medium"


class TestEstimateSizeLowConfidence:
    """Root has sizeInBytes but NO rowCount — fail-open, no blocking."""

    def test_returns_low_confidence(self):
        from spark_connect_mcp.preflight import estimate_size

        df = _mock_df_with_explain(EXPLAIN_LOW_CONFIDENCE)
        result = estimate_size(df, session_id="s1")

        assert result is not None
        assert result.confidence == "low"

    def test_low_confidence_is_not_blocking(self):
        from spark_connect_mcp.preflight import estimate_size

        df = _mock_df_with_explain(EXPLAIN_LOW_CONFIDENCE)
        result = estimate_size(df, session_id="s1")

        assert result is not None
        assert result.should_block is False


class TestEstimateSizeCartesianProduct:
    """CartesianProduct in plan → always warns."""

    def test_returns_cross_join_confidence(self):
        from spark_connect_mcp.preflight import estimate_size

        df = _mock_df_with_explain(EXPLAIN_CARTESIAN)
        result = estimate_size(df, session_id="s1")

        assert result is not None
        assert result.confidence == "cross_join"

    def test_cross_join_always_warns(self):
        from spark_connect_mcp.preflight import estimate_size

        df = _mock_df_with_explain(EXPLAIN_CARTESIAN)
        result = estimate_size(df, session_id="s1")

        assert result is not None
        assert result.warning is not None
        assert "CartesianProduct" in result.warning or "cross" in result.warning.lower()


class TestEstimateSizeParseFailures:
    """Parse fail / empty explain output → returns None (fail-open)."""

    def test_empty_explain_returns_none(self):
        from spark_connect_mcp.preflight import estimate_size

        df = _mock_df_with_explain(EXPLAIN_EMPTY)
        result = estimate_size(df, session_id="s1")

        assert result is None

    def test_garbage_explain_returns_none(self):
        from spark_connect_mcp.preflight import estimate_size

        df = _mock_df_with_explain("this is not a valid explain plan at all")
        result = estimate_size(df, session_id="s1")

        assert result is None

    def test_explain_raises_exception_returns_none(self):
        from spark_connect_mcp.preflight import estimate_size

        df = MagicMock()
        df.explain.side_effect = RuntimeError("explain failed")
        result = estimate_size(df, session_id="s1")

        assert result is None


class TestEstimateSizeBelowThreshold:
    """Size below threshold → returns None (no warning needed)."""

    def test_small_dataframe_returns_none(self):
        from spark_connect_mcp.preflight import estimate_size

        df = _mock_df_with_explain(EXPLAIN_SMALL)
        result = estimate_size(df, session_id="s1")

        assert result is None


class TestScientificNotationParsing:
    """Scientific notation rowCount parsing: '1.5E+8' → 150_000_000."""

    def test_parses_scientific_notation_rowcount(self):
        from spark_connect_mcp.preflight import estimate_size

        df = _mock_df_with_explain(EXPLAIN_HIGH_CONFIDENCE)
        result = estimate_size(df, session_id="s1")

        assert result is not None
        # The root plan node has rowCount=1.5E+8 = 150_000_000
        assert result.estimated_rows == 150_000_000


class TestHumanReadableSizeParsing:
    """Human-readable size parsing: '32.0 B', '47.3 GiB', '1.5 TiB'."""

    def test_parses_bytes(self):
        from spark_connect_mcp.preflight import _parse_size

        assert _parse_size("32.0 B") == 32

    def test_parses_kib(self):
        from spark_connect_mcp.preflight import _parse_size

        assert _parse_size("1.0 KiB") == 1024

    def test_parses_mib(self):
        from spark_connect_mcp.preflight import _parse_size

        assert _parse_size("1.0 MiB") == 1024 * 1024

    def test_parses_gib(self):
        from spark_connect_mcp.preflight import _parse_size

        assert _parse_size("47.3 GiB") == int(47.3 * 1024**3)

    def test_parses_tib(self):
        from spark_connect_mcp.preflight import _parse_size

        assert _parse_size("1.5 TiB") == int(1.5 * 1024**4)


class TestEnvVarThresholds:
    """Env var thresholds override defaults."""

    def test_max_bytes_env_var(self, monkeypatch):
        from spark_connect_mcp.preflight import estimate_size

        # Set a very low threshold so the "small" plan triggers a warning
        monkeypatch.setenv("SPARK_CONNECT_MCP_PREFLIGHT_MAX_BYTES", "10")
        df = _mock_df_with_explain(EXPLAIN_SMALL)
        result = estimate_size(df, session_id="s1")

        assert result is not None
        assert result.estimated_bytes >= 10

    def test_max_rows_env_var(self, monkeypatch):
        from spark_connect_mcp.preflight import estimate_size

        # Set a very low row threshold
        monkeypatch.setenv("SPARK_CONNECT_MCP_PREFLIGHT_MAX_ROWS", "1")
        df = _mock_df_with_explain(EXPLAIN_SMALL)
        result = estimate_size(df, session_id="s1")

        assert result is not None
        assert result.estimated_rows >= 1


class TestPreflightDisabledEnvVar:
    """SPARK_CONNECT_MCP_PREFLIGHT_ENABLED=false → returns None immediately."""

    def test_disabled_returns_none(self, monkeypatch):
        from spark_connect_mcp.preflight import estimate_size

        monkeypatch.setenv("SPARK_CONNECT_MCP_PREFLIGHT_ENABLED", "false")
        df = _mock_df_with_explain(EXPLAIN_HIGH_CONFIDENCE)
        result = estimate_size(df, session_id="s1")

        assert result is None

    def test_disabled_does_not_call_explain(self, monkeypatch):
        from spark_connect_mcp.preflight import estimate_size

        monkeypatch.setenv("SPARK_CONNECT_MCP_PREFLIGHT_ENABLED", "false")
        df = MagicMock()
        estimate_size(df, session_id="s1")

        df.explain.assert_not_called()


# ══════════════════════════════════════════════════════════════════════════════
# Integration-style tests for exec.py tools (mock estimate_size)
# ══════════════════════════════════════════════════════════════════════════════


def _make_preflight_warning(confidence="high"):
    """Build a mock PreflightResult that represents a warning."""
    from spark_connect_mcp.preflight import PreflightResult

    return PreflightResult(
        confidence=confidence,
        estimated_bytes=50 * 1024**3,  # 50 GiB
        estimated_rows=150_000_000,
        should_block=confidence not in ("low",),
        warning=f"Large DataFrame detected ({confidence} confidence): ~50.0 GiB, ~150M rows",
    )


class TestShowPreflight:
    """show() integration with preflight."""

    @patch("spark_connect_mcp.tools.exec.estimate_size")
    @patch("spark_connect_mcp.tools.exec.df_mod")
    def test_show_force_false_high_confidence_returns_warning(
        self, mock_df_mod, mock_estimate
    ):
        mock_frame = MagicMock()
        mock_df_mod.registry.get.return_value = mock_frame
        mock_estimate.return_value = _make_preflight_warning("high")

        result = show("df-1", n=20, truncate=True, force=False)

        parsed = json.loads(result)
        assert "warning" in parsed or "preflight" in parsed
        mock_frame.show.assert_not_called()

    @patch("spark_connect_mcp.tools.exec.estimate_size")
    @patch("spark_connect_mcp.tools.exec.df_mod")
    def test_show_force_true_skips_preflight(self, mock_df_mod, mock_estimate):
        mock_frame = MagicMock()
        mock_df_mod.registry.get.return_value = mock_frame

        def fake_show(n, truncate):
            print("+---+\n| id|\n+---+\n|  1|\n+---+")

        mock_frame.show.side_effect = fake_show

        result = show("df-1", n=20, truncate=True, force=True)

        mock_estimate.assert_not_called()
        assert "+---+" in result


class TestCountPreflight:
    """count() integration with preflight."""

    @patch("spark_connect_mcp.tools.exec.estimate_size")
    @patch("spark_connect_mcp.tools.exec.df_mod")
    def test_count_below_threshold_executes(self, mock_df_mod, mock_estimate):
        mock_frame = MagicMock()
        mock_df_mod.registry.get.return_value = mock_frame
        mock_frame.count.return_value = 42
        mock_estimate.return_value = None  # Below threshold

        result = json.loads(count("df-1", force=False))

        assert result["count"] == 42


class TestCollectPreflight:
    """collect() integration with preflight."""

    @patch("spark_connect_mcp.tools.exec.estimate_size")
    @patch("spark_connect_mcp.tools.exec.df_mod")
    def test_collect_cross_join_returns_warning(self, mock_df_mod, mock_estimate):
        mock_frame = MagicMock()
        mock_df_mod.registry.get.return_value = mock_frame
        mock_estimate.return_value = _make_preflight_warning("cross_join")

        result = json.loads(collect("df-1", limit=100, force=False))

        assert "warning" in result or "preflight" in result
        mock_frame.limit.assert_not_called()


class TestDescribePreflight:
    """describe() with low-confidence → executes (fail-open), includes stats_warning."""

    @patch("spark_connect_mcp.tools.exec.estimate_size")
    @patch("spark_connect_mcp.tools.exec.df_mod")
    def test_describe_low_confidence_executes_with_warning(
        self, mock_df_mod, mock_estimate
    ):
        mock_frame = MagicMock()
        mock_df_mod.registry.get.return_value = mock_frame
        stat_row = MagicMock()
        stat_row.asDict.return_value = {"summary": "count", "age": "100"}
        mock_frame.describe.return_value.collect.return_value = [stat_row]

        low_result = _make_preflight_warning("low")
        low_result.should_block = False
        mock_estimate.return_value = low_result

        result = json.loads(describe("df-1", force=False))

        # Should execute (fail-open) but include a stats_warning
        if isinstance(result, list):
            # Result is stats rows — check the tool still executed
            assert len(result) > 0
        else:
            # Result is a dict with stats + warning
            assert "stats_warning" in result or "warning" in result


class TestSavePreflight:
    """save() and save_as_table() with force=False, above threshold → warning."""

    @patch("spark_connect_mcp.tools.write.estimate_size")
    @patch("spark_connect_mcp.tools.write.df_mod")
    def test_save_above_threshold_returns_warning(self, mock_df_mod, mock_estimate):
        mock_frame = MagicMock()
        mock_df_mod.registry.get.return_value = mock_frame
        mock_estimate.return_value = _make_preflight_warning("high")

        from spark_connect_mcp.tools.write import save

        result = json.loads(save("df-1", path="/tmp/out", force=False))

        assert "warning" in result or "preflight" in result

    @patch("spark_connect_mcp.tools.write.estimate_size")
    @patch("spark_connect_mcp.tools.write.df_mod")
    def test_save_as_table_above_threshold_returns_warning(
        self, mock_df_mod, mock_estimate
    ):
        mock_frame = MagicMock()
        mock_df_mod.registry.get.return_value = mock_frame
        mock_estimate.return_value = _make_preflight_warning("high")

        from spark_connect_mcp.tools.write import save_as_table

        result = json.loads(save_as_table("df-1", table_name="db.t", force=False))

        assert "warning" in result or "preflight" in result


# ══════════════════════════════════════════════════════════════════════════════
# Tests for set_preflight_threshold tool
# ══════════════════════════════════════════════════════════════════════════════


class TestSetPreflightThreshold:
    """set_preflight_threshold overrides max_bytes/max_rows for session."""

    def test_overrides_max_bytes_and_max_rows(self):
        from spark_connect_mcp.preflight import (
            estimate_size,
            set_preflight_threshold,
        )

        set_preflight_threshold(
            session_id="s1", max_bytes=100, max_rows=10
        )

        # A plan that is small by default thresholds but above our custom ones
        df = _mock_df_with_explain(EXPLAIN_SMALL)
        result = estimate_size(df, session_id="s1")

        assert result is not None
        assert result.estimated_bytes >= 100 or result.estimated_rows >= 10

    def test_overrides_with_enabled_false_disables(self):
        from spark_connect_mcp.preflight import (
            estimate_size,
            set_preflight_threshold,
        )

        set_preflight_threshold(session_id="s1", enabled=False)

        df = _mock_df_with_explain(EXPLAIN_HIGH_CONFIDENCE)
        result = estimate_size(df, session_id="s1")

        assert result is None


# ── Import the tools with updated signatures for integration tests ───────────
# We re-import here so the patched versions with `force` param are used.

from spark_connect_mcp.tools.exec import collect, count, describe, show
