"""Preflight size-check module — estimates DataFrame size from CBO statistics."""

from __future__ import annotations

import os
import re
from contextlib import redirect_stdout
from dataclasses import dataclass
from io import StringIO
from typing import Any


@dataclass
class PreflightResult:
    confidence: str  # "high", "medium", "low", "cross_join"
    estimated_bytes: int | None
    estimated_rows: int | None
    should_block: bool
    warning: str | None


# Module-level dict for per-session threshold overrides.
_session_overrides: dict[str, dict] = {}

# Size unit multipliers (binary prefixes).
_SIZE_UNITS: dict[str, int] = {
    "B": 1,
    "KiB": 1024,
    "MiB": 1024**2,
    "GiB": 1024**3,
    "TiB": 1024**4,
}

# Default thresholds.
_DEFAULT_MAX_BYTES = 1_073_741_824  # 1 GB
_DEFAULT_MAX_ROWS = 10_000_000  # 10M

# Regex for Statistics lines.
_STATS_RE = re.compile(r"Statistics\(sizeInBytes=([^,)]+)(?:,\s*rowCount=([^,)]+))?\)")

# Regex to detect join lines (before their Statistics).
_JOIN_RE = re.compile(r"(Join\s+\w+|CartesianProduct)")


def _parse_size(size_str: str) -> int:
    """Parse a human-readable size string like '47.3 GiB' into bytes."""
    size_str = size_str.strip()
    # Check longest units first to avoid "B" matching "KiB", "GiB", etc.
    for unit in ("TiB", "GiB", "MiB", "KiB", "B"):
        if size_str.endswith(unit):
            num = size_str[: -len(unit)].strip()
            return int(float(num) * _SIZE_UNITS[unit])
    # Fallback: try parsing as plain number
    return int(float(size_str))


def _parse_row_count(rc_str: str) -> int:
    """Parse a row count string, handling scientific notation like '1.5E+8'."""
    return int(float(rc_str.strip()))


def _get_thresholds(session_id: str | None) -> tuple[int, int, bool]:
    """Return (max_bytes, max_rows, enabled) considering session overrides and env vars."""
    # Check session overrides first
    if session_id and session_id in _session_overrides:
        overrides = _session_overrides[session_id]
        if not overrides.get("enabled", True):
            return 0, 0, False
        max_bytes = overrides.get("max_bytes", None)
        max_rows = overrides.get("max_rows", None)
        if max_bytes is None:
            max_bytes = int(
                os.environ.get(
                    "SPARK_CONNECT_MCP_PREFLIGHT_MAX_BYTES", _DEFAULT_MAX_BYTES
                )
            )
        if max_rows is None:
            max_rows = int(
                os.environ.get(
                    "SPARK_CONNECT_MCP_PREFLIGHT_MAX_ROWS", _DEFAULT_MAX_ROWS
                )
            )
        return max_bytes, max_rows, True

    # Env var check for enabled
    enabled = os.environ.get("SPARK_CONNECT_MCP_PREFLIGHT_ENABLED", "true").lower()
    if enabled == "false":
        return 0, 0, False

    max_bytes = int(
        os.environ.get("SPARK_CONNECT_MCP_PREFLIGHT_MAX_BYTES", _DEFAULT_MAX_BYTES)
    )
    max_rows = int(
        os.environ.get("SPARK_CONNECT_MCP_PREFLIGHT_MAX_ROWS", _DEFAULT_MAX_ROWS)
    )
    return max_bytes, max_rows, True


def estimate_size(df: Any, session_id: str | None = None) -> PreflightResult | None:  # noqa: C901
    """Estimate DataFrame size from Spark CBO statistics.

    Returns a PreflightResult if the estimated size exceeds thresholds,
    or None if below thresholds / disabled / parse failure (fail-open).
    """
    max_bytes, max_rows, enabled = _get_thresholds(session_id)
    if not enabled:
        return None

    # Capture explain("cost") output
    try:
        buf = StringIO()
        with redirect_stdout(buf):
            df.explain("cost")
        plan = buf.getvalue()
    except Exception:  # noqa: BLE001
        return None

    if not plan or "== Optimized Logical Plan ==" not in plan:
        return None

    # Extract the Optimized Logical Plan section
    optimized_section = plan.split("== Optimized Logical Plan ==")[1]
    # If there's another section after, cut it off
    for marker in (
        "== Physical Plan ==",
        "== Parsed Logical Plan ==",
        "== Analyzed Logical Plan ==",
    ):
        if marker in optimized_section:
            optimized_section = optimized_section.split(marker)[0]

    # Check for CartesianProduct
    has_cartesian = "CartesianProduct" in optimized_section

    # Parse all Statistics lines and track which are join-adjacent
    lines = optimized_section.split("\n")
    all_stats: list[tuple[int | None, int | None]] = []  # (size_bytes, row_count)
    join_adjacent_stats: list[tuple[int | None, int | None]] = []
    in_join_context = False

    for line in lines:
        # Check if this line is a join line
        if _JOIN_RE.search(line):
            in_join_context = True
            continue

        stat_match = _STATS_RE.search(line)
        if stat_match:
            try:
                size_bytes = _parse_size(stat_match.group(1))
            except (ValueError, IndexError):
                size_bytes = None

            row_count = None
            if stat_match.group(2):
                try:
                    row_count = _parse_row_count(stat_match.group(2))
                except (ValueError, IndexError):
                    row_count = None

            all_stats.append((size_bytes, row_count))
            if in_join_context:
                join_adjacent_stats.append((size_bytes, row_count))
            in_join_context = False
        elif line.strip() and not line.strip().startswith((":", "+-", "|")):
            # Non-stat, non-tree-structure line — reset join context only if it's
            # a new operator node (not tree drawing characters)
            in_join_context = False

    if not all_stats:
        return None

    # Root node is the last Statistics entry (deepest/outermost in the plan)
    root_bytes, root_rows = all_stats[-1]

    if root_bytes is None:
        return None

    # Determine confidence
    if has_cartesian:
        confidence = "cross_join"
    elif root_rows is not None:
        if join_adjacent_stats:
            all_join_have_rows = all(rc is not None for _, rc in join_adjacent_stats)
            confidence = "high" if all_join_have_rows else "medium"
        else:
            confidence = "high"
    else:
        confidence = "low"

    # Apply thresholds based on confidence
    if confidence == "medium":
        effective_max_bytes = max_bytes * 10
        effective_max_rows = max_rows * 10
    else:
        effective_max_bytes = max_bytes
        effective_max_rows = max_rows

    # Check if exceeds thresholds
    bytes_exceed = root_bytes > effective_max_bytes
    rows_exceed = root_rows is not None and root_rows > effective_max_rows

    if confidence == "cross_join":
        # CartesianProduct always warns
        should_block = True
    elif confidence == "low":
        # Low confidence = fail-open, never block
        should_block = False
    elif bytes_exceed or rows_exceed:
        should_block = True
    else:
        # Below threshold
        return None

    # Build warning message
    size_display = _format_size(root_bytes)
    rows_display = _format_rows(root_rows) if root_rows else "unknown"
    if confidence == "cross_join":
        warning = f"CartesianProduct detected ({confidence} confidence): ~{size_display}, ~{rows_display} rows"
    else:
        warning = f"Large DataFrame detected ({confidence} confidence): ~{size_display}, ~{rows_display} rows"

    return PreflightResult(
        confidence=confidence,
        estimated_bytes=root_bytes,
        estimated_rows=root_rows,
        should_block=should_block,
        warning=warning,
    )


def set_preflight_threshold(
    session_id: str,
    max_bytes: int | None = None,
    max_rows: int | None = None,
    enabled: bool | None = None,
) -> None:
    """Set per-session preflight threshold overrides."""
    overrides: dict = _session_overrides.get(session_id, {})
    if max_bytes is not None:
        overrides["max_bytes"] = max_bytes
    if max_rows is not None:
        overrides["max_rows"] = max_rows
    if enabled is not None:
        overrides["enabled"] = enabled
    _session_overrides[session_id] = overrides


def _format_size(size_bytes: int) -> str:
    """Format bytes into a human-readable string."""
    for unit in ("TiB", "GiB", "MiB", "KiB"):
        divisor = _SIZE_UNITS[unit]
        if size_bytes >= divisor:
            return f"{size_bytes / divisor:.1f} {unit}"
    return f"{size_bytes} B"


def _format_rows(rows: int) -> str:
    """Format row count into a human-readable string."""
    if rows >= 1_000_000_000:
        return f"{rows / 1_000_000_000:.1f}B"
    if rows >= 1_000_000:
        return f"{rows / 1_000_000:.0f}M"
    if rows >= 1_000:
        return f"{rows / 1_000:.0f}K"
    return str(rows)
