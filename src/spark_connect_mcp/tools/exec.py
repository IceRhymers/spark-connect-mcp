"""Execution / action tools — trigger computation on lazy DataFrames."""

from __future__ import annotations

import io
import json
from contextlib import redirect_stdout
from typing import TYPE_CHECKING

from spark_connect_mcp import dataframes as df_mod

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
from spark_connect_mcp.preflight import estimate_size
from spark_connect_mcp.server import mcp

# Maximum rows collect() will return to prevent agent-triggered OOM.
MAX_COLLECT_LIMIT = 1000


def _run_preflight(df: DataFrame, force: bool) -> str | None:
    """Run preflight size check. Returns warning JSON string if blocked, else None."""
    if force:
        return None
    result = estimate_size(df)
    if result is None:
        return None
    if result.should_block:
        return json.dumps({"warning": result.warning, "preflight": result.confidence})
    return None


@mcp.tool()
def show(df_id: str, n: int = 20, truncate: bool = True, force: bool = False) -> str:
    """Show the first n rows of a DataFrame as a formatted ASCII table.

    Triggers a Spark job. Returns plain text (not JSON) for direct display.

    Args:
        df_id: DataFrame handle
        n: Number of rows to show (default 20)
        truncate: Truncate long values in cells (default True)
        force: Skip preflight size check (default False)
    """
    # NOTE: Issue #39 open question #1 — show(n=20) is not exempted from
    # preflight for now; the issue hasn't finalized that decision.
    try:
        df = df_mod.registry.get(df_id)
    except KeyError as e:
        return f"Error: {e}"
    warning = _run_preflight(df, force)
    if warning is not None:
        return warning
    try:
        buf = io.StringIO()
        with redirect_stdout(buf):
            df.show(n, truncate)
        return buf.getvalue()
    except Exception as e:  # noqa: BLE001
        return f"Error: {e}"


@mcp.tool()
def collect(df_id: str, limit: int = 100, force: bool = False) -> str:
    """Collect rows as a JSON list of dicts.

    Triggers a Spark job. Enforces a hard maximum of MAX_COLLECT_LIMIT rows
    to prevent agents from pulling entire large tables into memory.

    Args:
        df_id: DataFrame handle
        limit: Max rows to collect (default 100, hard max 1000)
        force: Skip preflight size check (default False)
    """
    try:
        df = df_mod.registry.get(df_id)
    except KeyError as e:
        return json.dumps({"error": str(e), "df_id": df_id})
    warning = _run_preflight(df, force)
    if warning is not None:
        return warning
    try:
        safe_limit = min(limit, MAX_COLLECT_LIMIT)
        rows = df.limit(safe_limit).collect()
        return json.dumps([row.asDict() for row in rows])
    except Exception as e:  # noqa: BLE001
        return json.dumps({"error": str(e), "df_id": df_id})


@mcp.tool()
def count(df_id: str, force: bool = False) -> str:
    """Return the row count of a DataFrame.

    Triggers a Spark job (full table scan).

    Args:
        df_id: DataFrame handle
        force: Skip preflight size check (default False)
    """
    try:
        df = df_mod.registry.get(df_id)
    except KeyError as e:
        return json.dumps({"error": str(e), "df_id": df_id})
    warning = _run_preflight(df, force)
    if warning is not None:
        return warning
    try:
        n = df.count()
        return json.dumps({"count": n, "df_id": df_id})
    except Exception as e:  # noqa: BLE001
        return json.dumps({"error": str(e), "df_id": df_id})


@mcp.tool()
def schema(df_id: str) -> str:
    """Return the schema of a DataFrame as JSON.

    May trigger Spark analysis but not a full computation job.

    Args:
        df_id: DataFrame handle
    """
    try:
        df = df_mod.registry.get(df_id)
    except KeyError as e:
        return json.dumps({"error": str(e), "df_id": df_id})
    try:
        return json.dumps(df.schema.jsonValue())
    except Exception as e:  # noqa: BLE001
        return json.dumps({"error": str(e), "df_id": df_id})


@mcp.tool()
def describe(df_id: str, columns: list[str] | None = None, force: bool = False) -> str:
    """Return summary statistics (count, mean, stddev, min, max) as JSON rows.

    Triggers a Spark job. Computes statistics for numeric and string columns.

    Args:
        df_id: DataFrame handle
        columns: Optional list of column names to describe. Describes all columns if omitted.
        force: Skip preflight size check (default False)
    """
    try:
        df = df_mod.registry.get(df_id)
    except KeyError as e:
        return json.dumps({"error": str(e), "df_id": df_id})
    warning = _run_preflight(df, force)
    if warning is not None:
        return warning
    try:
        desc_df = df.describe(*columns) if columns else df.describe()
        rows = desc_df.collect()
        return json.dumps([row.asDict() for row in rows])
    except Exception as e:  # noqa: BLE001
        return json.dumps({"error": str(e), "df_id": df_id})
