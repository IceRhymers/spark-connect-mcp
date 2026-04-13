"""Lazy DataFrame MCP tools: load, sql, filter, select, with_column, drop, sort, limit."""

from __future__ import annotations

import json

try:
    from pyspark.sql import functions as F

    _PYSPARK_AVAILABLE = True
except ImportError:
    F = None  # type: ignore[assignment]
    _PYSPARK_AVAILABLE = False

from spark_connect_mcp import dataframes as df_mod
from spark_connect_mcp import session as session_mod
from spark_connect_mcp.server import mcp

# ── Session-scoped tools (require a SparkSession) ────────────────────────────


@mcp.tool()
def load(session_id: str, path: str, format: str = "parquet") -> str:  # noqa: A002
    """Load a file path into a lazy DataFrame handle.

    Supports format: parquet (default), csv, json, orc, delta.
    No Spark job is triggered — use show or collect to materialize.

    Args:
        session_id: Active session handle from start_session
        path: File path or glob (e.g. 's3://bucket/data/*.parquet')
        format: File format — parquet, csv, json, orc, delta
    """
    try:
        spark = session_mod.registry.get(session_id)
    except KeyError as e:
        return json.dumps({"error": str(e), "session_id": session_id})
    try:
        df = spark.read.format(format).load(path)
        df_id = df_mod.registry.register(session_id, df)
        return json.dumps({"df_id": df_id, "message": f"Loaded {path} as {format}"})
    except Exception as e:  # noqa: BLE001
        return json.dumps({"error": str(e), "session_id": session_id})


@mcp.tool()
def sql(session_id: str, query: str) -> str:
    """Execute a SQL query lazily and return a df_id handle.

    No Spark job is triggered — the query plan is built but not executed.
    Use show or collect to materialize results.

    Args:
        session_id: Active session handle from start_session
        query: SQL query string (e.g. 'SELECT * FROM my_table WHERE age > 30')
    """
    try:
        spark = session_mod.registry.get(session_id)
    except KeyError as e:
        return json.dumps({"error": str(e), "session_id": session_id})
    try:
        df = spark.sql(query)
        df_id = df_mod.registry.register(session_id, df)
        return json.dumps({"df_id": df_id, "message": "SQL executed lazily"})
    except Exception as e:  # noqa: BLE001
        return json.dumps({"error": str(e), "session_id": session_id})


# ── Transform tools (df_id only — session resolved via DataFrameRegistry) ────


@mcp.tool()
def filter(df_id: str, condition: str) -> str:  # noqa: A001
    """Filter rows by a SQL condition string. Returns a new df_id.

    Args:
        df_id: Source DataFrame handle
        condition: SQL filter expression (e.g. 'age > 30 AND city = "Seattle"')
    """
    try:
        df = df_mod.registry.get(df_id)
        session_id = df_mod.registry.session_for(df_id)
    except KeyError as e:
        return json.dumps({"error": str(e), "df_id": df_id})
    try:
        new_df = df.filter(condition)
        new_df_id = df_mod.registry.register(session_id, new_df)
        return json.dumps({"df_id": new_df_id})
    except Exception as e:  # noqa: BLE001
        return json.dumps({"error": str(e), "df_id": df_id})


@mcp.tool()
def select(df_id: str, columns: list[str]) -> str:
    """Select columns or SQL expressions. Returns a new df_id.

    Supports SQL expressions such as 'col1 as alias', 'col1 + col2', 'upper(name)'.

    Args:
        df_id: Source DataFrame handle
        columns: List of column names or SQL expressions
    """
    try:
        df = df_mod.registry.get(df_id)
        session_id = df_mod.registry.session_for(df_id)
    except KeyError as e:
        return json.dumps({"error": str(e), "df_id": df_id})
    try:
        new_df = df.selectExpr(*columns)
        new_df_id = df_mod.registry.register(session_id, new_df)
        return json.dumps({"df_id": new_df_id})
    except Exception as e:  # noqa: BLE001
        return json.dumps({"error": str(e), "df_id": df_id})


@mcp.tool()
def with_column(df_id: str, name: str, expression: str) -> str:
    """Add or replace a column using a SQL expression. Returns a new df_id.

    Args:
        df_id: Source DataFrame handle
        name: Name for the new column
        expression: SQL expression (e.g. 'col1 * 2', 'upper(name)', 'col1 + col2')
    """
    try:
        df = df_mod.registry.get(df_id)
        session_id = df_mod.registry.session_for(df_id)
    except KeyError as e:
        return json.dumps({"error": str(e), "df_id": df_id})
    try:
        new_df = df.withColumn(name, F.expr(expression))
        new_df_id = df_mod.registry.register(session_id, new_df)
        return json.dumps({"df_id": new_df_id})
    except Exception as e:  # noqa: BLE001
        return json.dumps({"error": str(e), "df_id": df_id})


@mcp.tool()
def drop(df_id: str, columns: list[str]) -> str:
    """Drop one or more columns. Returns a new df_id.

    Args:
        df_id: Source DataFrame handle
        columns: List of column names to drop
    """
    try:
        df = df_mod.registry.get(df_id)
        session_id = df_mod.registry.session_for(df_id)
    except KeyError as e:
        return json.dumps({"error": str(e), "df_id": df_id})
    try:
        new_df = df.drop(*columns)
        new_df_id = df_mod.registry.register(session_id, new_df)
        return json.dumps({"df_id": new_df_id})
    except Exception as e:  # noqa: BLE001
        return json.dumps({"error": str(e), "df_id": df_id})


@mcp.tool()
def sort(df_id: str, columns: list[str], ascending: bool = True) -> str:
    """Sort by one or more columns. Returns a new df_id.

    Args:
        df_id: Source DataFrame handle
        columns: Column names to sort by
        ascending: Sort direction applied to all columns (True = ASC, False = DESC)
    """
    try:
        df = df_mod.registry.get(df_id)
        session_id = df_mod.registry.session_for(df_id)
    except KeyError as e:
        return json.dumps({"error": str(e), "df_id": df_id})
    try:
        sort_cols = [F.col(c).asc() if ascending else F.col(c).desc() for c in columns]
        new_df = df.sort(*sort_cols)
        new_df_id = df_mod.registry.register(session_id, new_df)
        return json.dumps({"df_id": new_df_id})
    except Exception as e:  # noqa: BLE001
        return json.dumps({"error": str(e), "df_id": df_id})


@mcp.tool()
def limit(df_id: str, n: int) -> str:
    """Limit the DataFrame to at most n rows lazily. Returns a new df_id.

    No Spark job is triggered. Use show or collect to materialize.

    Args:
        df_id: Source DataFrame handle
        n: Maximum number of rows
    """
    try:
        df = df_mod.registry.get(df_id)
        session_id = df_mod.registry.session_for(df_id)
    except KeyError as e:
        return json.dumps({"error": str(e), "df_id": df_id})
    try:
        new_df = df.limit(n)
        new_df_id = df_mod.registry.register(session_id, new_df)
        return json.dumps({"df_id": new_df_id})
    except Exception as e:  # noqa: BLE001
        return json.dumps({"error": str(e), "df_id": df_id})


# ── Deferred tools (implemented in subsequent issues) ────────────────────────


@mcp.tool()
def group_by_agg(df_id: str, group_cols: list[str], agg_exprs: list[str]) -> str:
    """Group and aggregate a DataFrame. (Not yet implemented — see issue #9.)"""
    raise NotImplementedError


@mcp.tool()
def join(left_df_id: str, right_df_id: str, on: str, how: str = "inner") -> str:
    """Join two DataFrames. (Not yet implemented — see issue #9.)"""
    raise NotImplementedError
