"""Write tools — persist DataFrames to storage, triggering Spark computation."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from spark_connect_mcp import dataframes as df_mod

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
from spark_connect_mcp.preflight import estimate_size
from spark_connect_mcp.server import mcp


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
def save(  # noqa: A001
    df_id: str,
    path: str,
    format: str = "delta",  # noqa: A002
    mode: str = "error",
    partition_by: list[str] | None = None,
    cluster_by: list[str] | None = None,
    force: bool = False,
) -> str:
    """Write a DataFrame to a file path, triggering a Spark job.

    Supported formats: delta (default), parquet, csv, json, orc, avro.
    Supported modes: error (default), overwrite, append, ignore.
    WARNING: overwrite mode is destructive — it replaces all existing data at path.

    partition_by and cluster_by are mutually exclusive. Use partition_by for
    Hive-style partitioning (all formats). Use cluster_by for Delta liquid
    clustering (requires Spark 4.0+ or Databricks Runtime 14.2+; delta format
    only). Providing both returns an error.

    Args:
        df_id: DataFrame handle from a lazy tool.
        path: Destination path (local, DBFS, S3, ADLS, GCS, etc.).
        format: Output file format. Defaults to "delta".
        mode: Write mode. One of: error, overwrite, append, ignore.
        partition_by: Column names for Hive-style partitioning.
        cluster_by: Column names for Delta liquid clustering.
            Requires Spark 4.0+ or Databricks Runtime 14.2+.
        force: Skip preflight size check (default False).

    Returns:
        JSON with status "ok" and path on success, or error details on failure.
    """
    if partition_by and cluster_by:
        return json.dumps(
            {
                "error": "partition_by and cluster_by are mutually exclusive. "
                "Use partition_by for Hive-style partitioning or cluster_by "
                "for Delta liquid clustering, not both.",
                "df_id": df_id,
            }
        )
    try:
        df = df_mod.registry.get(df_id)
    except KeyError as e:
        return json.dumps({"error": str(e), "df_id": df_id})
    warning = _run_preflight(df, force)
    if warning is not None:
        return warning
    try:
        writer = df.write.format(format).mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        if cluster_by:
            writer = writer.clusterBy(*cluster_by)
        writer.save(path)
        return json.dumps({"status": "ok", "path": path, "df_id": df_id})
    except Exception as e:  # noqa: BLE001
        return json.dumps({"error": str(e), "df_id": df_id})


@mcp.tool()
def save_as_table(
    df_id: str,
    table_name: str,
    format: str = "delta",  # noqa: A002
    mode: str = "error",
    partition_by: list[str] | None = None,
    cluster_by: list[str] | None = None,
    force: bool = False,
) -> str:
    """Write a DataFrame to a managed or external table, triggering a Spark job.

    Supports Unity Catalog three-part names (catalog.schema.table).
    Supported formats: delta (default), parquet, csv, json, orc, avro.
    Supported modes: error (default), overwrite, append, ignore.
    WARNING: overwrite mode is destructive — it replaces all existing table data.

    partition_by and cluster_by are mutually exclusive. Use partition_by for
    Hive-style partitioning (compatible with all formats). Use cluster_by for
    Delta liquid clustering (requires Spark 4.0+ or Databricks Runtime 14.2+;
    delta format only; keys can only be set on creation or overwrite mode).
    Providing both returns an error.

    Args:
        df_id: DataFrame handle from a lazy tool.
        table_name: Target table name. Supports catalog.schema.table format.
        format: Output file format. Defaults to "delta".
        mode: Write mode. One of: error, overwrite, append, ignore.
        partition_by: Column names for Hive-style partitioning.
        cluster_by: Column names for Delta liquid clustering.
            Requires Spark 4.0+ or Databricks Runtime 14.2+.
        force: Skip preflight size check (default False).

    Returns:
        JSON with status "ok" and table name on success, or error details on failure.
    """
    if partition_by and cluster_by:
        return json.dumps(
            {
                "error": "partition_by and cluster_by are mutually exclusive. "
                "Use partition_by for Hive-style partitioning or cluster_by "
                "for Delta liquid clustering, not both.",
                "df_id": df_id,
            }
        )
    try:
        df = df_mod.registry.get(df_id)
    except KeyError as e:
        return json.dumps({"error": str(e), "df_id": df_id})
    warning = _run_preflight(df, force)
    if warning is not None:
        return warning
    try:
        writer = df.write.format(format).mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        if cluster_by:
            writer = writer.clusterBy(*cluster_by)
        writer.saveAsTable(table_name)
        return json.dumps({"status": "ok", "table": table_name, "df_id": df_id})
    except Exception as e:  # noqa: BLE001
        return json.dumps({"error": str(e), "df_id": df_id})
