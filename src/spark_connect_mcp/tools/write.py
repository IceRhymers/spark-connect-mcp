"""Write tools — persist DataFrames to storage, triggering Spark computation."""

from __future__ import annotations

import json

from spark_connect_mcp import dataframes as df_mod
from spark_connect_mcp.server import mcp


@mcp.tool()
def save(  # noqa: A001
    df_id: str,
    path: str,
    format: str = "parquet",  # noqa: A002
    mode: str = "error",
    partition_by: list[str] | None = None,
    cluster_by: list[str] | None = None,
) -> str:
    """Write a DataFrame to a file path, triggering a Spark job.

    Supported formats: parquet (default), delta, csv, json, orc.
    Supported modes: error (default), overwrite, append, ignore.
    WARNING: overwrite mode is destructive — it replaces all existing data at path.

    cluster_by enables Delta Lake liquid clustering (requires Spark 4.0+ or
    Databricks Runtime 14.2+). Liquid clustering and partition_by are mutually
    exclusive — use one or the other, not both.

    Args:
        df_id: DataFrame handle from a lazy tool.
        path: Destination path (local, DBFS, S3, ADLS, GCS, etc.).
        format: Output file format. Defaults to "parquet".
        mode: Write mode. Defaults to "error" (fail if path exists).
        partition_by: Optional list of column names to partition the output by.
        cluster_by: Optional list of column names for Delta liquid clustering.
            Requires Spark 4.0+ or Databricks Runtime 14.2+.

    Returns:
        JSON with status "ok" and path on success, or error details on failure.
    """
    try:
        df = df_mod.registry.get(df_id)
    except KeyError as e:
        return json.dumps({"error": str(e), "df_id": df_id})
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
    cluster_by: list[str] | None = None,
) -> str:
    """Write a DataFrame to a managed or external table, triggering a Spark job.

    Supports Unity Catalog three-part names (catalog.schema.table).
    Supported formats: delta (default), parquet, csv, json, orc.
    Supported modes: error (default), overwrite, append, ignore.
    WARNING: overwrite mode is destructive — it replaces all existing table data.

    cluster_by enables Delta Lake liquid clustering, which is the recommended
    alternative to partition_by for Delta tables. Requires Spark 4.0+ or
    Databricks Runtime 14.2+. Clustering keys can only be set on table creation
    or when using overwrite mode.

    Args:
        df_id: DataFrame handle from a lazy tool.
        table_name: Target table name. Supports catalog.schema.table format.
        format: Output file format. Defaults to "delta".
        mode: Write mode. Defaults to "error" (fail if table exists).
        cluster_by: Optional list of column names for Delta liquid clustering.
            Requires Spark 4.0+ or Databricks Runtime 14.2+.

    Returns:
        JSON with status "ok" and table name on success, or error details on failure.
    """
    try:
        df = df_mod.registry.get(df_id)
    except KeyError as e:
        return json.dumps({"error": str(e), "df_id": df_id})
    try:
        writer = df.write.format(format).mode(mode)
        if cluster_by:
            writer = writer.clusterBy(*cluster_by)
        writer.saveAsTable(table_name)
        return json.dumps({"status": "ok", "table": table_name, "df_id": df_id})
    except Exception as e:  # noqa: BLE001
        return json.dumps({"error": str(e), "df_id": df_id})
