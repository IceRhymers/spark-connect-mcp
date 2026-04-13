"""Catalog MCP tools: list_tables, describe_table, table_schema."""

from __future__ import annotations

import json

from spark_connect_mcp import session as session_mod
from spark_connect_mcp.server import mcp


@mcp.tool()
def list_tables(session_id: str, database: str | None = None) -> str:
    """List tables in the given database (or current database if None).

    Args:
        session_id: Active session handle from start_session
        database: Database/schema name to list tables from. Uses current database if None.
    """
    try:
        spark = session_mod.registry.get(session_id)
    except KeyError as e:
        return json.dumps({"error": str(e), "session_id": session_id})
    try:
        tables = (
            spark.catalog.listTables(database)
            if database
            else spark.catalog.listTables()
        )
        return json.dumps(
            [
                {
                    "name": t.name,
                    "database": t.database,
                    "catalog": t.catalog,
                    "namespace": list(t.namespace) if t.namespace else None,
                    "tableType": t.tableType,
                    "isTemporary": t.isTemporary,
                }
                for t in tables
            ]
        )
    except Exception as e:  # noqa: BLE001
        return json.dumps({"error": str(e), "session_id": session_id})


@mcp.tool()
def describe_table(session_id: str, table_name: str) -> str:
    """Return a flat column summary for quick table inspection.

    Use this for a quick overview of column names, types, and partition info.
    For the full nested StructType schema (programmatic use, DDL generation), use table_schema.

    Args:
        session_id: Active session handle from start_session
        table_name: Table name, optionally qualified (e.g. 'catalog.db.table')
    """
    try:
        spark = session_mod.registry.get(session_id)
    except KeyError as e:
        return json.dumps({"error": str(e), "session_id": session_id})
    try:
        table_info = spark.catalog.getTable(table_name)
        cols = spark.catalog.listColumns(table_name)
        return json.dumps(
            {
                "table_name": table_name,
                "tableType": table_info.tableType,
                "catalog": table_info.catalog,
                "namespace": list(table_info.namespace)
                if table_info.namespace
                else None,
                "columns": [
                    {
                        "name": c.name,
                        "dataType": c.dataType,
                        "nullable": c.nullable,
                        "isPartition": c.isPartition,
                    }
                    for c in cols
                ],
            }
        )
    except Exception as e:  # noqa: BLE001
        return json.dumps({"error": str(e), "session_id": session_id})


@mcp.tool()
def table_schema(session_id: str, table_name: str) -> str:
    """Return the full nested StructType schema of a table as JSON.

    Use this for schema evolution, DDL generation, or when nested type detail is needed.
    For a quick column name/type overview, use describe_table instead.

    Args:
        session_id: Active session handle from start_session
        table_name: Table name, optionally qualified (e.g. 'catalog.db.table')
    """
    try:
        spark = session_mod.registry.get(session_id)
    except KeyError as e:
        return json.dumps({"error": str(e), "session_id": session_id})
    try:
        schema = spark.table(table_name).schema
        return json.dumps(schema.jsonValue())
    except Exception as e:  # noqa: BLE001
        return json.dumps({"error": str(e), "session_id": session_id})
