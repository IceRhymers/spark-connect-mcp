"""Tests for catalog MCP tools."""

from __future__ import annotations

import json
from collections import namedtuple
from unittest.mock import MagicMock, patch

import pytest

from spark_connect_mcp.tools.catalog import describe_table, list_tables, table_schema

Table = namedtuple(
    "Table", ["name", "database", "catalog", "namespace", "tableType", "isTemporary"]
)
Column = namedtuple("Column", ["name", "dataType", "nullable", "isPartition"])


@pytest.fixture()
def mock_spark():
    return MagicMock()


# ── list_tables ──────────────────────────────────────────────────────────


@patch("spark_connect_mcp.tools.catalog.session_mod")
def test_list_tables_success(mock_session_mod, mock_spark):
    mock_session_mod.registry.get.return_value = mock_spark
    mock_spark.catalog.listTables.return_value = [
        Table("t1", "db1", "cat1", ["cat1", "db1"], "MANAGED", False),
        Table("t2", "db1", "cat1", ["cat1", "db1"], "EXTERNAL", True),
    ]
    result = json.loads(list_tables("sid-1"))
    assert len(result) == 2
    assert result[0]["name"] == "t1"
    assert result[0]["database"] == "db1"
    assert result[0]["tableType"] == "MANAGED"
    assert result[0]["isTemporary"] is False
    assert result[1]["name"] == "t2"
    assert result[1]["isTemporary"] is True
    mock_spark.catalog.listTables.assert_called_once_with()


@patch("spark_connect_mcp.tools.catalog.session_mod")
def test_list_tables_with_database(mock_session_mod, mock_spark):
    mock_session_mod.registry.get.return_value = mock_spark
    mock_spark.catalog.listTables.return_value = []
    list_tables("sid-1", database="my_db")
    mock_spark.catalog.listTables.assert_called_once_with("my_db")


@patch("spark_connect_mcp.tools.catalog.session_mod")
def test_list_tables_invalid_session(mock_session_mod):
    mock_session_mod.registry.get.side_effect = KeyError("unknown session")
    result = json.loads(list_tables("bad-sid"))
    assert "error" in result
    assert result["session_id"] == "bad-sid"


@patch("spark_connect_mcp.tools.catalog.session_mod")
def test_list_tables_catalog_error(mock_session_mod, mock_spark):
    mock_session_mod.registry.get.return_value = mock_spark
    mock_spark.catalog.listTables.side_effect = Exception(
        "AnalysisException: db not found"
    )
    result = json.loads(list_tables("sid-1"))
    assert "error" in result
    assert "AnalysisException" in result["error"]
    assert result["session_id"] == "sid-1"


# ── describe_table ───────────────────────────────────────────────────────


@patch("spark_connect_mcp.tools.catalog.session_mod")
def test_describe_table_success(mock_session_mod, mock_spark):
    mock_session_mod.registry.get.return_value = mock_spark
    mock_spark.catalog.getTable.return_value = Table(
        "t1",
        "db1",
        "cat1",
        ["cat1", "db1"],
        "MANAGED",
        False,
    )
    mock_spark.catalog.listColumns.return_value = [
        Column("id", "int", False, False),
        Column("name", "string", True, False),
    ]
    result = json.loads(describe_table("sid-1", "t1"))
    assert result["table_name"] == "t1"
    assert result["tableType"] == "MANAGED"
    assert len(result["columns"]) == 2
    assert result["columns"][0]["name"] == "id"
    assert result["columns"][1]["dataType"] == "string"


@patch("spark_connect_mcp.tools.catalog.session_mod")
def test_describe_table_invalid_session(mock_session_mod):
    mock_session_mod.registry.get.side_effect = KeyError("unknown session")
    result = json.loads(describe_table("bad-sid", "t1"))
    assert "error" in result
    assert result["session_id"] == "bad-sid"


@patch("spark_connect_mcp.tools.catalog.session_mod")
def test_describe_table_table_not_found(mock_session_mod, mock_spark):
    mock_session_mod.registry.get.return_value = mock_spark
    mock_spark.catalog.getTable.side_effect = Exception("Table not found: t_missing")
    result = json.loads(describe_table("sid-1", "t_missing"))
    assert "error" in result
    assert "t_missing" in result["error"]


# ── table_schema ─────────────────────────────────────────────────────────


@patch("spark_connect_mcp.tools.catalog.session_mod")
def test_table_schema_success(mock_session_mod, mock_spark):
    mock_session_mod.registry.get.return_value = mock_spark
    expected_schema = {
        "type": "struct",
        "fields": [
            {"name": "id", "type": "integer", "nullable": False, "metadata": {}},
            {"name": "value", "type": "string", "nullable": True, "metadata": {}},
        ],
    }
    mock_spark.table.return_value.schema.jsonValue.return_value = expected_schema
    result = json.loads(table_schema("sid-1", "t1"))
    assert result == expected_schema
    mock_spark.table.assert_called_once_with("t1")


@patch("spark_connect_mcp.tools.catalog.session_mod")
def test_table_schema_invalid_session(mock_session_mod):
    mock_session_mod.registry.get.side_effect = KeyError("unknown session")
    result = json.loads(table_schema("bad-sid", "t1"))
    assert "error" in result
    assert result["session_id"] == "bad-sid"
