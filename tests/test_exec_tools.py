"""Tests for execution MCP tools."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from spark_connect_mcp.tools.exec import MAX_COLLECT_LIMIT, collect, count, describe, schema, show

# ── show ─────────────────────────────────────────────────────────────────────


@patch("spark_connect_mcp.tools.exec.df_mod")
def test_show_success(mock_df):
    mock_frame = MagicMock()
    mock_df.registry.get.return_value = mock_frame

    def fake_show(n, truncate):
        print("+---+----+\n| id|name|\n+---+----+\n|  1| foo|\n+---+----+")

    mock_frame.show.side_effect = fake_show

    result = show("df-1", n=5, truncate=True)

    mock_frame.show.assert_called_once_with(5, True)
    assert "+---+" in result
    assert "foo" in result


@patch("spark_connect_mcp.tools.exec.df_mod")
def test_show_invalid_df_id(mock_df):
    mock_df.registry.get.side_effect = KeyError("DataFrame not found")

    result = show("bad-df")

    assert "Error" in result
    assert "bad-df" not in result or "Error" in result


@patch("spark_connect_mcp.tools.exec.df_mod")
def test_show_spark_error(mock_df):
    mock_frame = MagicMock()
    mock_df.registry.get.return_value = mock_frame
    mock_frame.show.side_effect = RuntimeError("analysis failed")

    result = show("df-1")

    assert "Error" in result


# ── collect ──────────────────────────────────────────────────────────────────


@patch("spark_connect_mcp.tools.exec.df_mod")
def test_collect_success(mock_df):
    mock_frame = MagicMock()
    mock_df.registry.get.return_value = mock_frame
    row1 = MagicMock()
    row1.asDict.return_value = {"id": 1, "name": "Alice"}
    row2 = MagicMock()
    row2.asDict.return_value = {"id": 2, "name": "Bob"}
    mock_frame.limit.return_value.collect.return_value = [row1, row2]

    result = json.loads(collect("df-1", limit=10))

    mock_frame.limit.assert_called_once_with(10)
    assert result == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]


@patch("spark_connect_mcp.tools.exec.df_mod")
def test_collect_enforces_max_limit(mock_df):
    mock_frame = MagicMock()
    mock_df.registry.get.return_value = mock_frame
    mock_frame.limit.return_value.collect.return_value = []

    collect("df-1", limit=99999)

    mock_frame.limit.assert_called_once_with(MAX_COLLECT_LIMIT)


@patch("spark_connect_mcp.tools.exec.df_mod")
def test_collect_invalid_df_id(mock_df):
    mock_df.registry.get.side_effect = KeyError("DataFrame not found")

    result = json.loads(collect("bad-df"))

    assert "error" in result
    assert result["df_id"] == "bad-df"


@patch("spark_connect_mcp.tools.exec.df_mod")
def test_collect_spark_error(mock_df):
    mock_frame = MagicMock()
    mock_df.registry.get.return_value = mock_frame
    mock_frame.limit.return_value.collect.side_effect = RuntimeError("OOM")

    result = json.loads(collect("df-1"))

    assert "error" in result


# ── count ────────────────────────────────────────────────────────────────────


@patch("spark_connect_mcp.tools.exec.df_mod")
def test_count_success(mock_df):
    mock_frame = MagicMock()
    mock_df.registry.get.return_value = mock_frame
    mock_frame.count.return_value = 42

    result = json.loads(count("df-1"))

    mock_frame.count.assert_called_once()
    assert result == {"count": 42, "df_id": "df-1"}


@patch("spark_connect_mcp.tools.exec.df_mod")
def test_count_invalid_df_id(mock_df):
    mock_df.registry.get.side_effect = KeyError("DataFrame not found")

    result = json.loads(count("bad-df"))

    assert "error" in result
    assert result["df_id"] == "bad-df"


@patch("spark_connect_mcp.tools.exec.df_mod")
def test_count_spark_error(mock_df):
    mock_frame = MagicMock()
    mock_df.registry.get.return_value = mock_frame
    mock_frame.count.side_effect = RuntimeError("count failed")

    result = json.loads(count("df-1"))

    assert "error" in result


# ── schema ───────────────────────────────────────────────────────────────────


@patch("spark_connect_mcp.tools.exec.df_mod")
def test_schema_success(mock_df):
    mock_frame = MagicMock()
    mock_df.registry.get.return_value = mock_frame
    mock_frame.schema.jsonValue.return_value = {
        "type": "struct",
        "fields": [{"name": "id", "type": "integer", "nullable": True, "metadata": {}}],
    }

    result = json.loads(schema("df-1"))

    assert result["type"] == "struct"
    assert result["fields"][0]["name"] == "id"


@patch("spark_connect_mcp.tools.exec.df_mod")
def test_schema_invalid_df_id(mock_df):
    mock_df.registry.get.side_effect = KeyError("DataFrame not found")

    result = json.loads(schema("bad-df"))

    assert "error" in result
    assert result["df_id"] == "bad-df"


# ── describe ─────────────────────────────────────────────────────────────────


@patch("spark_connect_mcp.tools.exec.df_mod")
def test_describe_all_columns(mock_df):
    mock_frame = MagicMock()
    mock_df.registry.get.return_value = mock_frame
    stat_row = MagicMock()
    stat_row.asDict.return_value = {"summary": "count", "age": "100", "salary": "75000.0"}
    mock_frame.describe.return_value.collect.return_value = [stat_row]

    result = json.loads(describe("df-1"))

    mock_frame.describe.assert_called_once_with()
    assert result[0]["summary"] == "count"


@patch("spark_connect_mcp.tools.exec.df_mod")
def test_describe_specific_columns(mock_df):
    mock_frame = MagicMock()
    mock_df.registry.get.return_value = mock_frame
    stat_row = MagicMock()
    stat_row.asDict.return_value = {"summary": "mean", "age": "35.5"}
    mock_frame.describe.return_value.collect.return_value = [stat_row]

    result = json.loads(describe("df-1", columns=["age"]))

    mock_frame.describe.assert_called_once_with("age")
    assert result[0]["summary"] == "mean"


@patch("spark_connect_mcp.tools.exec.df_mod")
def test_describe_invalid_df_id(mock_df):
    mock_df.registry.get.side_effect = KeyError("DataFrame not found")

    result = json.loads(describe("bad-df"))

    assert "error" in result
    assert result["df_id"] == "bad-df"


@patch("spark_connect_mcp.tools.exec.df_mod")
def test_describe_spark_error(mock_df):
    mock_frame = MagicMock()
    mock_df.registry.get.return_value = mock_frame
    mock_frame.describe.return_value.collect.side_effect = RuntimeError("describe failed")

    result = json.loads(describe("df-1", columns=["revenue"]))

    assert "error" in result
