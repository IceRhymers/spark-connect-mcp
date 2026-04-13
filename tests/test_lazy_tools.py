"""Tests for lazy DataFrame MCP tools."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from spark_connect_mcp.tools.lazy import (
    drop,
    filter,
    limit,
    load,
    select,
    sort,
    sql,
    with_column,
)

# ── load ─────────────────────────────────────────────────────────────────────


@patch("spark_connect_mcp.tools.lazy.df_mod")
@patch("spark_connect_mcp.tools.lazy.session_mod")
def test_load_success(mock_session, mock_df):
    mock_spark = MagicMock()
    mock_session.registry.get.return_value = mock_spark
    mock_df.registry.register.return_value = "df-123"
    mock_reader = MagicMock()
    mock_spark.read.format.return_value = mock_reader

    result = json.loads(load("sess-1", "/data/test.parquet", "parquet"))

    mock_spark.read.format.assert_called_once_with("parquet")
    mock_reader.load.assert_called_once_with("/data/test.parquet")
    assert result["df_id"] == "df-123"
    assert "Loaded" in result["message"]


@patch("spark_connect_mcp.tools.lazy.session_mod")
def test_load_invalid_session(mock_session):
    mock_session.registry.get.side_effect = KeyError("Session not found")

    result = json.loads(load("bad-sess", "/data/test.parquet"))

    assert "error" in result
    assert result["session_id"] == "bad-sess"


@patch("spark_connect_mcp.tools.lazy.df_mod")
@patch("spark_connect_mcp.tools.lazy.session_mod")
def test_load_spark_error(mock_session, mock_df):
    mock_spark = MagicMock()
    mock_session.registry.get.return_value = mock_spark
    mock_spark.read.format.return_value.load.side_effect = RuntimeError(
        "File not found"
    )

    result = json.loads(load("sess-1", "/bad/path.parquet"))

    assert "error" in result
    assert result["session_id"] == "sess-1"


# ── sql ──────────────────────────────────────────────────────────────────────


@patch("spark_connect_mcp.tools.lazy.df_mod")
@patch("spark_connect_mcp.tools.lazy.session_mod")
def test_sql_success(mock_session, mock_df):
    mock_spark = MagicMock()
    mock_session.registry.get.return_value = mock_spark
    mock_df.registry.register.return_value = "df-456"

    result = json.loads(sql("sess-1", "SELECT * FROM t"))

    mock_spark.sql.assert_called_once_with("SELECT * FROM t")
    assert result["df_id"] == "df-456"
    assert result["message"] == "SQL executed lazily"


@patch("spark_connect_mcp.tools.lazy.session_mod")
def test_sql_invalid_session(mock_session):
    mock_session.registry.get.side_effect = KeyError("Session not found")

    result = json.loads(sql("bad-sess", "SELECT 1"))

    assert "error" in result
    assert result["session_id"] == "bad-sess"


# ── filter ───────────────────────────────────────────────────────────────────


@patch("spark_connect_mcp.tools.lazy.df_mod")
def test_filter_success(mock_df):
    mock_source = MagicMock()
    mock_df.registry.get.return_value = mock_source
    mock_df.registry.session_for.return_value = "sess-1"
    mock_df.registry.register.return_value = "df-filtered"

    result = json.loads(filter("df-orig", "age > 30"))

    mock_source.filter.assert_called_once_with("age > 30")
    assert result["df_id"] == "df-filtered"


@patch("spark_connect_mcp.tools.lazy.df_mod")
def test_filter_invalid_df_id(mock_df):
    mock_df.registry.get.side_effect = KeyError("DataFrame not found")

    result = json.loads(filter("bad-df", "age > 30"))

    assert "error" in result
    assert result["df_id"] == "bad-df"


# ── select ───────────────────────────────────────────────────────────────────


@patch("spark_connect_mcp.tools.lazy.df_mod")
def test_select_success(mock_df):
    mock_source = MagicMock()
    mock_df.registry.get.return_value = mock_source
    mock_df.registry.session_for.return_value = "sess-1"
    mock_df.registry.register.return_value = "df-selected"

    result = json.loads(select("df-orig", ["col1", "col2 as alias"]))

    mock_source.selectExpr.assert_called_once_with("col1", "col2 as alias")
    assert result["df_id"] == "df-selected"


@patch("spark_connect_mcp.tools.lazy.df_mod")
def test_select_invalid_df_id(mock_df):
    mock_df.registry.get.side_effect = KeyError("DataFrame not found")

    result = json.loads(select("bad-df", ["col1"]))

    assert "error" in result
    assert result["df_id"] == "bad-df"


# ── with_column ──────────────────────────────────────────────────────────────


@patch("spark_connect_mcp.tools.lazy.F")
@patch("spark_connect_mcp.tools.lazy.df_mod")
def test_with_column_success(mock_df, mock_F):
    mock_source = MagicMock()
    mock_df.registry.get.return_value = mock_source
    mock_df.registry.session_for.return_value = "sess-1"
    mock_df.registry.register.return_value = "df-new-col"
    mock_expr = MagicMock()
    mock_F.expr.return_value = mock_expr

    result = json.loads(with_column("df-orig", "doubled", "col1 * 2"))

    mock_F.expr.assert_called_once_with("col1 * 2")
    mock_source.withColumn.assert_called_once_with("doubled", mock_expr)
    assert result["df_id"] == "df-new-col"


# ── drop ─────────────────────────────────────────────────────────────────────


@patch("spark_connect_mcp.tools.lazy.df_mod")
def test_drop_success(mock_df):
    mock_source = MagicMock()
    mock_df.registry.get.return_value = mock_source
    mock_df.registry.session_for.return_value = "sess-1"
    mock_df.registry.register.return_value = "df-dropped"

    result = json.loads(drop("df-orig", ["col1", "col2"]))

    mock_source.drop.assert_called_once_with("col1", "col2")
    assert result["df_id"] == "df-dropped"


# ── sort ─────────────────────────────────────────────────────────────────────


@patch("spark_connect_mcp.tools.lazy.F")
@patch("spark_connect_mcp.tools.lazy.df_mod")
def test_sort_ascending(mock_df, mock_F):
    mock_source = MagicMock()
    mock_df.registry.get.return_value = mock_source
    mock_df.registry.session_for.return_value = "sess-1"
    mock_df.registry.register.return_value = "df-sorted"
    mock_col = MagicMock()
    mock_F.col.return_value = mock_col

    result = json.loads(sort("df-orig", ["age"], ascending=True))

    mock_F.col.assert_called_once_with("age")
    mock_col.asc.assert_called_once()
    assert result["df_id"] == "df-sorted"


@patch("spark_connect_mcp.tools.lazy.F")
@patch("spark_connect_mcp.tools.lazy.df_mod")
def test_sort_descending(mock_df, mock_F):
    mock_source = MagicMock()
    mock_df.registry.get.return_value = mock_source
    mock_df.registry.session_for.return_value = "sess-1"
    mock_df.registry.register.return_value = "df-sorted"
    mock_col = MagicMock()
    mock_F.col.return_value = mock_col

    result = json.loads(sort("df-orig", ["age"], ascending=False))

    mock_F.col.assert_called_once_with("age")
    mock_col.desc.assert_called_once()
    assert result["df_id"] == "df-sorted"


# ── limit ────────────────────────────────────────────────────────────────────


@patch("spark_connect_mcp.tools.lazy.df_mod")
def test_limit_success(mock_df):
    mock_source = MagicMock()
    mock_df.registry.get.return_value = mock_source
    mock_df.registry.session_for.return_value = "sess-1"
    mock_df.registry.register.return_value = "df-limited"

    result = json.loads(limit("df-orig", 10))

    mock_source.limit.assert_called_once_with(10)
    assert result["df_id"] == "df-limited"


@patch("spark_connect_mcp.tools.lazy.df_mod")
def test_limit_invalid_df_id(mock_df):
    mock_df.registry.get.side_effect = KeyError("DataFrame not found")

    result = json.loads(limit("bad-df", 10))

    assert "error" in result
    assert result["df_id"] == "bad-df"
