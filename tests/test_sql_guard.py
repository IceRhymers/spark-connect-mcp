"""Tests for read-only SQL enforcement via sqlglot."""

from __future__ import annotations

import json
import os
from unittest.mock import MagicMock, patch

import pytest
from sqlglot.errors import ParseError

from spark_connect_mcp.sql_guard import ReadOnlyViolation, validate_read_only

# ── Allowed queries ───────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "query",
    [
        "SELECT * FROM t",
        "WITH cte AS (SELECT 1) SELECT * FROM cte",
        "SELECT 1 UNION ALL SELECT 2",
        "SELECT 1 EXCEPT SELECT 2",
        "SELECT 1 INTERSECT SELECT 2",
        "DESCRIBE TABLE t",
        "DESC t",
        "DESCRIBE EXTENDED t",
        "SHOW TABLES",
        "SHOW DATABASES",
        "SHOW COLUMNS IN t",
        "SHOW SCHEMAS",
        "SHOW VIEWS",
        "EXPLAIN SELECT 1",
        "SELECT 1;",  # trailing semicolon — parsed as single statement
        "SELECT 1 -- ; DROP TABLE foo",  # comment injection safe
        "SELECT * FROM t WHERE id = :param",  # named params
        "SELECT `my col` FROM `my table`",  # backtick identifiers
    ],
)
def test_allowed(query: str) -> None:
    validate_read_only(query)  # must not raise


# ── Blocked queries → ReadOnlyViolation ──────────────────────────────────────


@pytest.mark.parametrize(
    "query",
    [
        "DROP TABLE t",
        "INSERT INTO t VALUES (1)",
        "DELETE FROM t WHERE 1=1",
        "UPDATE t SET x=1",
        "MERGE INTO target USING source ON target.id=source.id WHEN MATCHED THEN DELETE",
        "CREATE TABLE t AS SELECT 1",
        "ALTER TABLE t ADD COLUMN x INT",
        "TRUNCATE TABLE t",
        "SET spark.sql.x = 'value'",
        "GRANT ALL ON t TO user",
        "REVOKE ALL ON t FROM user",
        "COPY INTO target FROM '/path'",
        "OPTIMIZE my_table",
        "VACUUM my_table",
        "SELECT 1; DROP TABLE foo",  # multi-statement
        "",  # empty
        "   ",  # whitespace only
        "CACHE TABLE t",  # mutates cluster state
    ],
)
def test_blocked(query: str) -> None:
    with pytest.raises(ReadOnlyViolation):
        validate_read_only(query)


# ── Malformed SQL → ParseError (fail-closed) ─────────────────────────────────


def test_parse_error_fail_closed() -> None:
    with pytest.raises(ParseError):
        validate_read_only("SELEC * FROM t")


# ── Escape hatch ──────────────────────────────────────────────────────────────


def test_allow_write_sql_env_bypasses_guard() -> None:
    with patch.dict(os.environ, {"SPARK_CONNECT_MCP_ALLOW_WRITE_SQL": "true"}):
        validate_read_only("DROP TABLE t")  # must not raise
        validate_read_only("INSERT INTO t VALUES (1)")  # must not raise


def test_allow_write_sql_env_false_still_blocks() -> None:
    with (
        patch.dict(os.environ, {"SPARK_CONNECT_MCP_ALLOW_WRITE_SQL": "false"}),
        pytest.raises(ReadOnlyViolation),
    ):
        validate_read_only("DROP TABLE t")


# ── Error message quality ─────────────────────────────────────────────────────


def test_violation_message_names_blocked_type() -> None:
    with pytest.raises(ReadOnlyViolation, match="DROP"):
        validate_read_only("DROP TABLE t")


def test_violation_message_mentions_permitted_types() -> None:
    with pytest.raises(ReadOnlyViolation, match="SELECT"):
        validate_read_only("DROP TABLE t")


def test_multi_statement_message_includes_count() -> None:
    with pytest.raises(ReadOnlyViolation, match="2 statements"):
        validate_read_only("SELECT 1; DROP TABLE foo")


def test_empty_message_is_helpful() -> None:
    with pytest.raises(ReadOnlyViolation, match="Empty query"):
        validate_read_only("")


# ── lazy.sql() integration ────────────────────────────────────────────────────


@patch("spark_connect_mcp.tools.lazy.df_mod")
@patch("spark_connect_mcp.tools.lazy.session_mod")
def test_sql_blocked_query_returns_error_no_spark(
    mock_session: MagicMock, mock_df: MagicMock
) -> None:
    """ReadOnlyViolation → JSON error, spark.sql never called."""
    from spark_connect_mcp.tools.lazy import sql

    mock_spark = MagicMock()
    mock_session.registry.get.return_value = mock_spark

    result = json.loads(sql("sess-1", "DROP TABLE t"))

    assert "error" in result
    assert result["type"] == "read_only_violation"
    mock_spark.sql.assert_not_called()


@patch("spark_connect_mcp.tools.lazy.df_mod")
@patch("spark_connect_mcp.tools.lazy.session_mod")
def test_sql_parse_error_returns_error_no_spark(
    mock_session: MagicMock, mock_df: MagicMock
) -> None:
    """ParseError → JSON error, spark.sql never called."""
    from spark_connect_mcp.tools.lazy import sql

    mock_spark = MagicMock()
    mock_session.registry.get.return_value = mock_spark

    result = json.loads(sql("sess-1", "SELEC * FROM t"))

    assert "error" in result
    assert result["type"] == "parse_error"
    mock_spark.sql.assert_not_called()


@patch("spark_connect_mcp.tools.lazy.df_mod")
@patch("spark_connect_mcp.tools.lazy.session_mod")
def test_sql_valid_query_executes_normally(
    mock_session: MagicMock, mock_df: MagicMock
) -> None:
    """Valid SELECT → spark.sql called, df_id returned."""
    from spark_connect_mcp.tools.lazy import sql

    mock_spark = MagicMock()
    mock_session.registry.get.return_value = mock_spark
    mock_df.registry.register.return_value = "df-789"

    result = json.loads(sql("sess-1", "SELECT * FROM t"))

    mock_spark.sql.assert_called_once_with("SELECT * FROM t")
    assert result["df_id"] == "df-789"
