"""Tests for read-only SQL guard (Issue #40)."""

from __future__ import annotations

import json
import os
from unittest.mock import MagicMock, patch

import pytest
from sqlglot.errors import ParseError

from spark_connect_mcp.sql_guard import ReadOnlyViolation, validate_read_only

# ── Allowed read-only statements ────────────────────────────────────────────


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
        "SELECT 1;",
        "SELECT 1 -- ; DROP TABLE foo",
        "SELECT * FROM t WHERE id = :param",
        "SELECT `my col` FROM `my table`",
    ],
    ids=[
        "simple_select",
        "cte",
        "union_all",
        "except",
        "intersect",
        "describe_table",
        "desc",
        "describe_extended",
        "show_tables",
        "show_databases",
        "show_columns",
        "show_schemas",
        "show_views",
        "explain",
        "trailing_semicolon",
        "comment_injection",
        "named_params",
        "backtick_identifiers",
    ],
)
def test_allowed_read_only(query: str) -> None:
    validate_read_only(query)  # should not raise


# ── Blocked mutating statements ─────────────────────────────────────────────


@pytest.mark.parametrize(
    "query",
    [
        "DROP TABLE t",
        "INSERT INTO t VALUES (1)",
        "DELETE FROM t WHERE 1=1",
        "UPDATE t SET x=1",
        "MERGE INTO target USING source ON id=id WHEN MATCHED THEN DELETE",
        "CREATE TABLE t AS SELECT 1",
        "ALTER TABLE t ADD COLUMN x INT",
        "TRUNCATE TABLE t",
        "SET spark.sql.x = 'value'",
        "GRANT ALL ON t TO user",
        "REVOKE ALL ON t FROM user",
        "COPY INTO target FROM '/path'",
        "OPTIMIZE my_table",
        "VACUUM my_table",
        "SELECT 1; DROP TABLE foo",
        "",
        "   ",
        "CACHE TABLE t",
    ],
    ids=[
        "drop",
        "insert",
        "delete",
        "update",
        "merge",
        "create_table_as",
        "alter_table",
        "truncate",
        "set",
        "grant",
        "revoke",
        "copy_into",
        "optimize",
        "vacuum",
        "multi_statement",
        "empty",
        "whitespace_only",
        "cache_table",
    ],
)
def test_blocked_mutations(query: str) -> None:
    with pytest.raises(ReadOnlyViolation):
        validate_read_only(query)


# ── Malformed SQL → ParseError (fail-closed) ────────────────────────────────


def test_malformed_sql_raises_parse_error() -> None:
    with pytest.raises(ParseError):
        validate_read_only("SELEC * FROM t")


# ── Escape hatch ────────────────────────────────────────────────────────────


def test_escape_hatch_allows_write_sql() -> None:
    with patch.dict(os.environ, {"SPARK_CONNECT_MCP_ALLOW_WRITE_SQL": "true"}):
        validate_read_only("DROP TABLE t")  # should NOT raise
        validate_read_only("INSERT INTO t VALUES (1)")  # should NOT raise


# ── Error messages ──────────────────────────────────────────────────────────


def test_violation_message_includes_statement_type() -> None:
    with pytest.raises(ReadOnlyViolation, match=r"(?i)drop"):
        validate_read_only("DROP TABLE t")


def test_violation_message_mentions_permitted_types() -> None:
    with pytest.raises(ReadOnlyViolation, match=r"SELECT.*DESCRIBE|DESCRIBE.*SELECT"):
        validate_read_only("DROP TABLE t")


# ── Integration with lazy.py sql() ──────────────────────────────────────────


@patch("spark_connect_mcp.tools.lazy.df_mod")
@patch("spark_connect_mcp.tools.lazy.session_mod")
@patch("spark_connect_mcp.tools.lazy.validate_read_only")
def test_sql_calls_validate_before_spark(
    mock_validate: MagicMock,
    mock_session: MagicMock,
    mock_df: MagicMock,
) -> None:
    from spark_connect_mcp.tools.lazy import sql

    mock_spark = MagicMock()
    mock_session.registry.get.return_value = mock_spark
    mock_df.registry.register.return_value = "df-1"

    sql("sess-1", "SELECT 1")

    mock_validate.assert_called_once_with("SELECT 1")
    mock_spark.sql.assert_called_once_with("SELECT 1")


@patch("spark_connect_mcp.tools.lazy.df_mod")
@patch("spark_connect_mcp.tools.lazy.session_mod")
@patch("spark_connect_mcp.tools.lazy.validate_read_only")
def test_sql_read_only_violation_returns_json_error(
    mock_validate: MagicMock,
    mock_session: MagicMock,
    mock_df: MagicMock,
) -> None:
    from spark_connect_mcp.tools.lazy import sql

    mock_validate.side_effect = ReadOnlyViolation("DROP not allowed")
    mock_spark = MagicMock()
    mock_session.registry.get.return_value = mock_spark

    result = json.loads(sql("sess-1", "DROP TABLE t"))

    assert result["type"] == "read_only_violation"
    assert "DROP not allowed" in result["error"]
    mock_spark.sql.assert_not_called()


@patch("spark_connect_mcp.tools.lazy.df_mod")
@patch("spark_connect_mcp.tools.lazy.session_mod")
@patch("spark_connect_mcp.tools.lazy.validate_read_only")
def test_sql_parse_error_returns_json_error(
    mock_validate: MagicMock,
    mock_session: MagicMock,
    mock_df: MagicMock,
) -> None:
    from spark_connect_mcp.tools.lazy import sql

    mock_validate.side_effect = ParseError("bad sql")
    mock_spark = MagicMock()
    mock_session.registry.get.return_value = mock_spark

    result = json.loads(sql("sess-1", "SELEC * FROM t"))

    assert result["type"] == "parse_error"
    assert "parse error" in result["error"].lower()
    mock_spark.sql.assert_not_called()


@patch("spark_connect_mcp.tools.lazy.df_mod")
@patch("spark_connect_mcp.tools.lazy.session_mod")
@patch("spark_connect_mcp.tools.lazy.validate_read_only")
def test_sql_valid_query_calls_spark(
    mock_validate: MagicMock,
    mock_session: MagicMock,
    mock_df: MagicMock,
) -> None:
    from spark_connect_mcp.tools.lazy import sql

    mock_spark = MagicMock()
    mock_session.registry.get.return_value = mock_spark
    mock_df.registry.register.return_value = "df-ok"

    result = json.loads(sql("sess-1", "SELECT * FROM t"))

    mock_validate.assert_called_once_with("SELECT * FROM t")
    mock_spark.sql.assert_called_once_with("SELECT * FROM t")
    assert result["df_id"] == "df-ok"
