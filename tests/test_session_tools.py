"""Tests for session management MCP tools."""

from __future__ import annotations

import json
from datetime import UTC
from unittest.mock import MagicMock, patch


def _make_session_info(
    session_id="test-uuid",
    connection_type="spark_connect",
    url_or_profile="sc://localhost",
    created_at=None,
):
    from datetime import datetime

    from spark_connect_mcp.session import SessionInfo

    return SessionInfo(
        session_id=session_id,
        connection_type=connection_type,
        url_or_profile=url_or_profile,
        created_at=created_at or datetime(2026, 1, 1, tzinfo=UTC),
    )


def test_start_session_spark_connect():
    with (
        patch("spark_connect_mcp.tools.session.get_connector") as mock_gc,
        patch("spark_connect_mcp.tools.session.session_mod") as mock_sreg,
    ):
        mock_gc.return_value = MagicMock()
        mock_sreg.registry.start.return_value = "abc-123"
        from spark_connect_mcp.tools.session import start_session

        result = json.loads(start_session("spark_connect", url="sc://localhost:15002"))
        assert result["session_id"] == "abc-123"
        assert result["connection_type"] == "spark_connect"
        assert "message" in result


def test_start_session_unknown_type():
    with patch("spark_connect_mcp.tools.session.get_connector") as mock_gc:
        mock_gc.side_effect = ValueError("Unknown connection_type: bad")
        from spark_connect_mcp.tools.session import start_session

        result = json.loads(start_session("bad"))
        assert "error" in result
        assert "supported_types" in result


def test_start_session_databricks_not_installed():
    with patch("spark_connect_mcp.tools.session.get_connector") as mock_gc:
        mock_gc.side_effect = ImportError("Install spark-connect-mcp[databricks]")
        from spark_connect_mcp.tools.session import start_session

        result = json.loads(start_session("databricks"))
        assert "error" in result
        assert "hint" in result
        assert "databricks" in result["hint"]


def test_start_session_connector_error():
    with (
        patch("spark_connect_mcp.tools.session.get_connector") as mock_gc,
        patch("spark_connect_mcp.tools.session.session_mod") as mock_sreg,
    ):
        mock_gc.return_value = MagicMock()
        mock_sreg.registry.start.side_effect = RuntimeError("connection refused")
        from spark_connect_mcp.tools.session import start_session

        result = json.loads(start_session("spark_connect", url="sc://bad"))
        assert "error" in result


def test_start_session_serverless_success():
    """Serverless path: DatabricksSession.builder.serverless() is used, url_or_profile=serverless."""
    with (
        patch("spark_connect_mcp.tools.session.get_connector") as mock_gc,
        patch("spark_connect_mcp.tools.session.session_mod") as mock_sreg,
    ):
        mock_gc.return_value = MagicMock()
        mock_sreg.registry.start.return_value = "srv-uuid"
        from spark_connect_mcp.tools.session import start_session

        result = json.loads(start_session("databricks", serverless=True))
        assert result["session_id"] == "srv-uuid"
        assert "serverless" in result["message"]
        # Verify config passed to registry includes serverless=True
        call_args = mock_sreg.registry.start.call_args
        config = call_args[0][1]  # second positional arg
        assert config["serverless"] is True


def test_start_session_serverless_no_active_session():
    """Serverless path: RuntimeError when no active Databricks session exists."""
    with (
        patch("spark_connect_mcp.tools.session.get_connector") as mock_gc,
        patch("spark_connect_mcp.tools.session.session_mod") as mock_sreg,
    ):
        mock_gc.return_value = MagicMock()
        mock_sreg.registry.start.side_effect = RuntimeError(
            "Failed to create serverless Databricks session. "
            "serverless=True is only valid inside Databricks Apps or notebooks."
        )
        from spark_connect_mcp.tools.session import start_session

        result = json.loads(start_session("databricks", serverless=True))
        assert "error" in result
        assert "serverless" in result["error"].lower()


def test_close_session_success():
    with (
        patch("spark_connect_mcp.tools.session.df_mod") as mock_df,
        patch("spark_connect_mcp.tools.session.session_mod") as mock_sreg,
    ):
        mock_df.registry.clear_session.return_value = 3
        mock_sreg.registry.close.return_value = True
        from spark_connect_mcp.tools.session import close_session

        result = json.loads(close_session("abc-123"))
        assert result["closed"] is True
        assert result["session_id"] == "abc-123"
        assert result["dataframes_cleaned"] == 3


def test_close_session_not_found():
    with (
        patch("spark_connect_mcp.tools.session.df_mod") as mock_df,
        patch("spark_connect_mcp.tools.session.session_mod") as mock_sreg,
    ):
        mock_df.registry.clear_session.return_value = 0
        mock_sreg.registry.close.return_value = False
        from spark_connect_mcp.tools.session import close_session

        result = json.loads(close_session("no-such-id"))
        assert "error" in result
        assert result["session_id"] == "no-such-id"


def test_close_session_disconnect_error():
    with (
        patch("spark_connect_mcp.tools.session.df_mod") as mock_df,
        patch("spark_connect_mcp.tools.session.session_mod") as mock_sreg,
    ):
        mock_df.registry.clear_session.return_value = 1
        mock_sreg.registry.close.side_effect = RuntimeError(
            "Spark session already stopped"
        )
        from spark_connect_mcp.tools.session import close_session

        result = json.loads(close_session("abc-123"))
        assert result["closed"] is True
        assert "warning" in result
        assert result["dataframes_cleaned"] == 1


def test_close_session_serverless_no_stop():
    """Serverless close: disconnect() is a no-op, should succeed cleanly with no warning."""
    with (
        patch("spark_connect_mcp.tools.session.df_mod") as mock_df,
        patch("spark_connect_mcp.tools.session.session_mod") as mock_sreg,
    ):
        mock_df.registry.clear_session.return_value = 0
        mock_sreg.registry.close.return_value = True  # no-op disconnect succeeds
        from spark_connect_mcp.tools.session import close_session

        result = json.loads(close_session("srv-uuid"))
        assert result["closed"] is True
        assert "warning" not in result


def test_list_sessions_empty():
    with patch("spark_connect_mcp.tools.session.session_mod") as mock_sreg:
        mock_sreg.registry.list.return_value = []
        from spark_connect_mcp.tools.session import list_sessions

        result = json.loads(list_sessions())
        assert result == []


def test_list_sessions_with_data():
    with patch("spark_connect_mcp.tools.session.session_mod") as mock_sreg:
        mock_sreg.registry.list.return_value = [
            _make_session_info("s1", "spark_connect", "sc://host1"),
            _make_session_info("s2", "databricks", "serverless"),
        ]
        from spark_connect_mcp.tools.session import list_sessions

        result = json.loads(list_sessions())
        assert len(result) == 2
        assert result[0]["session_id"] == "s1"
        assert result[1]["url_or_profile"] == "serverless"
        assert "created_at" in result[0]
        assert "T" in result[0]["created_at"]  # ISO 8601
