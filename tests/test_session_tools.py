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
        patch(
            "spark_connect_mcp.tools.session.detect_connection_type",
            return_value="spark_connect",
        ),
        patch("spark_connect_mcp.tools.session.get_connector") as mock_gc,
        patch("spark_connect_mcp.tools.session.session_mod") as mock_sreg,
    ):
        mock_gc.return_value = MagicMock()
        mock_sreg.registry.start.return_value = "abc-123"
        from spark_connect_mcp.tools.session import start_session

        result = json.loads(start_session())
        assert result["session_id"] == "abc-123"
        assert result["connection_type"] == "spark_connect"
        assert "message" in result


def test_start_session_unknown_type():
    with (
        patch(
            "spark_connect_mcp.tools.session.detect_connection_type",
            return_value="bad",
        ),
        patch("spark_connect_mcp.tools.session.get_connector") as mock_gc,
    ):
        mock_gc.side_effect = ValueError("Unknown connection_type: bad")
        from spark_connect_mcp.tools.session import start_session

        result = json.loads(start_session())
        assert "error" in result
        assert "supported_types" in result


def test_start_session_databricks_not_installed():
    with (
        patch(
            "spark_connect_mcp.tools.session.detect_connection_type",
            return_value="databricks",
        ),
        patch("spark_connect_mcp.tools.session.get_connector") as mock_gc,
    ):
        mock_gc.side_effect = ImportError("Install spark-connect-mcp[databricks]")
        from spark_connect_mcp.tools.session import start_session

        result = json.loads(start_session())
        assert "error" in result
        assert "hint" in result
        assert "databricks" in result["hint"]


def test_start_session_connector_error():
    with (
        patch(
            "spark_connect_mcp.tools.session.detect_connection_type",
            return_value="spark_connect",
        ),
        patch("spark_connect_mcp.tools.session.get_connector") as mock_gc,
        patch("spark_connect_mcp.tools.session.session_mod") as mock_sreg,
    ):
        mock_gc.return_value = MagicMock()
        mock_sreg.registry.start.side_effect = RuntimeError("connection refused")
        from spark_connect_mcp.tools.session import start_session

        result = json.loads(start_session())
        assert "error" in result


def test_start_session_databricks_success():
    """Databricks path: connects via serverless, no extra args needed."""
    with (
        patch(
            "spark_connect_mcp.tools.session.detect_connection_type",
            return_value="databricks",
        ),
        patch("spark_connect_mcp.tools.session.get_connector") as mock_gc,
        patch("spark_connect_mcp.tools.session.session_mod") as mock_sreg,
    ):
        mock_gc.return_value = MagicMock()
        mock_sreg.registry.start.return_value = "srv-uuid"
        from spark_connect_mcp.tools.session import start_session

        result = json.loads(start_session())
        assert result["session_id"] == "srv-uuid"
        assert result["connection_type"] == "databricks"


def test_start_session_databricks_runtime_error():
    """Databricks path: RuntimeError surfaces cleanly."""
    with (
        patch(
            "spark_connect_mcp.tools.session.detect_connection_type",
            return_value="databricks",
        ),
        patch("spark_connect_mcp.tools.session.get_connector") as mock_gc,
        patch("spark_connect_mcp.tools.session.session_mod") as mock_sreg,
    ):
        mock_gc.return_value = MagicMock()
        mock_sreg.registry.start.side_effect = RuntimeError(
            "Failed to create Databricks session."
        )
        from spark_connect_mcp.tools.session import start_session

        result = json.loads(start_session())
        assert "error" in result


def test_start_session_uses_override():
    """When connection_type_override is set, detect_connection_type is NOT called."""
    with (
        patch("spark_connect_mcp.tools.session.server_mod") as mock_server,
        patch("spark_connect_mcp.tools.session.detect_connection_type") as mock_detect,
        patch("spark_connect_mcp.tools.session.get_connector") as mock_gc,
        patch("spark_connect_mcp.tools.session.session_mod") as mock_sreg,
    ):
        mock_server.connection_type_override = "databricks"
        mock_gc.return_value = MagicMock()
        mock_sreg.registry.start.return_value = "override-uuid"
        from spark_connect_mcp.tools.session import start_session

        result = json.loads(start_session())
        assert result["connection_type"] == "databricks"
        mock_detect.assert_not_called()


def test_start_session_override_none_falls_through():
    """When connection_type_override is None, detect_connection_type is called."""
    with (
        patch("spark_connect_mcp.tools.session.server_mod") as mock_server,
        patch(
            "spark_connect_mcp.tools.session.detect_connection_type",
            return_value="spark_connect",
        ) as mock_detect,
        patch("spark_connect_mcp.tools.session.get_connector") as mock_gc,
        patch("spark_connect_mcp.tools.session.session_mod") as mock_sreg,
    ):
        mock_server.connection_type_override = None
        mock_gc.return_value = MagicMock()
        mock_sreg.registry.start.return_value = "fallback-uuid"
        from spark_connect_mcp.tools.session import start_session

        result = json.loads(start_session())
        assert result["connection_type"] == "spark_connect"
        mock_detect.assert_called_once()


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
