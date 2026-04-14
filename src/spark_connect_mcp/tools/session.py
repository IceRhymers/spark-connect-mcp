"""Session management MCP tools: start_session, close_session, list_sessions."""

from __future__ import annotations

import json

from spark_connect_mcp import dataframes as df_mod
from spark_connect_mcp import session as session_mod
from spark_connect_mcp.connectors import detect_connection_type, get_connector
from spark_connect_mcp.server import mcp


@mcp.tool()
def start_session(
    serverless: bool = True,
) -> str:
    """Start a Spark session and return a session_id handle.

    The connection type (databricks or spark_connect) is detected automatically
    from the installed package — no need to specify it. All connection config
    is read from environment variables set at server deploy time.

    Call with no arguments for the common case: Databricks serverless compute
    (Databricks Apps, Jobs, or notebooks).

    For a Databricks classic cluster, pass serverless=False. The CLI profile
    is read from DATABRICKS_CONFIG_PROFILE (defaults to DEFAULT).
    For OSS Spark Connect, set SPARK_REMOTE=sc://localhost:15002 before starting
    the server.

    Args:
        serverless: Use Databricks serverless compute (default True).
    """
    connection_type = detect_connection_type()
    config = {
        "connection_type": connection_type,
        "serverless": serverless,
    }
    try:
        connector = get_connector(connection_type)
        session_id = session_mod.registry.start(connector, config)
        return json.dumps(
            {
                "session_id": session_id,
                "connection_type": connection_type,
                "message": f"Connected via {connection_type}"
                + (" (serverless)" if serverless else ""),
            }
        )
    except ImportError as e:
        hint = (
            "pip install spark-connect-mcp[databricks]"
            if connection_type == "databricks"
            else "pip install spark-connect-mcp[spark]"
        )
        return json.dumps(
            {
                "error": str(e),
                "hint": hint,
            }
        )
    except ValueError as e:
        return json.dumps(
            {
                "error": str(e),
                "supported_types": ["spark_connect", "databricks"],
            }
        )
    except Exception as e:  # noqa: BLE001
        return json.dumps({"error": str(e)})


@mcp.tool()
def close_session(session_id: str) -> str:
    """Close a session and clean up all its DataFrame handles.

    Args:
        session_id: The session_id returned by start_session
    """
    # Clear DataFrames first — infallible dict op, no Spark call needed
    df_count = df_mod.registry.clear_session(session_id)
    try:
        closed = session_mod.registry.close(session_id)
        if not closed:
            return json.dumps(
                {
                    "error": f"Session {session_id!r} not found",
                    "session_id": session_id,
                }
            )
        return json.dumps(
            {
                "closed": True,
                "session_id": session_id,
                "dataframes_cleaned": df_count,
            }
        )
    except Exception as e:  # noqa: BLE001
        # Session removed from registry and DFs cleared, but connector.disconnect() failed.
        # Resources may be leaked but registry is clean — surface as warning.
        return json.dumps(
            {
                "closed": True,
                "session_id": session_id,
                "dataframes_cleaned": df_count,
                "warning": f"disconnect error: {e}",
            }
        )


@mcp.tool()
def list_sessions() -> str:
    """List all active Spark sessions."""
    sessions = session_mod.registry.list()
    return json.dumps(
        [
            {
                "session_id": s.session_id,
                "connection_type": s.connection_type,
                "created_at": s.created_at.isoformat(),
                "url_or_profile": s.url_or_profile,
            }
            for s in sessions
        ]
    )
