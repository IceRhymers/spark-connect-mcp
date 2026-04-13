"""Session management MCP tools: start_session, close_session, list_sessions."""

from __future__ import annotations

import json

from spark_connect_mcp import dataframes as df_mod
from spark_connect_mcp import session as session_mod
from spark_connect_mcp.connectors import get_connector
from spark_connect_mcp.server import mcp


@mcp.tool()
def start_session(
    connection_type: str,
    url: str | None = None,
    profile: str | None = None,
    serverless: bool = False,
) -> str:
    """Start a Spark session and return a session_id handle.

    Args:
        connection_type: 'spark_connect' or 'databricks'
        url: Spark Connect URL (e.g. 'sc://localhost:15002') for spark_connect
        profile: Databricks CLI profile name for databricks connections
        serverless: If True, use DatabricksSession.getActiveSession() (Databricks Apps/notebooks)
    """
    config = {
        "connection_type": connection_type,
        "url": url,
        "profile": profile,
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
        return json.dumps(
            {
                "error": str(e),
                "hint": "pip install spark-connect-mcp[databricks]",
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
