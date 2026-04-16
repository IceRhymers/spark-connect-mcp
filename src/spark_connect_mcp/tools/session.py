"""Session management MCP tools: start_session, close_session, list_sessions, set_preflight_threshold."""

from __future__ import annotations

import json
import os
from typing import Optional

from spark_connect_mcp import dataframes as df_mod
from spark_connect_mcp import session as session_mod
from spark_connect_mcp.connectors import detect_connection_type, get_connector
from spark_connect_mcp.server import mcp


@mcp.tool()
def start_session() -> str:
    """Start a Spark session and return a session_id handle.

    The connection type is determined by which entrypoint launched the server.
    Each entrypoint requires its matching optional extra to be installed:

    - databricks-connect-mcp (extra: databricks): Connects via Databricks
      Connect serverless. Auth is read from DATABRICKS_CONFIG_PROFILE
      (defaults to DEFAULT) or standard Databricks env vars
      (DATABRICKS_HOST, DATABRICKS_TOKEN, etc.).
    - spark-connect-mcp (extra: spark): Connects via OSS Spark Connect.
      Requires SPARK_REMOTE (e.g. sc://localhost:15002).

    No arguments needed — just call start_session().
    """
    connection_type = (
        os.environ.get("SPARK_CONNECT_MCP_TYPE") or detect_connection_type()
    )
    config = {
        "connection_type": connection_type,
    }
    try:
        connector = get_connector(connection_type)
        session_id = session_mod.registry.start(connector, config)
        return json.dumps(
            {
                "session_id": session_id,
                "connection_type": connection_type,
                "message": f"Connected via {connection_type}",
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


@mcp.tool()
def set_preflight_threshold(
    session_id: str,
    max_bytes: Optional[int] = None,
    max_rows: Optional[int] = None,
    enabled: Optional[bool] = None,
) -> str:
    """Override preflight size-check thresholds for this session.

    Use this to raise or lower the byte/row limits that trigger preflight
    warnings, or to disable preflight entirely for a session.

    Args:
        session_id: The session to configure.
        max_bytes: Override the max bytes threshold (default env var or 1 GB).
        max_rows: Override the max rows threshold (default env var or 10M).
        enabled: Set False to disable preflight for this session.
    """
    try:
        from spark_connect_mcp.preflight import _session_overrides
    except ImportError:
        return json.dumps({"error": "preflight module not available"})
    overrides = _session_overrides.setdefault(session_id, {})
    if max_bytes is not None:
        overrides["max_bytes"] = max_bytes
    if max_rows is not None:
        overrides["max_rows"] = max_rows
    if enabled is not None:
        overrides["enabled"] = enabled
    return json.dumps(
        {"status": "ok", "session_id": session_id, "overrides": overrides}
    )
