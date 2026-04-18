"""DataFrame handle management tools: list_dataframes, drop_dataframe."""

from __future__ import annotations

import json

from spark_connect_mcp import dataframes as df_mod
from spark_connect_mcp.server import mcp


@mcp.tool()
def list_dataframes(session_id: str) -> str:
    """List all active DataFrame handles for a session.

    Returns metadata per handle — no Spark calls triggered. Sorted by
    creation time (oldest first) to show the workflow lineage.
    Use the schema tool on individual df_ids if you need column info.

    Args:
        session_id: Active session handle from start_session
    """
    frames = df_mod.registry.list_session(session_id)
    return json.dumps([
        {
            "df_id": f.df_id,
            "created_at": f.created_at.isoformat(),
            "origin": f.origin,
        }
        for f in frames
    ])


@mcp.tool()
def drop_dataframe(df_id: str) -> str:
    """Remove a DataFrame handle from the registry.

    Frees the handle so it can no longer be used. Other handles derived
    from this one (via filter, select, etc.) remain valid — Spark query
    plans are self-contained and don't reference the registry.

    Args:
        df_id: DataFrame handle to remove
    """
    removed = df_mod.registry.remove(df_id)
    if removed:
        return json.dumps({"dropped": True, "df_id": df_id})
    return json.dumps({"error": f"DataFrame {df_id!r} not found", "df_id": df_id})
