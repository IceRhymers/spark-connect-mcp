"""Session management tools."""

from spark_connect_mcp.server import mcp


@mcp.tool()
def start_session(connection_string: str) -> str:
    """Start a new Spark session."""
    raise NotImplementedError


@mcp.tool()
def close_session(session_id: str) -> str:
    """Close an active Spark session."""
    raise NotImplementedError


@mcp.tool()
def list_sessions() -> str:
    """List all active Spark sessions."""
    raise NotImplementedError
