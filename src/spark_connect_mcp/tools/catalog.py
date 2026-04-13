"""Catalog inspection tools."""

from spark_connect_mcp.server import mcp


@mcp.tool()
def list_tables(session_id: str) -> str:
    """List tables available in the session catalog."""
    raise NotImplementedError


@mcp.tool()
def describe_table(session_id: str, table_name: str) -> str:
    """Describe a table's metadata."""
    raise NotImplementedError


@mcp.tool()
def table_schema(session_id: str, table_name: str) -> str:
    """Return the schema of a table."""
    raise NotImplementedError
