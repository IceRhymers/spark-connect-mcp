"""Execution / action tools — trigger computation on lazy DataFrames."""

from spark_connect_mcp.server import mcp


@mcp.tool()
def show(df_id: str, n: int = 20) -> str:
    """Show the first n rows of a DataFrame as a formatted string."""
    raise NotImplementedError


@mcp.tool()
def collect(df_id: str) -> str:
    """Collect all rows of a DataFrame as JSON."""
    raise NotImplementedError


@mcp.tool()
def count(df_id: str) -> str:
    """Return the row count of a DataFrame."""
    raise NotImplementedError


@mcp.tool()
def schema(df_id: str) -> str:
    """Return the schema of a DataFrame."""
    raise NotImplementedError


@mcp.tool()
def describe(df_id: str) -> str:
    """Return summary statistics of a DataFrame."""
    raise NotImplementedError
