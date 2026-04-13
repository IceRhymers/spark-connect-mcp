"""Lazy DataFrame transformation tools."""

from spark_connect_mcp.server import mcp


@mcp.tool()
def load(session_id: str, table_name: str) -> str:
    """Load a table as a lazy DataFrame."""
    raise NotImplementedError


@mcp.tool()
def sql(session_id: str, query: str) -> str:
    """Execute a SQL query and return a lazy DataFrame handle."""
    raise NotImplementedError


@mcp.tool()
def filter(df_id: str, condition: str) -> str:
    """Apply a filter condition to a DataFrame."""
    raise NotImplementedError


@mcp.tool()
def select(df_id: str, columns: list[str]) -> str:
    """Select columns from a DataFrame."""
    raise NotImplementedError


@mcp.tool()
def with_column(df_id: str, name: str, expression: str) -> str:
    """Add or replace a column in a DataFrame."""
    raise NotImplementedError


@mcp.tool()
def drop(df_id: str, columns: list[str]) -> str:
    """Drop columns from a DataFrame."""
    raise NotImplementedError


@mcp.tool()
def group_by_agg(df_id: str, group_cols: list[str], agg_exprs: dict[str, str]) -> str:
    """Group by columns and aggregate."""
    raise NotImplementedError


@mcp.tool()
def join(left_df_id: str, right_df_id: str, on: list[str], how: str = "inner") -> str:
    """Join two DataFrames."""
    raise NotImplementedError


@mcp.tool()
def sort(df_id: str, columns: list[str], ascending: bool = True) -> str:
    """Sort a DataFrame by columns."""
    raise NotImplementedError


@mcp.tool()
def limit(df_id: str, n: int) -> str:
    """Limit a DataFrame to the first n rows."""
    raise NotImplementedError
