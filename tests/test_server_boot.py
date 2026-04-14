"""Smoke tests for MCP server bootstrap and tool registration."""

from mcp.server.fastmcp import FastMCP


def test_mcp_instance_is_fastmcp():
    """Server module exports a FastMCP instance."""
    from spark_connect_mcp.server import mcp

    assert isinstance(mcp, FastMCP)


def test_tools_are_registered():
    """All tool stubs are registered on startup.

    Uses FastMCP's internal _tool_manager._tools dict — internal API,
    pinned via mcp[cli] version constraint in pyproject.toml.
    """
    from spark_connect_mcp.server import mcp

    tools = mcp._tool_manager._tools
    assert len(tools) > 0, "No tools registered — check server.py imports"


def test_main_is_callable():
    """main() is importable and callable without args."""
    from spark_connect_mcp.server import main

    assert callable(main)
