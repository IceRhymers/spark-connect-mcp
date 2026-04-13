"""MCP server entry point for spark-connect-mcp."""

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("spark-connect-mcp")

# Import tool modules so @mcp.tool() decorators register on startup.
import spark_connect_mcp.tools.session  # noqa: F401, E402
import spark_connect_mcp.tools.catalog  # noqa: F401, E402
import spark_connect_mcp.tools.lazy  # noqa: F401, E402
import spark_connect_mcp.tools.exec  # noqa: F401, E402


def main():
    """Run the MCP server."""
    mcp.run()


if __name__ == "__main__":
    main()
