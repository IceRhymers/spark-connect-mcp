"""MCP server entry point for spark-connect-mcp."""

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("spark-connect-mcp")

# When set by a dedicated entrypoint (e.g. databricks-connect-mcp),
# start_session() uses this instead of runtime import detection.
connection_type_override: str | None = None

# Import tool modules so @mcp.tool() decorators register on startup.
import spark_connect_mcp.tools.catalog  # noqa: F401, E402
import spark_connect_mcp.tools.exec  # noqa: F401, E402
import spark_connect_mcp.tools.lazy  # noqa: F401, E402
import spark_connect_mcp.tools.session  # noqa: F401, E402
import spark_connect_mcp.tools.write  # noqa: F401, E402


def main(connection_type: str | None = None) -> None:
    """Run the MCP server.

    Args:
        connection_type: Lock the connector to "databricks" or "spark_connect",
            skipping runtime detection.  None preserves auto-detect.
    """
    global connection_type_override  # noqa: PLW0603
    connection_type_override = connection_type
    mcp.run()


if __name__ == "__main__":
    main()
