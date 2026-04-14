"""Dedicated CLI entrypoints that skip runtime connector detection."""

import os

from spark_connect_mcp.server import main


def databricks_main() -> None:
    """Entrypoint for databricks-connect-mcp: locks to Databricks Connect."""
    os.environ["SPARK_CONNECT_MCP_TYPE"] = "databricks"
    main()


def spark_main() -> None:
    """Entrypoint for spark-connect-mcp with explicit OSS mode."""
    os.environ["SPARK_CONNECT_MCP_TYPE"] = "spark_connect"
    main()
