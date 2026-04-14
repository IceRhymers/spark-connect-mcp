"""Dedicated CLI entrypoints that skip runtime connector detection."""

from spark_connect_mcp.server import main


def databricks_main() -> None:
    """Entrypoint for databricks-connect-mcp: locks to Databricks Connect."""
    main("databricks")


def spark_main() -> None:
    """Entrypoint for spark-connect-mcp with explicit OSS mode."""
    main("spark_connect")
