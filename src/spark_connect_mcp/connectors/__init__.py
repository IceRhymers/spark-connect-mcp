"""Connectors subpackage — Spark session providers."""

from spark_connect_mcp.connectors.base import BaseConnector
from spark_connect_mcp.connectors.spark import SparkConnector

__all__ = ["BaseConnector", "SparkConnector", "detect_connection_type", "get_connector"]


def detect_connection_type() -> str:
    """Return the connection type matching the installed extra.

    Tries databricks first (databricks-connect is the primary use case),
    falls back to spark_connect. Since only one extra is installed at a time,
    this reliably identifies the intended connector without user input.
    """
    try:
        from databricks.connect import DatabricksSession  # noqa: F401

        return "databricks"
    except ImportError:
        return "spark_connect"


def get_connector(connection_type: str) -> BaseConnector:
    """Factory that returns a connector instance by type name."""
    if connection_type == "spark_connect":
        return SparkConnector()
    elif connection_type == "databricks":
        from spark_connect_mcp.connectors.databricks import DatabricksConnector

        return DatabricksConnector()
    raise ValueError(f"Unknown connection_type: {connection_type!r}")
