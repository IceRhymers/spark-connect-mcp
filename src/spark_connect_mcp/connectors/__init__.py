"""Connectors subpackage — Spark session providers."""

from spark_connect_mcp.connectors.base import BaseConnector
from spark_connect_mcp.connectors.spark import SparkConnector

__all__ = ["BaseConnector", "SparkConnector", "get_connector"]


def get_connector(connection_type: str) -> BaseConnector:
    """Factory that returns a connector instance by type name."""
    if connection_type == "spark_connect":
        return SparkConnector()
    elif connection_type == "databricks":
        from spark_connect_mcp.connectors.databricks import DatabricksConnector

        return DatabricksConnector()
    raise ValueError(f"Unknown connection_type: {connection_type!r}")
