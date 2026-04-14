"""Databricks Connect connector (optional)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from spark_connect_mcp.connectors.base import BaseConnector

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

try:
    from databricks.connect import DatabricksSession

    _DATABRICKS_AVAILABLE = True
except ImportError:
    _DATABRICKS_AVAILABLE = False


class DatabricksConnector(BaseConnector):
    """Connect via Databricks Connect serverless. Requires spark-connect-mcp[databricks].

    Auth is resolved by the Databricks SDK in standard priority order:
    DATABRICKS_CONFIG_PROFILE env var, DATABRICKS_HOST + DATABRICKS_TOKEN,
    or the DEFAULT CLI profile.
    """

    def __init__(self) -> None:
        if not _DATABRICKS_AVAILABLE:
            raise ImportError(
                "Install spark-connect-mcp[databricks] for Databricks Connect support"
            )

    def connect(self, config: dict) -> SparkSession:
        return DatabricksSession.builder.serverless().getOrCreate()

    def disconnect(self, session: SparkSession) -> None:
        pass
