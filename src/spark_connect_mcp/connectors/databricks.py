"""Databricks Connect connector (optional)."""

from __future__ import annotations

from pyspark.sql import SparkSession

from spark_connect_mcp.connectors.base import BaseConnector

try:
    from databricks.connect import DatabricksSession

    _DATABRICKS_AVAILABLE = True
except ImportError:
    _DATABRICKS_AVAILABLE = False


class DatabricksConnector(BaseConnector):
    """Connect via Databricks Connect. Requires spark-connect-mcp[databricks].

    Two connection modes:
    - profile (default): DatabricksSession.builder.profile(profile).getOrCreate()
      Use for classic clusters and local dev with a ~/.databrickscfg profile.
    - serverless: DatabricksSession.builder.serverless().getOrCreate()
      Use inside Databricks Apps, Jobs, or notebooks running on serverless compute.
    """

    def __init__(self, profile: str | None = "DEFAULT", serverless: bool = False):
        if not _DATABRICKS_AVAILABLE:
            raise ImportError(
                "Install spark-connect-mcp[databricks] for Databricks Connect support"
            )
        self._profile = profile
        self._serverless = serverless
        self._last_serverless = False

    def connect(self, config: dict) -> SparkSession:
        profile = config.get("profile", self._profile)
        serverless = config.get("serverless", self._serverless)
        self._last_serverless = serverless
        if serverless:
            return DatabricksSession.builder.serverless().getOrCreate()
        if profile is None:
            profile = "DEFAULT"
        return DatabricksSession.builder.profile(profile).getOrCreate()

    def disconnect(self, session: SparkSession) -> None:
        if self._last_serverless:
            # Serverless sessions are owned by the runtime — do not stop them.
            return
        session.stop()
