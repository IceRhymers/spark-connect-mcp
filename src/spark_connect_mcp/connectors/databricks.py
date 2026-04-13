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
    - serverless: DatabricksSession.getActiveSession()
      Use inside Databricks Apps, Jobs, or notebooks running on serverless compute
      where a session is already active in the environment.
    """

    def __init__(self, profile: str | None = "DEFAULT", serverless: bool = False):
        if not _DATABRICKS_AVAILABLE:
            raise ImportError(
                "Install spark-connect-mcp[databricks] for Databricks Connect support"
            )
        self._profile = profile
        self._serverless = serverless

    def connect(self, config: dict) -> SparkSession:
        profile = config.get("profile", self._profile)
        serverless = config.get("serverless", self._serverless)
        if serverless:
            session = DatabricksSession.getActiveSession()
            if session is None:
                raise RuntimeError(
                    "No active Databricks session found. "
                    "getActiveSession() only works in serverless contexts "
                    "(Databricks Apps, notebooks). "
                    "Use profile= for classic cluster connections."
                )
            return session
        return DatabricksSession.builder.profile(profile).getOrCreate()

    def disconnect(self, session: SparkSession) -> None:
        session.stop()
