"""Databricks Connect connector (optional)."""

from __future__ import annotations

import os
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
    """Connect via Databricks Connect. Requires spark-connect-mcp[databricks].

    Two connection modes:
    - classic cluster: DatabricksSession.builder.profile(profile).getOrCreate()
      Profile is read from DATABRICKS_CONFIG_PROFILE env var (defaults to DEFAULT).
    - serverless: DatabricksSession.builder.serverless().getOrCreate()
      Use inside Databricks Apps, Jobs, or notebooks running on serverless compute.
    """

    def __init__(self, serverless: bool = False):
        if not _DATABRICKS_AVAILABLE:
            raise ImportError(
                "Install spark-connect-mcp[databricks] for Databricks Connect support"
            )
        self._serverless = serverless
        self._last_serverless = False

    def connect(self, config: dict) -> SparkSession:
        serverless = config.get("serverless", self._serverless)
        self._last_serverless = serverless
        if serverless:
            return DatabricksSession.builder.serverless().getOrCreate()
        profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
        return DatabricksSession.builder.profile(profile).getOrCreate()

    def disconnect(self, session: SparkSession) -> None:
        if self._last_serverless:
            # Serverless sessions are owned by the runtime — do not stop them.
            return
        session.stop()
