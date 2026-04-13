"""OSS Spark Connect connector."""

from __future__ import annotations

from spark_connect_mcp.connectors.base import BaseConnector

try:
    from pyspark.sql import SparkSession

    _PYSPARK_AVAILABLE = True
except ImportError:
    _PYSPARK_AVAILABLE = False


class SparkConnector(BaseConnector):
    """Connect via OSS Spark Connect (e.g. sc://localhost:15002)."""

    def connect(self, config: dict) -> SparkSession:
        if not _PYSPARK_AVAILABLE:
            raise ImportError(
                "Install spark-connect-mcp[spark] for OSS Spark Connect support"
            )
        url = config.get("url", "sc://localhost:15002")
        return SparkSession.builder.remote(url).getOrCreate()

    def disconnect(self, session: SparkSession) -> None:
        session.stop()
