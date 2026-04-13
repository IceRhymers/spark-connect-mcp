"""OSS Spark Connect connector."""

from pyspark.sql import SparkSession

from spark_connect_mcp.connectors.base import BaseConnector


class SparkConnector(BaseConnector):
    """Connect via OSS Spark Connect (e.g. sc://localhost:15002)."""

    def connect(self, config: dict) -> SparkSession:
        url = config.get("url", "sc://localhost:15002")
        return SparkSession.builder.remote(url).getOrCreate()

    def disconnect(self, session: SparkSession) -> None:
        session.stop()
