"""OSS Spark Connect connector."""

from pyspark.sql import SparkSession

from spark_connect_mcp.connectors.base import BaseConnector


class SparkConnector(BaseConnector):
    """Connect via OSS Spark Connect (e.g. sc://localhost:15002)."""

    def __init__(self, url: str):
        self._session = SparkSession.builder.remote(url).getOrCreate()

    def get_session(self) -> SparkSession:
        return self._session

    def close(self) -> None:
        self._session.stop()
