"""Databricks Connect connector (optional)."""

from spark_connect_mcp.connectors.base import BaseConnector

try:
    from databricks.connect import DatabricksSession

    _DATABRICKS_AVAILABLE = True
except ImportError:
    _DATABRICKS_AVAILABLE = False


class DatabricksConnector(BaseConnector):
    """Connect via Databricks Connect. Requires spark-connect-mcp[databricks]."""

    def __init__(self, profile: str = "DEFAULT"):
        if not _DATABRICKS_AVAILABLE:
            raise ImportError(
                "Install spark-connect-mcp[databricks] for Databricks Connect support"
            )
        self._session = DatabricksSession.builder.profile(profile).getOrCreate()

    def get_session(self):
        return self._session

    def close(self) -> None:
        self._session.stop()
