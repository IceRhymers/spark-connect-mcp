"""Databricks Connect connector (optional)."""

from __future__ import annotations

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
        if serverless:
            session = DatabricksSession.getActiveSession()
            if session is None:
                raise RuntimeError(
                    "No active Databricks session found. "
                    "getActiveSession() only works in serverless contexts "
                    "(Databricks Apps, notebooks). "
                    "Use profile= for classic cluster connections."
                )
            self._session = session
        else:
            self._session = DatabricksSession.builder.profile(profile).getOrCreate()

    def get_session(self):
        return self._session

    def close(self) -> None:
        self._session.stop()
