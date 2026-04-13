"""SessionRegistry — manages Spark session lifecycle."""
from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from spark_connect_mcp.connectors.base import BaseConnector


@dataclass
class SessionInfo:
    session_id: str
    connection_type: str
    created_at: datetime
    url_or_profile: str


@dataclass
class _SessionEntry:
    info: SessionInfo
    spark: "SparkSession"
    connector: "BaseConnector"


class SessionRegistry:
    """Thread-safe registry of active Spark sessions."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._sessions: dict[str, _SessionEntry] = {}

    def start(self, connector: "BaseConnector", config: dict) -> str:
        """Create a new session and return its session_id."""
        session_id = str(uuid.uuid4())
        spark = connector.connect(config)
        info = SessionInfo(
            session_id=session_id,
            connection_type=config.get("connection_type", "spark_connect"),
            created_at=datetime.now(timezone.utc),
            url_or_profile="serverless" if config.get("serverless") else (config.get("url") or config.get("profile") or ""),
        )
        with self._lock:
            self._sessions[session_id] = _SessionEntry(info=info, spark=spark, connector=connector)
        return session_id

    def close(self, session_id: str) -> bool:
        """Stop and remove a session. Returns False if not found."""
        with self._lock:
            entry = self._sessions.pop(session_id, None)
        if entry is None:
            return False
        entry.connector.disconnect(entry.spark)
        return True

    def get(self, session_id: str) -> "SparkSession":
        """Return the SparkSession for session_id. Raises KeyError if not found."""
        with self._lock:
            entry = self._sessions.get(session_id)
        if entry is None:
            raise KeyError(f"Session not found: {session_id!r}. Use start_session to create one.")
        return entry.spark

    def list(self) -> list[SessionInfo]:
        """Return info for all active sessions."""
        with self._lock:
            return [e.info for e in self._sessions.values()]


# Module-level singleton
registry = SessionRegistry()
