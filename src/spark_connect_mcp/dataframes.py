"""DataFrameRegistry — maps opaque df_id handles to PySpark DataFrames."""

from __future__ import annotations

import threading
import uuid
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class DataFrameRegistry:
    """Thread-safe registry mapping df_id handles to PySpark DataFrames.

    DataFrames are scoped to a session. Closing a session should call
    clear_session() to release all associated handles.

    Design note: get() and remove() accept only df_id (not session_id) to
    match the tool-layer signatures in tools/exec.py and tools/lazy.py, where
    only df_id is available at call time. Session-scoped ownership is enforced
    at the tool layer, not the registry layer. The _df_to_session reverse index
    requires df_id to be globally unique — guaranteed by UUID4 generation.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # (session_id, df_id) -> DataFrame
        self._frames: dict[tuple[str, str], DataFrame] = {}
        # session_id -> set of df_ids (for O(1) clear_session)
        self._session_index: dict[str, set[str]] = {}
        # df_id -> session_id (reverse lookup for get/remove)
        self._df_to_session: dict[str, str] = {}

    def register(self, session_id: str, df: DataFrame) -> str:
        """Store a DataFrame and return its opaque df_id handle."""
        df_id = str(uuid.uuid4())
        with self._lock:
            self._frames[(session_id, df_id)] = df
            self._session_index.setdefault(session_id, set()).add(df_id)
            self._df_to_session[df_id] = session_id
        return df_id

    def get(self, df_id: str) -> DataFrame:
        """Return the DataFrame for df_id. Raises KeyError if not found."""
        with self._lock:
            session_id = self._df_to_session.get(df_id)
            if session_id is None:
                raise KeyError(
                    f"DataFrame {df_id!r} not found. "
                    "Use a tool that returns a df_id handle to create one."
                )
            return self._frames[(session_id, df_id)]

    def remove(self, df_id: str) -> bool:
        """Delete a single DataFrame handle. Returns False if not found."""
        with self._lock:
            session_id = self._df_to_session.pop(df_id, None)
            if session_id is None:
                return False
            self._frames.pop((session_id, df_id), None)
            self._session_index.get(session_id, set()).discard(df_id)
            return True

    def clear_session(self, session_id: str) -> int:
        """Remove all DataFrames for a session. Returns count removed."""
        with self._lock:
            df_ids = self._session_index.pop(session_id, set())
            for df_id in df_ids:
                self._frames.pop((session_id, df_id), None)
                self._df_to_session.pop(df_id, None)
            return len(df_ids)

    def session_for(self, df_id: str) -> str:
        """Return the session_id that owns df_id. Raises KeyError if not found."""
        with self._lock:
            try:
                return self._df_to_session[df_id]
            except KeyError:
                raise KeyError(
                    f"DataFrame {df_id!r} not found. "
                    "Use a tool that returns a df_id handle to create one."
                ) from None


# Module-level singleton
registry = DataFrameRegistry()
