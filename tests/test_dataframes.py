"""Tests for DataFrameRegistry."""
from __future__ import annotations

import uuid
from unittest.mock import MagicMock

import pytest

from spark_connect_mcp.dataframes import DataFrameRegistry, registry


class TestDataFrameRegistry:
    """Unit tests for DataFrameRegistry."""

    def setup_method(self) -> None:
        self.reg = DataFrameRegistry()

    # 1. register returns a UUID4 string
    def test_register_returns_uuid(self) -> None:
        df = MagicMock()
        df_id = self.reg.register("sess-1", df)
        # Should not raise — valid UUID4
        parsed = uuid.UUID(df_id, version=4)
        assert str(parsed) == df_id

    # 2. get returns the registered DataFrame
    def test_get_returns_dataframe(self) -> None:
        df = MagicMock()
        df_id = self.reg.register("sess-1", df)
        assert self.reg.get(df_id) is df

    # 3. get raises KeyError for unknown df_id
    def test_get_unknown_raises_key_error(self) -> None:
        with pytest.raises(KeyError, match="not found"):
            self.reg.get("nonexistent-id")

    # 4. remove returns True and cleans up
    def test_remove_returns_true_and_cleans_up(self) -> None:
        df = MagicMock()
        df_id = self.reg.register("sess-1", df)
        assert self.reg.remove(df_id) is True
        with pytest.raises(KeyError):
            self.reg.get(df_id)

    # 5. remove returns False for unknown df_id
    def test_remove_unknown_returns_false(self) -> None:
        assert self.reg.remove("nonexistent-id") is False

    # 6. clear_session removes all and returns count
    def test_clear_session_removes_all_returns_count(self) -> None:
        for _ in range(3):
            self.reg.register("sess-1", MagicMock())
        assert self.reg.clear_session("sess-1") == 3

    # 7. clear_session on empty session returns zero
    def test_clear_session_empty_returns_zero(self) -> None:
        assert self.reg.clear_session("no-such-session") == 0

    # 8. sessions are isolated from each other
    def test_register_multiple_sessions_isolated(self) -> None:
        df_a = MagicMock()
        df_b = MagicMock()
        id_a = self.reg.register("sess-a", df_a)
        id_b = self.reg.register("sess-b", df_b)

        self.reg.clear_session("sess-a")

        # sess-b handle still works
        assert self.reg.get(id_b) is df_b
        # sess-a handle is gone
        with pytest.raises(KeyError):
            self.reg.get(id_a)

    # 9. module-level singleton exists and is correct type
    def test_module_singleton_exists(self) -> None:
        assert isinstance(registry, DataFrameRegistry)
