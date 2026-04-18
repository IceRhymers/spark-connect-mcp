"""Tests for RegisteredFrame metadata, list_session, and dataframe handle tools."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from spark_connect_mcp.dataframes import DataFrameRegistry, RegisteredFrame

# ── RegisteredFrame metadata on register() ──────────────────────────────────


class TestRegisteredFrameMetadata:
    def test_register_stores_metadata(self) -> None:
        reg = DataFrameRegistry()
        df = MagicMock()
        before = datetime.now(UTC)
        df_id = reg.register("sess-1", df, "load:data.parquet (parquet)")
        after = datetime.now(UTC)

        meta = reg._metadata[df_id]
        assert isinstance(meta, RegisteredFrame)
        assert meta.df_id == df_id
        assert meta.session_id == "sess-1"
        assert meta.origin == "load:data.parquet (parquet)"
        assert before <= meta.created_at <= after

    def test_register_default_origin_empty(self) -> None:
        reg = DataFrameRegistry()
        df_id = reg.register("sess-1", MagicMock())
        assert reg._metadata[df_id].origin == ""


# ── list_session() ───────────────────────────────────────────────────────────


class TestListSession:
    def test_returns_sorted_by_created_at(self) -> None:
        reg = DataFrameRegistry()
        id1 = reg.register("sess-1", MagicMock(), "first")
        id2 = reg.register("sess-1", MagicMock(), "second")
        id3 = reg.register("sess-1", MagicMock(), "third")

        frames = reg.list_session("sess-1")
        assert len(frames) == 3
        assert [f.df_id for f in frames] == [id1, id2, id3]
        assert [f.origin for f in frames] == ["first", "second", "third"]

    def test_returns_empty_for_unknown_session(self) -> None:
        reg = DataFrameRegistry()
        assert reg.list_session("no-such-session") == []

    def test_returns_empty_after_clear_session(self) -> None:
        reg = DataFrameRegistry()
        reg.register("sess-1", MagicMock(), "a")
        reg.register("sess-1", MagicMock(), "b")
        reg.clear_session("sess-1")
        assert reg.list_session("sess-1") == []


# ── remove() clears metadata ────────────────────────────────────────────────


class TestRemoveMetadata:
    def test_remove_clears_metadata(self) -> None:
        reg = DataFrameRegistry()
        df_id = reg.register("sess-1", MagicMock(), "origin")
        assert df_id in reg._metadata
        reg.remove(df_id)
        assert df_id not in reg._metadata

    def test_remove_unknown_returns_false(self) -> None:
        reg = DataFrameRegistry()
        assert reg.remove("nonexistent") is False


# ── clear_session() clears metadata ─────────────────────────────────────────


class TestClearSessionMetadata:
    def test_clear_session_removes_all_metadata(self) -> None:
        reg = DataFrameRegistry()
        id1 = reg.register("sess-1", MagicMock(), "a")
        id2 = reg.register("sess-1", MagicMock(), "b")
        reg.register("sess-2", MagicMock(), "other")

        reg.clear_session("sess-1")
        assert id1 not in reg._metadata
        assert id2 not in reg._metadata
        # sess-2 metadata untouched
        assert len(reg._metadata) == 1


# ── drop_dataframe tool ─────────────────────────────────────────────────────


class TestDropDataframeTool:
    def test_drop_existing(self) -> None:
        reg = DataFrameRegistry()
        df_id = reg.register("sess-1", MagicMock(), "origin")

        # Patch the module-level registry
        import spark_connect_mcp.dataframes as df_mod_ref

        original = df_mod_ref.registry
        df_mod_ref.registry = reg
        try:
            from spark_connect_mcp.tools.dataframes import drop_dataframe

            result = json.loads(drop_dataframe(df_id))
            assert result == {"dropped": True, "df_id": df_id}
            # Verify it's actually removed
            with pytest.raises(KeyError):
                reg.get(df_id)
        finally:
            df_mod_ref.registry = original

    def test_drop_unknown(self) -> None:
        reg = DataFrameRegistry()

        import spark_connect_mcp.dataframes as df_mod_ref

        original = df_mod_ref.registry
        df_mod_ref.registry = reg
        try:
            from spark_connect_mcp.tools.dataframes import drop_dataframe

            result = json.loads(drop_dataframe("fake-id"))
            assert "error" in result
            assert result["df_id"] == "fake-id"
        finally:
            df_mod_ref.registry = original
