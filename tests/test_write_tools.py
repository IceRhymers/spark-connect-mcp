"""Tests for write MCP tools."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from spark_connect_mcp.tools.write import save, save_as_table

# ── save ──────────────────────────────────────────────────────────────────────


@patch("spark_connect_mcp.tools.write.df_mod")
def test_save_success(mock_df: MagicMock) -> None:
    mock_frame = MagicMock()
    mock_df.registry.get.return_value = mock_frame

    result = json.loads(save("df-1", "/tmp/out"))

    assert result == {"status": "ok", "path": "/tmp/out", "df_id": "df-1"}
    mock_frame.write.format.assert_called_once_with("delta")
    mock_frame.write.format.return_value.mode.assert_called_once_with("error")
    mock_frame.write.format.return_value.mode.return_value.save.assert_called_once_with(
        "/tmp/out"
    )


@patch("spark_connect_mcp.tools.write.df_mod")
def test_save_invalid_df_id(mock_df: MagicMock) -> None:
    mock_df.registry.get.side_effect = KeyError("DataFrame 'bad-id' not found.")

    result = json.loads(save("bad-id", "/tmp/out"))

    assert result["df_id"] == "bad-id"
    assert "error" in result


@patch("spark_connect_mcp.tools.write.df_mod")
def test_save_spark_error(mock_df: MagicMock) -> None:
    mock_frame = MagicMock()
    mock_df.registry.get.return_value = mock_frame
    mock_frame.write.format.return_value.mode.return_value.save.side_effect = (
        RuntimeError("write failed")
    )

    result = json.loads(save("df-1", "/tmp/out"))

    assert result["df_id"] == "df-1"
    assert "write failed" in result["error"]


@patch("spark_connect_mcp.tools.write.df_mod")
def test_save_overwrite_mode(mock_df: MagicMock) -> None:
    mock_frame = MagicMock()
    mock_df.registry.get.return_value = mock_frame

    result = json.loads(save("df-1", "/tmp/out", mode="overwrite"))

    assert result["status"] == "ok"
    mock_frame.write.format.return_value.mode.assert_called_once_with("overwrite")


@patch("spark_connect_mcp.tools.write.df_mod")
def test_save_append_mode(mock_df: MagicMock) -> None:
    mock_frame = MagicMock()
    mock_df.registry.get.return_value = mock_frame

    result = json.loads(save("df-1", "/tmp/out", mode="append"))

    assert result["status"] == "ok"
    mock_frame.write.format.return_value.mode.assert_called_once_with("append")


@patch("spark_connect_mcp.tools.write.df_mod")
def test_save_partition_by(mock_df: MagicMock) -> None:
    mock_frame = MagicMock()
    mock_df.registry.get.return_value = mock_frame
    writer = mock_frame.write.format.return_value.mode.return_value
    writer.partitionBy.return_value = writer

    result = json.loads(save("df-1", "/tmp/out", partition_by=["year", "month"]))

    assert result["status"] == "ok"
    writer.partitionBy.assert_called_once_with("year", "month")
    writer.save.assert_called_once_with("/tmp/out")


@patch("spark_connect_mcp.tools.write.df_mod")
def test_save_cluster_by(mock_df: MagicMock) -> None:
    mock_frame = MagicMock()
    mock_df.registry.get.return_value = mock_frame
    writer = mock_frame.write.format.return_value.mode.return_value
    writer.clusterBy.return_value = writer

    result = json.loads(save("df-1", "/tmp/out", cluster_by=["event_date", "region"]))

    assert result["status"] == "ok"
    writer.clusterBy.assert_called_once_with("event_date", "region")
    writer.save.assert_called_once_with("/tmp/out")


def test_save_partition_and_cluster_mutually_exclusive() -> None:
    result = json.loads(
        save("df-1", "/tmp/out", partition_by=["year"], cluster_by=["event_date"])
    )

    assert "error" in result
    assert "mutually exclusive" in result["error"]
    assert result["df_id"] == "df-1"


# ── save_as_table ─────────────────────────────────────────────────────────────


@patch("spark_connect_mcp.tools.write.df_mod")
def test_save_as_table_success(mock_df: MagicMock) -> None:
    mock_frame = MagicMock()
    mock_df.registry.get.return_value = mock_frame

    result = json.loads(save_as_table("df-1", "main.default.my_table"))

    assert result == {
        "status": "ok",
        "table": "main.default.my_table",
        "df_id": "df-1",
    }
    mock_frame.write.format.assert_called_once_with("delta")
    mock_frame.write.format.return_value.mode.assert_called_once_with("error")
    mock_frame.write.format.return_value.mode.return_value.saveAsTable.assert_called_once_with(
        "main.default.my_table"
    )


@patch("spark_connect_mcp.tools.write.df_mod")
def test_save_as_table_invalid_df_id(mock_df: MagicMock) -> None:
    mock_df.registry.get.side_effect = KeyError("DataFrame 'bad-id' not found.")

    result = json.loads(save_as_table("bad-id", "main.default.my_table"))

    assert result["df_id"] == "bad-id"
    assert "error" in result


@patch("spark_connect_mcp.tools.write.df_mod")
def test_save_as_table_spark_error(mock_df: MagicMock) -> None:
    mock_frame = MagicMock()
    mock_df.registry.get.return_value = mock_frame
    mock_frame.write.format.return_value.mode.return_value.saveAsTable.side_effect = (
        RuntimeError("permission denied")
    )

    result = json.loads(save_as_table("df-1", "main.default.my_table"))

    assert result["df_id"] == "df-1"
    assert "permission denied" in result["error"]


@patch("spark_connect_mcp.tools.write.df_mod")
def test_save_as_table_uc_name(mock_df: MagicMock) -> None:
    """Three-part Unity Catalog name passes through to saveAsTable unchanged."""
    mock_frame = MagicMock()
    mock_df.registry.get.return_value = mock_frame

    result = json.loads(save_as_table("df-1", "prod_catalog.analytics.fact_sales"))

    assert result["status"] == "ok"
    assert result["table"] == "prod_catalog.analytics.fact_sales"
    mock_frame.write.format.return_value.mode.return_value.saveAsTable.assert_called_once_with(
        "prod_catalog.analytics.fact_sales"
    )


@patch("spark_connect_mcp.tools.write.df_mod")
def test_save_as_table_partition_by(mock_df: MagicMock) -> None:
    mock_frame = MagicMock()
    mock_df.registry.get.return_value = mock_frame
    writer = mock_frame.write.format.return_value.mode.return_value
    writer.partitionBy.return_value = writer

    result = json.loads(
        save_as_table("df-1", "main.default.events", partition_by=["year", "month"])
    )

    assert result["status"] == "ok"
    writer.partitionBy.assert_called_once_with("year", "month")
    writer.saveAsTable.assert_called_once_with("main.default.events")


@patch("spark_connect_mcp.tools.write.df_mod")
def test_save_as_table_cluster_by(mock_df: MagicMock) -> None:
    mock_frame = MagicMock()
    mock_df.registry.get.return_value = mock_frame
    writer = mock_frame.write.format.return_value.mode.return_value
    writer.clusterBy.return_value = writer

    result = json.loads(
        save_as_table(
            "df-1",
            "main.default.events",
            cluster_by=["event_date", "user_id"],
        )
    )

    assert result == {"status": "ok", "table": "main.default.events", "df_id": "df-1"}
    writer.clusterBy.assert_called_once_with("event_date", "user_id")
    writer.saveAsTable.assert_called_once_with("main.default.events")


def test_save_as_table_partition_and_cluster_mutually_exclusive() -> None:
    result = json.loads(
        save_as_table(
            "df-1",
            "main.default.events",
            partition_by=["year"],
            cluster_by=["event_date"],
        )
    )

    assert "error" in result
    assert "mutually exclusive" in result["error"]
    assert result["df_id"] == "df-1"
