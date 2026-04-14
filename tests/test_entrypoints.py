"""Tests for dedicated CLI entrypoints."""

from unittest.mock import patch


def test_databricks_main_sets_connection_type():
    with patch("spark_connect_mcp.entrypoints.main") as mock_main:
        from spark_connect_mcp.entrypoints import databricks_main

        databricks_main()
        mock_main.assert_called_once_with("databricks")


def test_spark_main_sets_connection_type():
    with patch("spark_connect_mcp.entrypoints.main") as mock_main:
        from spark_connect_mcp.entrypoints import spark_main

        spark_main()
        mock_main.assert_called_once_with("spark_connect")
