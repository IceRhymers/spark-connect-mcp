"""Tests for dedicated CLI entrypoints."""

import os
from unittest.mock import patch


def test_databricks_main_sets_env_and_calls_main():
    with (
        patch.dict(os.environ, {}, clear=False),
        patch("spark_connect_mcp.entrypoints.main") as mock_main,
    ):
        from spark_connect_mcp.entrypoints import databricks_main

        databricks_main()
        assert os.environ.get("SPARK_CONNECT_MCP_TYPE") == "databricks"
        mock_main.assert_called_once_with()


def test_spark_main_sets_env_and_calls_main():
    with (
        patch.dict(os.environ, {}, clear=False),
        patch("spark_connect_mcp.entrypoints.main") as mock_main,
    ):
        from spark_connect_mcp.entrypoints import spark_main

        spark_main()
        assert os.environ.get("SPARK_CONNECT_MCP_TYPE") == "spark_connect"
        mock_main.assert_called_once_with()
