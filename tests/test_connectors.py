"""Unit tests for connectors."""

from unittest.mock import MagicMock, patch

import pytest

from spark_connect_mcp.connectors import SparkConnector, get_connector
from spark_connect_mcp.connectors.base import BaseConnector


def test_spark_connector_is_base_connector():
    assert issubclass(SparkConnector, BaseConnector)


def test_get_connector_spark_connect():
    c = get_connector("spark_connect")
    assert isinstance(c, SparkConnector)


def test_spark_connector_connect():
    import spark_connect_mcp.connectors.spark as spark_mod

    mock_spark_session = MagicMock()
    mock_session = MagicMock()
    mock_spark_session.builder.remote.return_value.getOrCreate.return_value = mock_session
    with (
        patch.object(spark_mod, "_PYSPARK_AVAILABLE", True),
        patch.object(spark_mod, "SparkSession", mock_spark_session, create=True),
    ):
        connector = SparkConnector()
        session = connector.connect({"url": "sc://localhost:15002"})
        assert session is mock_session
        mock_spark_session.builder.remote.assert_called_once_with("sc://localhost:15002")


def test_spark_connector_disconnect():
    mock_session = MagicMock()
    connector = SparkConnector()
    connector.disconnect(mock_session)
    mock_session.stop.assert_called_once()


def test_get_connector_invalid():
    with pytest.raises(ValueError, match="Unknown connection_type"):
        get_connector("invalid")
