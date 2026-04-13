"""Tests for SessionRegistry."""

from datetime import UTC
from unittest.mock import MagicMock

import pytest

from spark_connect_mcp.session import SessionRegistry


def _make_connector(mock_session=None):
    connector = MagicMock()
    connector.connect.return_value = mock_session or MagicMock()
    return connector


def test_start_returns_uuid():
    reg = SessionRegistry()
    connector = _make_connector()
    sid = reg.start(
        connector, {"url": "sc://localhost:15002", "connection_type": "spark_connect"}
    )
    assert len(sid) == 36  # UUID4 format
    assert "-" in sid


def test_start_stores_session_info():
    reg = SessionRegistry()
    connector = _make_connector()
    sid = reg.start(
        connector, {"url": "sc://host:15002", "connection_type": "spark_connect"}
    )
    sessions = reg.list()
    assert len(sessions) == 1
    info = sessions[0]
    assert info.session_id == sid
    assert info.connection_type == "spark_connect"
    assert info.url_or_profile == "sc://host:15002"
    assert info.created_at.tzinfo == UTC


def test_get_returns_spark_session():
    reg = SessionRegistry()
    mock_spark = MagicMock()
    connector = _make_connector(mock_spark)
    sid = reg.start(connector, {"url": "sc://localhost"})
    assert reg.get(sid) is mock_spark


def test_get_missing_raises_key_error():
    reg = SessionRegistry()
    with pytest.raises(KeyError, match="Session not found"):
        reg.get("nonexistent-id")


def test_close_disconnects_and_removes():
    reg = SessionRegistry()
    mock_spark = MagicMock()
    connector = _make_connector(mock_spark)
    sid = reg.start(connector, {"url": "sc://localhost"})
    result = reg.close(sid)
    assert result is True
    connector.disconnect.assert_called_once_with(mock_spark)
    assert reg.list() == []


def test_close_returns_false_for_missing():
    reg = SessionRegistry()
    assert reg.close("no-such-session") is False


def test_list_multiple_sessions():
    reg = SessionRegistry()
    c1, c2 = _make_connector(), _make_connector()
    s1 = reg.start(c1, {"url": "sc://host1", "connection_type": "spark_connect"})
    s2 = reg.start(c2, {"url": "sc://host2", "connection_type": "spark_connect"})
    ids = {info.session_id for info in reg.list()}
    assert ids == {s1, s2}
