"""Integration tests for preflight — requires a live Spark / Databricks Connect session."""

from __future__ import annotations

import logging
import os
import pytest

from spark_connect_mcp.preflight import Confidence

logger = logging.getLogger(__name__)

@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("TABLE_NAME") is None,
    reason="TABLE_NAME is not set"
)
class TestPreflightIntegration:
    """Integration tests using a real Spark session via the project's connector stack."""

    def test_estimate_size_with_real_dataframe(self):
        """estimate_size() runs against a real DataFrame without crashing."""
        pytest.importorskip("pyspark")

        from spark_connect_mcp import session as session_mod
        from spark_connect_mcp.connectors import detect_connection_type, get_connector
        from spark_connect_mcp.preflight import estimate_size

        connection_type = detect_connection_type()
        logger.info("detected connection_type=%s", connection_type)
        connector = get_connector(connection_type)
        session_id = session_mod.registry.start(
            connector, {"connection_type": connection_type}
        )
        logger.info("started session session_id=%s", session_id)
        try:
            table_name  = os.environ.get("TABLE_NAME")
            spark = session_mod.registry.get(session_id)
            logger.info(f"table_name={table_name}")
            df = spark.table(table_name)
            logger.info("calling estimate_size() on DataFrame")
            result = estimate_size(df, session_id=session_id)
            logger.info("estimate_size result=%r", result)

            # Result is either None (below threshold / no stats) or a valid PreflightResult
            if result is not None:
                assert isinstance(result.confidence, Confidence)
                assert (
                    isinstance(result.estimated_bytes, int)
                    or result.estimated_bytes is None
                )
                assert isinstance(result.should_block, bool)
        finally:
            logger.info("closing session session_id=%s", session_id)
            session_mod.registry.close(session_id)
