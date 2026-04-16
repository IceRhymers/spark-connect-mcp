"""Integration tests for preflight — requires a real local Spark session."""

from __future__ import annotations

import pytest

from spark_connect_mcp.preflight import Confidence


@pytest.mark.integration
class TestPreflightIntegration:
    """Integration tests using a real local Spark session."""

    def test_estimate_size_with_real_dataframe(self):
        pytest.importorskip("pyspark")
        from pyspark.sql import SparkSession

        from spark_connect_mcp.preflight import estimate_size

        spark = (
            SparkSession.builder.master("local[1]")
            .appName("preflight-test")
            .getOrCreate()
        )
        try:
            df = spark.range(1000)
            result = estimate_size(df)

            # Result is either None (below threshold / no stats) or a valid PreflightResult
            if result is not None:
                assert isinstance(result.confidence, Confidence)
                assert (
                    isinstance(result.estimated_bytes, int)
                    or result.estimated_bytes is None
                )
                assert isinstance(result.should_block, bool)
        finally:
            spark.stop()
