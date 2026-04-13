"""Base connector interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class BaseConnector(ABC):
    """Abstract base class for Spark session connectors."""

    @abstractmethod
    def connect(self, config: dict) -> SparkSession:
        """Create and return a SparkSession from the given config."""
        ...

    @abstractmethod
    def disconnect(self, session: SparkSession) -> None:
        """Stop the session and release resources."""
        ...
