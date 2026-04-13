"""Base connector interface."""

from abc import ABC, abstractmethod


class BaseConnector(ABC):
    """Abstract base class for Spark session connectors."""

    @abstractmethod
    def get_session(self) -> "SparkSession":  # noqa: F821
        """Return the active SparkSession."""
        ...

    @abstractmethod
    def close(self) -> None:
        """Close the session and release resources."""
        ...
