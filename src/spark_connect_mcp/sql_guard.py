"""Read-only SQL enforcement using sqlglot parsing."""

from __future__ import annotations

import os

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError  # noqa: F401 — re-exported for callers


class ReadOnlyViolation(Exception):
    """Raised when a SQL query is not read-only."""


_ALLOWED_TYPES = (
    exp.Query,
    exp.Describe,
)

_ALLOWED_COMMAND_KEYWORDS: frozenset[str] = frozenset(
    {
        "SHOW",
        "EXPLAIN",
    }
)


def validate_read_only(query: str, dialect: str = "databricks") -> None:
    """Validate that a SQL string is a single read-only statement.

    Raises ReadOnlyViolation if not read-only.
    Raises ParseError on unparseable SQL (fail-closed).
    """
    if os.environ.get("SPARK_CONNECT_MCP_ALLOW_WRITE_SQL", "").lower() == "true":
        return

    statements = [s for s in sqlglot.parse(query, dialect=dialect) if s is not None]

    if len(statements) == 0:
        raise ReadOnlyViolation(
            "Empty query. Provide a SELECT, SHOW, DESCRIBE, or EXPLAIN statement."
        )
    if len(statements) > 1:
        types = ", ".join(type(s).__name__.upper() for s in statements)
        raise ReadOnlyViolation(
            f"Multi-statement SQL is not allowed. Got {len(statements)} statements "
            f"({types}). Submit one statement at a time."
        )

    stmt = statements[0]
    if isinstance(stmt, _ALLOWED_TYPES):
        return
    if isinstance(stmt, exp.Command) and stmt.this.upper() in _ALLOWED_COMMAND_KEYWORDS:
        return

    stmt_type = type(stmt).__name__.upper()
    raise ReadOnlyViolation(
        f"Statement type {stmt_type} is not allowed. Only SELECT, WITH...SELECT, "
        f"SHOW, DESCRIBE, and EXPLAIN are permitted. Query: {query.strip()[:80]!r}"
    )
