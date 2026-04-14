# spark-connect-mcp

MCP server exposing Apache Spark Connect (and Databricks Connect) as tool calls for AI agents. Tools follow a lazy/exec split: lazy tools build a DataFrame plan and return a `df_id` handle without touching the cluster; exec tools take a `df_id` and trigger actual Spark computation.

## Commands

```bash
uv sync --extra databricks   # install with Databricks Connect
uv sync --extra spark        # install with OSS PySpark

make check                   # lint + format-check + typecheck (run before every push)
make lint                    # ruff check src/ tests/
make fmt                     # ruff format src/ tests/  (auto-fix formatting)
make fmt-check               # ruff format --check (what CI runs)
make typecheck               # mypy src/
make test                    # pytest tests/
make build                   # uv build wheel
```

**Always run `make check` (not just `make lint`) before pushing.** `ruff check` and `ruff format --check` are separate steps in CI and both must pass. Long lines in test dicts/exceptions will fail format even when lint passes.

## Pre-commit

Ruff hooks run automatically on every commit. Activate after installing dev deps:

```bash
uv sync --extra dev
pre-commit install
```

This runs `ruff check --fix` and `ruff format` before each commit.

## Architecture

```
src/spark_connect_mcp/
  server.py          # FastMCP instance + tool module imports (registers @mcp.tool decorators)
  session.py         # SessionRegistry — manages SparkSession lifecycle
  dataframes.py      # DataFrameRegistry — maps df_id UUIDs to PySpark DataFrames
  connectors/        # BaseConnector + OSS/Databricks implementations
  tools/
    session.py       # start_session, close_session, list_sessions
    catalog.py       # list_tables, describe_table, table_exists, ...
    lazy.py          # load, sql, filter, select, with_column, drop, sort, limit, group_by_agg, join
    exec.py          # show, collect, count, schema, describe
```

### Registration pattern

`server.py` creates the `FastMCP` instance, then imports each tool module. The `@mcp.tool()` decorators run at import time, registering tools on the shared `mcp` instance. Adding a new tool module requires adding an import in `server.py`.

### Lazy tools (tools/lazy.py)

- Accept `session_id` (for load/sql) or `df_id` (for transforms)
- Build a lazy Spark plan — **no Spark job is triggered**
- Register the resulting DataFrame via `df_mod.registry.register(session_id, df)` and return `{"df_id": "...", "message": "..."}`
- Error format: `{"error": "...", "session_id": "..."}` or `{"error": "...", "df_id": "..."}`

### Exec tools (tools/exec.py)

- Accept only `df_id` — no `session_id` needed
- Trigger actual Spark computation
- `show()` returns plain-text ASCII table (not JSON)
- All others return JSON; errors: `{"error": "...", "df_id": "..."}`
- `MAX_COLLECT_LIMIT = 1000` — hard cap on `collect()` to prevent OOM

### Registry API

```python
# DataFrameRegistry (dataframes.py)
df_mod.registry.register(session_id, df) -> df_id
df_mod.registry.get(df_id) -> DataFrame          # KeyError if not found
df_mod.registry.session_for(df_id) -> session_id
df_mod.registry.remove(df_id) -> bool
df_mod.registry.clear_session(session_id) -> int

# SessionRegistry (session.py)
session_mod.registry.start(connector, config) -> session_id
session_mod.registry.get(session_id) -> SparkSession  # KeyError if not found
session_mod.registry.close(session_id) -> bool
session_mod.registry.list() -> list[SessionInfo]
```

## Writing tests

Mock `df_mod` and `session_mod` at the module level in the tool under test:

```python
@patch("spark_connect_mcp.tools.lazy.df_mod")
@patch("spark_connect_mcp.tools.lazy.session_mod")
def test_something(mock_session, mock_df):  # patch order is bottom-up
    ...
```

Every tool test should cover: success path, invalid df_id/session_id (KeyError), and Spark runtime error.

## Linting rules

- `ruff` with `select = ["E", "F", "I", "UP", "B", "SIM"]`, `ignore = ["E501"]`
- `isort` with `known-first-party = ["spark_connect_mcp"]`
- `mypy` on `src/` only (`disallow_untyped_defs = true`)
- Tests are not mypy-checked — only `src/` is in scope

## Issue / PR conventions

Issues include: Description, Acceptance Criteria, Dependencies sections.
Branch names: `issue/<N>-<slug>`.
