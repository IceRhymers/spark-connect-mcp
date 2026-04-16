# spark-connect-mcp

MCP server exposing Apache Spark Connect (and Databricks Connect) via DataFrame and SQL tools for AI agents.

## Install

Choose **one** backend — do not install both.

```bash
# OSS Spark Connect
pip install "spark-connect-mcp[spark]"

# Databricks Connect
pip install "spark-connect-mcp[databricks]"
```

## Quick Start

Add to your Claude Code MCP config:

```json
{
  "mcpServers": {
    "spark": {
      "command": "uvx",
      "args": ["--from", "spark-connect-mcp[databricks]", "spark-connect-mcp"]
    }
  }
}
```

For OSS Spark Connect, replace `[databricks]` with `[spark]`.

## Configuration

All connection config is set via environment variables — the MCP tools require no parameters to start a session.

### OSS Spark Connect

Set `SPARK_REMOTE` to your Spark Connect server URL (PySpark's native env var):

```bash
export SPARK_REMOTE=sc://localhost:15002
```

### Databricks Connect

Optionally set `DATABRICKS_CONFIG_PROFILE` to select a profile from `~/.databrickscfg` (defaults to `DEFAULT`):

```bash
export DATABRICKS_CONFIG_PROFILE=my-workspace
```

Serverless compute is used by default inside Databricks Apps, Jobs, and notebooks — no env var needed.

## Preflight Size Checks

Before executing an action tool (`show`, `collect`, `count`, `describe`, `save`,
`save_as_table`), spark-connect-mcp runs a lightweight preflight check that
inspects the Spark Catalyst optimized plan statistics to estimate result size
**without** triggering a Spark job. If the estimate exceeds configurable
thresholds the tool returns a warning instead of executing.

### Prerequisites — making statistics available

Preflight relies on Catalyst Cost-Based Optimization (CBO) statistics. How you
populate them depends on your environment:

| Environment | How to get statistics |
|---|---|
| Databricks UC managed tables | Enable **Predictive Optimization** — stats are computed automatically. |
| External / unmanaged tables | Run `ANALYZE TABLE <table> COMPUTE STATISTICS FOR ALL COLUMNS`. |
| OSS Spark | Set `spark.sql.cbo.enabled=true` and `spark.sql.cbo.planStats.enabled=true`, then run `ANALYZE TABLE`. |

### Confidence tiers

The quality of the estimate depends on what statistics are present in the plan:

| Tier | Condition | Behaviour |
|---|---|---|
| **High** | Root node has `sizeInBytes` + `rowCount`, and every join node has `rowCount` | Blocks if thresholds exceeded |
| **Medium** | Root has `rowCount` but some join nodes are missing `rowCount` | Uses 10× the configured thresholds before blocking |
| **Low** | Root has `sizeInBytes` only, no `rowCount` | Fail-open — warns but does not block |
| **Cross-join** | Plan contains `CartesianProduct` | Always warns regardless of thresholds |

### Threshold configuration

Set via environment variables (defaults shown):

```bash
# Maximum estimated bytes before warning (default 1 GB)
export SPARK_CONNECT_MCP_PREFLIGHT_MAX_BYTES=1073741824

# Maximum estimated rows before warning (default 10 million)
export SPARK_CONNECT_MCP_PREFLIGHT_MAX_ROWS=10000000

# Disable preflight entirely
export SPARK_CONNECT_MCP_PREFLIGHT_ENABLED=false
```

### Per-session overrides

Use the `set_preflight_threshold` tool to adjust thresholds for a single session
without changing env vars:

```json
{
  "tool": "set_preflight_threshold",
  "arguments": {
    "session_id": "abc123",
    "max_bytes": 5368709120,
    "max_rows": 50000000
  }
}
```

Pass `"enabled": false` to disable preflight for that session.

### The force escape hatch

Every action tool accepts a `force` parameter. Pass `force=True` to skip the
preflight check entirely and execute immediately:

```json
{
  "tool": "collect",
  "arguments": { "df_id": "df-001", "limit": 100, "force": true }
}
```

## Status

Under active development. See [issues](https://github.com/IceRhymers/spark-connect-mcp/issues) for the roadmap.

## License

Apache-2.0
