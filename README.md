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

## Status

Under active development. See [issues](https://github.com/IceRhymers/spark-connect-mcp/issues) for the roadmap.

## License

Apache-2.0
