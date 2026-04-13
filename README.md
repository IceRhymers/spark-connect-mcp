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

## Status

Under active development. See [issues](https://github.com/IceRhymers/spark-connect-mcp/issues) for the roadmap.

## License

Apache-2.0
