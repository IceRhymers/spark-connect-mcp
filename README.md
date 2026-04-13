# spark-connect-mcp

> MCP server exposing Spark Connect DataFrame API + SQL for AI agents.

A Python MCP server that gives AI agents (Claude Code, Agent SDK) a handle-based DataFrame API over Apache Spark Connect, with SQL as an escape hatch. Open-source Spark Connect is the primary target; Databricks Connect is an optional extra.

## Install

```bash
# Core (OSS Spark Connect)
pip install spark-connect-mcp

# With Databricks Connect support
pip install "spark-connect-mcp[databricks]"
```

## Usage with Claude Code

```json
{
  "mcpServers": {
    "spark": {
      "command": "uvx",
      "args": ["spark-connect-mcp"]
    }
  }
}
```

## Status

🚧 Under active development. See [issues](https://github.com/IceRhymers/spark-connect-mcp/issues) for the roadmap.

## License

MIT
