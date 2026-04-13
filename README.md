# spark-connect-mcp

MCP server exposing Apache Spark Connect (and Databricks Connect) via DataFrame and SQL tools for AI agents.

## Install

```bash
# Via uvx (recommended for Claude Code / MCP)
uvx spark-connect-mcp

# Core (OSS Spark Connect)
pip install spark-connect-mcp

# With Databricks Connect support
pip install "spark-connect-mcp[databricks]"
```

## Quick Start

Add to your Claude Code MCP config:

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

Under active development. See [issues](https://github.com/IceRhymers/spark-connect-mcp/issues) for the roadmap.

## License

Apache-2.0
