"""Allow `python -m spark_connect_mcp` as an alias for the CLI entry point."""

from spark_connect_mcp.server import main

if __name__ == "__main__":
    main()
