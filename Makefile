.PHONY: install test lint fmt build

install:
	uv sync --extra databricks

test:
	uv run pytest tests/

lint:
	uv run ruff check src/ tests/

fmt:
	uv run ruff format src/ tests/

build:
	uv build
