.PHONY: install test lint fmt fmt-check typecheck check build

install:
	uv sync --extra databricks

test:
	uv run pytest tests/

lint:
	uv run ruff check src/ tests/

fmt:
	uv run ruff format src/ tests/

fmt-check:
	uv run ruff format --check src/ tests/

typecheck:
	uv run mypy

check: lint fmt-check typecheck

build:
	uv build

clean:
	rm -rf build/ dist/ .mypy_cache/ .ruff_cache/ .pytest_cache/ .coverage .venv/
