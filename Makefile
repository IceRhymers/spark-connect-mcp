.PHONY: install test test-integration test-all lint fmt fmt-check typecheck check build

install:
	uv sync --extra databricks

test:
	uv run pytest

test-integration:
	uv run pytest -m integration --override-ini="addopts=" tests/integration/

test-all:
	uv run pytest --override-ini="addopts=" -m "" tests/

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
