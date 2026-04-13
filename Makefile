.PHONY: install test lint fmt build

install:
	pip install -e ".[databricks]"

test:
	pytest tests/

lint:
	ruff check src/ tests/

fmt:
	ruff format src/ tests/

build:
	python -m build
