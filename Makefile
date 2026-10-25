.PHONY: help install lint test test-fast render build clean
.DEFAULT_GOAL := help

help:           ## list the targets
	@awk -F ':.*## ' '/^[a-z-]+:.*## /{printf "  %-10s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install:        ## local dev install
	uv sync

lint:           ## what CI's lint job runs
	uv run ruff check
	uv run ruff format --check
	uv run mypy

test:           ## the full test suite
	uv run pytest

test-fast:      ## quiet, stop at the first failure
	uv run pytest -q -x

render:         ## render examples/postgres into output/
	uv run ingest render examples/postgres --output-dir output

build:          ## wheel and sdist into dist/
	uv build

clean:          ## remove build output and caches
	rm -rf dist build output .pytest_cache .mypy_cache .ruff_cache .hypothesis .coverage*
