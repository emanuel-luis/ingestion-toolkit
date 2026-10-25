.PHONY: help install lint test test-fast render build clean render-demo up down demo smoke
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

COMPOSE := docker compose
# Linux: run the Airflow container as the current user so it can write demo/data/landing
export AIRFLOW_UID ?= $(shell id -u)

.env:
	cp .env.example .env

render-demo:    ## render examples/postgres into demo/dags
	uv run ingest render examples/postgres --output-dir demo/dags

up: .env render-demo  ## start the demo stack (Postgres and Airflow) and wait until healthy
	$(COMPOSE) up -d --wait --wait-timeout 300

down:           ## stop the demo stack and delete its volumes
	$(COMPOSE) down -v --remove-orphans

demo: up        ## one command: start the stack, run both DAGs, show the result
	$(COMPOSE) exec -T airflow python - erp__orders__fullscan 2026-01-01 < demo/run_dag.py
	$(COMPOSE) exec -T airflow python - erp__customers__incremental 2026-01-01 < demo/run_dag.py
	$(COMPOSE) exec -T postgres psql -U postgres -d warehouse -f - < demo/summary.sql
	@echo "Airflow UI: http://127.0.0.1:$${AIRFLOW_PORT:-8080} (demo auth: no login needed)"

smoke: .env render-demo  ## the demo stack from scratch plus the end-to-end assertions
	bash demo/smoke.sh

clean:          ## remove build output and caches
	rm -rf dist build output .pytest_cache .mypy_cache .ruff_cache .hypothesis .coverage*
