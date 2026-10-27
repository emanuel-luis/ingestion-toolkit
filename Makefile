SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.PHONY: help install lint test test-fast render build clean render-demo up down demo smoke \
	d2-install diagrams diagrams-check diagrams-watch
.DEFAULT_GOAL := help

help:           ## list the targets
	@grep -hE '^[a-zA-Z0-9_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-16s %s\n", $$1, $$2}'

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

demo: up        ## one command: start the stack, run the three DAGs, show the result
	$(COMPOSE) exec -T airflow python - erp__orders__fullscan 2026-01-01 < demo/run_dag.py
	$(COMPOSE) exec -T airflow python - erp__customers__incremental 2026-01-01 < demo/run_dag.py
	$(COMPOSE) exec -T airflow python - erp__products__cdc 2026-01-01 < demo/run_dag.py
	$(COMPOSE) exec -T postgres psql -U postgres -d warehouse -f - < demo/summary.sql
	@echo "Airflow UI: http://127.0.0.1:$${AIRFLOW_PORT:-8080} (demo auth: no login needed)"

smoke: .env render-demo  ## the demo stack from scratch plus the end-to-end assertions
	bash demo/smoke.sh

clean:          ## remove build output and caches
	rm -rf dist build output .pytest_cache .mypy_cache .ruff_cache .hypothesis .coverage*

# Diagrams: docs/diagrams/<name>.d2 rendered to <name>.svg (committed). One pinned D2 everywhere.
# Layout engine, themes and padding live in docs/diagrams/_style.d2 (d2-config), so d2 runs in a
# clean environment: D2_* and SCALE variables would override that config. --scale 1 is not a
# d2-config key; it writes the natural width and height, so GitHub never upscales a small diagram.
D2_VERSION := 0.9.0
# sha256 of d2-v$(D2_VERSION)-linux-amd64.tar.gz, from the release's SHA256SUMS
D2_SHA256 := 5669ddc46b99e942cc96078f4a4e36d5e62103348f4c05179ede27802fdd87a9
D2_HOME ?= $(HOME)/.local/share/d2/v$(D2_VERSION)
D2 ?= $(D2_HOME)/bin/d2
D2_RUN = env -i PATH="$$PATH" HOME="$$HOME" "$(D2)" --scale 1
DIAGRAMS := $(filter-out docs/diagrams/_%,$(wildcard docs/diagrams/*.d2))

d2-install: ## install the pinned D2 (Linux x86_64) into D2_HOME, checksum verified
	@if [ -x "$(D2_HOME)/bin/d2" ]; then echo "D2 v$(D2_VERSION) already in $(D2_HOME)"; exit 0; fi; \
	[ "$$(uname -sm)" = "Linux x86_64" ] || { echo "d2-install covers Linux x86_64 only;" \
		"install D2 v$(D2_VERSION) yourself and pass D2=/path/to/d2" >&2; exit 1; }; \
	tmp="$$(mktemp -d)"; trap 'rm -rf "$$tmp"' EXIT; \
	curl -fsSLo "$$tmp/d2.tar.gz" \
		"https://github.com/terrastruct/d2/releases/download/v$(D2_VERSION)/d2-v$(D2_VERSION)-linux-amd64.tar.gz"; \
	echo "$(D2_SHA256)  $$tmp/d2.tar.gz" | sha256sum --check --strict; \
	tar -xzf "$$tmp/d2.tar.gz" -C "$$tmp"; \
	mkdir -p "$(D2_HOME)/bin"; \
	install -m 0755 "$$tmp/d2-v$(D2_VERSION)/bin/d2" "$(D2_HOME)/bin/d2"; \
	"$(D2_HOME)/bin/d2" --version

diagrams: ## render docs/diagrams/*.d2 to SVG with the pinned D2 (run make d2-install once)
	@v="$$($(D2_RUN) --version 2>/dev/null || true)"; [ "$$v" = "v$(D2_VERSION)" ] || { \
		echo "need D2 v$(D2_VERSION) at $(D2), found '$${v:-nothing}': run make d2-install" >&2; exit 1; }
	@for f in $(DIAGRAMS); do $(D2_RUN) "$$f" "$${f%.d2}.svg"; done

diagrams-check: diagrams ## render, then fail if a committed SVG is stale, missing or has no source
	@shopt -s nullglob; for s in docs/diagrams/*.svg; do \
		[ -f "$${s%.svg}.d2" ] || { echo "$$s has no .d2 source" >&2; exit 1; }; done
	@changed="$$(git status --porcelain --untracked-files=all -- 'docs/diagrams/*.svg')"; \
	if [ -n "$$changed" ]; then echo "$$changed" >&2; \
		echo "rendered SVGs differ from the committed ones: run make diagrams and commit" >&2; exit 1; fi

diagrams-watch: ## live preview in the browser while editing: make diagrams-watch NAME=architecture
	@# NAME only from the command line: WSL exports NAME (the hostname) into every shell.
	@[ "$(origin NAME)" = "command line" ] && [ -f "docs/diagrams/$(NAME).d2" ] || { \
		echo "usage: make diagrams-watch NAME=<one of: $(basename $(notdir $(DIAGRAMS)))>" >&2; exit 1; }
	$(D2_RUN) --watch --browser "$${BROWSER:-0}" docs/diagrams/$(NAME).d2 docs/diagrams/$(NAME).svg
