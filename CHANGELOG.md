# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Frozen Pydantic config models for relational sources with the fullscan and incremental
  patterns, an optional column contract, and validators whose messages end with a fix hint.
- Config I/O for YAML and JSON: duplicate keys are rejected, syntax errors report the line, and
  every error in every file is collected. Written files are deterministic.
- `ingest add rdbms fullscan` and `ingest add rdbms incremental`, which refuse to overwrite an
  existing file without `--force`.
- `ingest validate`, which checks schemas, file names (`{source}__{table}__{pattern}`) and
  duplicate DAG ids, prints one error line per problem and a summary, and exits 0, 1 or 2.
- `--version`, `python -m ingestion_toolkit` and a typed package (`py.typed`).
- Examples in `examples/postgres/`, rebuilt from the documented commands by a test.
- Unit, property-based (Hypothesis) and CLI tests.
- CI (lint, tests on Python 3.11 to 3.14, wheel build smoke test), a security workflow
  (pip-audit, zizmor, lowest-direct resolution) and a tag-driven release workflow that is not
  enabled yet.

- `ingest render`: one self-contained Airflow 3 DAG file per config for the fullscan and
  incremental patterns (`ensure_tables`, `extract_to_landing`, `load_bronze`). Landing is JSON
  Lines per run with a manifest, bronze is typed from the column contract, rows that break it go
  to `ops.ingestion_rejects`, and run metadata and the incremental watermark live in the `ops`
  schema, written in the same transaction as the load. Options: `--output-dir`, `--template-dir`,
  `--dry-run`. If any config is invalid or fails to render, nothing is written; each file is
  written atomically.
- Rendered loads refuse a stale extract: clearing only `load_bronze` of an older run after a
  later run succeeded fails with a hint to re-run the whole DAG run, instead of duplicating rows
  or moving the watermark back.
- Golden files for the two examples (`pytest --update-golden` rewrites them), a Hypothesis
  property that every valid config renders to parseable, deterministic code, and a
  `scripts/check_dag_imports.py` DagBag gate run by the new `dag-import` CI job against Airflow
  3.3.2.
- `docs/generated-dags.md`, `make render`, and a `slow` pytest marker (excluded by default).

- A docker compose demo (`make demo`): Postgres 18 with a synthetic `erp` source and a
  `warehouse` target, plus Airflow 3.3.2 on the stock image running the rendered DAGs. `make
  smoke` asserts a full scenario (first run, the same run id again, an empty next day, a source
  change) and the new `smoke` CI job runs it. `demo/run_dag.py` runs a DAG through `DAG.test`
  with the run id fixed by the logical date, because `airflow dags test` names every run after
  the current time. Also `make render-demo`, `make up` and `make down`.

- The CDC pattern and merge loads. `pattern: cdc` with a `cdc` block: `generic` (a sequence
  column read with `WHERE seq > bound ORDER BY seq`, optionally from a change table) or
  `rowversion` for SQL Server (`rv > bound AND rv < MIN_ACTIVE_ROWVERSION()`, watermark stored
  as fixed-width hex). Merge targets get a primary key on `load.merge_keys` and an
  `_ingest_deleted` flag; the upsert only lets a newer sequence replace an older one, so
  replaying a window changes nothing. Incremental can merge too. Delete operations set the flag.
- `ingest add rdbms cdc`, `ingest add mssql cdc` and `ingest init [DIR]`.
- Examples `examples/postgres/erp__products__cdc.yml` and
  `examples/mssql/erp_mssql__products__cdc.yml` with goldens, and `docs/cdc-contract.md`.
- Demo: `sales.products` with a trigger-stamped `version_stamp`, mutations and smoke assertions
  for the products CDC DAG.

### Changed

- `ingest render` no longer refuses `write_mode: merge`.
- The `dag-import` CI job renders and imports the postgres and mssql examples.
- Text values containing a NUL character are rejected as contract rejects, and reject payloads
  never contain the NUL escape (`jsonb` refuses it).
- A `psycopg2.DataError` while inserting a batch (numeric overflow, date out of range) is a
  quality failure (rollback, `failed` run row, no retry) instead of a retried crash.
- The orders example now declares `quantity` as not null, so the demo's one NULL `quantity` is
  rejected by the column contract.
- `tests/test_versions_in_sync.py` reads the Airflow image tag from `compose.yaml`.
- Integer `watermark.initial_value` beyond the bigint range is rejected, because bronze stores
  integers as bigint.
- The CI `build` job checks that the wheel ships the templates and renders an example from the
  installed wheel.

### Notes

- `watermark.initial_value` is stored as text: a canonical ISO datetime or a canonical integer.
- A section such as `load` that is present but not a mapping is a validation error, not a
  silent default.
