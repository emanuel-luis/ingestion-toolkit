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

### Notes

- `watermark.initial_value` is stored as text: a canonical ISO datetime or a canonical integer.
- A section such as `load` that is present but not a mapping is a validation error, not a
  silent default.
