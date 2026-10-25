"""Frozen contexts the templates read. Built by ``renderer.build_dag_context``."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ExtractTaskContext:
    """What the extract template needs."""

    template: str
    extract_sql: str
    bounded_sql: str | None = None  # incremental: SELECT with a {ph} lower bound
    bound_column: str | None = None
    bound_type: str | None = None
    initial_value: str | None = None


@dataclass(frozen=True)
class LoadTaskContext:
    """What the load template needs."""

    template: str
    bronze_schema_sql: str
    bronze_table: str
    bronze_schema: str
    bronze_name: str
    columns: tuple[tuple[str, str, bool], ...] | None  # (name, type, nullable); None = no contract
    pg_types: dict[str, str]
    prepare_sql: str
    duplicate_sql: str
    primary_key: tuple[str, ...]
    max_reject_ratio: float


@dataclass(frozen=True)
class DagContext:
    """Everything one rendered DAG file is built from."""

    dag_id: str
    config_name: str
    description: str | None
    owner: str
    schedule: str | None
    start_date: str
    retries: int
    retry_delay_minutes: int
    tags: tuple[str, ...]
    source_name: str
    source_table: str
    pattern: str
    source_conn_id: str
    target_conn_id: str
    landing_dir: str
    helper_templates: tuple[str, ...]
    extract: ExtractTaskContext
    load: LoadTaskContext
