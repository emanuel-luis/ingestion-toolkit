"""SQL text builders for rendered DAGs.

Every identifier is quoted here, after the config validators have checked it. Runtime values
never go into the text: the rendered DAG binds them as parameters.
"""

from __future__ import annotations

from ingestion_toolkit.models.config import Config
from ingestion_toolkit.models.enums import ColumnType, Pattern, WriteMode

# Placeholder in the incremental SELECT, filled with the source hook's `placeholder` at runtime.
BOUND_PLACEHOLDER = "{ph}"

PG_TYPES: dict[str, str] = {
    ColumnType.INTEGER.value: "bigint",
    ColumnType.NUMERIC.value: "numeric",
    ColumnType.FLOAT.value: "double precision",
    ColumnType.BOOLEAN.value: "boolean",
    ColumnType.TEXT.value: "text",
    ColumnType.DATE.value: "date",
    ColumnType.TIMESTAMP.value: "timestamp",
    ColumnType.TIMESTAMPTZ.value: "timestamptz",
}


def quote_ident(name: str) -> str:
    """Quote one identifier, doubling any embedded double quote."""
    return '"' + name.replace('"', '""') + '"'


def qualified_name(*parts: str) -> str:
    """Quote and join identifiers with dots."""
    return ".".join(quote_ident(part) for part in parts)


def source_table(config: Config) -> str:
    """The source table as "database"."schema"."table"."""
    source = config.source
    return qualified_name(source.database, source.schema_name, config.extract.table)


def select_sql(config: Config) -> str:
    """SELECT of the contract columns (or *) from the source table, without a bound."""
    columns = config.extract.columns
    names = ", ".join(quote_ident(c.name) for c in columns) if columns else "*"
    return f"SELECT {names} FROM {source_table(config)}"


def bounded_select_sql(config: Config) -> str:
    """The incremental SELECT with a strict lower bound on the watermark column."""
    mark = config.extract.watermark
    if config.extract.pattern is not Pattern.INCREMENTAL or mark is None:
        raise ValueError("a bounded select needs the incremental pattern")
    return f"{select_sql(config)} WHERE {quote_ident(mark.column)} > {BOUND_PLACEHOLDER}"


def target_schema(config: Config) -> str:
    """The quoted bronze schema."""
    return quote_ident(config.load.target_schema)


def target_table(config: Config) -> str:
    """The quoted bronze table, schema-qualified."""
    return qualified_name(config.load.target_schema, config.load.target_table or "")


def create_schema_sql(config: Config) -> str:
    """Idempotent creation of the bronze schema."""
    return f"CREATE SCHEMA IF NOT EXISTS {target_schema(config)}"


def prepare_sql(config: Config) -> str:
    """Statement run before the insert: replace everything (overwrite) or this run (append)."""
    table = target_table(config)
    if config.load.write_mode is WriteMode.OVERWRITE:
        return f"DELETE FROM {table}"
    if config.load.write_mode is WriteMode.APPEND:
        return f'DELETE FROM {table} WHERE "_ingest_run_id" = %s'
    raise ValueError(f"write mode {config.load.write_mode} has no prepare statement")


def duplicate_sql(config: Config) -> str:
    """Return one row if this run's rows repeat a primary key."""
    keys = ", ".join(quote_ident(k) for k in config.extract.primary_key)
    return (
        f'SELECT 1 FROM {target_table(config)} WHERE "_ingest_run_id" = %s '
        f"GROUP BY {keys} HAVING count(*) > 1 LIMIT 1"
    )
