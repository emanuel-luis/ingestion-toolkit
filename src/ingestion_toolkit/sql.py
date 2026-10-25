"""SQL text builders for rendered DAGs.

Every identifier is quoted here, after the config validators have checked it. Runtime values
never go into the text: the rendered DAG binds them as parameters.
"""

from __future__ import annotations

from ingestion_toolkit.models.config import Config
from ingestion_toolkit.models.enums import (
    CdcMechanism,
    ColumnType,
    Pattern,
    SequenceType,
    WriteMode,
)

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


def _from_table(config: Config) -> str:
    """The table the extract reads: the CDC change table when one is set, else the source table."""
    cdc = config.extract.cdc
    if cdc is not None and cdc.change_table is not None:
        source = config.source
        return qualified_name(source.database, source.schema_name, cdc.change_table)
    return source_table(config)


def _base_select(config: Config) -> str:
    columns = config.extract.columns
    names = ", ".join(quote_ident(c.name) for c in columns) if columns else "*"
    return f"SELECT {names} FROM {_from_table(config)}"


def select_sql(config: Config) -> str:
    """The extract SELECT without a lower bound (the first run, or a pattern without one).

    CDC adds the order, and for a rowversion the upper bound that skips uncommitted changes.
    """
    cdc = config.extract.cdc
    if cdc is None:
        return _base_select(config)
    column = quote_ident(cdc.sequence_column)
    if cdc.mechanism is CdcMechanism.ROWVERSION:
        return f"{_base_select(config)} WHERE {column} < MIN_ACTIVE_ROWVERSION() ORDER BY {column}"
    return f"{_base_select(config)} ORDER BY {column}"


def bounded_select_sql(config: Config) -> str:
    """The SELECT with a strict lower bound (``{ph}``) on the watermark or sequence column."""
    mark, cdc = config.extract.watermark, config.extract.cdc
    if mark is not None and config.extract.pattern is Pattern.INCREMENTAL:
        return f"{_base_select(config)} WHERE {quote_ident(mark.column)} > {BOUND_PLACEHOLDER}"
    if cdc is None:
        raise ValueError("a bounded select needs the incremental or cdc pattern")
    column = quote_ident(cdc.sequence_column)
    if cdc.mechanism is CdcMechanism.ROWVERSION:
        return (
            f"{_base_select(config)} WHERE {column} > CONVERT(binary(8), {BOUND_PLACEHOLDER}, 1) "
            f"AND {column} < MIN_ACTIVE_ROWVERSION() ORDER BY {column}"
        )
    return f"{_base_select(config)} WHERE {column} > {BOUND_PLACEHOLDER} ORDER BY {column}"


def sequence_column(config: Config) -> str | None:
    """The column that orders versions of one row: the CDC sequence or the incremental watermark."""
    cdc, mark = config.extract.cdc, config.extract.watermark
    return cdc.sequence_column if cdc is not None else mark.column if mark is not None else None


def sequence_kind(config: Config) -> str | None:
    """integer, datetime or rowversion: how the sequence column is bound and compared."""
    cdc, mark = config.extract.cdc, config.extract.watermark
    if cdc is not None:
        return "rowversion" if cdc.mechanism is CdcMechanism.ROWVERSION else str(cdc.sequence_type)
    return mark.type.value if mark is not None else None


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


def merge_extra_ddl(config: Config) -> tuple[str, ...]:
    """Extra table definitions of a merge target: the delete flag and the key constraint."""
    if config.load.write_mode is not WriteMode.MERGE:
        return ()
    keys = ", ".join(quote_ident(k) for k in config.load.merge_keys or ())
    return ('"_ingest_deleted" boolean NOT NULL DEFAULT false', f"PRIMARY KEY ({keys})")


def conflict_target_sql(config: Config) -> str:
    """The ``(keys)`` of ``ON CONFLICT``."""
    return "(" + ", ".join(quote_ident(k) for k in config.load.merge_keys or ()) + ")"


def merge_guard_sql(config: Config) -> str:
    """WHERE of the upsert: an older version never replaces a newer one.

    Bronze columns may be text (no contract), so both sides are cast by the sequence kind: a
    rowversion is fixed-width uppercase hex and compares as text under the C collation.
    """
    column, kind = sequence_column(config), sequence_kind(config)
    if column is None or kind is None:
        raise ValueError("a merge guard needs a watermark or a cdc sequence column")
    name = quote_ident(column)
    suffix = {
        SequenceType.INTEGER.value: "::bigint",
        SequenceType.DATETIME.value: "::timestamptz",
    }.get(kind, ' COLLATE "C"')
    return f"t.{name}{suffix} <= EXCLUDED.{name}{suffix}"
