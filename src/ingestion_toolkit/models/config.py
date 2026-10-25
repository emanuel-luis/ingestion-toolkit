"""Config models. Every model is frozen and rejects unknown fields."""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ingestion_toolkit import naming
from ingestion_toolkit.cron import PRESETS, is_valid_schedule
from ingestion_toolkit.models.enums import (
    CdcMechanism,
    ColumnType,
    Pattern,
    SequenceType,
    SourceType,
    WatermarkType,
    WriteMode,
)

RESERVED_PREFIX = "_ingest_"
MAX_TARGET_TABLE_LENGTH = 59
_INT_RE = re.compile(r"[+-]?[0-9]+")
_BIGINT_MIN, _BIGINT_MAX = -(2**63), 2**63 - 1  # bronze stores integer as bigint
_IDENT_HINT = (
    "use letters, digits and underscores, start with a letter or underscore, at most 63 characters"
)
_DEFAULT_WRITE_MODE: dict[str, WriteMode] = {
    Pattern.FULLSCAN.value: WriteMode.OVERWRITE,
    Pattern.INCREMENTAL.value: WriteMode.APPEND,
    Pattern.CDC.value: WriteMode.MERGE,
}
_OPTIONAL_SECTIONS = ("load", "landing", "schedule", "metadata")
_ALLOWED_WRITE_MODES = {
    Pattern.FULLSCAN: (WriteMode.OVERWRITE, WriteMode.APPEND),
    Pattern.INCREMENTAL: (WriteMode.APPEND, WriteMode.MERGE),
    Pattern.CDC: (WriteMode.MERGE,),
}
_WATERMARK_COLUMN_TYPES = {
    WatermarkType.DATETIME: (ColumnType.TIMESTAMP, ColumnType.TIMESTAMPTZ, ColumnType.DATE),
    WatermarkType.INTEGER: (ColumnType.INTEGER,),
}
_TIME_COLUMN_TYPES = (ColumnType.TIMESTAMP, ColumnType.TIMESTAMPTZ, ColumnType.DATE)


def _fail(message: str, hint: str) -> ValueError:
    """A validation error whose text ends with ``Fix: <hint>`` (parsed by config_io)."""
    return ValueError(f"{message}. Fix: {hint}")


def _ident(value: str, what: str) -> str:
    if not naming.IDENT_RE.fullmatch(value):
        raise _fail(f"{what} {value!r} is not a valid identifier", _IDENT_HINT)
    return value


def _attr(obj: Any, name: str) -> Any:
    """Read a field from a dict or a model instance."""
    return obj.get(name) if isinstance(obj, dict) else getattr(obj, name, None)


def _not_reserved(value: str, what: str) -> str:
    if value.startswith(RESERVED_PREFIX):
        raise _fail(
            f"{what} {value!r} uses the reserved prefix {RESERVED_PREFIX!r}",
            "rename it in the config or alias it in the source",
        )
    return value


def _in_range(value: int, low: int, high: int, what: str) -> int:
    if not low <= value <= high:
        raise _fail(
            f"{what} is {value}, allowed {low} to {high}", f"use a value from {low} to {high}"
        )
    return value


class _Model(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class Column(_Model):
    name: str
    type: ColumnType
    nullable: bool = True

    @field_validator("name")
    @classmethod
    def _name(cls, value: str) -> str:
        _ident(value, "column name")
        return _not_reserved(value, "column name")


class Source(_Model):
    name: str
    source_type: SourceType = SourceType.RDBMS
    connection_id: str
    database: str
    schema_name: str

    @model_validator(mode="before")
    @classmethod
    def _defaults(cls, data: Any) -> Any:
        if isinstance(data, dict):
            data = dict(data)
            if data.get("connection_id") is None and isinstance(data.get("name"), str):
                data["connection_id"] = data["name"]
            if data.get("schema_name") is None:
                data["schema_name"] = "dbo" if data.get("source_type") == "mssql" else "public"
        return data

    @field_validator("name")
    @classmethod
    def _name(cls, value: str) -> str:
        if not naming.SEGMENT_RE.fullmatch(value):
            raise _fail(
                f"source name {value!r} is not a valid segment",
                "use lowercase letters, digits and single underscores, starting with a letter",
            )
        return value

    @field_validator("connection_id")
    @classmethod
    def _connection_id(cls, value: str) -> str:
        if not naming.CONN_ID_RE.fullmatch(value):
            raise _fail(
                f"connection id {value!r} is not valid",
                "use letters, digits, underscores, dots and hyphens",
            )
        return value

    @field_validator("database")
    @classmethod
    def _database(cls, value: str) -> str:
        return _ident(value, "database")

    @field_validator("schema_name")
    @classmethod
    def _schema(cls, value: str) -> str:
        return _ident(value, "schema")


class Watermark(_Model):
    column: str
    type: WatermarkType = WatermarkType.DATETIME
    initial_value: str | None = None  # canonical: ISO datetime string, or integer as text

    @field_validator("column")
    @classmethod
    def _column(cls, value: str) -> str:
        return _not_reserved(_ident(value, "watermark column"), "watermark column")

    @model_validator(mode="before")
    @classmethod
    def _normalise(cls, data: Any) -> Any:
        """Store initial_value in one canonical text form, whatever way it was written."""
        if not isinstance(data, dict) or data.get("initial_value") is None:
            return data
        value = data["initial_value"]
        if isinstance(value, bool):
            raise _fail("watermark initial_value is a boolean", "use an ISO datetime or an integer")
        if data.get("type") == WatermarkType.INTEGER:
            if isinstance(value, int):
                value = str(value)
            if isinstance(value, str) and _INT_RE.fullmatch(value):
                value = str(int(value))
        elif isinstance(value, datetime | date | str):
            text = value.isoformat() if isinstance(value, datetime | date) else value
            try:
                value = datetime.fromisoformat(text).isoformat()
            except ValueError:
                value = text
        return {**data, "initial_value": value}

    @model_validator(mode="after")
    def _parses(self) -> Self:
        value = self.initial_value
        if value is None:
            return self
        integer = self.type is WatermarkType.INTEGER
        try:
            ok = bool(_INT_RE.fullmatch(value)) if integer else bool(datetime.fromisoformat(value))
        except ValueError:
            ok = False
        if ok and integer and not _BIGINT_MIN <= int(value) <= _BIGINT_MAX:
            raise _fail(
                f"watermark initial_value {value} is outside the bigint range",
                f"use an integer from {_BIGINT_MIN} to {_BIGINT_MAX}",
            )
        if not ok:
            raise _fail(
                f"watermark initial_value {value!r} is not "
                + ("an integer" if integer else "an ISO datetime"),
                "use "
                + ("an integer such as 0" if integer else "a value like 2026-01-01T00:00:00"),
            )
        return self


class CdcConfig(_Model):
    """Where a change feed lives and how to order it (see docs/cdc-contract.md)."""

    mechanism: CdcMechanism = CdcMechanism.GENERIC
    sequence_column: str
    sequence_type: SequenceType | None = None  # generic: integer by default; rowversion: unset
    change_table: str | None = None
    operation_column: str | None = None
    delete_values: tuple[str, ...] = ("D",)

    @model_validator(mode="before")
    @classmethod
    def _default_sequence_type(cls, data: Any) -> Any:
        if (
            isinstance(data, dict)
            and data.get("mechanism", CdcMechanism.GENERIC) != CdcMechanism.ROWVERSION
            and data.get("sequence_type") is None
        ):
            return {**data, "sequence_type": SequenceType.INTEGER}
        return data

    @field_validator("sequence_column")
    @classmethod
    def _sequence_column(cls, value: str) -> str:
        return _not_reserved(_ident(value, "sequence column"), "sequence column")

    @field_validator("change_table")
    @classmethod
    def _change_table(cls, value: str | None) -> str | None:
        return None if value is None else _ident(value, "change table")

    @field_validator("operation_column")
    @classmethod
    def _operation_column(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _not_reserved(_ident(value, "operation column"), "operation column")

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.mechanism is CdcMechanism.ROWVERSION and self.sequence_type is not None:
            raise _fail(
                "cdc.sequence_type is not allowed for mechanism rowversion",
                "remove sequence_type: a rowversion is compared as fixed-width hex text",
            )
        if self.operation_column is not None and not self.delete_values:
            raise _fail(
                "cdc.delete_values is empty while cdc.operation_column is set",
                "list the operation values that mean delete, such as D",
            )
        if not all(self.delete_values):
            raise _fail("cdc.delete_values has an empty value", "use non-empty values such as D")
        return self


class ExtractConfig(_Model):
    table: str
    primary_key: tuple[str, ...]
    pattern: Pattern
    columns: tuple[Column, ...] | None = None
    watermark: Watermark | None = None
    cdc: CdcConfig | None = None

    @field_validator("table")
    @classmethod
    def _table(cls, value: str) -> str:
        _ident(value, "table")
        if not naming.SEGMENT_RE.fullmatch(value.lower()):
            raise _fail(
                f"table {value!r} lowercased is not a valid DAG segment",
                "avoid double, leading or trailing underscores in the table name",
            )
        return value

    @field_validator("primary_key", mode="before")
    @classmethod
    def _primary_key_list(cls, value: Any) -> Any:
        return (value,) if isinstance(value, str) else value

    @model_validator(mode="before")
    @classmethod
    def _force_key_columns_not_null(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        columns, keys = data.get("columns"), data.get("primary_key")
        keys = (keys,) if isinstance(keys, str) else keys
        if not isinstance(columns, list | tuple) or not isinstance(keys, list | tuple):
            return data
        fixed: list[Any] = []
        for column in columns:
            if isinstance(column, Column) and column.name in keys:
                column = column.model_copy(update={"nullable": False})
            elif isinstance(column, dict) and column.get("name") in keys:
                column = {**column, "nullable": False}
            fixed.append(column)
        return {**data, "columns": fixed}

    @model_validator(mode="after")
    def _check(self) -> Self:
        keys = self.primary_key
        if not keys:
            raise _fail("primary_key is empty", "list at least one column")
        if len(set(keys)) != len(keys):
            raise _fail("primary_key has duplicate columns", "list each key column once")
        for key in keys:
            _not_reserved(_ident(key, "primary key column"), "primary key column")
        incremental = self.pattern is Pattern.INCREMENTAL
        if incremental and self.watermark is None:
            raise _fail(
                "extract.watermark is required when pattern is incremental",
                "add watermark with column and type",
            )
        if not incremental and self.watermark is not None:
            raise _fail(
                f"extract.watermark is not allowed when pattern is {self.pattern.value}",
                "remove the watermark or use pattern incremental",
            )
        is_cdc = self.pattern is Pattern.CDC
        if is_cdc and self.cdc is None:
            raise _fail(
                "extract.cdc is required when pattern is cdc",
                "add cdc with at least sequence_column",
            )
        if not is_cdc and self.cdc is not None:
            raise _fail(
                f"extract.cdc is not allowed when pattern is {self.pattern.value}",
                "remove the cdc block or use pattern cdc",
            )
        if self.columns is None:
            return self
        return self._check_columns(keys)

    def _check_columns(self, keys: tuple[str, ...]) -> Self:
        columns = self.columns or ()
        if not columns:
            raise _fail("extract.columns is empty", "list the columns or remove the field")
        names = [c.name for c in columns]
        if len(set(names)) != len(names):
            raise _fail("extract.columns has duplicate names", "list each column once")
        missing = [k for k in keys if k not in names]
        if missing:
            raise _fail(
                f"primary key column(s) {missing} are not in extract.columns",
                "add them to columns or fix primary_key",
            )
        mark = self.watermark
        if mark is not None:
            column = next((c for c in columns if c.name == mark.column), None)
            if column is None:
                raise _fail(
                    f"watermark column {mark.column!r} is not in extract.columns",
                    "add it to columns or fix watermark.column",
                )
            allowed = _WATERMARK_COLUMN_TYPES[mark.type]
            if column.type not in allowed:
                raise _fail(
                    f"watermark type {mark.type.value!r} does not fit column type "
                    f"{column.type.value!r}",
                    "use a column of type " + ", ".join(t.value for t in allowed),
                )
        if self.cdc is not None:
            self._check_cdc_columns(self.cdc, columns)
        return self

    def _check_cdc_columns(self, cdc: CdcConfig, columns: tuple[Column, ...]) -> None:
        by_name = {c.name: c for c in columns}
        sequence = by_name.get(cdc.sequence_column)
        if sequence is None:
            raise _fail(
                f"cdc sequence column {cdc.sequence_column!r} is not in extract.columns",
                "add it to columns or fix cdc.sequence_column",
            )
        allowed: tuple[ColumnType, ...]
        if cdc.mechanism is CdcMechanism.ROWVERSION:
            what, allowed = "mechanism rowversion", (ColumnType.TEXT,)
        elif cdc.sequence_type is SequenceType.DATETIME:
            what, allowed = "sequence type 'datetime'", _TIME_COLUMN_TYPES
        else:
            what, allowed = "sequence type 'integer'", (ColumnType.INTEGER,)
        if sequence.type not in allowed:
            raise _fail(
                f"cdc {what} does not fit column type {sequence.type.value!r}",
                "use a column of type " + ", ".join(t.value for t in allowed),
            )
        if cdc.operation_column is not None and cdc.operation_column not in by_name:
            raise _fail(
                f"cdc operation column {cdc.operation_column!r} is not in extract.columns",
                "add it to columns or fix cdc.operation_column",
            )


class LoadConfig(_Model):
    connection_id: str = "warehouse"
    target_schema: str = "bronze"
    target_table: str | None = None
    write_mode: WriteMode | None = None
    merge_keys: tuple[str, ...] | None = None
    max_reject_ratio: float = 0.0

    @field_validator("connection_id")
    @classmethod
    def _connection_id(cls, value: str) -> str:
        if not naming.CONN_ID_RE.fullmatch(value):
            raise _fail(
                f"load connection id {value!r} is not valid",
                "use letters, digits, underscores, dots and hyphens",
            )
        return value

    @field_validator("target_schema")
    @classmethod
    def _schema(cls, value: str) -> str:
        return _ident(value, "target schema")

    @field_validator("target_table")
    @classmethod
    def _table(cls, value: str | None) -> str | None:
        if value is None:
            return value
        _ident(value, "target table")
        if len(value) > MAX_TARGET_TABLE_LENGTH:
            raise _fail(
                f"target table {value!r} has {len(value)} characters, "
                f"the limit is {MAX_TARGET_TABLE_LENGTH}",
                "set a shorter load.target_table",
            )
        return value

    @field_validator("merge_keys", mode="before")
    @classmethod
    def _merge_keys_list(cls, value: Any) -> Any:
        return (value,) if isinstance(value, str) else value

    @field_validator("merge_keys")
    @classmethod
    def _merge_keys(cls, value: tuple[str, ...] | None) -> tuple[str, ...] | None:
        if value is None:
            return None
        if not value:
            raise _fail("load.merge_keys is empty", "list at least one column")
        if len(set(value)) != len(value):
            raise _fail("load.merge_keys has duplicate columns", "list each key column once")
        for key in value:
            _not_reserved(_ident(key, "merge key column"), "merge key column")
        return value

    @field_validator("max_reject_ratio")
    @classmethod
    def _ratio(cls, value: float) -> float:
        if not 0.0 <= value <= 1.0:
            raise _fail(f"max_reject_ratio is {value}, allowed 0 to 1", "use a value from 0 to 1")
        return value


class Landing(_Model):
    root: str = "/opt/airflow/data/landing"

    @field_validator("root")
    @classmethod
    def _root(cls, value: str) -> str:
        if not value.strip():
            raise _fail("landing.root is empty", "set a directory path")
        return value


class Schedule(_Model):
    cron: str | None = "@daily"
    start_date: date = date(2026, 1, 1)
    retries: int = 3
    retry_delay_minutes: int = 5
    tags: tuple[str, ...] = ()

    @field_validator("cron")
    @classmethod
    def _cron(cls, value: str | None) -> str | None:
        if not is_valid_schedule(value):
            raise _fail(
                f"schedule.cron {value!r} is not a valid schedule",
                "use null, one of " + " ".join(sorted(PRESETS)) + ", or five cron fields",
            )
        return value

    @field_validator("retries")
    @classmethod
    def _retries(cls, value: int) -> int:
        return _in_range(value, 0, 10, "schedule.retries")

    @field_validator("retry_delay_minutes")
    @classmethod
    def _delay(cls, value: int) -> int:
        return _in_range(value, 1, 1440, "schedule.retry_delay_minutes")

    @field_validator("tags")
    @classmethod
    def _tags(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        for tag in value:
            if not 1 <= len(tag) <= 100:
                raise _fail(f"tag {tag!r} has {len(tag)} characters", "use 1 to 100 characters")
        return value


class Metadata(_Model):
    owner: str = "data-engineering"
    description: str | None = None

    @field_validator("owner")
    @classmethod
    def _owner(cls, value: str) -> str:
        if not value.strip():
            raise _fail("metadata.owner is empty", "set an owner name")
        return value


class Config(_Model):
    version: Literal[1]
    source: Source
    extract: ExtractConfig
    load: LoadConfig = Field(default_factory=LoadConfig)
    landing: Landing = Field(default_factory=Landing)
    schedule: Schedule = Field(default_factory=Schedule)
    metadata: Metadata = Field(default_factory=Metadata)

    @model_validator(mode="before")
    @classmethod
    def _resolve(cls, data: Any) -> Any:
        """Default null sections, then fill write_mode and target_table (dicts or instances)."""
        if not isinstance(data, dict):
            return data
        data = {k: v for k, v in data.items() if not (k in _OPTIONAL_SECTIONS and v is None)}
        source, extract, load = data.get("source"), data.get("extract"), data.get("load", {})
        if isinstance(load, LoadConfig):
            load = load.model_dump()
        if not isinstance(load, dict):
            return data  # pydantic reports the bad section
        load = dict(load)
        pattern = _attr(extract, "pattern")
        if (
            load.get("write_mode") is None
            and isinstance(pattern, str)
            and pattern in _DEFAULT_WRITE_MODE
        ):
            load["write_mode"] = _DEFAULT_WRITE_MODE[pattern]
        keys = _attr(extract, "primary_key")
        if load.get("merge_keys") is None and load.get("write_mode") == WriteMode.MERGE and keys:
            load["merge_keys"] = [keys] if isinstance(keys, str) else list(keys)
        name, table = _attr(source, "name"), _attr(extract, "table")
        if load.get("target_table") is None and isinstance(name, str) and isinstance(table, str):
            load["target_table"] = naming.bronze_table_name(name, table)
        return {**data, "load": load}

    @model_validator(mode="after")
    def _write_mode_allowed(self) -> Self:
        allowed = _ALLOWED_WRITE_MODES[self.extract.pattern]
        if self.load.write_mode not in allowed:
            raise _fail(
                f"load.write_mode {self.load.write_mode} is not allowed for pattern "
                f"{self.extract.pattern.value!r}",
                "use one of " + ", ".join(m.value for m in allowed),
            )
        cdc = self.extract.cdc
        if (
            cdc is not None
            and cdc.mechanism is CdcMechanism.ROWVERSION
            and self.source.source_type is not SourceType.MSSQL
        ):
            raise _fail(
                "cdc mechanism rowversion needs source_type mssql",
                "set source.source_type to mssql or use mechanism generic",
            )
        self._check_merge_keys()
        return self

    def _check_merge_keys(self) -> None:
        keys = self.load.merge_keys
        if self.load.write_mode is not WriteMode.MERGE:
            if keys is not None:
                raise _fail(
                    f"load.merge_keys is not allowed when write_mode is {self.load.write_mode}",
                    "remove merge_keys or use write_mode merge",
                )
            return
        columns = self.extract.columns
        if columns is None:
            return
        names = {c.name for c in columns}
        missing = [k for k in keys or () if k not in names]
        if missing:
            raise _fail(
                f"merge key column(s) {missing} are not in extract.columns",
                "add them to columns or fix load.merge_keys",
            )

    @property
    def dag_id(self) -> str:
        """``{source}__{table}__{pattern}``, also the config file stem."""
        return naming.dag_id(self.source.name, self.extract.table, self.extract.pattern.value)
