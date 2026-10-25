"""Enumerations used by the config models."""

from __future__ import annotations

from enum import StrEnum


class SourceType(StrEnum):
    RDBMS = "rdbms"
    MSSQL = "mssql"


class Pattern(StrEnum):
    FULLSCAN = "fullscan"
    INCREMENTAL = "incremental"
    CDC = "cdc"


class WriteMode(StrEnum):
    OVERWRITE = "overwrite"
    APPEND = "append"
    MERGE = "merge"


class ColumnType(StrEnum):
    INTEGER = "integer"
    NUMERIC = "numeric"
    FLOAT = "float"
    BOOLEAN = "boolean"
    TEXT = "text"
    DATE = "date"
    TIMESTAMP = "timestamp"
    TIMESTAMPTZ = "timestamptz"


class WatermarkType(StrEnum):
    DATETIME = "datetime"
    INTEGER = "integer"


class CdcMechanism(StrEnum):
    GENERIC = "generic"
    ROWVERSION = "rowversion"


class SequenceType(StrEnum):
    INTEGER = "integer"
    DATETIME = "datetime"
