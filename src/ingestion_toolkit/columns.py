"""The ``--columns`` mini-language: ``id:integer!,total:numeric`` (``!`` means not null)."""

from __future__ import annotations

from collections.abc import Iterable

from ingestion_toolkit.models.config import Column
from ingestion_toolkit.models.enums import ColumnType


def parse_columns_spec(spec: str) -> list[Column]:
    """Parse ``name:type[!]`` items separated by commas. Raises ValueError on bad syntax."""
    columns: list[Column] = []
    for item in spec.split(","):
        name, colon, type_text = item.strip().partition(":")
        not_null = type_text.endswith("!")
        type_text = type_text.removesuffix("!")
        if not colon or not name:
            raise ValueError(f"column {item.strip()!r} must look like name:type")
        try:
            column_type = ColumnType(type_text)
        except ValueError:
            allowed = ", ".join(t.value for t in ColumnType)
            raise ValueError(f"unknown column type {type_text!r}, use one of {allowed}") from None
        columns.append(Column(name=name, type=column_type, nullable=not not_null))
    return columns


def format_columns_spec(columns: Iterable[Column]) -> str:
    """Inverse of ``parse_columns_spec``."""
    return ",".join(f"{c.name}:{c.type.value}{'' if c.nullable else '!'}" for c in columns)
