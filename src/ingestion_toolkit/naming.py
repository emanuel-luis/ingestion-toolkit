"""Naming rules: identifiers, DAG ids and bronze table names."""

from __future__ import annotations

import re

SEGMENT_RE = re.compile(r"^[a-z][a-z0-9]*(_[a-z0-9]+)*$")
IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
CONN_ID_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.-]*$")


def dag_id(source: str, table: str, pattern: str) -> str:
    """Return ``{source}__{table}__{pattern}`` with the table lowercased.

    Every part must be a SEGMENT (no ``__``), so the id can be split back unambiguously.
    """
    parts = (source, table.lower(), pattern)
    for part in parts:
        if not SEGMENT_RE.fullmatch(part):
            raise ValueError(f"{part!r} is not a valid name segment")
    return "__".join(parts)


config_stem = dag_id  # the config file name is the DAG id


def split_dag_id(value: str) -> tuple[str, str, str]:
    """Inverse of ``dag_id``."""
    parts = value.split("__")
    if len(parts) != 3 or not all(SEGMENT_RE.fullmatch(p) for p in parts):
        raise ValueError(f"{value!r} is not a valid dag_id")
    return parts[0], parts[1], parts[2]


def bronze_table_name(source: str, table: str) -> str:
    """Default bronze table: ``{source}__{table}``."""
    return f"{source}__{table.lower()}"
