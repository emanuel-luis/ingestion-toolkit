"""Hypothesis strategies that build valid config dicts."""

from __future__ import annotations

from typing import Any

from hypothesis import strategies as st

from ingestion_toolkit.models.enums import ColumnType

segments = st.from_regex(r"[a-z][a-z0-9]*(_[a-z0-9]+)*", fullmatch=True).filter(
    lambda s: len(s) <= 20
)
_TIMES = (ColumnType.TIMESTAMP.value, ColumnType.TIMESTAMPTZ.value, ColumnType.DATE.value)
_OTHER_TYPES = [t.value for t in ColumnType]


@st.composite
def config_dicts(draw: st.DrawFn) -> dict[str, Any]:
    pattern = draw(st.sampled_from(["fullscan", "incremental", "cdc"]))
    names = draw(st.lists(segments, min_size=2, max_size=5, unique=True))
    key = names[0]
    extract: dict[str, Any] = {
        "table": draw(segments),
        "primary_key": [key],
        "pattern": pattern,
    }
    with_columns = draw(st.booleans())
    watermark_type = draw(st.sampled_from(["datetime", "integer"]))
    if pattern == "incremental":
        wm_column = names[1]
        extract["watermark"] = {"column": wm_column, "type": watermark_type}
        if draw(st.booleans()):
            extract["watermark"]["initial_value"] = (
                "2026-01-01T00:00:00"
                if watermark_type == "datetime"
                else draw(st.integers(-(2**63), 2**63 - 1))
            )
    source_type = "rdbms"
    seq_type = "integer"
    if pattern == "cdc":
        mechanism = draw(st.sampled_from(["generic", "rowversion"]))
        cdc: dict[str, Any] = {"mechanism": mechanism, "sequence_column": names[1]}
        if mechanism == "rowversion":
            source_type = "mssql"
        else:
            seq_type = draw(st.sampled_from(["integer", "datetime"]))
            cdc["sequence_type"] = seq_type
            if draw(st.booleans()):
                cdc["change_table"] = draw(segments)
            if draw(st.booleans()):
                cdc["operation_column"] = names[-1] if len(names) > 2 else "_op"
                cdc["delete_values"] = draw(st.sampled_from([["D"], ["D", "X"]]))
        extract["cdc"] = cdc
    if with_columns:
        columns = []
        for name in names:
            if pattern == "incremental" and name == names[1]:
                type_ = draw(st.sampled_from(_TIMES)) if watermark_type == "datetime" else "integer"
            elif pattern == "cdc" and name == names[1]:
                if cdc["mechanism"] == "rowversion":
                    type_ = "text"
                else:
                    type_ = draw(st.sampled_from(_TIMES)) if seq_type == "datetime" else "integer"
            else:
                type_ = draw(st.sampled_from(_OTHER_TYPES))
            columns.append({"name": name, "type": type_, "nullable": draw(st.booleans())})
        if pattern == "cdc" and cdc.get("operation_column") == "_op":
            columns.append({"name": "_op", "type": "text", "nullable": True})
        extract["columns"] = columns
    modes = {
        "fullscan": ["overwrite", "append"],
        "incremental": ["append", "merge"],
        "cdc": ["merge"],
    }[pattern]
    return {
        "version": 1,
        "source": {
            "name": draw(segments),
            "source_type": source_type,
            "database": "db",
            "schema_name": "s",
        },
        "extract": extract,
        "load": {
            "write_mode": draw(st.sampled_from(modes)),
            "max_reject_ratio": draw(st.sampled_from([0.0, 0.01, 0.5, 1.0])),
        },
        "schedule": {"cron": draw(st.sampled_from([None, "@daily", "*/15 0-6 * jan-mar mon"]))},
    }
