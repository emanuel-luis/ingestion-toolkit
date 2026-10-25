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
    pattern = draw(st.sampled_from(["fullscan", "incremental"]))
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
    if with_columns:
        columns = []
        for name in names:
            if pattern == "incremental" and name == names[1]:
                type_ = draw(st.sampled_from(_TIMES)) if watermark_type == "datetime" else "integer"
            else:
                type_ = draw(st.sampled_from(_OTHER_TYPES))
            columns.append({"name": name, "type": type_, "nullable": draw(st.booleans())})
        extract["columns"] = columns
    modes = ["overwrite", "append"] if pattern == "fullscan" else ["append", "merge"]
    return {
        "version": 1,
        "source": {"name": draw(segments), "database": "db", "schema_name": "s"},
        "extract": extract,
        "load": {
            "write_mode": draw(st.sampled_from(modes)),
            "max_reject_ratio": draw(st.sampled_from([0.0, 0.01, 0.5, 1.0])),
        },
        "schedule": {"cron": draw(st.sampled_from([None, "@daily", "*/15 0-6 * jan-mar mon"]))},
    }
