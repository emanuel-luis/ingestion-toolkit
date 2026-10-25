from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from ingestion_toolkit.columns import format_columns_spec, parse_columns_spec
from ingestion_toolkit.models.enums import ColumnType
from tests.strategies import segments

_items = st.tuples(segments, st.sampled_from(list(ColumnType)), st.sampled_from(["", "!"]))


@given(st.lists(_items, min_size=1, max_size=6, unique_by=lambda i: i[0]))
def test_format_parse_round_trip(items: list[tuple[str, ColumnType, str]]) -> None:
    spec = ",".join(f"{n}:{t.value}{bang}" for n, t, bang in items)
    assert format_columns_spec(parse_columns_spec(spec)) == spec


def test_not_null_marker() -> None:
    cols = parse_columns_spec("id:integer!, total:numeric")
    assert [(c.name, c.nullable) for c in cols] == [("id", False), ("total", True)]


@pytest.mark.parametrize("spec", ["id", "id:", ":integer", "id:bigint", "id:integer,,x:text"])
def test_bad_syntax(spec: str) -> None:
    with pytest.raises(ValueError):
        parse_columns_spec(spec)
