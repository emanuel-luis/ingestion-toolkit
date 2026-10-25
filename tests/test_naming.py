from __future__ import annotations

import pytest
from hypothesis import given

from ingestion_toolkit import naming
from tests.strategies import segments


@given(segments, segments, segments)
def test_dag_id_round_trip(source: str, table: str, pattern: str) -> None:
    assert naming.split_dag_id(naming.dag_id(source, table.upper(), pattern)) == (
        source,
        table,
        pattern,
    )


@pytest.mark.parametrize("bad", ["a__b", "_a", "a_", "A", "1a", ""])
def test_double_underscore_and_bad_segments_rejected(bad: str) -> None:
    with pytest.raises(ValueError, match="valid"):
        naming.dag_id(bad, "t", "fullscan")


def test_split_rejects_wrong_shape() -> None:
    with pytest.raises(ValueError, match="valid"):
        naming.split_dag_id("a__b")


def test_bronze_table_name() -> None:
    assert naming.bronze_table_name("erp", "Orders") == "erp__orders"
