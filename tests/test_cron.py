from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from ingestion_toolkit.cron import PRESETS, is_valid_schedule

_RANGES = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 7)]


@pytest.mark.parametrize("value", [None, *sorted(PRESETS), "*/5 * * * *", "0 0 1 jan-mar *"])
def test_valid(value: str | None) -> None:
    assert is_valid_schedule(value)


@pytest.mark.parametrize(
    "value", ["", "@never", "* * * *", "* * * * * *", "*/0 * * * *", "5-1 * * * *", "a * * * *"]
)
def test_invalid(value: str) -> None:
    assert not is_valid_schedule(value)


@given(st.data())
def test_in_range_fields_are_valid(data: st.DataObject) -> None:
    fields = [str(data.draw(st.integers(lo, hi))) for lo, hi in _RANGES]
    assert is_valid_schedule(" ".join(fields))


@given(st.integers(0, 4), st.data())
def test_out_of_range_field_is_invalid(index: int, data: st.DataObject) -> None:
    _, hi = _RANGES[index]
    fields = [str(r[0]) for r in _RANGES]
    fields[index] = str(
        data.draw(st.one_of(st.integers(hi + 1, hi + 1000), st.integers(-1000, -1)))
    )
    assert not is_valid_schedule(" ".join(fields))
