from __future__ import annotations

import copy
from typing import Any

import pytest
from hypothesis import given
from pydantic import ValidationError

from ingestion_toolkit.models.config import Config
from ingestion_toolkit.models.enums import WriteMode
from tests.strategies import config_dicts


def base(**extract: Any) -> dict[str, Any]:
    return {
        "version": 1,
        "source": {"name": "erp", "database": "erp"},
        "extract": {"table": "orders", "primary_key": "id", "pattern": "fullscan", **extract},
    }


def incremental(**watermark: Any) -> dict[str, Any]:
    data = base(pattern="incremental", watermark={"column": "updated_at", **watermark})
    return data


def rejected(data: dict[str, Any], *, loc: str, text: str) -> None:
    with pytest.raises(ValidationError) as info:
        Config.model_validate(data)
    found = [(".".join(map(str, e["loc"])), e["msg"]) for e in info.value.errors()]
    assert any(loc == fl and text in msg for fl, msg in found), found
    assert all("Fix:" in msg for fl, msg in found if "Value error" in msg)


def test_defaults_resolved() -> None:
    config = Config.model_validate(base())
    assert config.source.connection_id == "erp"
    assert config.source.schema_name == "public"
    assert config.extract.primary_key == ("id",)
    assert config.load.write_mode is WriteMode.OVERWRITE
    assert config.load.target_table == "erp__orders"
    assert config.schedule.start_date.isoformat() == "2026-01-01"
    assert config.dag_id == "erp__orders__fullscan"


def test_incremental_defaults_to_append() -> None:
    assert Config.model_validate(incremental()).load.write_mode is WriteMode.APPEND


def test_mssql_schema_default() -> None:
    data = base()
    data["source"]["source_type"] = "mssql"
    assert Config.model_validate(data).source.schema_name == "dbo"


def test_frozen_and_extra_forbidden() -> None:
    config = Config.model_validate(base())
    with pytest.raises(ValidationError):
        config.version = 1  # type: ignore[misc]
    data = base()
    data["bogus"] = 1
    rejected(data, loc="bogus", text="Extra inputs")


def test_version_must_be_1() -> None:
    data = base()
    data["version"] = 2
    with pytest.raises(ValidationError):
        Config.model_validate(data)


@pytest.mark.parametrize("name", ["Erp", "a__b", "_a", "a_", "1a"])
def test_bad_source_name(name: str) -> None:
    data = base()
    data["source"]["name"] = name
    rejected(data, loc="source.name", text="not a valid segment")


def test_bad_identifiers() -> None:
    data = base()
    data["source"]["database"] = "bad-name"
    rejected(data, loc="source.database", text="not a valid identifier")
    rejected(base(table="a b"), loc="extract.table", text="not a valid identifier")
    rejected(base(table="Bad__Name"), loc="extract.table", text="DAG segment")
    rejected(base(primary_key=["1x"]), loc="extract", text="primary key column")


def test_connection_id_pattern() -> None:
    data = base()
    data["load"] = {"connection_id": "-bad"}
    rejected(data, loc="load.connection_id", text="not valid")


def test_target_table_length() -> None:
    data = base()
    data["load"] = {"target_table": "t" * 60}
    rejected(data, loc="load.target_table", text="limit is 59")
    data["load"] = {"target_table": "t" * 59}
    Config.model_validate(data)


def test_primary_key_rules() -> None:
    rejected(base(primary_key=[]), loc="extract", text="primary_key is empty")
    rejected(base(primary_key=["id", "id"]), loc="extract", text="duplicate")


def test_columns_rules() -> None:
    cols = [{"name": "id", "type": "integer"}]
    rejected(base(columns=[]), loc="extract", text="columns is empty")
    rejected(base(columns=cols + cols), loc="extract", text="duplicate names")
    rejected(
        base(columns=[{"name": "x", "type": "text"}]), loc="extract", text="not in extract.columns"
    )
    rejected(
        base(columns=[{"name": "_ingest_run_id", "type": "text"}, *cols]),
        loc="extract.columns.0.name",
        text="reserved prefix",
    )
    rejected(
        base(columns=[{"name": "id", "type": "uuid"}]), loc="extract.columns.0.type", text="Input"
    )


def test_primary_key_columns_forced_not_null() -> None:
    config = Config.model_validate(
        base(columns=[{"name": "id", "type": "integer"}, {"name": "n", "type": "text"}])
    )
    assert [c.nullable for c in config.extract.columns or ()] == [False, True]


def test_watermark_required_and_forbidden() -> None:
    data = base(pattern="incremental")
    rejected(data, loc="extract", text="watermark is required")
    rejected(base(watermark={"column": "c"}), loc="extract", text="watermark is not allowed")


def test_watermark_type_compatibility() -> None:
    cols = [{"name": "id", "type": "integer"}, {"name": "updated_at", "type": "text"}]
    data = incremental()
    data["extract"]["columns"] = cols
    rejected(data, loc="extract", text="does not fit column type")
    data["extract"]["columns"] = [cols[0]]
    rejected(data, loc="extract", text="not in extract.columns")
    data["extract"]["columns"] = [cols[0], {"name": "updated_at", "type": "date"}]
    Config.model_validate(data)
    data = incremental(type="integer")
    data["extract"]["columns"] = [cols[0], {"name": "updated_at", "type": "timestamp"}]
    rejected(data, loc="extract", text="does not fit column type")


@pytest.mark.parametrize(
    ("kind", "value", "ok"),
    [
        ("datetime", "2026-01-01T00:00:00", True),
        ("datetime", "not a date", False),
        ("datetime", 5, False),
        ("integer", 5, True),
        ("integer", "12", True),
        ("integer", "x", False),
        ("integer", True, False),
    ],
)
def test_initial_value_parses(kind: str, value: Any, ok: bool) -> None:
    data = incremental(type=kind, initial_value=value)
    if ok:
        Config.model_validate(data)
    else:
        with pytest.raises(ValidationError):
            Config.model_validate(data)


def test_write_mode_allowed_per_pattern() -> None:
    data = base()
    data["load"] = {"write_mode": "merge"}
    rejected(data, loc="", text="not allowed for pattern")
    data = incremental()
    data["load"] = {"write_mode": "overwrite"}
    rejected(data, loc="", text="not allowed for pattern")
    data["load"] = {"write_mode": "merge"}
    Config.model_validate(data)


@pytest.mark.parametrize("ratio", [-0.1, 1.5])
def test_reject_ratio_range(ratio: float) -> None:
    data = base()
    data["load"] = {"max_reject_ratio": ratio}
    rejected(data, loc="load.max_reject_ratio", text="allowed 0 to 1")


def test_schedule_rules() -> None:
    for field, bad in [
        ("cron", "61 * * * *"),
        ("retries", 11),
        ("retry_delay_minutes", 0),
        ("retry_delay_minutes", 1441),
        ("tags", [""]),
        ("tags", ["x" * 101]),
    ]:
        data = base()
        data["schedule"] = {field: bad}
        with pytest.raises(ValidationError):
            Config.model_validate(data)
    data = base()
    data["schedule"] = {
        "cron": None,
        "retries": 0,
        "retry_delay_minutes": 1440,
        "tags": ["x" * 100],
    }
    Config.model_validate(data)


def test_load_none_section_is_defaulted() -> None:
    data = base()
    data["load"] = None
    assert Config.model_validate(data).load.target_table == "erp__orders"


@given(config_dicts())
def test_round_trip(data: dict[str, Any]) -> None:
    config = Config.model_validate(copy.deepcopy(data))
    assert Config.model_validate(config.model_dump(mode="json")) == config
