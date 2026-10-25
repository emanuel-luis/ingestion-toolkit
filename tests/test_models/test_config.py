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


def cdc(columns: list[dict[str, Any]] | None = None, **block: Any) -> dict[str, Any]:
    extra = {} if columns is None else {"columns": columns}
    return base(pattern="cdc", cdc={"sequence_column": "seq", **block}, **extra)


def test_cdc_defaults() -> None:
    config = Config.model_validate(cdc())
    assert config.load.write_mode is WriteMode.MERGE
    assert config.load.merge_keys == ("id",)
    assert config.extract.cdc is not None
    assert config.extract.cdc.sequence_type == "integer"
    assert config.extract.cdc.delete_values == ("D",)
    assert config.dag_id == "erp__orders__cdc"


def test_cdc_block_required_and_forbidden() -> None:
    rejected(base(pattern="cdc"), loc="extract", text="extract.cdc is required")
    rejected(base(cdc={"sequence_column": "seq"}), loc="extract", text="extract.cdc is not allowed")


def test_cdc_only_merges() -> None:
    data = cdc()
    data["load"] = {"write_mode": "append"}
    rejected(data, loc="", text="not allowed for pattern")


def test_rowversion_needs_mssql_and_no_sequence_type() -> None:
    data = cdc(mechanism="rowversion")
    rejected(data, loc="", text="needs source_type mssql")
    data["source"]["source_type"] = "mssql"
    config = Config.model_validate(data)
    assert config.extract.cdc is not None and config.extract.cdc.sequence_type is None
    data = cdc(mechanism="rowversion", sequence_type="integer")
    data["source"]["source_type"] = "mssql"
    rejected(data, loc="extract.cdc", text="not allowed for mechanism rowversion")


def test_operation_column_needs_delete_values() -> None:
    rejected(
        cdc(operation_column="op", delete_values=[]),
        loc="extract.cdc",
        text="delete_values is empty",
    )
    rejected(cdc(delete_values=[""]), loc="extract.cdc", text="empty value")


def test_cdc_columns_are_checked_against_the_contract() -> None:
    columns = [
        {"name": "id", "type": "integer"},
        {"name": "seq", "type": "text"},
        {"name": "op", "type": "text"},
    ]
    rejected(cdc(columns=columns), loc="extract", text="does not fit column type")
    good = [{**c, "type": "integer"} if c["name"] == "seq" else c for c in columns]
    Config.model_validate(cdc(columns=good, operation_column="op"))
    rejected(cdc(columns=good[:2], operation_column="op"), loc="extract", text="operation column")
    rejected(cdc(columns=good[:1]), loc="extract", text="sequence column")
    rowversion = cdc(mechanism="rowversion", columns=good)
    rowversion["source"]["source_type"] = "mssql"
    rejected(rowversion, loc="extract", text="mechanism rowversion does not fit")


def test_merge_keys_rules() -> None:
    data = incremental()
    data["load"] = {"write_mode": "merge", "merge_keys": ["a", "a"]}
    rejected(data, loc="load.merge_keys", text="duplicate")
    data["load"] = {"write_mode": "append", "merge_keys": ["a"]}
    rejected(data, loc="", text="merge_keys is not allowed")
    data = incremental()
    data["extract"]["columns"] = [
        {"name": "id", "type": "integer"},
        {"name": "updated_at", "type": "timestamp"},
    ]
    data["load"] = {"write_mode": "merge", "merge_keys": "other"}
    rejected(data, loc="", text="merge key column(s) ['other']")
    data["load"] = {"write_mode": "merge"}
    assert Config.model_validate(data).load.merge_keys == ("id",)


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


def test_direct_construction_forces_key_columns_not_null() -> None:
    import warnings

    from ingestion_toolkit.models.config import Column, ExtractConfig, LoadConfig, Source
    from ingestion_toolkit.models.enums import ColumnType, Pattern

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        extract = ExtractConfig(
            table="orders",
            primary_key=("id",),
            pattern=Pattern.FULLSCAN,
            columns=(Column(name="id", type=ColumnType.INTEGER),),
        )
        assert extract.columns is not None
        assert extract.columns[0].nullable is False
        config = Config(
            version=1,
            source=Source(name="erp", connection_id="erp", database="erp", schema_name="public"),
            extract=extract,
        )
        assert config.load.write_mode is WriteMode.OVERWRITE
        assert config.load.target_table == "erp__orders"
        again = Config(
            version=1,
            source=Source(name="erp", connection_id="erp", database="erp", schema_name="public"),
            extract=extract,
            load=LoadConfig(max_reject_ratio=0.5),
        )
        assert again.load.write_mode is WriteMode.OVERWRITE
        assert again.load.max_reject_ratio == 0.5


def test_initial_value_is_canonical_text() -> None:
    def value(kind: str, raw: Any) -> str | None:
        return Config.model_validate(
            incremental(type=kind, initial_value=raw)
        ).extract.watermark.initial_value  # type: ignore[union-attr]

    assert value("datetime", "2026-01-01") == "2026-01-01T00:00:00"
    assert value("datetime", "2026-01-01T00:00:00") == "2026-01-01T00:00:00"
    assert value("datetime", __import__("datetime").date(2026, 1, 1)) == "2026-01-01T00:00:00"
    assert value("integer", 5) == value("integer", "5") == value("integer", "+05") == "5"
    assert value("integer", None) is None


@pytest.mark.parametrize("bad", [[], "", 0, "x", ["a"]])
def test_non_mapping_section_is_an_error_with_hint(bad: Any) -> None:
    from ingestion_toolkit.config_io import validation_errors

    for section in ("load", "landing", "schedule", "metadata"):
        data = base()
        data[section] = bad
        with pytest.raises(ValidationError) as info:
            Config.model_validate(data)
        errors = validation_errors(__import__("pathlib").Path("x.yml"), info.value)
        assert any(e.loc == section for e in errors)
        assert all(e.hint and "README" not in e.hint for e in errors if e.loc == section)


def test_null_sections_use_defaults() -> None:
    data = base()
    data.update(load=None, landing=None, schedule=None, metadata=None)
    config = Config.model_validate(data)
    assert config.schedule.cron == "@daily"
    assert config.load.write_mode is WriteMode.OVERWRITE


def test_reserved_prefix_without_columns() -> None:
    rejected(base(primary_key=["_ingest_id"]), loc="extract", text="reserved prefix")
    data = incremental()
    data["extract"]["watermark"]["column"] = "_ingest_loaded_at"
    rejected(data, loc="extract.watermark.column", text="reserved prefix")


@pytest.mark.parametrize("value", [2**63, -(2**63) - 1])
def test_integer_initial_value_beyond_bigint_is_rejected(value: int) -> None:
    rejected(
        incremental(type="integer", initial_value=value),
        loc="extract.watermark",
        text="outside the bigint range",
    )


@pytest.mark.parametrize("value", [2**63 - 1, -(2**63)])
def test_integer_initial_value_at_bigint_edge_is_accepted(value: int) -> None:
    Config.model_validate(incremental(type="integer", initial_value=value))
