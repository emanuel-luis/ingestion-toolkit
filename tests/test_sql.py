from __future__ import annotations

from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from ingestion_toolkit import sql
from ingestion_toolkit.models.config import Config
from ingestion_toolkit.models.enums import ColumnType


def make(**extract: Any) -> Config:
    data = {
        "version": 1,
        "source": {"name": "erp", "database": "erp", "schema_name": "sales"},
        "extract": {"table": "orders", "primary_key": ["id", "line"], "pattern": "fullscan"}
        | extract,
    }
    return Config.model_validate(data)


def test_quote_ident_doubles_quotes() -> None:
    assert sql.quote_ident("id") == '"id"'
    assert sql.quote_ident('a"b') == '"a""b"'


@given(st.text())
def test_quote_ident_round_trips(name: str) -> None:
    quoted = sql.quote_ident(name)
    assert quoted[0] == quoted[-1] == '"'
    assert quoted[1:-1].replace('""', '"') == name
    assert '"' not in quoted[1:-1].replace('""', "")


def test_qualified_name() -> None:
    assert sql.qualified_name("a", "b", "c") == '"a"."b"."c"'


def test_select_without_contract_is_star() -> None:
    assert sql.select_sql(make()) == 'SELECT * FROM "erp"."sales"."orders"'


def test_select_with_contract_lists_columns() -> None:
    config = make(columns=[{"name": "id", "type": "integer"}, {"name": "line", "type": "text"}])
    assert sql.select_sql(config) == 'SELECT "id", "line" FROM "erp"."sales"."orders"'


def test_bounded_select_uses_the_placeholder_marker() -> None:
    config = make(pattern="incremental", watermark={"column": "updated_at"}, primary_key=["id"])
    assert sql.bounded_select_sql(config).endswith('WHERE "updated_at" > {ph}')


def test_bounded_select_needs_incremental_or_cdc() -> None:
    with pytest.raises(ValueError, match="incremental or cdc"):
        sql.bounded_select_sql(make())


def make_cdc(**cdc: Any) -> Config:
    return make(pattern="cdc", primary_key=["id"], cdc={"sequence_column": "seq", **cdc})


def test_cdc_generic_orders_by_the_sequence() -> None:
    config = make_cdc(change_table="orders_changes")
    base = 'SELECT * FROM "erp"."sales"."orders_changes"'
    assert sql.select_sql(config) == f'{base} ORDER BY "seq"'
    assert sql.bounded_select_sql(config) == f'{base} WHERE "seq" > {{ph}} ORDER BY "seq"'


def test_cdc_rowversion_stops_before_open_transactions() -> None:
    config = Config.model_validate(
        {
            "version": 1,
            "source": {"name": "erp", "source_type": "mssql", "database": "erp"},
            "extract": {
                "table": "orders",
                "primary_key": "id",
                "pattern": "cdc",
                "cdc": {"mechanism": "rowversion", "sequence_column": "seq"},
            },
        }
    )
    assert sql.select_sql(config).endswith('WHERE "seq" < MIN_ACTIVE_ROWVERSION() ORDER BY "seq"')
    assert sql.bounded_select_sql(config).endswith(
        'WHERE "seq" > CONVERT(binary(8), {ph}, 1) AND "seq" < MIN_ACTIVE_ROWVERSION() '
        'ORDER BY "seq"'
    )


def test_merge_sql_pieces() -> None:
    config = make_cdc()
    assert sql.merge_extra_ddl(config) == (
        '"_ingest_deleted" boolean NOT NULL DEFAULT false',
        'PRIMARY KEY ("id")',
    )
    assert sql.conflict_target_sql(config) == '("id")'
    assert sql.merge_guard_sql(config) == 't."seq"::bigint <= EXCLUDED."seq"::bigint'
    assert sql.merge_extra_ddl(make()) == ()


def test_prepare_by_write_mode() -> None:
    assert sql.prepare_sql(make()) == 'DELETE FROM "bronze"."erp__orders"'
    config = Config.model_validate(
        {
            "version": 1,
            "source": {"name": "erp", "database": "erp"},
            "extract": {"table": "orders", "primary_key": "id", "pattern": "fullscan"},
            "load": {"write_mode": "append"},
        }
    )
    assert sql.prepare_sql(config) == (
        'DELETE FROM "bronze"."erp__orders" WHERE "_ingest_run_id" = %s'
    )


def test_duplicate_sql_groups_by_every_key() -> None:
    assert 'GROUP BY "id", "line" HAVING' in sql.duplicate_sql(make())


def test_pg_types_cover_every_column_type() -> None:
    assert set(sql.PG_TYPES) == {t.value for t in ColumnType}
    assert sql.PG_TYPES["integer"] == "bigint"
