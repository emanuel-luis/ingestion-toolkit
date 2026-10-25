"""Run the pure helpers of a rendered DAG without Airflow (imports and the DAG block skipped)."""

from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import Any

import pytest

from ingestion_toolkit.config_io import read_config
from ingestion_toolkit.renderer import Renderer

ROOT = Path(__file__).resolve().parents[1]
STDLIB_IMPORTS = """
import json, math, re, shutil
from contextlib import closing
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID
"""


class FakeDataError(Exception):
    """Stands in for psycopg2.DataError."""


class FakeFailure(Exception):
    """Stands in for AirflowFailException."""


def load(example: str) -> dict[str, Any]:
    path = ROOT / "examples" / example
    tree = ast.parse(Renderer().render(read_config(path), Path(path.name)))
    keep = [n for n in tree.body if not isinstance(n, ast.With | ast.Import | ast.ImportFrom)]
    namespace: dict[str, Any] = {
        "task": lambda func: func,
        "DataError": FakeDataError,
        "AirflowFailException": FakeFailure,
        "SQLExecuteQueryOperator": None,
        "PostgresHook": None,
        "BaseHook": None,
        "get_current_context": None,
    }
    exec(STDLIB_IMPORTS, namespace)  # noqa: S102
    exec(compile(ast.Module(keep, []), example, "exec"), namespace)  # noqa: S102
    return namespace


@pytest.fixture(scope="module")
def products() -> dict[str, Any]:
    return load("postgres/erp__products__cdc.yml")


def test_text_with_nul_is_a_contract_reject(products: dict[str, Any]) -> None:
    specs = products["_column_specs"](["id"])
    values, reason = products["_validate_row"](
        {"id": 1, "name": "a\x00b", "category": None, "price": None, "version_stamp": 1}, specs
    )
    assert values is None and reason is not None
    assert "name" in reason and "\x00" not in reason


def test_reject_payload_never_holds_the_nul_escape(products: dict[str, Any]) -> None:
    row = {"id": 1, "name": "a\x00b", "note": "literal \\u0000 text", "price": 2}
    payload = products["_reject_payload"](row)
    assert "\\u0000" not in payload.replace("\\\\u0000", "")  # only the escaped backslash form
    parsed = json.loads(payload)
    assert parsed["name"] == "a\\x00b"
    assert parsed["note"] == "literal \\u0000 text"


def test_data_error_becomes_a_quality_error(products: dict[str, Any]) -> None:
    class Cursor:
        def executemany(self, statement: str, batch: list[tuple]) -> None:
            raise FakeDataError("numeric field overflow\nDETAIL: precision 5")

    with pytest.raises(products["QualityError"]) as info:
        products["_execute_batch"](Cursor(), "INSERT", [(1,), (2,)])
    assert "numeric field overflow" in str(info.value)
    assert "DETAIL" not in str(info.value)  # no row values leak into the message
    assert isinstance(info.value.__cause__, FakeDataError)


def test_other_database_errors_still_propagate(products: dict[str, Any]) -> None:
    class Cursor:
        def executemany(self, statement: str, batch: list[tuple]) -> None:
            raise RuntimeError("connection lost")

    with pytest.raises(RuntimeError):
        products["_execute_batch"](Cursor(), "INSERT", [(1,)])


def test_merge_sql(products: dict[str, Any]) -> None:
    specs = products["_column_specs"](["id"])
    assert products["_create_sql"](specs).endswith(
        '"_ingest_deleted" boolean NOT NULL DEFAULT false, PRIMARY KEY ("id"))'
    )
    insert = products["_insert_sql"](specs)
    assert insert.startswith('INSERT INTO "bronze"."erp__products" AS t (')
    assert '"_ingest_run_id", "_ingest_deleted", "_ingest_loaded_at") VALUES' in insert
    assert 'ON CONFLICT ("id") DO UPDATE SET "name" = EXCLUDED."name"' in insert
    assert '"id" = EXCLUDED."id"' not in insert
    assert insert.endswith('WHERE t."version_stamp"::bigint <= EXCLUDED."version_stamp"::bigint')


def test_no_delete_flag_without_an_operation_column(products: dict[str, Any]) -> None:
    assert products["_extra_values"]({"id": 1}) == (False,)


def test_rowversion_watermark_is_fixed_width_uppercase_hex() -> None:
    ns = load("mssql/erp_mssql__products__cdc.yml")
    assert ns["_watermark_text"]("rv", b"\x00\x00\x00\x00\x00\x00\x0f\xa1") == "0x0000000000000FA1"
    assert ns["_bound_value"]("0x0000000000000FA1") == "0x0000000000000FA1"
    assert ns["_jsonable"]("rv", b"\x00\x0a") == "0x000A"


def test_append_has_no_merge_clause() -> None:
    ns = load("postgres/erp__customers__incremental.yml")
    assert ns["_conflict_sql"]([("id", "integer", False)]) == ""
    assert ns["_extra_values"]({}) == ()
