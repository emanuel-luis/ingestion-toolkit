from __future__ import annotations

import ast
import os
from pathlib import Path
from typing import Any

import pytest

from ingestion_toolkit.errors import ConfigFailure, RenderError
from ingestion_toolkit.models.config import Config
from ingestion_toolkit.renderer import (
    Renderer,
    build_dag_context,
    py_literal,
    render_all,
    write_dag,
)


def make(pattern: str = "fullscan", **extra: Any) -> Config:
    extract: dict[str, Any] = {"table": "orders", "primary_key": "id", "pattern": pattern}
    if pattern == "incremental":
        extract["watermark"] = {"column": "updated_at", "initial_value": "2026-01-01T00:00:00"}
    return Config.model_validate(
        {"version": 1, "source": {"name": "erp", "database": "erp"}, "extract": extract, **extra}
    )


def test_fullscan_context() -> None:
    ctx = build_dag_context(make(), "erp__orders__fullscan.yml")
    assert ctx.dag_id == "erp__orders__fullscan"
    assert ctx.extract.template == "tasks/extract/fullscan.py.j2"
    assert ctx.load.template == "tasks/load/overwrite.py.j2"
    assert ctx.helper_templates[-1] == "helpers/state_none.py.j2"
    assert ctx.extract.bounded_sql is None
    assert ctx.load.columns is None
    assert ctx.landing_dir == "/opt/airflow/data/landing/erp/orders"
    assert ctx.tags == ("erp", "fullscan", "ingestion-toolkit")


def test_incremental_context() -> None:
    ctx = build_dag_context(make("incremental"), "x.yml")
    assert ctx.extract.template == "tasks/extract/windowed.py.j2"
    assert ctx.load.template == "tasks/load/append.py.j2"
    assert ctx.helper_templates[-1] == "helpers/state.py.j2"
    assert ctx.extract.bound_column == "updated_at"
    assert ctx.extract.bounded_sql is not None and "{ph}" in ctx.extract.bounded_sql
    assert ctx.extract.initial_value == "2026-01-01T00:00:00"


def test_tags_are_sorted_user_tags_then_fixed_ones() -> None:
    ctx = build_dag_context(make(schedule={"tags": ["zeta", "alpha", "erp"]}), "x.yml")
    assert ctx.tags == ("alpha", "erp", "zeta", "fullscan", "ingestion-toolkit")


def test_context_is_deterministic() -> None:
    assert build_dag_context(make(), "x.yml") == build_dag_context(make(), "x.yml")


def make_cdc(mechanism: str = "generic", **cdc: Any) -> Config:
    extra: dict[str, Any] = {"source_type": "mssql"} if mechanism == "rowversion" else {}
    return Config.model_validate(
        {
            "version": 1,
            "source": {"name": "erp", "database": "erp", **extra},
            "extract": {
                "table": "orders",
                "primary_key": "id",
                "pattern": "cdc",
                "cdc": {"mechanism": mechanism, "sequence_column": "seq", **cdc},
            },
        }
    )


def test_incremental_merge_context() -> None:
    ctx = build_dag_context(make("incremental", load={"write_mode": "merge"}), "x.yml")
    assert ctx.load.template == "tasks/load/merge.py.j2"
    assert ctx.load.merge_keys == ("id",)
    assert ctx.load.required_columns == ("id", "updated_at")
    assert ctx.load.conflict_target_sql == '("id")'
    assert (
        ctx.load.merge_guard_sql
        == 't."updated_at"::timestamptz <= EXCLUDED."updated_at"::timestamptz'
    )
    assert ctx.load.prepare_sql == "" and ctx.load.duplicate_sql == ""
    assert ctx.load.extra_ddl[0] == '"_ingest_deleted" boolean NOT NULL DEFAULT false'
    assert ctx.load.extra_ddl[1] == 'PRIMARY KEY ("id")'


def test_generic_cdc_context() -> None:
    config = make_cdc(
        change_table="orders_changes", operation_column="op", delete_values=["D", "X"]
    )
    ctx = build_dag_context(config, "x.yml")
    assert ctx.extract.template == "tasks/extract/windowed.py.j2"
    assert ctx.helper_templates[-1] == "helpers/state.py.j2"
    assert ctx.extract.bound_type == "integer" and ctx.extract.initial_value is None
    assert ctx.extract.bounded_sql == (
        'SELECT * FROM "erp"."public"."orders_changes" WHERE "seq" > {ph} ORDER BY "seq"'
    )
    assert ctx.extract.extract_sql.endswith('ORDER BY "seq"')
    assert ctx.load.operation_column == "op" and ctx.load.delete_values == ("D", "X")
    assert ctx.load.merge_guard_sql == 't."seq"::bigint <= EXCLUDED."seq"::bigint'


def test_rowversion_context() -> None:
    ctx = build_dag_context(make_cdc("rowversion"), "x.yml")
    assert ctx.extract.bound_type == "rowversion"
    assert ctx.extract.bounded_sql is not None
    assert "> CONVERT(binary(8), {ph}, 1) AND" in ctx.extract.bounded_sql
    assert ctx.extract.bounded_sql.count("MIN_ACTIVE_ROWVERSION()") == 1
    assert 'WHERE "seq" < MIN_ACTIVE_ROWVERSION()' in ctx.extract.extract_sql
    assert ctx.load.merge_guard_sql == 't."seq" COLLATE "C" <= EXCLUDED."seq" COLLATE "C"'


def test_merge_render_is_valid_python() -> None:
    for config in (
        make_cdc(),
        make_cdc("rowversion"),
        make("incremental", load={"write_mode": "merge"}),
    ):
        text = Renderer().render(config, Path("x.yml"))
        ast.parse(text)
        assert "ON CONFLICT" in text and "_ingest_deleted" in text


def test_render_is_valid_python_and_deterministic() -> None:
    renderer = Renderer()
    text = renderer.render(make(), Path("erp__orders__fullscan.yml"))
    ast.parse(text)
    assert text == renderer.render(make(), Path("erp__orders__fullscan.yml"))
    assert text.startswith(
        "# Generated by ingestion-toolkit from erp__orders__fullscan.yml. Do not edit: "
        "change the config and run `ingest render`.\n"
    )
    assert text.endswith("\n") and not text.endswith("\n\n")


def test_hostile_values_stay_inside_their_literals() -> None:
    hostile = 'x\'"\n"""; import os #'
    config = make(metadata={"description": hostile, "owner": "o'wner"})
    tree = ast.parse(Renderer().render(config, Path("x.yml")))
    assigned = {
        t.id: ast.literal_eval(n.value)
        for n in tree.body
        if isinstance(n, ast.Assign)
        for t in n.targets
        if isinstance(t, ast.Name) and t.id in {"DESCRIPTION", "OWNER"}
    }
    assert assigned == {"DESCRIPTION": hostile, "OWNER": "o'wner"}


def test_override_directory_replaces_one_file_and_falls_back(tmp_path: Path) -> None:
    (tmp_path / "tasks").mkdir()
    (tmp_path / "tasks" / "ensure_tables.py.j2").write_text(
        "    # custom ensure for {{ dag.dag_id }}\n"
        "    ensure_tables = SQLExecuteQueryOperator(\n"
        '        task_id="ensure_tables", conn_id=TARGET_CONN_ID, sql=ENSURE_TABLES_SQL\n'
        "    )\n",
        encoding="utf-8",
    )
    text = Renderer(tmp_path).render(make(), Path("x.yml"))
    assert "# custom ensure for erp__orders__fullscan" in text
    assert "def load_bronze" in text  # built-in templates still used


def test_undefined_variable_in_a_template_is_a_render_error(tmp_path: Path) -> None:
    (tmp_path / "constants.py.j2").write_text("X = {{ dag.nope | py }}\n", encoding="utf-8")
    with pytest.raises(RenderError) as info:
        Renderer(tmp_path).render(make(), Path("x.yml"))
    assert "nope" in info.value.error.message
    assert info.value.error.loc.startswith("template ")


def test_invalid_python_output_is_a_render_error(tmp_path: Path) -> None:
    (tmp_path / "constants.py.j2").write_text("X = (\n", encoding="utf-8")
    with pytest.raises(RenderError) as info:
        Renderer(tmp_path).render(make(), Path("x.yml"))
    assert "not valid Python" in info.value.error.message


def test_missing_template_is_a_render_error(tmp_path: Path) -> None:
    (tmp_path / "dag_base.py.j2").write_text("{% include 'nope.j2' %}", encoding="utf-8")
    with pytest.raises(RenderError) as info:
        Renderer(tmp_path).render(make(), Path("x.yml"))
    assert "nope.j2" in info.value.error.loc


def test_bulk_render_is_all_or_nothing(tmp_path: Path) -> None:
    broken = tmp_path / "templates" / "tasks" / "extract"
    broken.mkdir(parents=True)
    (broken / "windowed.py.j2").write_text("X = (\n", encoding="utf-8")  # only incremental uses it
    good = (Path("erp__orders__fullscan.yml"), make())
    bad = (Path("erp__orders__incremental.yml"), make("incremental"))
    with pytest.raises(ConfigFailure) as info:
        render_all([good, bad], tmp_path / "out", tmp_path / "templates")
    assert [e.path for e in info.value.errors] == ["erp__orders__incremental.yml"]
    assert not (tmp_path / "out").exists()


def test_dry_run_writes_nothing(tmp_path: Path) -> None:
    out = tmp_path / "out"
    paths = render_all([(Path("a.yml"), make())], out, dry_run=True)
    assert paths == [out / "erp__orders__fullscan.py"]
    assert not out.exists()


def test_write_is_atomic(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    target = write_dag("A = 1\n", tmp_path, "d")
    assert target.read_text(encoding="utf-8") == "A = 1\n"

    def boom(src: str, dst: object) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(os, "replace", boom)
    with pytest.raises(OSError, match="disk full"):
        write_dag("A = 2\n", tmp_path, "d")
    assert target.read_text(encoding="utf-8") == "A = 1\n"
    assert [p.name for p in tmp_path.iterdir()] == ["d.py"]


@pytest.mark.parametrize(
    "value",
    [
        "plain",
        'has "double" quotes',
        "it's",
        "both ' and \"",
        "line\nbreak",
        None,
        True,
        3,
        0.01,
        [],
        ["a"],
        ["a", "b"],
        [("n", "integer", False)],
        ("t",),
        {"k": "v"},
        {},
    ],
)
def test_py_literal_round_trips(value: Any) -> None:
    expected = list(value) if type(value) is tuple else value
    assert ast.literal_eval(py_literal(value)) == expected


def test_py_literal_wraps_long_strings_like_ruff() -> None:
    text = "x" * 92
    assert py_literal(text, "NAME") == f'NAME = (\n    "{text}"\n)'
    assert py_literal("short", "NAME") == 'NAME = "short"'
    long = "x" * 120
    assert py_literal(long, "NAME") == f'NAME = "{long}"'
