from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from ingestion_toolkit.cli.main import app
from ingestion_toolkit.config_io import read_config

runner = CliRunner()
BASE = ["--source", "erp", "--database", "erp", "--table", "orders", "--primary-key", "id"]


def add(group: str, *args: str, out: Path) -> tuple[int, str, str]:
    result = runner.invoke(app, ["add", group, "cdc", *args, "--output-dir", str(out)])
    return result.exit_code, result.stdout, result.stderr


def test_rdbms_cdc_options_reach_the_config(tmp_path: Path) -> None:
    code, _, err = add(
        "rdbms",
        *BASE,
        *["--sequence-column", "seq", "--sequence-type", "datetime"],
        *["--change-table", "orders_changes", "--operation-column", "op"],
        *["--delete-values", "D, X", "--merge-keys", "id,line"],
        out=tmp_path,
    )
    assert code == 0, err
    config = read_config(tmp_path / "erp__orders__cdc.yml")
    cdc = config.extract.cdc
    assert cdc is not None
    assert (cdc.sequence_type, cdc.change_table, cdc.operation_column) == (
        "datetime",
        "orders_changes",
        "op",
    )
    assert cdc.delete_values == ("D", "X")
    assert config.load.merge_keys == ("id", "line")
    assert config.load.write_mode == "merge"


def test_rdbms_cdc_needs_a_sequence_column(tmp_path: Path) -> None:
    assert add("rdbms", *BASE, out=tmp_path)[0] == 2


def test_rdbms_cdc_reports_validation_errors(tmp_path: Path) -> None:
    args = ["--sequence-column", "seq", "--operation-column", "op", "--delete-values", ","]
    code, _, err = add("rdbms", *BASE, *args, out=tmp_path)
    assert code == 1
    assert "delete_values has an empty value. Fix: " in err
    assert not list(tmp_path.iterdir())


def test_mssql_cdc_is_rowversion_with_dbo_default(tmp_path: Path) -> None:
    code, _, err = add("mssql", *BASE, "--rowversion-column", "rv", out=tmp_path)
    assert code == 0, err
    config = read_config(tmp_path / "erp__orders__cdc.yml")
    assert config.source.source_type == "mssql"
    assert config.source.schema_name == "dbo"
    assert config.extract.cdc is not None
    assert config.extract.cdc.mechanism == "rowversion"
    assert config.extract.cdc.sequence_type is None


def test_mssql_cdc_needs_a_rowversion_column(tmp_path: Path) -> None:
    assert add("mssql", *BASE, out=tmp_path)[0] == 2
