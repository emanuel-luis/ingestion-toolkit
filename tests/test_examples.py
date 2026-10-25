"""The documented `ingest add` commands must reproduce examples/ byte for byte."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from typer.testing import CliRunner

from ingestion_toolkit.cli.main import app

ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = ROOT / "examples" / "postgres"
COMMON = ["--source", "erp", "--database", "erp", "--schema", "sales", "--primary-key", "id"]
ORDER_COLUMNS = (
    "id:integer!,customer_id:integer,product_id:integer,quantity:integer,"
    "total:numeric,order_date:date"
)
COMMANDS = {
    "erp__orders__fullscan.yml": [
        "fullscan",
        *COMMON,
        *["--table", "orders", "--max-reject-ratio", "0.01"],
        "--columns",
        ORDER_COLUMNS,
    ],
    "erp__customers__incremental.yml": [
        "incremental",
        *COMMON,
        *["--table", "customers", "--watermark-column", "updated_at"],
        *["--watermark-type", "datetime"],
        "--columns",
        "id:integer!,name:text,email:text,city:text,created_at:timestamp,updated_at:timestamp",
    ],
}


@pytest.mark.parametrize("name", sorted(COMMANDS))
def test_command_reproduces_example(name: str, tmp_path: Path) -> None:
    args = ["add", "rdbms", *COMMANDS[name], "--output-dir", str(tmp_path)]
    result = CliRunner().invoke(app, args)
    assert result.exit_code == 0, result.stderr
    assert (tmp_path / name).read_bytes() == (EXAMPLES / name).read_bytes()


def test_examples_dir_has_only_documented_files() -> None:
    assert sorted(p.name for p in EXAMPLES.iterdir()) == sorted(COMMANDS)


def test_readme_yaml_matches_example() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    match = re.search(
        r"<!-- example:start -->\s*```yaml\n(.*?)```\s*<!-- example:end -->", readme, re.DOTALL
    )
    assert match, "README.md needs the example:start / example:end markers around a yaml block"
    expected = (EXAMPLES / "erp__orders__fullscan.yml").read_text(encoding="utf-8")
    assert match.group(1) == expected
