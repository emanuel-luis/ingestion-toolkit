from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from ingestion_toolkit.cli.main import app
from ingestion_toolkit.config_io import read_config

runner = CliRunner()
BASE = ["--source", "erp", "--database", "erp", "--table", "orders", "--primary-key", "id"]


def add(*args: str, out: Path) -> tuple[int, str, str]:
    result = runner.invoke(app, ["add", "rdbms", *args, "--output-dir", str(out)])
    return result.exit_code, result.stdout, result.stderr


def test_fullscan_writes_validated_yaml(tmp_path: Path) -> None:
    code, stdout, _ = add("fullscan", *BASE, out=tmp_path)
    assert code == 0
    assert "erp__orders__fullscan.yml" in stdout
    config = read_config(tmp_path / "erp__orders__fullscan.yml")
    assert config.load.write_mode == "overwrite"
    assert config.load.target_table == "erp__orders"


def test_output_is_deterministic(tmp_path: Path) -> None:
    add("fullscan", *BASE, out=tmp_path / "a")
    add("fullscan", *BASE, out=tmp_path / "b")
    name = "erp__orders__fullscan.yml"
    assert (tmp_path / "a" / name).read_bytes() == (tmp_path / "b" / name).read_bytes()


def test_incremental_json(tmp_path: Path) -> None:
    code, _, _ = add(
        "incremental",
        *BASE,
        "--watermark-column",
        "updated_at",
        "--initial-value",
        "2026-01-01T00:00:00",
        "--format",
        "json",
        out=tmp_path,
    )
    assert code == 0
    text = (tmp_path / "erp__orders__incremental.json").read_text(encoding="utf-8")
    assert text.endswith("}\n")
    data = json.loads(text)
    assert data["extract"]["watermark"] == {
        "column": "updated_at",
        "type": "datetime",
        "initial_value": "2026-01-01T00:00:00",
    }
    assert data["load"]["write_mode"] == "append"


def test_options_reach_the_config(tmp_path: Path) -> None:
    code, _, _ = add(
        "fullscan",
        *BASE,
        *["--schema", "sales", "--connection-id", "erp_ro", "--target-connection-id", "wh"],
        *["--columns", "id:integer!,total:numeric", "--write-mode", "append"],
        *["--max-reject-ratio", "0.1", "--cron", "0 6 * * *", "--start-date", "2026-03-01"],
        *["--tags", "a,b", "--owner", "me", "--description", "d"],
        out=tmp_path,
    )
    assert code == 0
    config = read_config(tmp_path / "erp__orders__fullscan.yml")
    assert (config.source.schema_name, config.source.connection_id) == ("sales", "erp_ro")
    assert config.load.connection_id == "wh"
    assert [c.name for c in config.extract.columns or ()] == ["id", "total"]
    assert config.schedule.tags == ("a", "b")
    assert config.schedule.start_date.isoformat() == "2026-03-01"
    assert config.metadata.description == "d"


def test_refuses_overwrite_without_force(tmp_path: Path) -> None:
    add("fullscan", *BASE, out=tmp_path)
    code, _, err = add("fullscan", *BASE, out=tmp_path)
    assert code == 1
    assert err.startswith("error: ")
    assert "already exists. Fix: pass --force" in err
    code, _, _ = add("fullscan", *BASE, "--force", out=tmp_path)
    assert code == 0


def test_invalid_input_exits_1_with_error_lines(tmp_path: Path) -> None:
    code, _, err = add("fullscan", *BASE, "--max-reject-ratio", "2", "--cron", "bad", out=tmp_path)
    assert code == 1
    lines = err.strip().splitlines()
    assert len(lines) == 2
    assert all(line.startswith("error: <arguments>: ") and ". Fix: " in line for line in lines)
    assert not list(tmp_path.iterdir())


def test_bad_columns_spec(tmp_path: Path) -> None:
    for spec in ("id", "id:uuid", "_ingest_x:text"):
        code, _, err = add("fullscan", *BASE, "--columns", spec, out=tmp_path)
        assert code == 1
        assert "--columns" in err


def test_incremental_needs_watermark_column(tmp_path: Path) -> None:
    code, _, _ = add("incremental", *BASE, out=tmp_path)
    assert code == 2


def test_bad_format_is_usage_error(tmp_path: Path) -> None:
    code, _, _ = add("fullscan", *BASE, "--format", "toml", out=tmp_path)
    assert code == 2
