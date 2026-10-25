from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from ingestion_toolkit.cli.main import app

runner = CliRunner()
ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = ROOT / "examples" / "postgres"
GOOD = (
    "version: 1\nsource: {name: erp, database: erp}\n"
    "extract: {table: orders, primary_key: id, pattern: fullscan}\n"
)
MERGE = (
    "version: 1\nsource: {name: erp, database: erp}\n"
    "extract: {table: orders, primary_key: id, pattern: incremental,"
    " watermark: {column: updated_at}}\nload: {write_mode: merge}\n"
)


def test_render_examples(tmp_path: Path) -> None:
    result = runner.invoke(app, ["render", str(EXAMPLES), "--output-dir", str(tmp_path / "out")])
    assert result.exit_code == 0, result.stderr
    assert result.stdout.strip().splitlines()[-1] == "2 rendered"
    assert sorted(p.name for p in (tmp_path / "out").iterdir()) == [
        "erp__customers__incremental.py",
        "erp__orders__fullscan.py",
    ]


def test_dry_run_lists_paths_and_writes_nothing(tmp_path: Path) -> None:
    out = tmp_path / "out"
    result = runner.invoke(app, ["render", str(EXAMPLES), "--output-dir", str(out), "--dry-run"])
    assert result.exit_code == 0
    assert "erp__orders__fullscan.py" in result.stdout
    assert result.stdout.strip().splitlines()[-1] == "2 would be rendered"
    assert not out.exists()


def test_invalid_config_writes_nothing(tmp_path: Path) -> None:
    configs = tmp_path / "configs"
    configs.mkdir()
    (configs / "erp__orders__fullscan.yml").write_text(GOOD, encoding="utf-8")
    (configs / "bad.yml").write_text("version: 2\n", encoding="utf-8")
    out = tmp_path / "out"
    result = runner.invoke(app, ["render", str(configs), "--output-dir", str(out)])
    assert result.exit_code == 1
    assert result.stderr.startswith("error: ")
    assert not out.exists()


def test_render_failure_is_exit_1_and_writes_nothing(tmp_path: Path) -> None:
    configs = tmp_path / "configs"
    configs.mkdir()
    (configs / "erp__orders__fullscan.yml").write_text(GOOD, encoding="utf-8")
    (configs / "erp__orders__incremental.yml").write_text(MERGE, encoding="utf-8")
    out = tmp_path / "out"
    result = runner.invoke(app, ["render", str(configs), "--output-dir", str(out)])
    assert result.exit_code == 1
    assert "load.write_mode" in result.stderr and ". Fix: use write_mode append" in result.stderr
    assert result.stdout.strip() == "0 rendered, 1 failed"
    assert not out.exists()


def test_template_dir_override(tmp_path: Path) -> None:
    templates = tmp_path / "t"
    (templates / "tasks").mkdir(parents=True)
    (templates / "tasks" / "ensure_tables.py.j2").write_text(
        "    # custom\n    ensure_tables = SQLExecuteQueryOperator(task_id='ensure_tables',"
        " conn_id=TARGET_CONN_ID, sql=ENSURE_TABLES_SQL)\n",
        encoding="utf-8",
    )
    out = tmp_path / "out"
    args = ["render", str(EXAMPLES), "--output-dir", str(out), "--template-dir", str(templates)]
    assert runner.invoke(app, args).exit_code == 0
    assert "# custom" in (out / "erp__orders__fullscan.py").read_text(encoding="utf-8")


def test_usage_error_is_exit_2() -> None:
    assert runner.invoke(app, ["render", "--bogus"]).exit_code == 2


def test_default_path_is_configs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["render"])
    assert result.exit_code == 1
    assert "path does not exist" in result.stderr
