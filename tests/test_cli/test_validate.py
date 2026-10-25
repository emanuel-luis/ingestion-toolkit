from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from ingestion_toolkit.cli.main import app

runner = CliRunner()
GOOD = (
    "version: 1\nsource: {name: erp, database: erp}\n"
    "extract: {table: orders, primary_key: id, pattern: fullscan}\n"
)


def test_valid_directory(tmp_path: Path) -> None:
    (tmp_path / "erp__orders__fullscan.yml").write_text(GOOD, encoding="utf-8")
    result = runner.invoke(app, ["validate", str(tmp_path)])
    assert result.exit_code == 0
    assert result.stdout.strip() == "1 valid, 0 invalid"


def test_invalid_files_reported_together(tmp_path: Path) -> None:
    (tmp_path / "erp__orders__fullscan.yml").write_text(GOOD, encoding="utf-8")
    (tmp_path / "erp__x__fullscan.yml").write_text("version: 1\nbogus: 1\n", encoding="utf-8")
    (tmp_path / "bad.yml").write_text("a: 1\na: 2\n", encoding="utf-8")
    result = runner.invoke(app, ["validate", str(tmp_path)])
    assert result.exit_code == 1
    assert result.stdout.strip() == "1 valid, 2 invalid"
    errors = result.stderr.strip().splitlines()
    assert len(errors) >= 4
    assert all(line.startswith("error: ") and ". Fix: " in line for line in errors)


def test_default_path_is_configs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["validate"])
    assert result.exit_code == 1
    assert "path does not exist" in result.stderr
