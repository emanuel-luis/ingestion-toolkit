from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from ingestion_toolkit.cli.main import app
from ingestion_toolkit.config_io import read_configs
from ingestion_toolkit.renderer import render_all

runner = CliRunner()


def test_init_creates_a_project_that_validates_and_renders(tmp_path: Path) -> None:
    project = tmp_path / "proj"
    result = runner.invoke(app, ["init", str(project)])
    assert result.exit_code == 0, result.stderr
    assert (project / "output").is_dir()
    assert (project / ".gitignore").read_text(encoding="utf-8") == "output/\n"
    starter = project / "configs" / "erp__orders__fullscan.yml"
    assert starter.read_text(encoding="utf-8").startswith("# Starter config")
    valid, errors = read_configs([project / "configs"])
    assert not errors and len(valid) == 1
    assert len(render_all(valid, project / "output")) == 1


def test_init_refuses_to_overwrite_without_force(tmp_path: Path) -> None:
    runner.invoke(app, ["init", str(tmp_path)])
    starter = tmp_path / "configs" / "erp__orders__fullscan.yml"
    starter.write_text("mine\n", encoding="utf-8")
    result = runner.invoke(app, ["init", str(tmp_path)])
    assert result.exit_code == 1
    assert "already exists. Fix: pass --force" in result.stderr
    assert starter.read_text(encoding="utf-8") == "mine\n"
    assert runner.invoke(app, ["init", str(tmp_path), "--force"]).exit_code == 0
    assert starter.read_text(encoding="utf-8").startswith("# Starter config")


def test_init_into_a_file_exits_1(tmp_path: Path) -> None:
    blocker = tmp_path / "blocker"
    blocker.write_text("x", encoding="utf-8")
    result = runner.invoke(app, ["init", str(blocker)])
    assert result.exit_code == 1
    assert result.stderr.startswith("error: ")
