from __future__ import annotations

from typer.testing import CliRunner

from ingestion_toolkit import __version__
from ingestion_toolkit.cli.main import app

runner = CliRunner()


def test_version() -> None:
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert result.stdout.strip() == __version__


def test_help_lists_only_add_and_validate() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "add" in result.stdout
    assert "validate" in result.stdout
    assert "render" not in result.stdout


def test_no_args_shows_help() -> None:
    result = runner.invoke(app, [])
    assert "Usage" in result.stdout


def test_unknown_command_is_usage_error() -> None:
    assert runner.invoke(app, ["render"]).exit_code == 2
