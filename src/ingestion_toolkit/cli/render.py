"""``ingest render``."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ingestion_toolkit.cli._report import finish, print_errors
from ingestion_toolkit.config_io import read_configs
from ingestion_toolkit.errors import ConfigFailure
from ingestion_toolkit.renderer import render_all


def render(
    paths: Annotated[
        list[Path] | None,
        typer.Argument(help="Config files or directories. Default: configs/."),
    ] = None,
    output_dir: Annotated[Path, typer.Option(help="Directory for the DAG files.")] = Path("output"),
    template_dir: Annotated[
        Path | None, typer.Option(help="Templates that override the built-in ones by path.")
    ] = None,
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Print the files that would be written.")
    ] = False,
) -> None:
    """Render one Airflow DAG file per config. Writes nothing if any config is invalid."""
    valid, errors = read_configs(paths or [Path("configs")])
    if errors:
        finish(len(valid), errors)
    try:
        written = render_all(valid, output_dir, template_dir, dry_run)
    except ConfigFailure as failure:
        print_errors(failure.errors)
        typer.echo(f"0 rendered, {len({e.path for e in failure.errors})} failed")
        raise typer.Exit(1) from failure
    for path in written:
        typer.echo(path.as_posix())
    typer.echo(f"{len(written)} {'would be rendered' if dry_run else 'rendered'}")
