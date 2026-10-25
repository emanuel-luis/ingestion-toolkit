"""``ingest init``: lay out a project with a starter config."""

from __future__ import annotations

from importlib import resources
from pathlib import Path
from typing import Annotated

import typer

from ingestion_toolkit.cli._report import print_errors
from ingestion_toolkit.errors import ConfigError

STARTER = "erp__orders__fullscan.yml"
GITIGNORE = "output/\n"


def init(
    directory: Annotated[
        Path, typer.Argument(help="Project directory, created if missing.")
    ] = Path("."),
    force: Annotated[bool, typer.Option("--force", help="Overwrite existing files.")] = False,
) -> None:
    """Create configs/ with a commented starter config, output/ and a .gitignore."""
    config = directory / "configs" / STARTER
    gitignore = directory / ".gitignore"
    existing = [p for p in (config, gitignore) if p.exists()]
    if existing and not force:
        print_errors(
            [
                ConfigError(
                    p.as_posix(), "file", "file already exists", "pass --force to overwrite it"
                )
                for p in existing
            ]
        )
        raise typer.Exit(1)
    starter = (resources.files("ingestion_toolkit") / "scaffold" / STARTER).read_text("utf-8")
    try:
        config.parent.mkdir(parents=True, exist_ok=True)
        (directory / "output").mkdir(exist_ok=True)
        config.write_text(starter, encoding="utf-8", newline="\n")
        gitignore.write_text(GITIGNORE, encoding="utf-8", newline="\n")
    except OSError as exc:
        print_errors(
            [
                ConfigError(
                    directory.as_posix(),
                    "file",
                    f"cannot write: {exc.strerror or exc}",
                    "check that the directory is writable",
                )
            ]
        )
        raise typer.Exit(1) from exc
    typer.echo(f"wrote {config.as_posix()}")
    typer.echo(f"wrote {gitignore.as_posix()}")
    typer.echo(f"next: ingest validate {(directory / 'configs').as_posix()}")
