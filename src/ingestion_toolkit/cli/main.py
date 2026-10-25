"""The ``ingest`` command."""

from __future__ import annotations

from typing import Annotated

import typer

from ingestion_toolkit import __version__
from ingestion_toolkit.cli import add, validate

app = typer.Typer(
    name="ingest",
    help="Write and validate ingestion configs for relational sources.",
    no_args_is_help=True,
    add_completion=False,
)
app.add_typer(add.app, name="add")
app.command("validate")(validate.validate)


def _show_version(value: bool) -> None:
    if value:
        typer.echo(__version__)
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool,
        typer.Option("--version", callback=_show_version, is_eager=True, help="Show the version."),
    ] = False,
) -> None:
    """Write and validate ingestion configs for relational sources."""
