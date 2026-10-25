"""``ingest validate``."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ingestion_toolkit.cli._report import finish
from ingestion_toolkit.config_io import read_configs


def validate(
    paths: Annotated[
        list[Path] | None,
        typer.Argument(help="Config files or directories. Default: configs/."),
    ] = None,
) -> None:
    """Check every config: schema, file name and duplicate DAG ids."""
    valid, errors = read_configs(paths or [Path("configs")])
    finish(len(valid), errors)
