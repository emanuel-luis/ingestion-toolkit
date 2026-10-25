"""``ingest init``: lay out a project with a starter config."""

from __future__ import annotations

from importlib import resources
from pathlib import Path
from typing import Annotated

import typer

from ingestion_toolkit.cli._report import print_errors
from ingestion_toolkit.errors import ConfigError

STARTER = "erp__orders__fullscan.yml"
IGNORE_LINE = "output/"
_IGNORE_FORMS = {"output", "output/", "/output", "/output/"}


def _gitignore_addition(path: Path) -> str:
    """Text to append so ``output/`` is ignored: the whole file when missing, else one line."""
    if not path.exists():
        return IGNORE_LINE + "\n"
    text = path.read_text(encoding="utf-8", errors="replace")
    if any(line.strip() in _IGNORE_FORMS for line in text.splitlines()):
        return ""
    return ("" if not text or text.endswith("\n") else "\n") + IGNORE_LINE + "\n"


def init(
    directory: Annotated[
        Path, typer.Argument(help="Project directory, created if missing.")
    ] = Path("."),
    force: Annotated[
        bool, typer.Option("--force", help="Overwrite an existing starter config.")
    ] = False,
) -> None:
    """Create configs/ with a commented starter config and output/, and ignore output/ in git.

    An existing .gitignore is never replaced: ``output/`` is appended when no line ignores it.
    """
    config = directory / "configs" / STARTER
    gitignore = directory / ".gitignore"
    if config.exists() and not force:
        print_errors(
            [
                ConfigError(
                    config.as_posix(), "file", "file already exists", "pass --force to overwrite it"
                )
            ]
        )
        raise typer.Exit(1)
    starter = (resources.files("ingestion_toolkit") / "scaffold" / STARTER).read_text("utf-8")
    try:
        # read .gitignore first, so a failure there leaves nothing half written
        created = not gitignore.exists()
        addition = _gitignore_addition(gitignore)
        config.parent.mkdir(parents=True, exist_ok=True)
        (directory / "output").mkdir(exist_ok=True)
        config.write_text(starter, encoding="utf-8", newline="\n")
        if addition:
            with gitignore.open("a", encoding="utf-8", newline="\n") as handle:
                handle.write(addition)
    except OSError as exc:
        print_errors(
            [
                ConfigError(
                    directory.as_posix(),
                    "file",
                    f"cannot create the project: {exc.strerror or exc}",
                    "check that the directory is writable",
                )
            ]
        )
        raise typer.Exit(1) from exc
    typer.echo(f"wrote {config.as_posix()}")
    if created:
        typer.echo(f"wrote {gitignore.as_posix()}")
    elif addition:
        typer.echo(f"added {IGNORE_LINE} to {gitignore.as_posix()}")
    typer.echo(f"next: ingest validate {(directory / 'configs').as_posix()}")
