"""Shared output helpers for CLI commands."""

from __future__ import annotations

from collections.abc import Sequence

import typer

from ingestion_toolkit.errors import ConfigError, format_error


def print_errors(errors: Sequence[ConfigError]) -> None:
    """One ``error: ...`` line per problem, on stderr."""
    for error in errors:
        typer.echo(format_error(error), err=True)


def finish(valid: int, errors: Sequence[ConfigError]) -> None:
    """Print errors and the ``N valid, M invalid`` summary, then exit 0 or 1."""
    print_errors(errors)
    invalid = len({e.path for e in errors})
    typer.echo(f"{valid} valid, {invalid} invalid")
    raise typer.Exit(1 if invalid else 0)
