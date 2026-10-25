"""Error values shared by config I/O and the CLI."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ConfigError:
    """One problem found in a config: where it is, what is wrong and how to fix it."""

    path: str
    loc: str
    message: str
    hint: str


class ConfigFailure(Exception):
    """Raised by ``read_config`` with every error found in one file."""

    def __init__(self, errors: list[ConfigError]) -> None:
        super().__init__(f"{len(errors)} config error(s)")
        self.errors = errors


def format_error(error: ConfigError) -> str:
    """Render an error as the single stderr line the CLI prints."""
    message = error.message.rstrip(". ")
    return f"error: {error.path}: {error.loc}: {message}. Fix: {error.hint}"
