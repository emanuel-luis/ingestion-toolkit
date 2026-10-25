"""Shared pytest setup."""

from __future__ import annotations

import pytest
from hypothesis import settings

settings.register_profile("default", derandomize=True, deadline=None, max_examples=50)
settings.load_profile("default")


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--update-golden",
        action="store_true",
        default=False,
        help="rewrite tests/golden/*.py from the current templates",
    )
