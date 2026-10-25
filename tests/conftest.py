"""Shared pytest setup."""

from __future__ import annotations

from hypothesis import settings

settings.register_profile("default", derandomize=True, deadline=None, max_examples=50)
settings.load_profile("default")
