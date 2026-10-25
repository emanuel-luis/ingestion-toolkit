"""Any valid config renders to code that parses, the same way every time."""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

import pytest
from hypothesis import given, settings

from ingestion_toolkit.models.config import Config
from ingestion_toolkit.renderer import Renderer
from tests.strategies import config_dicts

RENDERER = Renderer()


@pytest.mark.slow
@pytest.mark.timeout(600)  # 300 renders: ~40 s idle, over 60 s on a loaded machine
@settings(max_examples=300)
@given(config_dicts())
def test_valid_configs_render_to_parseable_deterministic_code(data: dict[str, Any]) -> None:
    config = Config.model_validate(data)
    path = Path(f"{config.dag_id}.yml")
    text = RENDERER.render(config, path)
    ast.parse(text)
    assert text == RENDERER.render(config, path)
    assert f'DAG_ID = "{config.dag_id}"' in text
