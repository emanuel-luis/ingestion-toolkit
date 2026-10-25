"""Rendered DAGs for the examples must match tests/golden byte for byte.

After changing a template or the context: ``uv run pytest tests/test_golden.py --update-golden``,
then read the diff.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ingestion_toolkit.config_io import read_config
from ingestion_toolkit.renderer import Renderer

ROOT = Path(__file__).resolve().parents[1]
GOLDEN = Path(__file__).parent / "golden"
EXAMPLES = sorted((ROOT / "examples" / "postgres").glob("*.yml"))


@pytest.mark.parametrize("config_path", EXAMPLES, ids=lambda p: p.stem)
def test_golden(config_path: Path, request: pytest.FixtureRequest) -> None:
    text = Renderer().render(read_config(config_path), Path(config_path.name))
    golden = GOLDEN / f"{config_path.stem}.py"
    if request.config.getoption("--update-golden"):
        golden.parent.mkdir(exist_ok=True)
        golden.write_text(text, encoding="utf-8", newline="\n")
    assert golden.read_text(encoding="utf-8") == text


def test_every_golden_has_an_example() -> None:
    assert sorted(p.stem for p in GOLDEN.glob("*.py")) == sorted(p.stem for p in EXAMPLES)
