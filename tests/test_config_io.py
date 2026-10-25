from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

import pytest
from hypothesis import given

from ingestion_toolkit.config_io import (
    Format,
    discover,
    dump_text,
    read_config,
    read_configs,
    write_config,
)
from ingestion_toolkit.errors import ConfigFailure, format_error
from ingestion_toolkit.models.config import Config
from tests.strategies import config_dicts

GOOD = """\
version: 1
source: {name: erp, database: erp}
extract: {table: orders, primary_key: id, pattern: fullscan}
"""
NAME = "erp__orders__fullscan.yml"


def make(directory: Path, name: str = NAME, text: str = GOOD) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    path.write_text(text, encoding="utf-8")
    return path


@given(config_dicts())
def test_round_trip_both_formats(data: dict[str, Any]) -> None:
    config = Config.model_validate(data)
    with tempfile.TemporaryDirectory() as tmp:
        for fmt, suffix in ((Format.YAML, "yml"), (Format.JSON, "json")):
            path = Path(tmp) / f"x.{suffix}"
            write_config(path, config, fmt)
            assert read_config(path) == config
            assert path.read_text(encoding="utf-8") == dump_text(config, fmt)


def test_json_is_indented_with_trailing_newline(tmp_path: Path) -> None:
    config = read_config(make(tmp_path))
    text = dump_text(config, Format.JSON)
    assert text.endswith("}\n")
    assert text.startswith('{\n  "version": 1,')
    assert json.loads(text)["load"]["write_mode"] == "overwrite"


def test_yaml_omits_none_and_keeps_field_order(tmp_path: Path) -> None:
    text = dump_text(read_config(make(tmp_path)), Format.YAML)
    assert "description" not in text
    assert "null" not in text
    assert text.index("source:") < text.index("extract:") < text.index("load:")
    assert "start_date: 2026-01-01\n" in text


def test_duplicate_keys_rejected(tmp_path: Path) -> None:
    path = make(tmp_path, text=GOOD + "version: 1\n")
    with pytest.raises(ConfigFailure) as info:
        read_config(path)
    (error,) = info.value.errors
    assert "duplicate key 'version'" in error.message
    assert error.loc == "line 4"


def test_yaml_syntax_error_reports_path_and_line(tmp_path: Path) -> None:
    path = make(tmp_path, text="version: 1\nsource: [unclosed\n")
    with pytest.raises(ConfigFailure) as info:
        read_config(path)
    line = format_error(info.value.errors[0])
    assert line.startswith(f"error: {path.as_posix()}: line ")
    assert ". Fix: " in line


def test_json_syntax_error(tmp_path: Path) -> None:
    path = make(tmp_path, "erp__orders__fullscan.json", '{"version": 1,\n "x": }')
    with pytest.raises(ConfigFailure) as info:
        read_config(path)
    assert info.value.errors[0].loc == "line 2"


def test_top_level_must_be_mapping(tmp_path: Path) -> None:
    with pytest.raises(ConfigFailure):
        read_config(make(tmp_path, text="- a\n- b\n"))


def test_bad_extension_and_missing_path(tmp_path: Path) -> None:
    txt = make(tmp_path, "a.txt", "x")
    files, errors = discover([txt, tmp_path / "nope.yml"])
    assert files == []
    assert [e.message for e in errors] == ["unsupported extension '.txt'", "path does not exist"]


def test_empty_directory_is_an_error(tmp_path: Path) -> None:
    _, errors = discover([tmp_path])
    assert errors[0].message == "no config files found"


def test_all_errors_are_collected(tmp_path: Path) -> None:
    make(tmp_path)
    make(tmp_path, "erp__a__fullscan.yml", "version: 2\nsource: {name: Bad}\n")
    make(tmp_path, "erp__b__fullscan.yml", "not: [valid")
    make(tmp_path, "wrong_name.yml", GOOD)
    valid, errors = read_configs([tmp_path])
    assert [p.name for p, _ in valid] == [NAME]
    by_file = [Path(e.path).name for e in errors]
    assert set(by_file) == {"erp__a__fullscan.yml", "erp__b__fullscan.yml", "wrong_name.yml"}
    assert by_file.count("erp__a__fullscan.yml") >= 3  # version, source name, and the rest


def test_file_stem_must_match_dag_id(tmp_path: Path) -> None:
    _, errors = read_configs([make(tmp_path, "orders.yml")])
    assert errors[0].loc == "file name"
    assert errors[0].hint == f"rename the file to {NAME}"


def test_duplicate_dag_ids(tmp_path: Path) -> None:
    make(tmp_path / "a")
    make(tmp_path / "b")
    valid, errors = read_configs([tmp_path])
    assert len(valid) == 1
    assert errors[0].loc == "dag_id"
