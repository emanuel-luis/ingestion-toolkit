"""Read, validate and write config files (YAML or JSON)."""

from __future__ import annotations

import json
from collections.abc import Sequence
from enum import StrEnum
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from ingestion_toolkit import naming
from ingestion_toolkit.errors import ConfigError, ConfigFailure
from ingestion_toolkit.models.config import Config

SUFFIXES = {".yml": "yaml", ".yaml": "yaml", ".json": "json"}
_FIX = ". Fix: "
_GENERIC_HINTS = {
    "missing": "add the required field",
    "extra_forbidden": "remove the field or fix its spelling",
}


class Format(StrEnum):
    YAML = "yaml"
    JSON = "json"


class _Loader(yaml.SafeLoader):
    """SafeLoader that rejects duplicate mapping keys."""

    def construct_mapping(self, node: yaml.MappingNode, deep: bool = False) -> dict[Any, Any]:
        seen: set[Any] = set()
        for key_node, _ in node.value:
            key = self.construct_object(key_node, deep=True)
            if key in seen:
                raise yaml.constructor.ConstructorError(
                    None, None, f"duplicate key {key!r}", key_node.start_mark
                )
            seen.add(key)
        return super().construct_mapping(node, deep)


class _Dumper(yaml.SafeDumper):
    def increase_indent(self, flow: bool = False, indentless: bool = False) -> None:
        super().increase_indent(flow, False)  # indent list items under their key


_Dumper.add_multi_representer(StrEnum, lambda d, v: d.represent_str(v.value))


def _error(path: Path, loc: str, message: str, hint: str) -> ConfigError:
    return ConfigError(path.as_posix(), loc, message, hint)


def validation_errors(path: Path, exc: ValidationError, prefix: str = "") -> list[ConfigError]:
    """Turn a pydantic error into one ConfigError per problem."""
    errors = []
    for item in exc.errors():
        loc = ".".join([*([prefix] if prefix else []), *(str(p) for p in item["loc"])])
        message = item["msg"].removeprefix("Value error, ")
        message, _, hint = message.partition(_FIX)
        hint = hint or _GENERIC_HINTS.get(item["type"], "see the config reference in README.md")
        errors.append(_error(path, loc or "(root)", message, hint))
    return errors


def discover(paths: Sequence[Path]) -> tuple[list[Path], list[ConfigError]]:
    """Expand files and directories into config files, sorted, without duplicates."""
    files: list[Path] = []
    errors: list[ConfigError] = []
    for path in paths:
        if path.is_dir():
            found = sorted(p for p in path.rglob("*") if p.is_file() and p.suffix in SUFFIXES)
            if not found:
                errors.append(
                    _error(path, "path", "no config files found", "add .yml, .yaml or .json files")
                )
            files.extend(found)
        elif not path.exists():
            errors.append(_error(path, "path", "path does not exist", "check the path"))
        elif path.suffix not in SUFFIXES:
            errors.append(
                _error(
                    path,
                    "path",
                    f"unsupported extension {path.suffix!r}",
                    "use .yml, .yaml or .json",
                )
            )
        else:
            files.append(path)
    return list(dict.fromkeys(files)), errors


def _parse(path: Path) -> Any:
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise ConfigFailure(
            [_error(path, "file", f"cannot read file: {exc}", "check the file")]
        ) from exc
    try:
        if SUFFIXES[path.suffix] == "json":
            return json.loads(text)
        return yaml.load(text, Loader=_Loader)
    except json.JSONDecodeError as exc:
        raise ConfigFailure(
            [_error(path, f"line {exc.lineno}", exc.msg, "fix the JSON syntax")]
        ) from exc
    except yaml.YAMLError as exc:
        mark = getattr(exc, "problem_mark", None)
        loc = f"line {mark.line + 1}" if mark is not None else "file"
        message = getattr(exc, "problem", None) or str(exc)
        hint = (
            "remove or rename the repeated key"
            if "duplicate key" in message
            else "fix the YAML syntax"
        )
        raise ConfigFailure([_error(path, loc, message, hint)]) from exc


def read_config(path: Path) -> Config:
    """Load and validate one file. Raises ConfigFailure with every error found."""
    data = _parse(path)
    if not isinstance(data, dict):
        raise ConfigFailure(
            [
                _error(
                    path,
                    "(root)",
                    "config must be a mapping",
                    "start with keys such as version and source",
                )
            ]
        )
    try:
        return Config.model_validate(data)
    except ValidationError as exc:
        raise ConfigFailure(validation_errors(path, exc)) from exc


def read_configs(paths: Sequence[Path]) -> tuple[list[tuple[Path, Config]], list[ConfigError]]:
    """Load every file, collecting all errors. Also checks file stems and duplicate DAG ids."""
    files, errors = discover(paths)
    valid: list[tuple[Path, Config]] = []
    first_seen: dict[str, Path] = {}
    for path in files:
        try:
            config = read_config(path)
        except ConfigFailure as failure:
            errors.extend(failure.errors)
            continue
        expected = naming.config_stem(
            config.source.name, config.extract.table, config.extract.pattern
        )
        if path.stem != expected:
            errors.append(
                _error(
                    path,
                    "file name",
                    f"file stem {path.stem!r} does not match {expected!r}",
                    f"rename the file to {expected}{path.suffix}",
                )
            )
        elif config.dag_id in first_seen:
            errors.append(
                _error(
                    path,
                    "dag_id",
                    f"dag_id {config.dag_id!r} is already used by "
                    f"{first_seen[config.dag_id].as_posix()}",
                    "keep one config per source table and pattern",
                )
            )
        else:
            first_seen[config.dag_id] = path
            valid.append((path, config))
    return valid, errors


def _prune(value: Any, path: tuple[str, ...] = ()) -> Any:
    """Drop None fields, except schedule.cron: null means "no schedule", not "unset"."""
    if isinstance(value, dict):
        return {
            k: _prune(v, (*path, k))
            for k, v in value.items()
            if v is not None or path + (k,) == ("schedule", "cron")
        }
    if isinstance(value, list | tuple):
        return [_prune(v, path) for v in value]
    return value


def dump_text(config: Config, fmt: Format) -> str:
    """Deterministic text: model field order, None fields omitted, LF line ends."""
    if fmt is Format.JSON:
        data = _prune(config.model_dump(mode="json"))
        return json.dumps(data, indent=2, ensure_ascii=False) + "\n"
    data = _prune(config.model_dump(mode="python"))
    return yaml.dump(data, Dumper=_Dumper, sort_keys=False, allow_unicode=True)


def write_config(path: Path, config: Config, fmt: Format = Format.YAML) -> None:
    """Write the config, creating parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dump_text(config, fmt), encoding="utf-8", newline="\n")
