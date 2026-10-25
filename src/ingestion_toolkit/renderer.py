"""Turn validated configs into DAG source files."""

from __future__ import annotations

import ast
import os
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import jinja2

from ingestion_toolkit import sql
from ingestion_toolkit.errors import ConfigError, ConfigFailure, RenderError
from ingestion_toolkit.models.config import Config
from ingestion_toolkit.models.enums import Pattern, WriteMode
from ingestion_toolkit.models.task import DagContext, ExtractTaskContext, LoadTaskContext

_BASE_TEMPLATE = "dag_base.py.j2"
_LINE_LENGTH = 100
_LOAD_TEMPLATES = {
    WriteMode.OVERWRITE: "tasks/load/overwrite.py.j2",
    WriteMode.APPEND: "tasks/load/append.py.j2",
}
_HELPERS = (
    "helpers/landing.py.j2",
    "helpers/contract.py.j2",
    "helpers/run_metadata.py.j2",
)


def _fail(path: Path | str, loc: str, message: str, hint: str) -> RenderError:
    return RenderError(ConfigError(Path(path).as_posix(), loc, message, hint))


def py_literal(value: Any, name: str | None = None, nested: bool = False) -> str:
    """Python source for a literal, laid out the way ``ruff format`` would.

    Scalars are ``repr`` (double quotes when the text has none). A non-empty top-level list,
    tuple or dict goes one item per line with a trailing comma (a list, so one item keeps its
    layout); nested ones stay on one line. With ``name`` the result is the whole assignment
    ``name = value``, and a string too long for the line is wrapped in parentheses.
    """
    if isinstance(value, jinja2.Undefined):
        str(value)  # raises UndefinedError, naming the missing variable
    prefix = "" if name is None else f"{name} = "
    if isinstance(value, str):
        text = repr(value)
        text = f'"{text[1:-1]}"' if text[0] == "'" and '"' not in value else text
        if name is not None and len(prefix + text) > _LINE_LENGTH and len(text) + 4 <= _LINE_LENGTH:
            return f"{prefix}(\n    {text}\n)"
        return prefix + text
    if isinstance(value, bool | int | float) or value is None:
        return prefix + repr(value)
    if isinstance(value, dict):
        items = [
            f"{py_literal(k, nested=True)}: {py_literal(v, nested=True)}" for k, v in value.items()
        ]
        opening, closing = "{", "}"
    elif isinstance(value, list | tuple):
        items = [py_literal(v, nested=True) for v in value]
        tuple_style = nested and isinstance(value, tuple)
        opening, closing = ("(", ")") if tuple_style else ("[", "]")
        if tuple_style and len(items) == 1:
            items[0] += ","
    else:
        raise TypeError(f"cannot render {type(value).__name__} as a Python literal")
    if nested or not items:
        return opening + ", ".join(items) + closing
    body = "".join(f"    {item},\n" for item in items)
    return f"{prefix}{opening}\n{body}{closing}"


def build_dag_context(config: Config, config_name: str) -> DagContext:
    """Pure: the template context for one config. Raises ValueError for unsupported modes."""
    extract, load = config.extract, config.load
    if load.write_mode not in _LOAD_TEMPLATES:
        raise ValueError(
            f"write_mode {load.write_mode} cannot be rendered yet. Fix: use write_mode append"
        )
    select = sql.select_sql(config)
    mark = extract.watermark
    if extract.pattern is Pattern.INCREMENTAL and mark is not None:
        extract_ctx = ExtractTaskContext(
            template="tasks/extract/windowed.py.j2",
            extract_sql=select,
            bounded_sql=sql.bounded_select_sql(config),
            bound_column=mark.column,
            bound_type=mark.type.value,
            initial_value=mark.initial_value,
        )
        helpers = (*_HELPERS, "helpers/state.py.j2")
    else:
        extract_ctx = ExtractTaskContext(
            template="tasks/extract/fullscan.py.j2", extract_sql=select
        )
        helpers = (*_HELPERS, "helpers/state_none.py.j2")
    columns = (
        tuple((c.name, c.type.value, c.nullable) for c in extract.columns)
        if extract.columns
        else None
    )
    table = extract.table.lower()
    load_ctx = LoadTaskContext(
        template=_LOAD_TEMPLATES[load.write_mode],
        bronze_schema_sql=sql.create_schema_sql(config),
        bronze_table=sql.target_table(config),
        bronze_schema=load.target_schema,
        bronze_name=load.target_table or "",
        columns=columns,
        pg_types=dict(sql.PG_TYPES),
        prepare_sql=sql.prepare_sql(config),
        duplicate_sql=sql.duplicate_sql(config),
        primary_key=extract.primary_key,
        max_reject_ratio=load.max_reject_ratio,
    )
    schedule = config.schedule
    tags = dict.fromkeys(
        [*sorted(schedule.tags), config.source.name, extract.pattern.value, "ingestion-toolkit"]
    )
    return DagContext(
        dag_id=config.dag_id,
        config_name=config_name,
        description=config.metadata.description,
        owner=config.metadata.owner,
        schedule=schedule.cron,
        start_date=schedule.start_date.isoformat(),
        retries=schedule.retries,
        retry_delay_minutes=schedule.retry_delay_minutes,
        tags=tuple(tags),
        source_name=config.source.name,
        source_table=table,
        pattern=extract.pattern.value,
        source_conn_id=config.source.connection_id,
        target_conn_id=load.connection_id,
        landing_dir=f"{config.landing.root.rstrip('/')}/{config.source.name}/{table}",
        helper_templates=helpers,
        extract=extract_ctx,
        load=load_ctx,
    )


class Renderer:
    """Renders DAG files. ``template_dir`` overrides built-in templates by relative path."""

    def __init__(self, template_dir: Path | None = None) -> None:
        loaders: list[jinja2.BaseLoader] = []
        if template_dir is not None:
            loaders.append(jinja2.FileSystemLoader(template_dir))
        loaders.append(jinja2.PackageLoader("ingestion_toolkit", "templates"))
        self.env = jinja2.Environment(  # autoescape is off: this renders Python, not HTML
            loader=jinja2.ChoiceLoader(loaders),
            undefined=jinja2.StrictUndefined,
            trim_blocks=True,
            lstrip_blocks=True,
            keep_trailing_newline=True,
            autoescape=False,
        )
        self.env.filters["py"] = py_literal

    def render(self, config: Config, config_path: Path) -> str:
        """Render one config. Raises RenderError, naming the config and the template."""
        try:
            context = build_dag_context(config, config_path.name)
        except ValueError as exc:
            message, _, hint = str(exc).partition(". Fix: ")
            raise _fail(config_path, "load.write_mode", message, hint) from exc
        try:
            text = self.env.get_template(_BASE_TEMPLATE).render(dag=context)
        except jinja2.TemplateNotFound as exc:
            raise _fail(
                config_path, f"template {exc.name}", "template not found", "check --template-dir"
            ) from exc
        except jinja2.TemplateError as exc:
            where = getattr(exc, "name", None) or _BASE_TEMPLATE
            raise _fail(
                config_path, f"template {where}", str(exc), "fix the template or report a bug"
            ) from exc
        try:
            ast.parse(text)
        except SyntaxError as exc:
            raise _fail(
                config_path,
                f"template {_BASE_TEMPLATE}",
                f"rendered code is not valid Python (line {exc.lineno}: {exc.msg})",
                "fix the template override or report a bug",
            ) from exc
        return text


def write_dag(text: str, output_dir: Path, dag_id: str) -> Path:
    """Write ``{output_dir}/{dag_id}.py`` through a temp file and ``os.replace``."""
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / f"{dag_id}.py"
    fd, name = tempfile.mkstemp(dir=output_dir, prefix=f".{dag_id}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
        os.replace(name, target)
    except BaseException:
        Path(name).unlink(missing_ok=True)
        raise
    return target


def render_all(
    items: Sequence[tuple[Path, Config]],
    output_dir: Path,
    template_dir: Path | None = None,
    dry_run: bool = False,
) -> list[Path]:
    """Render every config, then write. Raises ConfigFailure, writing nothing, if any fails."""
    renderer = Renderer(template_dir)
    rendered: list[tuple[Config, str]] = []
    errors: list[ConfigError] = []
    for path, config in items:
        try:
            rendered.append((config, renderer.render(config, path)))
        except RenderError as exc:
            errors.append(exc.error)
    if errors:
        raise ConfigFailure(errors)
    if dry_run:
        return [output_dir / f"{config.dag_id}.py" for config, _ in rendered]
    try:
        return [write_dag(text, output_dir, config.dag_id) for config, text in rendered]
    except OSError as exc:
        raise _fail(output_dir, "output", f"cannot write: {exc}", "check the directory") from exc
