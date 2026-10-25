"""``ingest add rdbms fullscan|incremental``: write one validated config file."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any

import typer
from pydantic import ValidationError

from ingestion_toolkit.cli._report import print_errors
from ingestion_toolkit.columns import parse_columns_spec
from ingestion_toolkit.config_io import Format, validation_errors, write_config
from ingestion_toolkit.errors import ConfigError
from ingestion_toolkit.models.config import Config
from ingestion_toolkit.models.enums import Pattern, WatermarkType, WriteMode

app = typer.Typer(help="Write a new config file.", no_args_is_help=True)
rdbms = typer.Typer(
    help="Relational sources read through an Airflow DbApiHook.", no_args_is_help=True
)
app.add_typer(rdbms, name="rdbms")

_ARGS = Path("<arguments>")

Source = Annotated[str, typer.Option(help="Source system name, a lowercase segment such as erp.")]
Database = Annotated[str, typer.Option(help="Database name in the source.")]
Table = Annotated[str, typer.Option(help="Source table name.")]
PrimaryKey = Annotated[str, typer.Option(help="Primary key columns, comma-separated.")]
Schema = Annotated[str | None, typer.Option(help="Source schema. Default: public.")]
ConnectionId = Annotated[
    str | None, typer.Option(help="Source connection id. Default: the source name.")
]
TargetConnectionId = Annotated[
    str | None, typer.Option(help="Target connection id. Default: warehouse.")
]
Columns = Annotated[
    str | None,
    typer.Option(help='Data contract such as "id:integer!,total:numeric" (! means not null).'),
]
Mode = Annotated[
    WriteMode | None, typer.Option(help="Default: overwrite for fullscan, append for incremental.")
]
RejectRatio = Annotated[
    float | None, typer.Option(help="Share of rejected rows tolerated, 0 to 1. Default: 0.")
]
Cron = Annotated[str | None, typer.Option(help="Schedule. Default: @daily.")]
StartDate = Annotated[str | None, typer.Option(help="First schedule date. Default: 2026-01-01.")]
Tags = Annotated[str | None, typer.Option(help="Extra DAG tags, comma-separated.")]
Owner = Annotated[str | None, typer.Option(help="Owner. Default: data-engineering.")]
Description = Annotated[str | None, typer.Option(help="Free-text description.")]
OutputDir = Annotated[Path, typer.Option(help="Directory for the config file.")]
FormatOpt = Annotated[Format, typer.Option("--format", help="File format.")]
Force = Annotated[bool, typer.Option("--force", help="Overwrite an existing file.")]


def _clean(values: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in values.items() if v is not None}


def _split(value: str) -> list[str]:
    return [part.strip() for part in value.split(",")]


def _add(
    pattern: Pattern,
    *,
    source: str,
    database: str,
    schema: str | None,
    connection_id: str | None,
    table: str,
    primary_key: str,
    columns: str | None,
    watermark: dict[str, Any] | None,
    target_connection_id: str | None,
    write_mode: WriteMode | None,
    max_reject_ratio: float | None,
    cron: str | None,
    start_date: str | None,
    tags: str | None,
    owner: str | None,
    description: str | None,
    output_dir: Path,
    fmt: Format,
    force: bool,
) -> None:
    try:
        parsed = parse_columns_spec(columns) if columns is not None else None
    except ValidationError as exc:
        print_errors(validation_errors(_ARGS, exc, prefix="--columns"))
        raise typer.Exit(1) from exc
    except ValueError as exc:
        print_errors([ConfigError(_ARGS.as_posix(), "--columns", str(exc), "use name:type pairs")])
        raise typer.Exit(1) from exc
    data = {
        "version": 1,
        "source": _clean(
            {
                "name": source,
                "connection_id": connection_id,
                "database": database,
                "schema_name": schema,
            }
        ),
        "extract": _clean(
            {
                "table": table,
                "primary_key": _split(primary_key),
                "pattern": pattern.value,
                "columns": parsed,
                "watermark": watermark,
            }
        ),
        "load": _clean(
            {
                "connection_id": target_connection_id,
                "write_mode": write_mode,
                "max_reject_ratio": max_reject_ratio,
            }
        ),
        "schedule": _clean(
            {
                "cron": cron,
                "start_date": start_date,
                "tags": _split(tags) if tags is not None else None,
            }
        ),
        "metadata": _clean({"owner": owner, "description": description}),
    }
    try:
        config = Config.model_validate(data)
    except ValidationError as exc:
        print_errors(validation_errors(_ARGS, exc))
        raise typer.Exit(1) from exc
    suffix = "yml" if fmt is Format.YAML else "json"
    path = output_dir / f"{config.dag_id}.{suffix}"
    if path.exists() and not force:
        print_errors(
            [
                ConfigError(
                    path.as_posix(), "file", "file already exists", "pass --force to overwrite it"
                )
            ]
        )
        raise typer.Exit(1)
    try:
        write_config(path, config, fmt)
    except OSError as exc:
        print_errors(
            [
                ConfigError(
                    path.as_posix(),
                    "file",
                    f"cannot write file: {exc.strerror or exc}",
                    "check that --output-dir is a writable directory",
                )
            ]
        )
        raise typer.Exit(1) from exc
    typer.echo(f"wrote {path.as_posix()}")


@rdbms.command()
def fullscan(
    *,
    source: Source,
    database: Database,
    table: Table,
    primary_key: PrimaryKey,
    schema: Schema = None,
    connection_id: ConnectionId = None,
    target_connection_id: TargetConnectionId = None,
    columns: Columns = None,
    write_mode: Mode = None,
    max_reject_ratio: RejectRatio = None,
    cron: Cron = None,
    start_date: StartDate = None,
    tags: Tags = None,
    owner: Owner = None,
    description: Description = None,
    output_dir: OutputDir = Path("configs"),
    fmt: FormatOpt = Format.YAML,
    force: Force = False,
) -> None:
    """Re-read the whole table on every run."""
    _add(
        Pattern.FULLSCAN,
        source=source,
        database=database,
        schema=schema,
        connection_id=connection_id,
        table=table,
        primary_key=primary_key,
        columns=columns,
        watermark=None,
        target_connection_id=target_connection_id,
        write_mode=write_mode,
        max_reject_ratio=max_reject_ratio,
        cron=cron,
        start_date=start_date,
        tags=tags,
        owner=owner,
        description=description,
        output_dir=output_dir,
        fmt=fmt,
        force=force,
    )


@rdbms.command()
def incremental(
    *,
    source: Source,
    database: Database,
    table: Table,
    primary_key: PrimaryKey,
    watermark_column: Annotated[str, typer.Option(help="Column used to find new rows.")],
    watermark_type: Annotated[
        WatermarkType, typer.Option(help="Type of the watermark column.")
    ] = WatermarkType.DATETIME,
    initial_value: Annotated[
        str | None, typer.Option(help="Lower bound for the first run. Default: none.")
    ] = None,
    schema: Schema = None,
    connection_id: ConnectionId = None,
    target_connection_id: TargetConnectionId = None,
    columns: Columns = None,
    write_mode: Mode = None,
    max_reject_ratio: RejectRatio = None,
    cron: Cron = None,
    start_date: StartDate = None,
    tags: Tags = None,
    owner: Owner = None,
    description: Description = None,
    output_dir: OutputDir = Path("configs"),
    fmt: FormatOpt = Format.YAML,
    force: Force = False,
) -> None:
    """Read only rows whose watermark column is above the last value seen."""
    _add(
        Pattern.INCREMENTAL,
        source=source,
        database=database,
        schema=schema,
        connection_id=connection_id,
        table=table,
        primary_key=primary_key,
        columns=columns,
        watermark=_clean(
            {
                "column": watermark_column,
                "type": watermark_type.value,
                "initial_value": initial_value,
            }
        ),
        target_connection_id=target_connection_id,
        write_mode=write_mode,
        max_reject_ratio=max_reject_ratio,
        cron=cron,
        start_date=start_date,
        tags=tags,
        owner=owner,
        description=description,
        output_dir=output_dir,
        fmt=fmt,
        force=force,
    )
