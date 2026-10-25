"""Entry point for ``python -m ingestion_toolkit``."""

from __future__ import annotations

from ingestion_toolkit.cli.main import app

if __name__ == "__main__":
    app(prog_name="ingest")
