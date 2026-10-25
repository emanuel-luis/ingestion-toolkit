"""The Airflow version CI installs must match the version the demo image will use.

The tag is read from compose.yaml, the file the demo actually runs.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_ci_airflow_version_matches_the_documented_image() -> None:
    ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    ci_version = re.search(r'AIRFLOW_VERSION: "([0-9.]+)"', ci)
    assert ci_version, "ci.yml needs AIRFLOW_VERSION in the dag-import job env"
    compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")
    image = re.search(r"image: apache/airflow:([0-9.]+)-python", compose)
    assert image, "compose.yaml needs the apache/airflow image with a version-pythonX.Y tag"
    assert ci_version.group(1) == image.group(1)
