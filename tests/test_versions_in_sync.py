"""The Airflow version CI installs must match the version the demo image will use.

The compose file arrives with the demo; until then the image tag in the plan stands in for it.
When compose.yaml exists, read the tag from it here instead of docs/REFACTOR_PLAN.md.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_ci_airflow_version_matches_the_documented_image() -> None:
    ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    ci_version = re.search(r'AIRFLOW_VERSION: "([0-9.]+)"', ci)
    assert ci_version, "ci.yml needs AIRFLOW_VERSION in the dag-import job env"
    plan = (ROOT / "docs" / "REFACTOR_PLAN.md").read_text(encoding="utf-8")
    image = re.search(r"apache/airflow:([0-9.]+)-python", plan)
    assert image, "docs/REFACTOR_PLAN.md section 9 names the Airflow image tag"
    assert ci_version.group(1) == image.group(1)
