"""Load rendered DAG files with Airflow's DagBag and fail on import errors or DAG warnings.

Runs inside an environment that has Apache Airflow installed (not the project's own):

    python scripts/check_dag_imports.py DAG_FOLDER --expected 2

Exit 0 when every file imports, the DAG count matches and no warning comes from a DAG file.
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
import warnings
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dag_folder", type=Path)
    parser.add_argument("--expected", type=int, required=True, help="number of DAGs to find")
    args = parser.parse_args()
    folder = args.dag_folder.resolve()

    with tempfile.TemporaryDirectory() as home:
        os.environ["AIRFLOW_HOME"] = home
        os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
        os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = f"sqlite:///{home}/airflow.db"
        from airflow.dag_processing.dagbag import DagBag

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            bag = DagBag(dag_folder=folder, bundle_path=folder, bundle_name="rendered")

    problems: list[str] = []
    for path, trace in bag.import_errors.items():
        problems.append(f"import error in {path}:\n{trace}")
    if len(bag.dags) != args.expected:
        problems.append(f"expected {args.expected} DAGs, found {len(bag.dags)}")
    for item in caught:
        if Path(item.filename).resolve().is_relative_to(folder):
            problems.append(
                f"{item.category.__name__} in {item.filename}:{item.lineno}: {item.message}"
            )

    for dag_id, dag in sorted(bag.dags.items()):
        print(f"{dag_id}: {', '.join(sorted(dag.task_ids))}")
    print(f"{len(bag.dags)} DAGs, {len(bag.import_errors)} import errors, {len(problems)} problems")
    for problem in problems:
        print(f"FAIL: {problem}", file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
