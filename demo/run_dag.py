"""Run one rendered DAG for a logical date, with a run_id fixed by that date.

`airflow dags test DAG DATE` in Airflow 3.3.2 builds the run_id from the wall clock
(`manual__<now>`), so running the same date twice never repeats a run_id and cannot show how the
DAG treats a re-run of the same run. This does what that command does (load the DAG from the
dags folder, `DAG.test`, exit 1 when the run failed), with `run_after` set to the logical date, so
the run_id is always `manual__<date>T00:00:00+00:00`.

Task retries are set to 0: a broken run fails at once instead of waiting out retry delays.

Inside the Airflow container: `python - DAG_ID YYYY-MM-DD < demo/run_dag.py`
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime

from airflow.utils.cli import get_bagged_dag
from airflow.utils.providers_configuration_loader import providers_configuration_loaded


@providers_configuration_loaded
def main(dag_id: str, day: str) -> int:
    when = datetime.fromisoformat(day).replace(tzinfo=UTC)
    dag = get_bagged_dag(bundle_names=None, dag_id=dag_id)
    for task in dag.tasks:
        task.retries = 0
    run = dag.test(logical_date=when, run_after=when)
    print(f"{dag_id} {run.run_id}: {run.state}")
    return 1 if run.state != "success" else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1], sys.argv[2]))
