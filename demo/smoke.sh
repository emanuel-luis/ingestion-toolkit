#!/usr/bin/env bash
# End-to-end check of the rendered DAGs against the demo stack. Starts from an empty stack, so
# it can be run again at any time. Leaves the stack running: stop it with `make down`.
set -euo pipefail
cd "$(dirname "$0")/.."

ORDERS=erp__orders__fullscan
CUSTOMERS=erp__customers__incremental

[ -f .env ] || cp .env.example .env
compose() { docker compose "$@"; }
# Assertion files go in through stdin, so the container needs no extra mount.
psql_wh() { compose exec -T postgres psql -U postgres -d warehouse -qAt -v ON_ERROR_STOP=1 "$@"; }
check() { local file=$1; shift; echo "check: $file"; psql_wh "$@" -f - < "demo/smoke/$file"; }
scalar() { psql_wh -c "$1"; }
run_dag() {
  echo "run: $1 at $2"
  compose exec -T airflow airflow dags test "$1" "$2"
}
run_both() { run_dag "$ORDERS" "$1"; run_dag "$CUSTOMERS" "$1"; }
state_snapshot() {
  scalar "SELECT string_agg(dag_id || '|' || coalesce(watermark, '') || '|' || coalesce(previous_watermark, '') || '|' || run_id, ';' ORDER BY dag_id) FROM ops.ingestion_state"
}

# Clean start: no old volume, no old landing partitions, fresh render.
compose down -v --remove-orphans
find demo/data/landing -mindepth 1 ! -name .gitkeep -delete
compose up -d --wait --wait-timeout 300

# 1. First run.
run_both 2026-01-01
check 01_first_run.sql

# 2. The same logical date again re-executes the same run_id: nothing may change.
state_before=$(state_snapshot)
runs_before=$(scalar "SELECT string_agg(dag_id || '=' || finished_at::text, ';' ORDER BY dag_id) FROM ops.ingestion_runs")
run_both 2026-01-01
check 02_same_run_again.sql -v "state_before=$state_before" -v "runs_before=$runs_before"

# 3. Next day, source unchanged: the incremental run extracts nothing.
state_before=$(state_snapshot)
run_both 2026-01-02
check 03_next_day_empty.sql -v "state_before=$state_before"

# 4. Source changes, next day: fullscan sees the new orders, incremental the changed customers.
watermark_before=$(scalar "SELECT watermark FROM ops.ingestion_state WHERE dag_id = '$CUSTOMERS'")
compose exec -T postgres psql -U postgres -d erp -q -v ON_ERROR_STOP=1 -f /seed/mutate.sql
run_both 2026-01-03
check 04_after_mutation.sql -v "watermark_before=$watermark_before"

echo "smoke: ok"
