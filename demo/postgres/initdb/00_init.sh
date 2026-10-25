#!/usr/bin/env bash
# Runs once, on the first start of an empty data volume.
# Roles: airflow (metadata DB owner), erp_reader (SELECT on erp.sales only), warehouse_writer
# (owns the warehouse DB: the DAGs create bronze and ops there).
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  -v airflow_pw="$AIRFLOW_DB_PASSWORD" \
  -v reader_pw="$ERP_READER_PASSWORD" \
  -v writer_pw="$WAREHOUSE_WRITER_PASSWORD" <<'SQL'
CREATE ROLE airflow LOGIN PASSWORD :'airflow_pw';
CREATE ROLE erp_reader LOGIN PASSWORD :'reader_pw';
CREATE ROLE warehouse_writer LOGIN PASSWORD :'writer_pw';
CREATE DATABASE airflow OWNER airflow;
CREATE DATABASE warehouse OWNER warehouse_writer;
CREATE DATABASE erp;
REVOKE ALL ON DATABASE erp FROM PUBLIC;
GRANT CONNECT ON DATABASE erp TO erp_reader;
SQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname erp -f /seed/erp.sql

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname erp <<'SQL'
GRANT USAGE ON SCHEMA sales TO erp_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA sales TO erp_reader;
SQL
