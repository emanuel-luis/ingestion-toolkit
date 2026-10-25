\echo 'bronze row counts'
SELECT 'bronze.erp__orders' AS "table", count(*) AS "rows" FROM bronze.erp__orders
UNION ALL
SELECT 'bronze.erp__customers', count(*) FROM bronze.erp__customers
UNION ALL
SELECT 'bronze.erp__products', count(*) FROM bronze.erp__products
UNION ALL
SELECT 'ops.ingestion_rejects', count(*) FROM ops.ingestion_rejects;
\echo 'runs'
SELECT dag_id, status, rows_extracted, rows_loaded, rows_rejected
FROM ops.ingestion_runs ORDER BY dag_id, run_id;
\echo 'rejected rows'
SELECT dag_id, row_number, reason FROM ops.ingestion_rejects;
