-- After 2026-01-02 with no source change. psql variable: state_before.
DO $$
DECLARE n bigint;
BEGIN
  SELECT count(*) INTO n FROM bronze.erp__orders;
  IF n <> 999 THEN RAISE EXCEPTION 'bronze.erp__orders has % rows, expected 999', n; END IF;

  SELECT count(*) INTO n FROM bronze.erp__customers;
  IF n <> 200 THEN RAISE EXCEPTION 'bronze.erp__customers has % rows, expected 200', n; END IF;

  SELECT count(*) INTO n FROM ops.ingestion_runs
  WHERE dag_id = 'erp__customers__incremental' AND status = 'success'
    AND run_id LIKE '%2026-01-02%' AND rows_extracted = 0;
  IF n <> 1 THEN RAISE EXCEPTION 'the 2026-01-02 incremental run did not extract 0 rows'; END IF;

  SELECT count(*) INTO n FROM ops.ingestion_runs WHERE status = 'success';
  IF n <> 6 THEN RAISE EXCEPTION 'expected 6 success rows, found %', n; END IF;

  SELECT count(*) INTO n FROM bronze.erp__products;
  IF n <> 100 THEN RAISE EXCEPTION 'bronze.erp__products has % rows, expected 100', n; END IF;

  SELECT count(*) INTO n FROM ops.ingestion_runs
  WHERE dag_id = 'erp__products__cdc' AND status = 'success'
    AND run_id LIKE '%2026-01-02%' AND rows_extracted = 0;
  IF n <> 1 THEN RAISE EXCEPTION 'the 2026-01-02 CDC run did not extract 0 rows'; END IF;
END $$;

SELECT string_agg(dag_id || '|' || coalesce(watermark, '') || '|' || coalesce(previous_watermark, '')
                  || '|' || run_id, ';' ORDER BY dag_id) AS state_now FROM ops.ingestion_state \gset
SELECT :'state_now' = :'state_before' AS state_same \gset
\if :state_same
\else
  DO $$ BEGIN RAISE EXCEPTION 'an empty incremental or CDC run changed ops.ingestion_state'; END $$;
\endif
