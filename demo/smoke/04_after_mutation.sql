-- After mutate.sql and 2026-01-03. psql variable: watermark_before.
DO $$
DECLARE n bigint;
BEGIN
  SELECT count(*) INTO n FROM bronze.erp__orders;
  IF n <> 1009 THEN RAISE EXCEPTION 'bronze.erp__orders has % rows, expected 1009', n; END IF;

  -- append keeps history: 200 first rows plus 5 updated and 3 new customers
  SELECT count(*) INTO n FROM bronze.erp__customers;
  IF n <> 208 THEN RAISE EXCEPTION 'bronze.erp__customers has % rows, expected 208', n; END IF;

  SELECT count(*) INTO n FROM ops.ingestion_runs
  WHERE dag_id = 'erp__customers__incremental' AND run_id LIKE '%2026-01-03%'
    AND status = 'success' AND rows_extracted = 8 AND rows_loaded = 8;
  IF n <> 1 THEN RAISE EXCEPTION 'the 2026-01-03 incremental run did not load 8 rows'; END IF;

END $$;

-- The watermark advanced, and the old one became previous_watermark.
SELECT count(*) = 1 AS watermark_ok FROM ops.ingestion_state
WHERE dag_id = 'erp__customers__incremental'
  AND watermark::timestamp > :'watermark_before'::timestamp
  AND previous_watermark = :'watermark_before' \gset
\if :watermark_ok
\else
  DO $$ BEGIN RAISE EXCEPTION 'the customers watermark did not advance from the previous one'; END $$;
\endif
