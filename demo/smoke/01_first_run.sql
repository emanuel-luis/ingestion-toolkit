-- After both DAGs ran once at 2026-01-01.
DO $$
DECLARE n bigint;
BEGIN
  SELECT count(*) INTO n FROM bronze.erp__orders;
  IF n <> 999 THEN RAISE EXCEPTION 'bronze.erp__orders has % rows, expected 999', n; END IF;

  SELECT count(*) INTO n FROM ops.ingestion_rejects WHERE dag_id = 'erp__orders__fullscan';
  IF n <> 1 THEN RAISE EXCEPTION 'expected 1 reject for orders, found %', n; END IF;

  SELECT count(*) INTO n FROM ops.ingestion_rejects;
  IF n <> 1 THEN RAISE EXCEPTION 'expected 1 reject in total, found %', n; END IF;

  SELECT count(*) INTO n FROM bronze.erp__customers;
  IF n <> 200 THEN RAISE EXCEPTION 'bronze.erp__customers has % rows, expected 200', n; END IF;

  SELECT count(*) INTO n FROM ops.ingestion_runs WHERE status = 'success';
  IF n <> 2 THEN RAISE EXCEPTION 'expected 2 success rows in ops.ingestion_runs, found %', n; END IF;

  SELECT count(*) INTO n FROM ops.ingestion_runs;
  IF n <> 2 THEN RAISE EXCEPTION 'expected 2 rows in ops.ingestion_runs, found %', n; END IF;

  SELECT count(*) INTO n FROM ops.ingestion_state
  WHERE dag_id = 'erp__customers__incremental' AND watermark IS NOT NULL;
  IF n <> 1 THEN RAISE EXCEPTION 'the customers watermark was not stored'; END IF;
END $$;
