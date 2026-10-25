-- After the SAME logical date (so the same run_id) ran a second time.
-- psql variables: runs_before and state_before are snapshots taken before the second run.
DO $$
DECLARE n bigint;
BEGIN
  SELECT count(*) INTO n FROM bronze.erp__orders;
  IF n <> 999 THEN RAISE EXCEPTION 'rerun: bronze.erp__orders has % rows, expected 999', n; END IF;

  SELECT count(*) INTO n FROM ops.ingestion_rejects;
  IF n <> 1 THEN RAISE EXCEPTION 'rerun: expected 1 reject, found %', n; END IF;

  SELECT count(*) INTO n FROM bronze.erp__customers;
  IF n <> 200 THEN RAISE EXCEPTION 'rerun: bronze.erp__customers has % rows, expected 200', n; END IF;

  SELECT count(*) INTO n FROM ops.ingestion_runs WHERE status = 'success';
  IF n <> 3 THEN RAISE EXCEPTION 'rerun: expected 3 success rows, found %', n; END IF;

  SELECT count(*) INTO n FROM ops.ingestion_runs;
  IF n <> 3 THEN RAISE EXCEPTION 'rerun: expected 3 rows in ops.ingestion_runs, found %', n; END IF;

  -- the CDC rerun re-reads the same window and merges it onto itself: still 100 rows
  SELECT count(*) INTO n FROM bronze.erp__products;
  IF n <> 100 THEN RAISE EXCEPTION 'rerun: bronze.erp__products has % rows, expected 100', n; END IF;

  SELECT count(*) INTO n FROM bronze.erp__products WHERE _ingest_deleted;
  IF n <> 0 THEN RAISE EXCEPTION 'rerun: expected no deleted products, found %', n; END IF;
END $$;

-- The watermark did not move.
SELECT string_agg(dag_id || '|' || coalesce(watermark, '') || '|' || coalesce(previous_watermark, '')
                  || '|' || run_id, ';' ORDER BY dag_id) AS state_now FROM ops.ingestion_state \gset
SELECT :'state_now' = :'state_before' AS state_same \gset
\if :state_same
\else
  DO $$ BEGIN RAISE EXCEPTION 'rerun changed ops.ingestion_state'; END $$;
\endif

-- And the run really executed again: every runs row has a newer finished_at.
SELECT string_agg(dag_id || '=' || finished_at::text, ';' ORDER BY dag_id) AS runs_now
FROM ops.ingestion_runs \gset
SELECT :'runs_now' <> :'runs_before' AS reexecuted \gset
\if :reexecuted
\else
  DO $$ BEGIN RAISE EXCEPTION 'the second run did not execute again (finished_at is unchanged)'; END $$;
\endif
