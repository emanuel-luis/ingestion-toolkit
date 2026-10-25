# CDC contract

What a source has to provide for the `cdc` pattern, what the rendered DAG does with it, and where
it can go wrong. The pattern reads a feed of changes and merges it into a bronze table that
mirrors the current state of the source. The landing partitions keep the change history; bronze
does not.

```yaml
extract:
  pattern: cdc
  cdc:
    mechanism: generic        # generic | rowversion
    sequence_column: version_stamp
    sequence_type: integer    # generic only: integer | datetime
    change_table: null        # generic only in practice: read this table or view instead
    operation_column: null    # which rows are deletes
    delete_values: [D]        # values of operation_column that mean delete
load:
  write_mode: merge           # the only mode for cdc
  merge_keys: [id]            # defaults to the primary key
```

## Two mechanisms

| | generic | rowversion |
| --- | --- | --- |
| Source | any `DbApiHook` database | SQL Server (`source_type: mssql`) |
| Sequence column | a number or a timestamp that grows with every change | the table's `rowversion` column |
| Extract | `WHERE seq > bound ORDER BY seq` | `WHERE rv > CONVERT(binary(8), bound, 1) AND rv < MIN_ACTIVE_ROWVERSION() ORDER BY rv` |
| Stored watermark | the highest sequence extracted, as text | the highest rowversion, as `0x` plus 16 uppercase hex digits |
| Bronze type of the sequence column | integer or a time type, as `sequence_type` says | text |

### What a generic source must provide

- One row per change, or one row per entity with a sequence that moves on every update. The
  table can be the table itself (when it carries a sequence) or a change table or view set with
  `change_table`, in the same database and schema. The primary key (and the merge keys) must
  identify the entity, not the change.
- A sequence column that increases with every change to a row: a bigint from a database
  sequence set by a trigger, a log sequence number, or an `updated_at` that is always touched.
- Two changes to the same key must not share a sequence value. Ties are applied in the order the
  rows arrive, so an update and a delete of one key in the same transaction, both stamped with
  that transaction's `now()`, can end with the update winning. A database sequence or a log
  position is unique; a transaction timestamp is not.
- The sequence column must be in the data contract when there is one, with a compatible type:
  `integer` for an integer sequence, `timestamp`, `timestamptz` or `date` for a datetime one.
  The extract reads exactly the columns of the contract, so list the operation column too.
- Values must be comparable with the parameter the DAG binds: an integer, or a datetime parsed
  from ISO text.

### What a rowversion source must provide

- A SQL Server table with a `rowversion` (or `timestamp`) column. The config lists it as a text
  column: the driver returns it as bytes and the landing file stores it as `0x...` hex.
- The reader needs only SELECT. `MIN_ACTIVE_ROWVERSION()` is available to any login.
- Deletes are not visible in the table. Use a soft-delete column as the operation column, or do
  not use this mechanism for tables that delete rows. `ingest add mssql cdc` has no
  `--operation-column` option: set `extract.cdc.operation_column` and `delete_values` in the
  config file, then run `ingest validate`.

This path is covered by a golden file and the DagBag import check only: the demo has no SQL
Server.

## Merge semantics

The load upserts every extracted row into bronze:

```sql
INSERT INTO bronze.erp__products AS t (...) VALUES (...)
ON CONFLICT ("id") DO UPDATE SET ... WHERE t."version_stamp"::bigint <= EXCLUDED."version_stamp"::bigint
```

- Bronze has a primary key on the merge keys, so the merge keys must not be null; they and the
  sequence column are stored as not null, and a row with a null in one of them is a contract
  reject.
- An older version never replaces a newer one. Because of the `WHERE` guard it does not matter
  in which order the rows of a batch arrive: the highest sequence per key wins, and the same
  holds across runs. Equal sequences are re-applied, so re-running a load changes nothing.
- Repeated keys within a run are expected (several changes to one entity) and are not an error.
  The duplicate-key check applies to the other write modes only.
- The sequence is compared after a cast: integer as bigint, datetime as timestamptz, rowversion
  as text under the C collation (fixed-width uppercase hex sorts like the binary value). The
  cast makes it work when there is no column contract and bronze columns are text.
- `rows_loaded` counts the valid rows sent to the upsert, including versions that lost against a
  newer row already in bronze.
- Bronze also gets `_ingest_run_id`, `_ingest_loaded_at` and `_ingest_deleted`, which says
  whether the latest version of the row is a delete.

## Deletes

- With `operation_column`, a row whose value (compared as text) is in `delete_values` is merged
  with `_ingest_deleted = true`. The row stays in bronze, so downstream models decide what a
  delete means. A later non-delete change to the same key sets the flag back to false.
- A delete row must carry non-null values for the primary key, the sequence column and every
  column the contract marks not null, or it is rejected like any other row. Source change tables
  usually repeat the old row for a delete, which satisfies this.
- Without `operation_column`, nothing is ever marked deleted. Hard deletes in the source are
  invisible to a table that is read by sequence. Use a change table that records deletes, or a
  periodic fullscan to reconcile.

## Ordering and late commits

- Generic: a strict `>` on the stored high mark loses a change that commits after a higher one
  was already extracted. This happens when the sequence is assigned at statement time, not at
  commit time: transaction A takes sequence 10, transaction B takes 11 and commits first, the
  extract stores 11, and A's row (10) is never read. It is also the case for an `updated_at`
  set with `now()` in a long transaction. Mitigations: keep source transactions short, have the
  source assign the sequence at commit, or use a database feature that orders by commit (a log
  position), or run a periodic fullscan.
- Rowversion: SQL Server assigns a rowversion at write time, so the same race exists in
  principle. The extract closes it with the upper bound `MIN_ACTIVE_ROWVERSION()`, the lowest
  rowversion that may still be uncommitted: rows at or above it are left for the next run.
  A long-running open transaction therefore delays newer rows, but loses none.
- Datetimes without a time zone compare in the source's time zone.

## State and re-runs

CDC uses the same state as the incremental pattern (`ops.ingestion_state`, advanced in the
transaction that writes bronze): a retry of the same run re-reads its own window, an empty
extract leaves the watermark alone, the out-of-order guard refuses to re-run a run a later one
has superseded, and the stale-load guard refuses clearing only `load_bronze` of an older run.
The merge is idempotent on top of that, so re-reading a window never duplicates rows.

The first run has no lower bound and reads the whole feed (for rowversion, everything below
`MIN_ACTIVE_ROWVERSION()`). The CDC block has no `initial_value`.

## Not covered

- The log-based mechanisms of a database (SQL Server CDC tables, Debezium topics) are not read
  directly. Expose them as a table or view with a sequence column and use `change_table`.
- Schema drift fails the run, as in the other patterns.
- Landing keeps one partition per run on the worker's disk, so the change history lasts as long
  as you keep those files.
