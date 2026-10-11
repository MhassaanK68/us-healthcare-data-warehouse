# Phase 2 submission recording walkthrough

Use a Databricks Git folder synced to this repository's latest `main`. Record one roughly five-minute screen video, or save executed notebook outputs and screenshots in `docs/evidence/` and push them. The steps below use the current `carewatch.pipeline` run history for the repeat/backfill and a separate `carewatch.phase2_demo` namespace for synthetic drift data. Do not enter the address-hash salt itself in any widget or recording; enter only the Databricks secret scope and key names already used for the working Silver runs.

## 1. Show the implementation and schemas

In Databricks, open `src/carewatch/schemas.py` and show the explicit `StructType` definitions. Open `src/carewatch/bronze.py` and show `.schema(schema)` before `.load(...)`. Open `src/carewatch/silver.py` at `merge_silver_batch` and show the Delta `MERGE` with matched updates and unmatched inserts. Briefly show the README Bronze/Silver model section.

In separate Databricks SQL cells, run:

```sql
DESCRIBE TABLE carewatch.pipeline.bronze_nh_provider_info;
```

```sql
DESCRIBE TABLE carewatch.pipeline.silver_facility;
```

Show `load_timestamp` and the typed Silver columns.

## 2. Show a historical batch replay and no-op MERGE

Use the existing historical Provider Information full batch:

- `acquisition_run_id`: `d7252e0e-7df8-463d-b377-f6fd7f6c5221`
- `batch_id`: `14c281a6e8c6c099c0dbad46`

First record the Silver state:

```sql
SELECT count(*) AS rows, count(DISTINCT facility_key) AS distinct_keys,
       count_if(load_timestamp IS NULL) AS null_load_timestamps
FROM carewatch.pipeline.silver_facility;
```

Expected from the 2026-10-11 validation snapshot: 14,690 rows, 14,690 distinct keys, and 0 null timestamps. A newer CMS run may legitimately change the count; the key check and the before/after equality matter.

Run `notebooks/02_raw_to_bronze.py` with these widgets:

- `dataset=provider_info`
- `load_type=full`
- `acquisition_run_id=d7252e0e-7df8-463d-b377-f6fd7f6c5221`
- `source_path=` empty
- `reprocess=true`
- `catalog=carewatch`, `schema=pipeline`

This replays the registered older file; it does not download CMS data. Its result should show the historical batch and a successful Raw-to-Bronze log.

Run `notebooks/03_bronze_to_silver.py` with:

- `dataset=provider_info`, `load_type=full`
- `batch_ids=14c281a6e8c6c099c0dbad46`
- `acquisition_run_id=` empty, `ingest_from=` empty, `ingest_to=` empty
- `reprocess=true`, `allow_soft_deletes=false`
- `address_secret_scope` and `address_secret_key`: the existing secret **names**, not the salt value
- `catalog=carewatch`, `schema=pipeline`

Run the Silver count query again. Show that the row count and distinct-key count did not change. Then run:

```sql
SELECT pipeline_layer, dataset, load_type, parameter_processed,
       start_time, end_time, status, rows_read, rows_inserted, rows_updated
FROM carewatch.pipeline.pipeline_execution_logs
WHERE batch_id = '14c281a6e8c6c099c0dbad46'
ORDER BY start_time DESC
LIMIT 6;
```

Point to the new `Bronze-to-Silver` row with `rows_inserted=0` and `rows_updated=0`. This is both a parameterized historical batch replay and an idempotent MERGE rerun. If a newer current snapshot changed the provider row hashes, the old backfill still must not overwrite newer rows; inspect the log and count instead of assuming exact numbers.

## 3. Create and run the isolated schema-drift fixture

Run `notebooks/00_setup_tables.py` with `catalog=carewatch`, `schema=phase2_demo`, and `volume=landing`. This creates separate demo control and Silver tables. Do not use `schema=pipeline` for this fixture.

Run `notebooks/98_prepare_drift_demo.py` with the same three widgets. It creates a two-row synthetic Provider Information CSV inside `/Volumes/carewatch/phase2_demo/landing` and registers its exact hash and path in the demo manifest. One row has a valid rating, one has a non-numeric rating, and the file has an extra source column. **Copy the printed `acquisition_run_id` and `batch_id`** for the next steps. This is a test fixture, not a CMS full acquisition.

Run `notebooks/02_raw_to_bronze.py` with:

- `dataset=provider_info`, `load_type=full`
- `acquisition_run_id=<the new printed acquisition_run_id>`
- `source_path=` empty, `reprocess=false`
- `catalog=carewatch`, `schema=phase2_demo`

The file result should show 2 Bronze rows and `phase2_demo_added_column` in `added_columns`. Run these SQL queries, replacing the placeholder with the printed batch ID:

```sql
SELECT drift_type, column_name, detail
FROM carewatch.phase2_demo.schema_drift_log
WHERE batch_id = '<new batch_id>';

SELECT count(*) AS bronze_rows,
       count_if(phase2_demo_added_column IS NOT NULL) AS rows_with_new_column
FROM carewatch.phase2_demo.bronze_nh_provider_info
WHERE _batch_id = '<new batch_id>';
```

Expect one `ADDED_COLUMN` drift log entry and 2 Bronze rows with the added column.

Run `notebooks/03_bronze_to_silver.py` with:

- `dataset=provider_info`, `load_type=full`
- `batch_ids=<the new printed batch_id>`
- `acquisition_run_id=` empty, `ingest_from=` empty, `ingest_to=` empty
- `reprocess=false`, `allow_soft_deletes=false`
- the same `address_secret_scope` and `address_secret_key` names
- `catalog=carewatch`, `schema=phase2_demo`

Then run:

```sql
SELECT count(*) AS accepted_rows
FROM carewatch.phase2_demo.silver_facility
WHERE source_batch_id = '<new batch_id>';

SELECT concat_ws(', ', failed_rules) AS failed_rules, count(*) AS rejected_rows
FROM carewatch.phase2_demo.silver_quarantine
WHERE source_batch_id = '<new batch_id>'
GROUP BY concat_ws(', ', failed_rules);

SELECT pipeline_layer, status, rows_read, rows_inserted,
       rows_updated, rows_quarantined
FROM carewatch.phase2_demo.pipeline_execution_logs
WHERE batch_id = '<new batch_id>'
ORDER BY start_time;
```

Expect 1 accepted Silver row, 1 quarantined row with a `SAFE_CAST` rating failure, and successful or partially quarantined per-layer logs. This demonstrates that an added column reaches Bronze while a bad typed value is quarantined and a valid row continues to Silver.

## 4. Show the run history and submit

Finish with:

```sql
SELECT pipeline_layer, load_type, status, count(*) AS runs
FROM carewatch.pipeline.pipeline_execution_logs
GROUP BY pipeline_layer, load_type, status
ORDER BY pipeline_layer, load_type, status;
```

Show full and incremental records for `CMS-to-Landing`, `Raw-to-Bronze`, and `Bronze-to-Silver`. If you save notebook outputs instead of a video, export or screenshot each result above and commit it under `docs/evidence/`. Confirm the GitHub repository link opens in a private browser window, and submit `https://github.com/MhassaanK68/us-healthcare-data-warehouse` plus the recording if using video proof.
