# Databricks validation snapshot — 2026-10-11

This is a read-only check of the connected `carewatch.pipeline` Unity Catalog tables through Databricks SQL. It records observed results, not simulated notebook output. Query timestamps in the source log are UTC. The repository still needs saved Databricks notebook outputs or a short screen recording for the full rubric demonstration, especially schema drift.

## Deployed tables and types

`SHOW TABLES IN carewatch.pipeline` returned all four Bronze tables, all four Silver tables, `source_file_manifest`, `pipeline_execution_logs`, `schema_drift_log`, `bronze_quarantine`, and `silver_quarantine`.

`carewatch.information_schema.columns` confirmed these representative Silver types: `silver_deficiency.survey_date DATE`, `silver_deficiency.inspection_cycle INT`, `silver_penalty.fine_amount DECIMAL`, `silver_facility.overall_rating INT`, `silver_mds_quality.q1_measure_score DOUBLE`, and `load_timestamp TIMESTAMP` on all four Silver tables. The complete declared schemas are in `src/carewatch/schemas.py` and the generated README models.

## Row and key checks

The following read-only aggregates ran against the live tables:

```sql
SELECT count(*) AS rows,
       count(DISTINCT deficiency_key) AS distinct_keys,
       count_if(load_timestamp IS NULL) AS null_load_timestamps
FROM carewatch.pipeline.silver_deficiency;
```

The corresponding query was repeated for `penalty_key`, `facility_key`, and `mds_key`. Results:

- `silver_deficiency`: 418,911 rows, 418,911 distinct keys, 0 null load timestamps.
- `silver_penalty`: 15,419 rows, 15,419 distinct keys, 0 null load timestamps.
- `silver_facility`: 14,690 rows, 14,690 distinct keys, 0 null load timestamps.
- `silver_mds_quality`: 249,730 rows, 249,730 distinct keys, 0 null load timestamps.

The same null check on the four Bronze tables returned 0 in every table. Their row counts were 419,034 deficiencies, 15,427 penalties, 29,380 provider records, and 499,460 MDS records. Bronze includes separate full and incremental source batches, so those totals are not expected to equal the current Silver entity counts.

## Execution logs and repeat MERGE

```sql
SELECT pipeline_layer, load_type, status, count(*) AS runs,
       sum(rows_inserted) AS inserted, sum(rows_updated) AS updated,
       sum(rows_quarantined) AS quarantined
FROM carewatch.pipeline.pipeline_execution_logs
GROUP BY pipeline_layer, load_type, status
ORDER BY pipeline_layer, load_type, status;
```

The result includes `CMS-to-Landing`, `Raw-to-Bronze`, and `Bronze-to-Silver` logs, each with full and incremental records. The log schema includes `parameter_processed`, `start_time`, `end_time`, `status`, `rows_inserted`, `rows_updated`, and `load_timestamp`.

A targeted `Bronze-to-Silver` provider full-batch rerun logged `reprocess=true` for batch `14c281a6e8c6c099c0dbad46` at `2026-10-10 21:58:35 UTC`. It read 14,690 Bronze rows and reported 0 inserts, 0 updates, and `SUCCESS`. The prior run of that batch reported 14,690 inserts. A later full deficiency rerun likewise read 418,947 rows with 0 inserts and 0 updates. These are log-level evidence of no-op repeats; the unique-key query above checks the current target state.

Incremental `Bronze-to-Silver` runs succeeded for all four datasets. The two event-feed batches read 87 deficiency rows and 8 penalty rows, each with 0 target inserts or updates because the events were already represented in the current Silver snapshot. The provider and MDS snapshot refreshes read 14,690 and 249,730 rows, respectively, also with 0 target changes.

## Data quality and remaining evidence

The first full deficiency merge logged 418,911 accepted rows and 36 quarantined rows. The live `silver_quarantine` table has those 36 rows, all flagged `CONDITIONAL:correction_date_required`. Current `bronze_quarantine` and `schema_drift_log` row counts are 0. This proves one Silver bad-record path ran; it does **not** demonstrate added-column or changed-type schema drift.

`source_file_manifest` records four successful full bulk snapshots totaling 698,786 source rows, two successful API date-window incremental pages totaling 95 rows, and two snapshot-diff incremental records marked `SKIPPED_ALREADY_ACQUIRED` because the source content was unchanged.

## What remains for the rubric

- Save executed Databricks notebook outputs in the repo, or provide the requested short screen recording. The existing acquisition notebook has no saved outputs.
- Run a controlled schema-drift example through Raw-to-Bronze and capture `schema_drift_log`, quarantine or evolved Bronze schema, and a successful good-file batch.
- Capture a backfill run with the widget values visible. The logged `reprocess=true` batch proves the historical batch can be reprocessed but does not show the full parameterized workflow on screen.
- The reviewed project was pushed to `https://github.com/MhassaanK68/us-healthcare-data-warehouse`; an unauthenticated HTTP request to the repository page returned `200`.
