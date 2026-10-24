# Phase 3 Steps 1–3: Copy, Deploy, Run, and Prove

This runbook covers the work assigned to you:

1. Lock the Gold dimensional model.
2. Implement and prove the Gold transformations.
3. Attach Gold to the existing pipeline and audit trail.

The code is complete. Observed run IDs, audit values, and reconciliation counts from this Databricks workspace are saved in [`docs/evidence/phase3/deployment_validation_2026-10-24.md`](evidence/phase3/deployment_validation_2026-10-24.md). Capture screenshots of those runs in your own workspace for the final report.

## 1. Files in this package

### New files

| File | Why it exists |
|---|---|
| `src/carewatch/gold.py` | The reusable Gold implementation: schemas, keys, transformations, validations, nine idempotent Delta MERGEs, aggregate reconciliation, and two sanitized views. |
| `notebooks/04_silver_to_gold.ipynb` | Thin Databricks orchestration. It receives widgets, opens one audit context, calls the reusable Gold refresh, and displays real Delta metrics/counts. |
| `deployment/carewatch_daily_incremental.job.json` | Versioned four-task Git-backed Job, daily UTC schedule, parameters, success dependencies, and secret **names**. |
| `docs/gold_data_dictionary.md` | The locked dimensional contract: grains, columns, PK/FK relationships, ER diagram, nullability, derivations, and BI meanings. |
| `docs/evidence/phase3/deployment_validation_2026-10-24.md` | Observed Job runs, audit metrics, production fact reconciliation, and isolated empty/deletion regression evidence. |

### Changed files

| File | What changed |
|---|---|
| `src/carewatch/audit.py` | Adds `Silver-to-Gold`, optional shared `pipeline_run_id`, and nonnegative `rows_aggregated`. Existing Phase 2 rows remain valid after the physical migration. |
| `notebooks/00_setup_tables.ipynb` | Creates or verifies the nine typed Gold Delta tables as well as existing control/Silver tables. |
| `notebooks/01_acquire_cms.ipynb` | Accepts the Job correlation ID and includes it in normal, skipped, and failed acquisition audit rows. |
| `notebooks/02_raw_to_bronze.ipynb` | Accepts/passes the Job correlation ID and raises when any file/reconciliation is failed or partial, blocking downstream tasks. |
| `notebooks/03_bronze_to_silver.ipynb` | Accepts/passes the same Job correlation ID to each Silver audit attempt. |
| `README.md` | Adds deployment order, Job wiring, backfill rules, audit query, and the Power BI refresh gate. |

## 2. Copy into your local branch

Make sure VS Code is open at your existing repository folder, for example:

```powershell
cd "C:\Users\Laptop Expert\AppData\Local\Projects\us-healthcare-data-warehouse"
git branch --show-current
git status
```

The first command should show the Phase 3 branch you already created. If it does not, switch using its exact name as shown by `git branch`.

Extract the supplied ZIP into that repository root and allow it to merge folders/replace the listed files. It contains only the Phase 3 files listed above; it does not contain `.git` or overwrite unrelated project files.

Then inspect exactly what changed:

```powershell
git status --short
git diff --stat
git diff --check
```

Open the changed files in VS Code and read this runbook plus the Gold dictionary. The notebook files are valid Databricks/Jupyter JSON and should not be manually pasted cell by cell.

## 3. Commit the code before running it

```powershell
git add README.md bi docs notebooks src
git commit -m "Implement Phase 3 Gold model and pipeline integration"
git push -u origin phase-1-3
```

Do not add tokens, secrets, raw CMS downloads, or quarantine records. Your friend can review this branch or you can create a pull request later.

## 4. Perform the one-time audit migration

The physical table must be migrated before the updated notebooks write to it. In a Databricks SQL cell, first record the old count and schema:

```sql
SELECT COUNT(*) AS rows_before
FROM carewatch.pipeline.pipeline_execution_logs;

DESCRIBE TABLE carewatch.pipeline.pipeline_execution_logs;
```

If neither new field exists, run exactly once:

```sql
ALTER TABLE carewatch.pipeline.pipeline_execution_logs
ADD COLUMNS (pipeline_run_id STRING, rows_aggregated BIGINT);
```

If one was already added, add only the missing field. Never drop/recreate the audit table. Check preservation:

```sql
SELECT COUNT(*) AS rows_after,
       COUNT_IF(pipeline_run_id IS NOT NULL) AS old_or_new_rows_with_job_id,
       COUNT_IF(rows_aggregated IS NOT NULL) AS rows_with_aggregation_metric
FROM carewatch.pipeline.pipeline_execution_logs;
```

Immediately after migration, historical rows should normally have null in both new fields and `rows_after` must equal `rows_before`.

## 5. Create the Gold tables

Pull/sync your branch into the Databricks Git folder, then run `notebooks/00_setup_tables.ipynb` with:

- `catalog = carewatch`
- `schema = pipeline`
- `volume = landing`

It should report nine `GOLD_DELTA_TABLE` objects as `CREATED` on the first execution or `VERIFIED` later. A schema mismatch is meant to fail rather than silently destroy a populated table.

## 6. Run Gold manually once for initial validation

Run `notebooks/04_silver_to_gold.ipynb` once after setup using the normal catalog/schema, `load_type=full`, and an empty `pipeline_run_id`. This is a setup/evidence check only; normal operation must use the Job chain.

Expected behavior:

- All four Silver tables are read in full.
- All nine staged outputs pass PK, FK, count, period, privacy, and aggregate checks before writes start.
- Five dimensions, then three facts, then the monthly aggregate are merged.
- The two operational views are created.
- The displayed result contains one Delta metrics row and one count row per Gold table.
- One `Silver-to-Gold` audit row is written with a `gold:manual:<uuid>` batch ID.

Run it a second time without changing Silver. All `rows_inserted`, `rows_updated`, and `rows_deleted` should be zero. Sample stable keys before and after and prove their `load_timestamp` is unchanged.

## 7. Deploy the four-task Databricks Job

Deploy the versioned settings in `deployment/carewatch_daily_incremental.job.json` after the audit migration and Gold setup. The Job uses the Git repository's `codex/fix-phase3-review` branch and snapshots one commit for all tasks at run start. Its schedule is daily at 03:00 UTC with one concurrent run. After merging this branch into `main`, change the Job Git reference and this definition to `main` together. Verify that the Job's Git reference points to the commit containing this implementation before enabling its schedule.

The definition enforces these success dependencies:

```text
01_acquire_cms
      ↓
02_raw_to_bronze
      ↓
03_bronze_to_silver
      ↓
04_silver_to_gold
```

The Job-level parameters are:

| Name | Default |
|---|---|
| `catalog` | `carewatch` |
| `schema` | `pipeline` |
| `load_type` | `incremental` |

Databricks pushes those three parameters into the notebook widgets. Every task also receives this dynamic task parameter:

```text
pipeline_run_id={{job.run_id}}
```

The definition fixes the existing acquisition/Bronze/Silver parameters required by Phase 2. On the Silver task, it passes the existing secret **scope and key names**, never the secret value. Override the Job's `load_type` to `full` only for the first complete bootstrap.

Important behavior:

- A failed acquisition fails task 1.
- A failed or partial Bronze reconciliation now fails task 2.
- A failed Silver batch fails task 3.
- Gold starts only after task 3 succeeds.
- Power BI Import refresh starts only after task 4 succeeds.
- A failed Gold run can be retried against Silver; deterministic keys and hash-aware MERGE prevent duplicate facts.
- A physical Silver row removal is reflected by deletion of the corresponding current-state Gold fact; a Silver soft delete is retained in Gold with `is_deleted=true`.
- A facility-only bootstrap with no accepted event rows leaves `dim_date` and all facts empty without failing the refresh.

## 8. Run the acceptance evidence

Create evidence with real run IDs and UTC dates under `docs/evidence/phase3/`.

### A. Full bootstrap

Run the whole Job with `load_type=full`. Save:

- the four-task dependency graph;
- the successful Job run ID;
- the Gold table metrics/counts;
- the audit query below;
- PK/FK/count/reconciliation results.

### B. Real incremental

Run when CMS has a genuinely new/changed input. Save the manifest batch ID, matching Silver audit, matching Gold audit, and before/after Gold counts. All must share the Job `pipeline_run_id`.

### C. No-change replay

Run again against unchanged Silver. Prove zero Gold changes, identical PK counts, and unchanged timestamps for sampled stable keys.

### D. Controlled failure and repair

Use a separate `carewatch.phase3_demo` schema. Cause a deliberate Gold failure after one target has merged, save the `FAILURE` audit row and failed Job status, repair the fixture, then rerun. Prove no duplicate keys and a final `SUCCESS`. Do not inject this fixture into production tables.

### E. Bronze gate

In the demo schema, make one acquisition incomplete. Prove task 2 fails and tasks 3–4 do not start.

## 9. Queries to save as evidence

Replace the placeholder with the actual Job run ID:

```sql
SELECT pipeline_run_id, pipeline_layer, dataset, batch_id, status,
       start_time, end_time,
       timestampdiff(SECOND, start_time, end_time) AS duration_seconds,
       rows_read, rows_aggregated, rows_inserted, rows_updated, rows_deleted,
       rows_quarantined, error_message
FROM carewatch.pipeline.pipeline_execution_logs
WHERE pipeline_run_id = '<actual-job-run-id>'
ORDER BY start_time, pipeline_layer, dataset;
```

Core count and reconciliation proof:

```sql
SELECT
  (SELECT COUNT(*) FROM carewatch.pipeline.silver_deficiency) AS silver_deficiencies,
  (SELECT COUNT(*) FROM carewatch.pipeline.fact_deficiency) AS gold_deficiencies,
  (SELECT COUNT(*) FROM carewatch.pipeline.silver_penalty) AS silver_penalties,
  (SELECT COUNT(*) FROM carewatch.pipeline.fact_penalty) AS gold_penalties,
  (SELECT COUNT(*) FROM carewatch.pipeline.silver_mds_quality) AS silver_mds,
  (SELECT COUNT(*) FROM carewatch.pipeline.fact_mds_quality) AS gold_mds;

SELECT
  (SELECT COALESCE(SUM(serious_citation_count), 0)
   FROM carewatch.pipeline.fact_deficiency WHERE is_deleted = false) AS fact_serious,
  (SELECT COALESCE(SUM(serious_citation_count), 0)
   FROM carewatch.pipeline.agg_state_monthly_compliance) AS aggregate_serious,
  (SELECT COALESCE(SUM(fine_amount), 0)
   FROM carewatch.pipeline.fact_penalty WHERE is_deleted = false) AS fact_fines,
  (SELECT COALESCE(SUM(fine_amount_total), 0)
   FROM carewatch.pipeline.agg_state_monthly_compliance) AS aggregate_fines;
```

The reusable code automatically performs the full checks and fails on violations; these queries are readable evidence for the marker.

## 10. Spark UI evidence for the later report

Open the successful Gold task’s Spark UI/SQL details and locate the monthly aggregate action. Capture a readable screenshot showing the real scans, filters, exchanges/shuffles, group-by aggregations, full outer join, and Delta MERGE/write metrics that actually appear. Record the run ID, timestamp, stage duration, shuffle read/write, spill, and skew. Do not claim predicate pushdown or broadcast joins unless the plan explicitly shows them.

## 11. Optional optimization demonstration

Leave the nine Gold tables unpartitioned. Record a representative state/month query first, then run once:

```sql
OPTIMIZE carewatch.pipeline.fact_deficiency;
```

Rerun the same query and record scan/runtime plus maintenance duration. This is evidence for the recommendation, not a command to run after every refresh.

## 12. What remains for your friend’s Steps 4–6

The `bi/CareWatch.pbix`, actual dashboard pages, screenshots, observed Spark UI metrics, measured FinOps values, and final PDF cannot truthfully be generated from source code alone. They require the live Databricks data/workspace and Power BI. Steps 1–3 prepare the exact tables, relationships, views, logs, and evidence locations those later steps need.

