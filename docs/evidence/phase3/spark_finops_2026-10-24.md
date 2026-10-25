# Spark execution and FinOps evidence (24 October 2026)

This is a historical baseline captured before the scheduled 25 October run posted usage and before the query-profile screenshot was provided. Use the [25 October evidence record](spark_finops_2026-10-25.md) for the final report's measured DBUs, list cost, and actual DAG.

## Provenance and scope

All timestamps below are UTC. The production workspace is Databricks on AWS, `us-east-2`, using `carewatch.pipeline`. The full-chain successful Job run is [61923714949354](https://dbc-70278be5-69c3.cloud.databricks.com/?o=7474659241236053#job/1053304552502201/run/61923714949354). The Job is Git-backed, scheduled daily at 03:00 UTC, and has the dependency order acquisition -> Raw-to-Bronze -> Bronze-to-Silver -> Silver-to-Gold. This run was an unchanged retry: it proves orchestration and idempotency, not a newly changed Silver record.

## Observed execution

| Scope | UTC start | UTC end | Reported execution time |
|---|---|---|---:|
| Whole Job | 16:33:23.486 | 16:39:04.475 | 340.989 s wall time |
| Acquisition | 16:33:23.513 | 16:34:10.455 | 43 s task execution |
| Raw-to-Bronze | 16:34:10.803 | 16:35:07.755 | 54 s |
| Bronze-to-Silver | 16:35:08.175 | 16:35:59.717 | 49 s |
| Silver-to-Gold | 16:36:00.122 | 16:39:04.052 | 182 s |

The Gold `pipeline_execution_logs` row has `pipeline_run_id=61923714949354`, `status=SUCCESS`, `rows_read=698750`, `rows_aggregated=434330`, and zero inserted, updated, and deleted rows. Detailed Gold/Silver row counts reconcile: deficiencies 418,911, penalties 15,419, MDS quality 249,730. The other Gold table counts and test scope are in [deployment validation](deployment_validation_2026-10-24.md).

The user-supplied [Gold task run overview](dashboard/gold_task_overview.png) confirms the `silver_to_gold` task is marked Succeeded. Its sidebar reports 100 queries, 12,490,070 rows read, and 1,654,142 rows written, with a warning that aggregation includes only the **first 100 queries**. These are partial task-observability counters, not the distinct Gold audit metrics and not a Spark execution DAG.

The Gold deficiency fact's 24 October `MERGE` history reports `executionTimeMs=4284`, `scanTimeMs=1833`, `rewriteTimeMs=2422`, `numSourceRows=418911`, and no changed target rows. Its statement ID `1c5e81de-d164-40d2-becf-c2332f5b356c` resolves in `system.query.history` to `total_duration_ms=5399`, `execution_duration_ms=4806`, `read_bytes=176280847`, `shuffle_read_bytes=0`, `spilled_local_bytes=0`, and `from_result_cache=false`. These are **one MERGE statement's** metrics, not the whole Gold task or a Spark UI DAG. A zero shuffle-read counter here does not prove that every Gold transformation avoids shuffles. A separate actual query-profile DAG was provided for the 25 October run; see the later evidence record.

## File-layout test

Before `OPTIMIZE`, `DESCRIBE DETAIL carewatch.pipeline.fact_deficiency` reported one active data file, 30,324,421 bytes, no partition columns, and no clustering columns. An `OPTIMIZE carewatch.pipeline.fact_deficiency` on 24 October returned `totalConsideredFiles=1`, `totalFilesSkipped=1`, `totalScheduledTasks=0`, `numFilesAdded=0`, `numFilesRemoved=0`, and `totalTaskExecutionTimeMs=0`. Databricks found no file-compaction work. This is a measured reason to defer scheduled `OPTIMIZE`, partitioning, and clustering until data size and workload metrics justify them.

Across all nine Gold tables, `DESCRIBE DETAIL` active data-file sizes total 54,299,045 bytes (51.78 MiB), one active data file per table. This excludes Delta log/history, Bronze/Silver storage, and cloud storage overhead, so it is not a total project storage bill.

## Billing observation and scenario

At the report's billing query time, `system.billing.usage` had data only through 16:00 UTC, before the 16:33 Job start. No row with `usage_metadata.job_run_id='61923714949354'` had posted. The run's actual DBUs or billed dollars therefore **cannot yet be attributed**. The 340.989 seconds above are wall time, not billable active time. The `system.billing.list_prices` effective list rows for AWS Ohio show `PREMIUM_JOBS_SERVERLESS_COMPUTE_US_EAST_OHIO` at USD 0.35/DBU and `PREMIUM_SERVERLESS_SQL_COMPUTE_US_EAST_OHIO` at USD 0.70/DBU. A separate 15:00–16:00 SQL warehouse billing hour contained 0.799309 DBU; it is not attributed to this Job or to a particular Power BI refresh.

The initial draft's **illustrative paid scenario**, not an observed invoice, assumed 0.5 job DBU per unchanged-style run and 0.8 SQL DBU/hour for ten active minutes per BI Import refresh. At the above rates, a daily run plus one daily BI refresh would be `0.5*0.35 + (0.8/6)*0.70 = USD 0.2683`, or USD 97.94/year for 365 days, before storage and transfer. An hourly run and hourly BI refresh would be `8760 * 0.2683 = USD 2,350.60/year`. This assumption was superseded when the scheduled 25 October Job's 1.359740075 DBU usage posted; use the later evidence record and final PDF for the observed-usage annual scenario. The 0.8 SQL DBU/hour remains a rounded planning assumption informed by one unrelated billed hour, not a measured Power BI-refresh rate.

For the official interpretation of usage and list-price system tables, see [Databricks cost monitoring](https://docs.databricks.com/aws/en/admin/usage/system-tables) and the [pricing table reference](https://docs.databricks.com/aws/en/admin/system-tables/pricing). For actual Spark UI stage capture and shuffle/spill interpretation, see the [Spark UI guide](https://docs.databricks.com/aws/en/optimizations/spark-ui-guide).
