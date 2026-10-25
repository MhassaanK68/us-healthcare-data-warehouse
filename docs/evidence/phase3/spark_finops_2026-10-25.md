# Phase 3 scheduled-run, Spark, and FinOps evidence (25 October 2026)

All times are UTC. Workspace: Databricks on AWS `us-east-2`; namespace: `carewatch.pipeline`.

## Scheduled end-to-end run

[Job run 886646173959938](https://dbc-70278be5-69c3.cloud.databricks.com/?o=7474659241236053#job/1053304552502201/run/886646173959938) was launched by the daily 03:00 UTC schedule and succeeded. It started at 03:00:08.076, ended at 03:08:17.396, and took 489.320 seconds wall time. The four task executions were acquisition 103 s, Raw-to-Bronze 76 s, Bronze-to-Silver 62 s, and Silver-to-Gold 227 s. Their setup and hand-off time is part of the whole-Job wall duration. See the [actual timeline screenshot](dashboard/scheduled_job_timeline.png).

The Gold execution log for this run reports `SUCCESS`, `rows_read=698750`, `rows_aggregated=434330`, and zero inserts, updates, or deletes. The acquisition downloaded one `health_deficiencies` item, while the provider and MDS snapshots were already acquired. The run demonstrates scheduled chaining and no-change idempotency; it does not prove a new or changed Silver fact reached Gold.

## Query profile and physical operators

The user-supplied [query-profile DAG](dashboard/gold_query_profile_dag.png) is for Gold task run `450438229000020`, statement `be30292f-1e16-4500-901f-7c06a79217ff`, shown as `merge.execute()` and execution 2 of 5. The screenshot's visible portion contains a `Scan Table` of `dim_date` (206 ms), `Inner Join` on `date_key` (1 ms), `Object Hash Aggregate` nodes (392 ms and 97 ms), and `Shuffle` nodes (4 ms and 0 ms). It does not expose the full graph, stage task skew, or predicate-pushdown proof. The `Shuffle` operator is visible, while `system.query.history` reports `shuffle_read_bytes=0` for the statement; these are different summary views and should not be interpreted as a claim that no exchange occurred.

`system.query.history` reports for that statement: `total_duration_ms=4767`, `execution_duration_ms=3693`, `read_bytes=753338`, `read_rows=13864`, `read_files=4`, `spilled_local_bytes=0`, and `from_result_cache=false`. The screenshot shows zero rows/files written. This is a sampled date-key MERGE path, not the most complex Gold fact operation.

For scale, `fact_deficiency`'s MERGE in the same scheduled run has statement ID `24a5c023-44d2-4b38-9760-3402d40ffabe`. Delta history ties it to the Job and records 418,911 source rows, no changed target rows, 5,431 ms execution, 2,209 ms scan, and 3,175 ms rewrite. Query history reports 7,142 ms total, 1,675,644 rows read, 176,060,293 bytes read, zero reported shuffle-read bytes, and zero spill. No operator screenshot for this larger statement was supplied; do not describe its physical plan from the sampled date-key graph.

## Measured cost and scenario

`system.billing.usage` contains one `PREMIUM_JOBS_SERVERLESS_COMPUTE_US_EAST_OHIO` attribution to Job run `886646173959938`: **1.359740075 DBU**. Joining to the rate effective at usage time in `system.billing.list_prices` gives **USD 0.475909** list-price cost at USD 0.35/DBU. This is calculated list cost, not an invoice, credit balance, or a Power BI warehouse charge. No SQL warehouse usage row for warehouse `faecbef214ffd0c4` appeared in the queried 03:00-05:00 UTC window. The nine Gold tables had 54,299,045 bytes (51.78 MiB) of active data files at the last `DESCRIBE DETAIL` measurement; this excludes Delta history and upstream storage.

An hourly enterprise scenario repeats the observed Job usage 8,760 times: `8760 * 0.475909 = USD 4,168.96/year` for Jobs compute. A separate illustrative BI Import refresh assumes a SQL warehouse active for 10 minutes per hour, at 0.8 DBU/hour and USD 0.70/DBU: `8760 * (0.8/6 * 0.70) = USD 817.60/year`. Combined illustrative total: **USD 4,986.56/year**, before storage, transfer, taxes, discounts, and data growth. A daily Job at observed usage is USD 173.71/year at list price. The SQL rate is an assumption informed by a separate historical hour, not an observed Power BI refresh. The hourly scenario may substantially overstate a justified schedule because provider and MDS snapshots do not necessarily change hourly.

The 24 October [file-layout test](spark_finops_2026-10-24.md) found `OPTIMIZE` skipped the only 30.3 MB active `fact_deficiency` file and rewrote none. Keep the unpartitioned layout until file count and query profiles justify maintenance. To reduce cost, skip unchanged source publications, refresh BI only after a successful Gold task, keep SQL auto-stop, and measure actual BI warehouse time.

Platform sources: [Databricks serverless Job query profiles](https://docs.databricks.com/aws/en/jobs/run-serverless-jobs), [query profile operators](https://docs.databricks.com/aws/en/sql/user/queries/query-profile), and [billing system-table cost methodology](https://docs.databricks.com/aws/en/admin/usage/system-tables).
