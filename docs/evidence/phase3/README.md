# Phase 3 evidence index

- [Deployed Job validation](deployment_validation_2026-10-24.md): full-chain success, shared run ID, row reconciliation, unchanged retry, and isolated empty/deletion tests.
- [Scheduled-run Spark and FinOps evidence](spark_finops_2026-10-25.md): actual query-profile DAG, statement metrics, posted Job DBUs/list cost, and hourly scenario.
- [24 October file-layout and baseline evidence](spark_finops_2026-10-24.md): earlier run timings, `OPTIMIZE` no-op, and the historical billing cutoff at that time.
- [Quality overview screenshot](dashboard/quality_overview.png), [facility/staffing screenshot](dashboard/facility_staffing.png), [selected MDS screenshot](dashboard/mds_single_period.png), and [pipeline health/data quality screenshot](dashboard/pipeline_health_data_quality.png): user-supplied Power BI evidence embedded in the [report](../../../Architectural-Report.pdf).
- [Gold task overview screenshot](dashboard/gold_task_overview.png): user-supplied Databricks task page for run 61923714949354. It shows partial query totals, not a Spark execution DAG.
- [Power BI Model view](dashboard/power_bi_model_view.png), [scheduled Job timeline](dashboard/scheduled_job_timeline.png), and [Gold query-profile DAG](dashboard/gold_query_profile_dag.png): user-supplied evidence used in the final report. The query profile is a partial path for one date-key MERGE; its scope is explained in the 25 October evidence record.

The supplied monitoring screenshot displays 53 logged execution records and 42 error events. A direct warehouse query on 25 October found 61 audit-view records, so refresh the saved Power BI file after a successful Gold run to present current numbers. The capture shows an error card but no error-by-rule breakdown or error detail matrix. The saved `.pbix` at `BI/business-intelligence-dashboard.pbix` contains four pages, including Pipeline Health & Data Quality. Do not present the Job timeline or task overview as the physical query DAG.
