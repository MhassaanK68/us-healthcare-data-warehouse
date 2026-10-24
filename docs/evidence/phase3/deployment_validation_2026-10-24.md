# Phase 3 deployment validation — 2026-10-24

This records observed Databricks results for the Phase 3 Steps 1–3 code review fixes. The deployed Job reads the Git branch `codex/fix-phase3-review`; merge that branch and switch the Job to `main` together when releasing it.

## Production end-to-end Job

- Job: `CareWatch Daily Incremental`, ID `1053304552502201`, daily at 03:00 UTC, schedule `UNPAUSED`, maximum one concurrent run.
- [Manual verification run 61923714949354](https://dbc-70278be5-69c3.cloud.databricks.com/?o=7474659241236053#job/1053304552502201/run/61923714949354): all four dependent tasks succeeded in about 341 seconds.
- The acquisition, Raw-to-Bronze, Bronze-to-Silver, and Silver-to-Gold audit records share `pipeline_run_id=61923714949354`. Gold logged `SUCCESS`, `rows_read=698750`, `rows_aggregated=434330`, and zero inserts, updates, or deletes on this unchanged retry. This run verifies orchestration and idempotency; it does not demonstrate a new Silver fact.
- Detailed fact row counts reconciled exactly: deficiency Silver/Gold `418911/418911`, penalty `15419/15419`, and MDS quality `249730/249730`.
- The deployed Job retains `ALL_SUCCESS` dependencies, so an upstream failure prevents downstream tasks from starting.

## Isolated empty bootstrap and physical deletion

The following checks used `carewatch.phase3_empty_bootstrap_20261024`, separate from production. The unscheduled setup and Gold smoke Jobs were used for these tests. The small schema and its audit logs remain available for inspection.

1. [Empty bootstrap run 148311791083865](https://dbc-70278be5-69c3.cloud.databricks.com/?o=7474659241236053#job/966126211607465/run/148311791083865) succeeded with empty Silver inputs. `dim_date`, `fact_deficiency`, `fact_penalty`, and `fact_mds_quality` each had zero rows. Its Gold audit logged `SUCCESS`, `rows_read=0`, and `rows_aggregated=0`. One unknown geography member was inserted by design.
2. Copied one existing facility and its one deficiency into the isolated Silver tables. [Fixture insert run 803407535697040](https://dbc-70278be5-69c3.cloud.databricks.com/?o=7474659241236053#job/966126211607465/run/803407535697040) succeeded. Silver deficiency, Gold deficiency fact, and monthly compliance aggregate each had one row; Gold audit logged `rows_inserted=7` across all dimensions and facts.
3. Physically deleted that deficiency from the isolated Silver table. [Fixture delete run 64731577210928](https://dbc-70278be5-69c3.cloud.databricks.com/?o=7474659241236053#job/966126211607465/run/64731577210928) succeeded. Silver deficiency, Gold deficiency fact, and monthly aggregate each had zero rows; Gold audit logged `rows_deleted=2` for the fact and aggregate. The facility remains in Silver and Gold.

These isolated checks exercise the empty event path and `whenNotMatchedBySourceDelete` behavior. They do not change production Silver or Gold data.
