# Phase 2 submission evidence

Submit the [GitHub repository](https://github.com/MhassaanK68/us-healthcare-data-warehouse). Use the rubric's **saved notebook outputs** option; no video is needed.

## Open the saved outputs

Open [phase2_live_outputs.ipynb](evidence/phase2_live_outputs.ipynb) in GitHub. The output cells were generated from live Spark SQL queries by [99_export_phase2_evidence.ipynb](../notebooks/99_export_phase2_evidence.ipynb) running in Databricks on 17 October 2026. They show:

1. Bronze and Silver physical schemas.
2. Populated rows and zero missing `load_timestamp` values in all four Bronze and four Silver tables; Silver distinct-key counts equal row counts.
3. A Provider Silver initial MERGE and a no-change rerun with zero inserts and updates.
4. An isolated added-column drift event, two Bronze rows, one accepted Silver row, and one Safe Cast quarantine row.
5. Successful full and incremental execution logs for both transformation layers.

The final output cell states **All Phase 2 evidence checks passed.** The notebook says how its results were captured; it is a generated saved-output artifact, not a Databricks UI export of the pipeline notebooks.

## Reproduce the controlled drift proof

The production data is in `carewatch.pipeline`. The demonstration uses `carewatch.phase2_demo`, so it cannot change production rows. In a fresh demo schema, run the notebooks in this order:

1. `00_setup_tables.ipynb` with `catalog=carewatch`, `schema=phase2_demo`, `volume=landing`.
2. `98_prepare_drift_demo.ipynb` with the same namespace. Note its `acquisition_run_id` and `batch_id`.
3. `02_raw_to_bronze.ipynb` with `dataset=provider_info`, `load_type=full`, `acquisition_run_id=<fixture ID>`, `reprocess=false`, `catalog=carewatch`, `schema=phase2_demo`.
4. `03_bronze_to_silver.ipynb` with `dataset=provider_info`, `load_type=full`, `batch_ids=<fixture batch>`, `reprocess=false`, `allow_soft_deletes=false`, the demo namespace, and the production address-hashing **scope and key names** (never the secret value).
5. `99_export_phase2_evidence.ipynb` with `demo_batch_id=<fixture batch>`. It asserts the required outcomes and writes `/Volumes/carewatch/phase2_demo/landing/evidence/phase2_live_outputs.ipynb` for download into `docs/evidence/`.

The submitted proof uses fixture batch `demo_fd48e31d03b649c9bb158b10`. Raw-to-Bronze job run `611519289944167`, Silver job run `192941495324672`, and evidence export job run `736918120403269` identify the Databricks executions. The historical MERGE batch is `14c281a6e8c6c099c0dbad46` in the production logs.
