# Phase 2 submission: save real notebook outputs

Use the rubric's **saved notebook outputs** option. The repository already contains the runnable PySpark notebooks and README models. The remaining proof is a completed drift run plus exported notebooks whose output cells are present in GitHub. Do not create a video.

## 1. Sync Databricks to the latest GitHub `main`

Open the Databricks Git folder for this repository and pull the latest `main` so `notebooks/99_phase2_submission_evidence.ipynb` is available. Run notebooks in that Git folder. If a widget bar is missing, run the cell containing `dbutils.widgets` once, set the widgets, then run the whole notebook.

## 2. Run the isolated schema-change demonstration

Use `catalog=carewatch`, `schema=phase2_demo`, and `volume=landing` in `00_setup_tables.ipynb`. Then run `98_prepare_drift_demo.ipynb` with the same values. Copy its printed **acquisition_run_id** and **batch_id**. This fixture has two synthetic Provider Information rows, an added column, and one invalid rating. It uses a separate schema from production.

Run `02_raw_to_bronze.ipynb` with `dataset=provider_info`, `load_type=full`, `acquisition_run_id=<ID from 98>`, `reprocess=false`, `catalog=carewatch`, `schema=phase2_demo`, and blank `source_path`.

Run `03_bronze_to_silver.ipynb` with `dataset=provider_info`, `load_type=full`, `batch_ids=<batch ID from 98>`, `reprocess=false`, `allow_soft_deletes=false`, `catalog=carewatch`, and `schema=phase2_demo`. Leave the other scope/date widgets blank. Set `address_secret_scope` and `address_secret_key` to the **names** already used for the production Silver run; never expose the secret value.

## 3. Run the single evidence notebook

Run `99_phase2_submission_evidence.ipynb` with `catalog=carewatch`, `schema=pipeline`, `historical_batch_id=14c281a6e8c6c099c0dbad46`, `demo_schema=phase2_demo`, and `demo_batch_id=<batch ID from 98>`. Its final output must say **All Phase 2 evidence checks passed.** It displays actual table schemas, all Bronze/Silver timestamp and key checks, the first MERGE and no-op historical rerun, the drift/quarantine result, and full/incremental logs. If an assertion fails, resolve the issue and rerun before exporting.

## 4. Export with outputs and put the files in GitHub

For each completed notebook `98_prepare_drift_demo.ipynb`, `02_raw_to_bronze.ipynb`, `03_bronze_to_silver.ipynb`, and `99_phase2_submission_evidence.ipynb`, choose **File → Export → IPython Notebook → Include outputs → Export** in Databricks. Create `docs/evidence/` if it is absent, then save the downloaded files there. Name the demo pipeline exports `phase2_demo_02_raw_to_bronze.ipynb` and `phase2_demo_03_bronze_to_silver.ipynb` so they cannot overwrite the source notebooks.

Open each downloaded `.ipynb` and confirm its code cells contain nonempty `outputs`; the source notebooks in `notebooks/` have mostly empty output arrays. Commit and push the four evidence files. Open the GitHub copies and check that the result tables and final **PASS** lines are visible. Submit the [GitHub repository link](https://github.com/MhassaanK68/us-healthcare-data-warehouse).

Databricks documents the [export flow and Include outputs option](https://docs.databricks.com/aws/en/notebooks/notebook-export-import). If the Git folder refuses to commit outputs directly, exporting the four IPython notebooks with outputs to `docs/evidence/` avoids relying on that workspace setting.
